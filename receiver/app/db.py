"""SQLite-backed storage and the replica failover state machine.

Design summary
--------------
* One row per time series. The replica label is stripped before a series is
  identified, so the two Prometheus replicas feed the *same* series.
* Per series only samples newer than the current tail point are stored;
  retransmitted old batches are counted but never inserted and never refresh
  the activity timer.
* ``cluster_state`` remembers the currently feeding replica. A replica is
  stale when it has not stored a new point for ``stale_after`` seconds.
  Another replica that actually carries new points may then take over.
* Switching the active replica and inserting samples happen in a single
  ``BEGIN IMMEDIATE`` transaction, so they are atomic and the committed
  cluster_state row always describes the last successful write -- even after
  a crash or restart (WAL enabled).

Active-series limit
-------------------
* An optional per-cluster limit bounds how many *series* may hold a slot at
  the same time; when unset, ingestion is unlimited.
* A series holds a slot for ``lease_seconds`` (default 30) starting at the
  wall-clock time of its last *successful new-point write*. A series that
  still holds a slot keeps writing new points even if the cluster is at or
  above its limit (for example after the limit was lowered).
* A series without a live slot is admitted only while free slots remain;
  otherwise every new point of that series in the batch is dropped. Dropped
  points never renew a slot and can never select/take over a replica.
* The slot table is written in the same transaction as samples and
  cluster_state. Occupancy is counted from that indexed table with a time
  window, so restart recovery never scans all series or historical samples.
* Limit changes and writes are both serialised by ``BEGIN IMMEDIATE`` and
  read the limit inside the transaction, so they take effect in commit order.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
from dataclasses import dataclass, field

from .remote_write import TimeSeries

CLUSTER_LABEL = "cluster"
REPLICA_LABEL = "replica"


class IngestError(ValueError):
    """A request that cannot be ingested (bad labels, mixed groups ...)."""


@dataclass
class IngestOutcome:
    accepted: bool
    cluster: str
    replica: str
    role: str  # initial | active | takeover | standby | rejected
    series_seen: int = 0
    series_created: int = 0
    samples_received: int = 0
    samples_stored: int = 0
    samples_stale: int = 0
    samples_limit_dropped: int = 0
    series_limit: int | None = None
    active_series: int = 0
    reason: str = ""
    old_replica: str | None = None
    new_replica: str | None = None
    switched: bool = False


@dataclass
class ClusterStatus:
    cluster: str
    active_replica: str | None
    switched_ms: int | None
    last_seen_ms: int | None
    last_write_ms: int | None
    stale: bool
    replicas: list[dict] = field(default_factory=list)
    series_limit: int | None = None
    active_series: int = 0
    limit_dropped_total: int = 0


class Database:
    def __init__(
        self,
        path: str,
        stale_after_seconds: float,
        lease_seconds: float = 30.0,
    ):
        self.path = path
        self.stale_after_ms = int(stale_after_seconds * 1000)
        self.lease_ms = max(0, int(lease_seconds * 1000))
        self._init_lock = threading.Lock()
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        self.init_db()

    # ------------------------------------------------------------------ #
    # connection / schema
    # ------------------------------------------------------------------ #
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def init_db(self) -> None:
        with self._init_lock, self._connect() as conn:
            # Detect a pre-limit schema before creating the new tables so the
            # one-off upgrade marker is only written for real legacy data.
            had_limit_schema = conn.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type='table' AND name='series_leases'"
            ).fetchone() is not None
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS series (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    fingerprint TEXT NOT NULL UNIQUE,
                    cluster     TEXT NOT NULL,
                    name        TEXT NOT NULL,
                    labels_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS samples (
                    series_id    INTEGER NOT NULL REFERENCES series(id),
                    timestamp_ms INTEGER NOT NULL,
                    value        REAL NOT NULL,
                    PRIMARY KEY (series_id, timestamp_ms)
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_samples_series_ts
                    ON samples(series_id, timestamp_ms);

                CREATE TABLE IF NOT EXISTS cluster_state (
                    cluster        TEXT PRIMARY KEY,
                    active_replica TEXT NOT NULL,
                    last_write_ms  INTEGER,
                    updated_ms     INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS replica_liveness (
                    cluster       TEXT NOT NULL,
                    replica       TEXT NOT NULL,
                    last_seen_ms  INTEGER,
                    last_write_ms INTEGER,
                    PRIMARY KEY (cluster, replica)
                ) WITHOUT ROWID;

                CREATE TABLE IF NOT EXISTS events (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_ms       INTEGER NOT NULL,
                    cluster     TEXT NOT NULL,
                    event_type  TEXT NOT NULL,
                    old_replica TEXT,
                    new_replica TEXT,
                    reason      TEXT
                );

                -- Optional active-series cap per cluster; NULL/absent = no cap.
                CREATE TABLE IF NOT EXISTS cluster_limits (
                    cluster     TEXT PRIMARY KEY,
                    max_series  INTEGER
                );

                -- One row per series holding an active-series slot.
                -- last_write_ms is the wall clock of the last *committed*
                -- successful new-point write; occupancy lasts LEASE_SECONDS
                -- from that committed time, even across a restart.
                CREATE TABLE IF NOT EXISTS series_leases (
                    cluster       TEXT NOT NULL,
                    series_id     INTEGER NOT NULL REFERENCES series(id),
                    last_write_ms INTEGER NOT NULL,
                    PRIMARY KEY (cluster, series_id)
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_series_leases_write
                    ON series_leases(cluster, last_write_ms);

                -- Per-cluster counter of points dropped because every slot
                -- was occupied by another series (persisted for the UI).
                CREATE TABLE IF NOT EXISTS cluster_drop_counters (
                    cluster   TEXT PRIMARY KEY,
                    dropped   INTEGER NOT NULL DEFAULT 0
                );
                """
            )
            # One-off, O(1) upgrade marker; safe to re-run on every start.
            if not had_limit_schema:
                self._mark_upgrade(conn, int(time.time() * 1000))

    def _lease_cutoff(self, now_ms: int) -> int:
        """Writes at or before this timestamp have finished their 30s lease."""
        return now_ms - self.lease_ms

    def _lease_rows(
        self, conn: sqlite3.Connection, cluster: str, now_ms: int
    ) -> dict[int, int]:
        """series_id -> lease expiry for slots live at ``now_ms``.

        A slot occupies exactly ``lease_ms`` from the committed write
        timestamp: at ``last_write_ms + lease_ms`` it is already free.
        """
        rows = conn.execute(
            "SELECT series_id, last_write_ms FROM series_leases "
            "WHERE cluster=? AND last_write_ms > ?",
            (cluster, self._lease_cutoff(now_ms)),
        ).fetchall()
        return {
            row["series_id"]: row["last_write_ms"] + self.lease_ms
            for row in rows
        }

    def _prune_expired_leases(
        self, conn: sqlite3.Connection, cluster: str, now_ms: int
    ) -> None:
        conn.execute(
            "DELETE FROM series_leases WHERE cluster=? AND last_write_ms <= ?",
            (cluster, self._lease_cutoff(now_ms)),
        )

    @staticmethod
    def _series_limit(
        conn: sqlite3.Connection, cluster: str
    ) -> int | None:
        row = conn.execute(
            "SELECT max_series FROM cluster_limits WHERE cluster=?",
            (cluster,),
        ).fetchone()
        if row is None:
            return None
        return row["max_series"]

    # ------------------------------------------------------------------ #
    # legacy upgrade
    # ------------------------------------------------------------------ #
    @staticmethod
    def _legacy_series_epoch(
        conn: sqlite3.Connection,
    ) -> int | None:
        """Latest committed ingest time from before this feature was deployed.

        Pre-limit builds persisted "last successful write" times in
        ``replica_liveness.last_write_ms`` / ``cluster_state.updated_ms``.
        Their maximum is the pre-upgrade write epoch: it never grants a slot
        (occupancy is always derived from ``series_leases``, which legacy
        writes never populated), and it lets us record an informational event
        exactly once instead of inferring occupancy from old samples.
        """
        row = conn.execute(
            "SELECT MAX(ts) AS ts FROM ("
            "  SELECT MAX(last_write_ms) AS ts FROM replica_liveness"
            "  UNION ALL SELECT MAX(updated_ms) FROM cluster_state"
            ")"
        ).fetchone()
        return row["ts"] if row else None

    def _mark_upgrade(self, conn: sqlite3.Connection, now_ms: int) -> None:
        """Record legacy-upgrade information once, if there was legacy data.

        Occupancy itself is never derived from legacy tables or samples; this
        event only documents that pre-upgrade writes hold no slots.
        """
        epoch = self._legacy_series_epoch(conn)
        if epoch is None:
            return  # nothing existed before this feature: no upgrade to note
        existing = conn.execute(
            "SELECT 1 FROM events WHERE event_type='LIMIT_SCHEMA_UPGRADE' LIMIT 1"
        ).fetchone()
        if existing is not None:
            return
        conn.execute(
            "INSERT INTO events(ts_ms, cluster, event_type, old_replica, "
            "new_replica, reason) VALUES (?, ?, ?, NULL, NULL, ?)",
            (
                now_ms,
                "*",
                "LIMIT_SCHEMA_UPGRADE",
                "active-series limit enabled; pre-upgrade writes do not hold "
                f"slots (legacy write epoch ms={epoch})",
            ),
        )

    # ------------------------------------------------------------------ #
    # ingestion
    # ------------------------------------------------------------------ #
    def ingest(
        self, series_list: list[TimeSeries], now_ms: int
    ) -> IngestOutcome:
        groups = self._group_by_replica(series_list)
        if len(groups) > 1:
            raise IngestError(
                "one remote-write request must carry a single "
                "(cluster, replica) pair"
            )
        (cluster, replica), group_series = next(iter(groups.items()))
        return self._ingest_group(cluster, replica, group_series, now_ms)

    @staticmethod
    def _group_by_replica(
        series_list: list[TimeSeries],
    ) -> dict[tuple[str, str], list[TimeSeries]]:
        groups: dict[tuple[str, str], list[TimeSeries]] = {}
        for series in series_list:
            labels = series.label_map()
            cluster = labels.get(CLUSTER_LABEL)
            replica = labels.get(REPLICA_LABEL)
            if not cluster or not replica:
                raise IngestError(
                    "every series must carry non-empty 'cluster' and "
                    "'replica' labels"
                )
            groups.setdefault((cluster, replica), []).append(series)
        return groups

    @staticmethod
    def _series_identity(labels: dict[str, str]) -> tuple[str, str, str]:
        """Return (fingerprint, metric name, canonical labels) without replica."""
        identity = {k: v for k, v in labels.items() if k != REPLICA_LABEL}
        if not identity:
            raise IngestError("series has no labels left after stripping replica")
        canonical = json.dumps(
            identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        return fingerprint, identity.get("__name__", ""), canonical

    def _ingest_group(
        self,
        cluster: str,
        replica: str,
        series_list: list[TimeSeries],
        now_ms: int,
    ) -> IngestOutcome:
        outcome = IngestOutcome(
            accepted=False, cluster=cluster, replica=replica, role="rejected"
        )
        conn = self._connect()
        try:
            # Serialise writers: switch + inserts must be one atomic step.
            conn.execute("BEGIN IMMEDIATE")

            conn.execute(
                """
                INSERT INTO replica_liveness(cluster, replica, last_seen_ms, last_write_ms)
                VALUES (?, ?, ?, NULL)
                ON CONFLICT(cluster, replica) DO UPDATE SET last_seen_ms=excluded.last_seen_ms
                """,
                (cluster, replica, now_ms),
            )
            state = conn.execute(
                "SELECT active_replica, last_write_ms, updated_ms "
                "FROM cluster_state WHERE cluster=?",
                (cluster,),
            ).fetchone()

            active = state["active_replica"] if state else None
            if active is None:
                role = "initial"
            elif active == replica:
                role = "active"
            else:
                active_liveness = conn.execute(
                    "SELECT last_write_ms FROM replica_liveness "
                    "WHERE cluster=? AND replica=?",
                    (cluster, active),
                ).fetchone()
                active_last_write = (
                    active_liveness["last_write_ms"] if active_liveness else None
                )
                stale = (
                    active_last_write is None
                    or now_ms - active_last_write >= self.stale_after_ms
                )
                # Note the difference is measured against the *current* clock,
                # so retransmitted batches cannot keep a dead replica fresh.
                role = "takeover" if stale else "standby"
            outcome.role = role
            outcome.old_replica = active
            outcome.series_seen = len(series_list)
            outcome.samples_received = sum(
                len(series.samples) for series in series_list
            )

            if role == "standby":
                # Active replica is still fresh: nothing from this one is stored.
                conn.commit()
                outcome.reason = (
                    f"active replica '{active}' is fresh; standby data rejected"
                )
                return outcome

            # Snapshot the slot table and the (possibly just changed) limit
            # inside the write transaction: limit changes and writes therefore
            # take effect strictly in commit order. Reading the lease table
            # uses the (cluster, last_write_ms) index and never scans series
            # or historical samples, so restart needs no recovery pass either.
            self._prune_expired_leases(conn, cluster, now_ms)
            lease_until: dict[int, int] = self._lease_rows(conn, cluster, now_ms)
            # ``lease_until`` values are the expiry of each live slot. If the
            # limit was changed concurrently (or LEASE_SECONDS was reduced on
            # restart), a previously granted slot stays valid until its
            # stored expiry rather than being revoked.
            series_limit = self._series_limit(conn, cluster)
            outcome.series_limit = series_limit

            stored_total = 0
            stale_total = 0
            created_total = 0
            limit_dropped_total = 0
            for series in series_list:
                labels = series.label_map()
                fingerprint, name, canonical = self._series_identity(labels)
                row = conn.execute(
                    "SELECT id FROM series WHERE fingerprint=?", (fingerprint,)
                ).fetchone()
                series_id = row["id"] if row is not None else None

                if series_id is not None:
                    tail = conn.execute(
                        "SELECT MAX(timestamp_ms) AS tail FROM samples "
                        "WHERE series_id=?",
                        (series_id,),
                    ).fetchone()["tail"]
                else:
                    tail = None

                fresh_samples: list[tuple[int, float]] = []
                seen_ts: set[int] = set()
                for sample in series.samples:
                    ts = sample.timestamp_ms
                    if ts in seen_ts:
                        continue
                    seen_ts.add(ts)
                    if tail is not None and ts <= tail:
                        stale_total += 1
                        continue
                    fresh_samples.append((ts, sample.value))

                # An existing holder of a live slot always keeps writing; a
                # series without a slot needs a free slot. Without a limit
                # every series is admitted.
                admitted = (
                    series_limit is None
                    or (series_id is not None and series_id in lease_until)
                    or len(lease_until) < series_limit
                )
                if not admitted:
                    # No room: drop this series' new points entirely and do
                    # not even materialise the series row. The slot-less
                    # series neither consumes a slot nor refreshes liveness.
                    limit_dropped_total += len(fresh_samples)
                    continue

                if series_id is None:
                    conn.execute(
                        "INSERT INTO series(fingerprint, cluster, name, labels_json) "
                        "VALUES (?, ?, ?, ?)",
                        (fingerprint, cluster, name, canonical),
                    )
                    created_total += 1
                    row = conn.execute(
                        "SELECT id FROM series WHERE fingerprint=?", (fingerprint,)
                    ).fetchone()
                    if row is None:  # pragma: no cover - defensive
                        raise IngestError("failed to resolve series id")
                    series_id = row["id"]

                for ts, value in fresh_samples:
                    # OR IGNORE keeps retransmits idempotent even on races.
                    cur = conn.execute(
                        "INSERT OR IGNORE INTO samples(series_id, timestamp_ms, value) "
                        "VALUES (?, ?, ?)",
                        (series_id, ts, value),
                    )
                    if cur.rowcount:
                        stored_total += cur.rowcount
                        # Successful new-point writes renew the occupancy from
                        # the committed write time; this timestamp is what
                        # makes the lease survive a restart unchanged.
                        lease_until[series_id] = now_ms + self.lease_ms
                        conn.execute(
                            "INSERT INTO series_leases(cluster, series_id, last_write_ms) "
                            "VALUES (?, ?, ?) "
                            "ON CONFLICT(cluster, series_id) DO UPDATE SET "
                            "last_write_ms=excluded.last_write_ms",
                            (cluster, series_id, now_ms),
                        )

            outcome.samples_stale = stale_total
            outcome.samples_stored = stored_total
            outcome.series_created = created_total
            outcome.samples_limit_dropped = limit_dropped_total
            outcome.active_series = len(lease_until)
            outcome.accepted = True

            new_points = outcome.samples_stored > 0
            if role == "takeover" and not new_points:
                # A stale active replica may only be replaced by *new* points.
                # Limit-dropped points count as "no new points": a replica
                # whose batch was entirely dropped must not take over.
                conn.rollback()
                outcome.accepted = False
                outcome.role = "rejected"
                outcome.samples_stored = 0
                outcome.series_created = 0
                outcome.active_series = 0  # nothing of this batch was committed
                # Keep the limit-drop count for observability: it explains why
                # the batch carried no admissible new point, but the rollback
                # means it is not persisted in the per-cluster counter.
                outcome.reason = (
                    "active replica is stale but this batch contains no "
                    "admissible point newer than the stored tail; takeover denied"
                )
                if limit_dropped_total:
                    outcome.reason = (
                        "active replica is stale, but every point of this batch "
                        "was limit-dropped (no free active-series slot); "
                        "takeover denied"
                    )
                return outcome

            if limit_dropped_total:
                conn.execute(
                    "INSERT INTO cluster_drop_counters(cluster, dropped) VALUES (?, ?) "
                    "ON CONFLICT(cluster) DO UPDATE SET "
                    "dropped=dropped+excluded.dropped",
                    (cluster, limit_dropped_total),
                )

            if new_points:
                conn.execute(
                    "UPDATE replica_liveness SET last_write_ms=? "
                    "WHERE cluster=? AND replica=?",
                    (now_ms, cluster, replica),
                )

            if role in ("initial", "takeover") and new_points:
                conn.execute(
                    """
                    INSERT INTO cluster_state(cluster, active_replica, last_write_ms, updated_ms)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(cluster) DO UPDATE SET
                        active_replica=excluded.active_replica,
                        last_write_ms=excluded.last_write_ms,
                        updated_ms=excluded.updated_ms
                    """,
                    (cluster, replica, now_ms, now_ms),
                )
                event_type = "SELECTED" if role == "initial" else "TAKEOVER"
                conn.execute(
                    "INSERT INTO events(ts_ms, cluster, event_type, old_replica, "
                    "new_replica, reason) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        now_ms,
                        cluster,
                        event_type,
                        active,
                        replica,
                        outcome.reason or None,
                    ),
                )
                outcome.switched = role == "takeover"
                outcome.new_replica = replica
            elif new_points:
                conn.execute(
                    "UPDATE cluster_state SET last_write_ms=? WHERE cluster=?",
                    (now_ms, cluster),
                )

            if not new_points:
                if limit_dropped_total:
                    outcome.reason = (
                        f"{limit_dropped_total} new point(s) dropped: every "
                        "active-series slot is held by another series"
                    )
                elif not any(series.samples for series in series_list):
                    outcome.reason = "batch contained no samples"
                else:
                    outcome.reason = (
                        "all samples are at or behind the stored tail (retransmit)"
                    )
            conn.commit()
            return outcome
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    # active-series limit
    # ------------------------------------------------------------------ #
    def set_series_limit(self, cluster: str, max_series: int | None) -> dict:
        """Set (or clear with ``None``) a cluster's active-series cap.

        The write is a normal committed transaction, so it orders against
        concurrent ingest transactions exactly as it commits. Shrinking the
        limit never evicts a series that already holds a slot; those keep
        writing until their 30s occupancy expires naturally.
        """
        if not cluster:
            raise IngestError("cluster must be non-empty")
        if max_series is not None and max_series < 0:
            raise IngestError("max_series must be >= 0 or null")
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            if max_series is None:
                conn.execute(
                    "DELETE FROM cluster_limits WHERE cluster=?", (cluster,)
                )
            else:
                conn.execute(
                    "INSERT INTO cluster_limits(cluster, max_series) VALUES (?, ?) "
                    "ON CONFLICT(cluster) DO UPDATE SET "
                    "max_series=excluded.max_series",
                    (cluster, max_series),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return {"cluster": cluster, "series_limit": max_series}

    def get_series_limit(self, cluster: str) -> int | None:
        with self._connect() as conn:
            return self._series_limit(conn, cluster)

    def live_series(self, cluster: str, now_ms: int, limit: int = 50) -> list[dict]:
        """Series that currently hold a slot (index range scan + tiny join)."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT s.id, s.name, s.labels_json, l.last_write_ms "
                "FROM series_leases l JOIN series s ON s.id=l.series_id "
                "WHERE l.cluster=? AND l.last_write_ms > ? "
                "ORDER BY l.last_write_ms DESC, s.id LIMIT ?",
                (cluster, self._lease_cutoff(now_ms), limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def limits_overview(self, now_ms: int) -> list[dict]:
        """Limit/occupancy view for every cluster we know about.

        Includes clusters that only have a configured limit (no write yet).
        Occupancy is a time-window count over the indexed lease table.
        """
        with self._connect() as conn:
            clusters = [
                row["cluster"]
                for row in conn.execute(
                    "SELECT cluster FROM cluster_limits "
                    "UNION SELECT cluster FROM cluster_state "
                    "UNION SELECT cluster FROM series_leases "
                    "ORDER BY cluster"
                ).fetchall()
            ]
            limits = {
                row["cluster"]: row["max_series"]
                for row in conn.execute(
                    "SELECT cluster, max_series FROM cluster_limits"
                ).fetchall()
            }
            active = {
                row["cluster"]: row["n"]
                for row in conn.execute(
                    "SELECT cluster, COUNT(*) AS n FROM series_leases "
                    "WHERE last_write_ms > ? GROUP BY cluster",
                    (self._lease_cutoff(now_ms),),
                ).fetchall()
            }
            drops = {
                row["cluster"]: row["dropped"]
                for row in conn.execute(
                    "SELECT cluster, dropped FROM cluster_drop_counters"
                ).fetchall()
            }
        return [
            {
                "cluster": cluster,
                "series_limit": limits.get(cluster),
                "active_series": active.get(cluster, 0),
                "limit_dropped_total": drops.get(cluster, 0),
            }
            for cluster in clusters
        ]

    # ------------------------------------------------------------------ #
    # read models for the web UI
    # ------------------------------------------------------------------ #
    def status(self, now_ms: int) -> list[ClusterStatus]:
        with self._connect() as conn:
            liveness_rows = conn.execute(
                "SELECT cluster, replica, last_seen_ms, last_write_ms "
                "FROM replica_liveness ORDER BY cluster, replica"
            ).fetchall()
            state_rows = conn.execute(
                "SELECT cluster, active_replica, last_write_ms, updated_ms "
                "FROM cluster_state"
            ).fetchall()
            limits = {
                row["cluster"]: row["max_series"]
                for row in conn.execute(
                    "SELECT cluster, max_series FROM cluster_limits"
                ).fetchall()
            }
            # Index range scan over live slots only; never touches series or
            # historical samples.
            active_counts = {
                row["cluster"]: row["n"]
                for row in conn.execute(
                    "SELECT cluster, COUNT(*) AS n FROM series_leases "
                    "WHERE last_write_ms > ? GROUP BY cluster",
                    (self._lease_cutoff(now_ms),),
                ).fetchall()
            }
            drops = {
                row["cluster"]: row["dropped"]
                for row in conn.execute(
                    "SELECT cluster, dropped FROM cluster_drop_counters"
                ).fetchall()
            }

        replicas: dict[str, list[dict]] = {}
        for row in liveness_rows:
            replicas.setdefault(row["cluster"], []).append(dict(row))

        result: list[ClusterStatus] = []
        for row in state_rows:
            cluster = row["cluster"]
            last_write = row["last_write_ms"]
            stale = (
                last_write is None
                or now_ms - last_write >= self.stale_after_ms
            )
            active_last_seen = None
            for item in replicas.get(cluster, []):
                if item["replica"] == row["active_replica"]:
                    active_last_seen = item["last_seen_ms"]
            result.append(
                ClusterStatus(
                    cluster=cluster,
                    active_replica=row["active_replica"],
                    switched_ms=row["updated_ms"],
                    last_seen_ms=active_last_seen,
                    last_write_ms=last_write,
                    stale=stale,
                    replicas=replicas.get(cluster, []),
                    series_limit=limits.get(cluster),
                    active_series=active_counts.get(cluster, 0),
                    limit_dropped_total=drops.get(cluster, 0),
                )
            )
        return result

    def list_series(self) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT s.id, s.cluster, s.name, s.labels_json,
                       (SELECT COUNT(*) FROM samples m WHERE m.series_id=s.id) AS sample_count,
                       (SELECT timestamp_ms FROM samples m
                         WHERE m.series_id=s.id ORDER BY timestamp_ms DESC LIMIT 1) AS last_ts,
                       (SELECT value FROM samples m
                         WHERE m.series_id=s.id ORDER BY timestamp_ms DESC LIMIT 1) AS last_value
                FROM series s ORDER BY s.cluster, s.name, s.id
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def get_samples(self, series_id: int, limit: int = 240) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT timestamp_ms, value FROM samples "
                "WHERE series_id=? ORDER BY timestamp_ms DESC LIMIT ?",
                (series_id, limit),
            ).fetchall()
        return [dict(row) for row in reversed(rows)]

    def list_events(self, limit: int = 30) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT ts_ms, cluster, event_type, old_replica, new_replica, reason "
                "FROM events ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]
