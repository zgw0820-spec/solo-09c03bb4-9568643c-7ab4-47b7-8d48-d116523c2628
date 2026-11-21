"""Active-series limit and 30-second occupancy (lease) semantics."""
from __future__ import annotations

import threading

import pytest

from app.db import Database
from tests.conftest import BASE_TS, series


LEASE_MS = 30_000


@pytest.fixture()
def db(tmp_path):
    return Database(
        str(tmp_path / "limit.db"),
        stale_after_seconds=10.0,
        lease_seconds=30.0,
    )


def named(cluster, replica, name, ts=0, value=1.0):
    return series(cluster, replica, [(ts, value)], name=name)


def ingest(db, cluster, replica, series_list, now):
    return db.ingest(series_list, now_ms=now)


def sample_count(db, cluster):
    return sum(
        row["sample_count"]
        for row in db.list_series()
        if row["cluster"] == cluster
    )


def test_unlimited_by_default_accepts_every_series(db):
    out = ingest(
        db, "demo", "A",
        [named("demo", "A", f"m{i}", ts=i * 1000) for i in range(50)],
        now=BASE_TS + 60_000,
    )
    assert out.samples_stored == 50
    assert out.samples_limit_dropped == 0
    assert out.series_limit is None
    # without a limit every series also occupies a slot
    assert out.active_series == 50


def test_limit_admits_up_to_cap_then_drops_new_series(db):
    db.set_series_limit("demo", 2)
    first = ingest(
        db, "demo", "A",
        [named("demo", "A", f"m{i}", ts=0) for i in range(2)],
        now=BASE_TS + 1000,
    )
    assert first.samples_stored == 2 and first.samples_limit_dropped == 0

    # Same instant, both slots live: a third series in the same window is
    # dropped, but the two holders keep writing new points.
    second = ingest(
        db, "demo", "A",
        [
            named("demo", "A", "m0", ts=2000),
            named("demo", "A", "m1", ts=2000),
            named("demo", "A", "m2", ts=2000),
        ],
        now=BASE_TS + 2000,
    )
    assert second.samples_stored == 2
    assert second.samples_limit_dropped == 1
    assert second.active_series == 2
    # dropped series never materialise in the series table
    names = {row["name"] for row in db.list_series()}
    assert names == {"m0", "m1"}


def test_same_batch_mixed_holders_and_newcomers(db):
    db.set_series_limit("demo", 3)
    ingest(
        db, "demo", "A",
        [named("demo", "A", f"m{i}", ts=0) for i in range(3)],
        now=BASE_TS + 1000,
    )
    out = ingest(
        db, "demo", "A",
        [
            named("demo", "A", "m0", ts=2000),
            named("demo", "A", "m1", ts=2000),
            named("demo", "A", "m2", ts=2000),
            named("demo", "A", "new0", ts=2000),
            named("demo", "A", "new1", ts=2000),
        ],
        now=BASE_TS + 2000,
    )
    assert out.samples_stored == 3
    assert out.samples_limit_dropped == 2
    assert out.active_series == 3


def test_empty_slot_needs_explicit_value_setting_zero(db):
    db.set_series_limit("demo", 0)
    out = ingest(db, "demo", "A", [named("demo", "A", "m0")], now=BASE_TS + 1000)
    assert out.samples_stored == 0
    assert out.samples_limit_dropped == 1
    # zero limit means nothing is ever admitted, not even at first contact
    assert db.list_series() == []
    # clearing the limit restores unlimited ingestion
    db.set_series_limit("demo", None)
    out = ingest(db, "demo", "A", [named("demo", "A", "m0")], now=BASE_TS + 2000)
    assert out.samples_stored == 1
    assert out.series_limit is None


def test_slot_expires_after_thirty_seconds(db):
    db.set_series_limit("demo", 1)
    ingest(db, "demo", "A", [named("demo", "A", "m0")], now=BASE_TS)
    # 29.999s later still held -> m1 dropped
    held = ingest(db, "demo", "A", [named("demo", "A", "m1")],
                  now=BASE_TS + LEASE_MS - 1)
    assert held.samples_limit_dropped == 1
    # exactly at 30s the slot is gone and m1 may enter
    freed = ingest(db, "demo", "A", [named("demo", "A", "m1")],
                   now=BASE_TS + LEASE_MS)
    assert freed.samples_stored == 1
    assert freed.active_series == 1  # m0 expired, m1 now holds the slot


def test_successful_new_point_renews_slot(db):
    db.set_series_limit("demo", 1)
    ingest(db, "demo", "A", [named("demo", "A", "m0")], now=BASE_TS)
    # m0 writes a new point at t=20s -> slot now lasts until t=50s
    renew = ingest(db, "demo", "A", [named("demo", "A", "m0", ts=1000)],
                   now=BASE_TS + 20_000)
    assert renew.samples_stored == 1
    out = ingest(db, "demo", "A", [named("demo", "A", "m1")],
                 now=BASE_TS + 45_000)
    assert out.samples_limit_dropped == 1
    # ... and becomes free at t=50s
    ok = ingest(db, "demo", "A", [named("demo", "A", "m1")],
                now=BASE_TS + 50_000)
    assert ok.samples_stored == 1


def test_retransmit_does_not_renew_slot(db):
    db.set_series_limit("demo", 1)
    ingest(db, "demo", "A", [named("demo", "A", "m0")], now=BASE_TS)
    # old retransmit at t=25s: no new point, so no lease extension
    again = ingest(db, "demo", "A", [named("demo", "A", "m0")],
                   now=BASE_TS + 25_000)
    assert again.samples_stored == 0
    assert again.samples_stale == 1
    # slot still ends at t=30s
    out = ingest(db, "demo", "A", [named("demo", "A", "m1")],
                 now=BASE_TS + 30_000)
    assert out.samples_stored == 1


def test_holder_writes_normally_after_limit_lowered_below_occupancy(db):
    ingest(
        db, "demo", "A",
        [named("demo", "A", f"m{i}", ts=0) for i in range(4)],
        now=BASE_TS,
    )
    db.set_series_limit("demo", 2)  # 4 live slots, limit now 2
    out = ingest(
        db, "demo", "A",
        [named("demo", "A", f"m{i}", ts=1000) for i in range(4)],
        now=BASE_TS + 1000,
    )
    # all four already hold slots -> all keep writing, none evicted
    assert out.samples_stored == 4
    assert out.samples_limit_dropped == 0
    assert out.active_series == 4
    # but a fifth series cannot enter until the window frees slots
    newcomer = ingest(db, "demo", "A", [named("demo", "A", "m9")],
                      now=BASE_TS + 2000)
    assert newcomer.samples_limit_dropped == 1


def test_dropped_batch_does_not_take_over_stale_replica(db):
    db.set_series_limit("demo", 1)
    ingest(db, "demo", "A", [named("demo", "A", "m0")], now=BASE_TS)
    # A is stale at t=10s, but B only offers a brand-new series (no free slot)
    out = ingest(db, "demo", "B", [named("demo", "B", "m1")],
                 now=BASE_TS + 10_000)
    assert not out.accepted
    assert out.role == "rejected"
    status = db.status(BASE_TS + 10_000)[0]
    assert status.active_replica == "A"
    assert [e["event_type"] for e in db.list_events()] == ["SELECTED"]
    # B's dropped point is not stored anywhere
    assert sample_count(db, "demo") == 1


def test_limit_dropped_points_not_counted_as_liveness_writes(db):
    db.set_series_limit("demo", 1)
    ingest(db, "demo", "A", [named("demo", "A", "m0")], now=BASE_TS)
    # B is standby; its status must not be refreshed by limit drops either.
    standby = ingest(db, "demo", "B", [named("demo", "B", "m1")],
                     now=BASE_TS + 1000)
    assert standby.role == "standby"

    # A writes again at t=5s: its slot now runs until t=35s and it is fresh
    # until t=15s, so B remains standby at t=20s...
    ingest(db, "demo", "A", [named("demo", "A", "m0", ts=1000)],
           now=BASE_TS + 5000)
    denied = ingest(db, "demo", "B", [named("demo", "B", "m1")],
                    now=BASE_TS + 20_000)
    assert not denied.accepted and denied.role == "rejected"
    # ...and even though A is now stale, B's new series has no free slot, so
    # the limit-dropped point can never trigger a takeover.
    assert denied.samples_limit_dropped == 1
    assert db.status(BASE_TS + 20_000)[0].active_replica == "A"

    # Once A's slot expires (t=35s) B's series is admitted and may take over.
    taken = ingest(db, "demo", "B", [named("demo", "B", "m1")],
                   now=BASE_TS + 36_000)
    assert taken.switched
    assert db.status(BASE_TS + 36_000)[0].active_replica == "B"


def test_limit_and_samples_commit_atomically(db, monkeypatch):
    db.set_series_limit("demo", 1)
    ingest(db, "demo", "A", [named("demo", "A", "m0")], now=BASE_TS)
    original = db._series_limit

    def boom(conn, cluster):
        raise RuntimeError("simulated failure after slot check")

    monkeypatch.setattr(db, "_series_limit", boom)
    with pytest.raises(RuntimeError):
        ingest(db, "demo", "A", [named("demo", "A", "m1")],
               now=BASE_TS + 1000)

    reopened = Database(db.path, stale_after_seconds=10.0, lease_seconds=30.0)
    # Nothing from the failed transaction stuck: no m1, no new sample/lease.
    assert {row["name"] for row in reopened.list_series()} == {"m0"}
    assert sample_count(reopened, "demo") == 1
    status = reopened.status(BASE_TS + 1000)[0]
    assert status.active_series == 1
    assert status.series_limit == 1
    monkeypatch.setattr(db, "_series_limit", original)


def test_limit_survives_restart_and_expiry_is_time_based(db):
    db.set_series_limit("demo", 2)
    ingest(
        db, "demo", "A",
        [named("demo", "A", f"m{i}", ts=0) for i in range(2)],
        now=BASE_TS,
    )
    restarted = Database(db.path, stale_after_seconds=10.0, lease_seconds=30.0)
    assert restarted.get_series_limit("demo") == 2
    status = restarted.status(BASE_TS + 1000)[0]
    assert status.active_series == 2
    # 30s after the committed write the slots are free again without any
    # probe of the series/sample tables.
    out = restarted.ingest(
        [named("demo", "A", "m3")], now_ms=BASE_TS + LEASE_MS
    )
    assert out.samples_stored == 1
    assert out.active_series == 1


def test_two_clusters_have_independent_limits(db):
    db.set_series_limit("c1", 1)
    a = ingest(db, "c1", "A", [named("c1", "A", "x")], now=BASE_TS)
    b = ingest(db, "c2", "A", [named("c2", "A", "x")], now=BASE_TS)
    assert a.samples_stored == 1 and b.samples_stored == 1
    assert b.series_limit is None
    drop = ingest(db, "c1", "A", [named("c1", "A", "y")], now=BASE_TS + 1)
    assert drop.samples_limit_dropped == 1


def test_concurrent_limit_races_never_exceed_cap(db):
    db.set_series_limit("demo", 3)
    ingest(db, "demo", "A", [named("demo", "A", "seed")], now=BASE_TS)
    errors: list[Exception] = []

    def writer(worker: int):
        try:
            for i in range(10):
                db.ingest(
                    [series("demo", "A", [(i * 100, 1.0)],
                            name=f"w{worker}_s{i}")],
                    now_ms=BASE_TS + 1000,
                )
        except Exception as exc:  # pragma: no cover - failure signal
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(n,)) for n in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    status = db.status(BASE_TS + 1000)[0]
    # seed + at most two more while every admitted series keeps its slot live
    assert status.active_series <= 3


def test_negative_limit_rejected(db):
    from app.db import IngestError

    with pytest.raises(IngestError):
        db.set_series_limit("demo", -2)
    with pytest.raises(IngestError):
        db.set_series_limit("", 3)


def _build_legacy_db(path):
    """Create the pre-limit schema and a couple of committed legacy samples."""
    import sqlite3

    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE series (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fingerprint TEXT NOT NULL UNIQUE,
            cluster TEXT NOT NULL,
            name TEXT NOT NULL,
            labels_json TEXT NOT NULL
        );
        CREATE TABLE samples (
            series_id INTEGER NOT NULL REFERENCES series(id),
            timestamp_ms INTEGER NOT NULL,
            value REAL NOT NULL,
            PRIMARY KEY (series_id, timestamp_ms)
        ) WITHOUT ROWID;
        CREATE TABLE cluster_state (
            cluster TEXT PRIMARY KEY,
            active_replica TEXT NOT NULL,
            last_write_ms INTEGER,
            updated_ms INTEGER NOT NULL
        );
        CREATE TABLE replica_liveness (
            cluster TEXT NOT NULL,
            replica TEXT NOT NULL,
            last_seen_ms INTEGER,
            last_write_ms INTEGER,
            PRIMARY KEY (cluster, replica)
        ) WITHOUT ROWID;
        CREATE TABLE events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_ms INTEGER NOT NULL,
            cluster TEXT NOT NULL,
            event_type TEXT NOT NULL,
            old_replica TEXT,
            new_replica TEXT,
            reason TEXT
        );
        """
    )
    conn.execute(
        "INSERT INTO series(fingerprint, cluster, name, labels_json) "
        "VALUES ('legacy-fp', 'demo', 'legacy_metric', "
        "'{\"__name__\":\"legacy_metric\",\"cluster\":\"demo\"}')"
    )
    conn.execute(
        "INSERT INTO samples(series_id, timestamp_ms, value) VALUES (1, ?, 42.0)",
        (BASE_TS,),
    )
    conn.execute(
        "INSERT INTO cluster_state(cluster, active_replica, last_write_ms, updated_ms) "
        "VALUES ('demo', 'A', ?, ?)",
        (BASE_TS, BASE_TS),
    )
    conn.execute(
        "INSERT INTO replica_liveness(cluster, replica, last_seen_ms, last_write_ms) "
        "VALUES ('demo', 'A', ?, ?)",
        (BASE_TS, BASE_TS),
    )
    conn.commit()
    conn.close()


def test_legacy_database_upgrades_without_scanning_or_charging_slots(tmp_path):
    path = str(tmp_path / "legacy.db")
    _build_legacy_db(path)

    db = Database(path, stale_after_seconds=10.0, lease_seconds=30.0)
    db.set_series_limit("demo", 1)

    # Old samples stay queryable and legacy writes hold no slot.
    rows = db.list_series()
    assert [row["name"] for row in rows] == ["legacy_metric"]
    assert db.get_samples(rows[0]["id"])[0]["value"] == 42.0
    status = db.status(BASE_TS + 1000)[0]
    assert status.active_series == 0
    assert status.series_limit == 1

    # A brand-new series can take the one free slot despite the legacy data.
    out = db.ingest([named("demo", "A", "fresh_metric", ts=1000)],
                    now_ms=BASE_TS + 1000)
    assert out.samples_stored == 1
    assert out.active_series == 1

    # The upgrade marker is written once and mentions the legacy epoch.
    upgrade = [
        e for e in db.list_events() if e["event_type"] == "LIMIT_SCHEMA_UPGRADE"
    ]
    assert len(upgrade) == 1
    assert f"ms={BASE_TS}" in upgrade[0]["reason"]

    # Re-opening does not duplicate the marker.
    Database(path, stale_after_seconds=10.0, lease_seconds=30.0)
    assert len(
        [
            e
            for e in db.list_events()
            if e["event_type"] == "LIMIT_SCHEMA_UPGRADE"
        ]
    ) == 1


def test_fresh_database_gets_no_upgrade_marker(tmp_path):
    db = Database(str(tmp_path / "fresh.db"), stale_after_seconds=10.0)
    assert db.list_events() == []

