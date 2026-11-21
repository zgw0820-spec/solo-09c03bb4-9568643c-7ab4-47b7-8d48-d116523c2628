"""FastAPI entrypoint: remote-write ingestion plus the demo web UI."""
from __future__ import annotations

import json
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import config, dockerctl
from .db import Database, IngestError
from .remote_write import (
    PROTOBUF,
    REMOTE_WRITE_VERSION,
    SNAPPY,
    Label,
    RemoteWriteError,
    Sample,
    TimeSeries,
    decode_write_request,
)

STATIC_DIR = Path(__file__).parent / "static"


class LimitUpdate(BaseModel):
    max_series: int | None = None


def _local_series(
    cluster: str,
    replica: str,
    name: str,
    points: list[tuple[int, float]],
    extra: dict[str, str] | None = None,
) -> TimeSeries:
    fake_labels = {
        "__name__": name,
        "cluster": cluster,
        "replica": replica,
        "job": "local-demo",
    }
    for key, value in (extra or {}).items():
        # cluster/replica routing labels are authoritative from the caller.
        if key not in ("cluster", "replica"):
            fake_labels[key] = value
    return TimeSeries(
        labels=tuple(Label(k, v) for k, v in sorted(fake_labels.items())),
        samples=tuple(Sample(value=v, timestamp_ms=ts) for ts, v in points),
    )


def now_ms() -> int:
    return int(time.time() * 1000)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = Database(
        path=config.DB_PATH,
        stale_after_seconds=config.STALE_AFTER_SECONDS,
        lease_seconds=config.LEASE_SECONDS,
    )
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="Remote-Write Failover Receiver", lifespan=lifespan)

    @app.post("/api/v1/write")
    async def remote_write(request: Request) -> Response:
        if config.REQUIRE_REMOTE_WRITE_HEADER:
            if request.headers.get("x-prometheus-remote-write-version") != (
                REMOTE_WRITE_VERSION
            ):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "missing or invalid X-Prometheus-Remote-Write-Version, "
                        f"expected {REMOTE_WRITE_VERSION}"
                    ),
                )
            content_type = request.headers.get("content-type", "")
            if PROTOBUF not in content_type or SNAPPY not in content_type:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Content-Type must be 'application/x-protobuf' with the "
                        "'snappy' protocol parameter"
                    ),
                )

        raw = await request.body()
        try:
            series_list = decode_write_request(raw)
        except RemoteWriteError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        # An empty write request is valid on the wire; nothing to apply.
        if not series_list:
            return Response(status_code=204, headers={
                "X-Ingest-Accepted": "1",
                "X-Ingest-Role": "noop",
                "X-Ingest-Stored": "0",
                "X-Ingest-Stale": "0",
                "X-Ingest-Limit-Dropped": "0",
                "X-Ingest-Active-Series": "0",
            })

        db: Database = app.state.db
        try:
            outcome = db.ingest(series_list, now_ms())
        except IngestError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        # Remote-write always answers 204 once the request was processed;
        # rejected standby/retransmit batches are a normal state-machine result.
        return Response(status_code=204, headers={
            "X-Ingest-Accepted": "1" if outcome.accepted else "0",
            "X-Ingest-Role": outcome.role,
            "X-Ingest-Stored": str(outcome.samples_stored),
            "X-Ingest-Stale": str(outcome.samples_stale),
            "X-Ingest-Limit-Dropped": str(outcome.samples_limit_dropped),
            "X-Ingest-Active-Series": str(outcome.active_series),
        })

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"status": "ok"}

    @app.get("/api/status")
    async def get_status() -> dict:
        db: Database = app.state.db
        clusters = db.status(now_ms())
        return {
            "now_ms": now_ms(),
            "stale_after_ms": db.stale_after_ms,
            "lease_ms": db.lease_ms,
            "clusters": [cluster.__dict__ for cluster in clusters],
        }

    @app.get("/api/limits")
    async def get_limits() -> dict:
        db: Database = app.state.db
        return {
            "lease_ms": db.lease_ms,
            "clusters": db.limits_overview(now_ms()),
        }

    @app.put("/api/clusters/{cluster}/limit")
    async def set_limit(cluster: str, update: LimitUpdate) -> dict:
        db: Database = app.state.db
        try:
            db.set_series_limit(cluster, update.max_series)
        except IngestError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "cluster": cluster,
            "series_limit": update.max_series,
            "lease_ms": db.lease_ms,
        }

    @app.post("/api/demo/batch")
    async def demo_batch(request: Request) -> JSONResponse:
        """Inject one mixed local batch through the normal ingest path.

        Body (all optional): ``cluster`` (default ``demo``), ``replica``
        (default ``A``), ``existing_series`` (how many live series to include,
        default 3) and ``new_series`` (fresh series to attempt, default 3).
        Every included series carries one new point stamped with the current
        wall clock, so the batch exercises slot admission deterministically.
        """
        db: Database = app.state.db
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        cluster = str(payload.get("cluster") or "demo")
        replica = str(payload.get("replica") or "A")
        try:
            new_series = max(0, int(payload.get("new_series", 3)))
            existing_series = max(0, int(payload.get("existing_series", 3)))
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=400,
                detail="existing_series/new_series must be integers",
            )

        ts = now_ms()
        batch = []
        # Prefer series that still hold a slot so the demo really exercises
        # "holders keep writing while newcomers compete for free slots".
        rows = db.live_series(cluster, ts, limit=max(existing_series, 1))
        if len(rows) < existing_series:
            have = {row["id"] for row in rows}
            for row in db.list_series():
                if row["cluster"] != cluster or row["id"] in have:
                    continue
                rows.append(row)
                if len(rows) >= existing_series:
                    break
        rows = rows[:existing_series]
        for row in rows:
            labels = json.loads(row["labels_json"])
            batch.append(
                _local_series(
                    cluster, replica, labels.get("__name__", ""),
                    [(ts, ts / 1000.0)], extra=labels,
                )
            )
        for index in range(new_series):
            batch.append(
                _local_series(
                    cluster, replica, "limit_demo_series",
                    [(ts, float(index))],
                    extra={"instance": f"local-{ts}-{index}"},
                )
            )

        try:
            outcome = db.ingest(batch, ts)
        except IngestError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(
            {
                "cluster": cluster,
                "replica": replica,
                "existing_included": len(rows),
                "new_included": new_series,
                "outcome": outcome.__dict__,
                "lease_ms": db.lease_ms,
                "series_limit": db.get_series_limit(cluster),
            }
        )

    @app.get("/api/series")
    async def get_series() -> dict:
        db: Database = app.state.db
        return {"series": db.list_series()}

    @app.get("/api/series/{series_id}/samples")
    async def get_series_samples(series_id: int, limit: int = 240) -> dict:
        db: Database = app.state.db
        return {
            "series_id": series_id,
            "limit": limit,
            "samples": db.get_samples(series_id, limit),
        }

    @app.get("/api/events")
    async def get_events(limit: int = 30) -> dict:
        db: Database = app.state.db
        return {"events": db.list_events(limit)}

    @app.get("/api/replicas")
    async def get_replicas() -> JSONResponse:
        try:
            return JSONResponse({"replicas": dockerctl.list_replica_containers()})
        except dockerctl.DockerUnavailable as exc:
            return JSONResponse(
                status_code=503,
                content={"detail": f"docker control unavailable: {exc}"},
            )

    @app.post("/api/replicas/{replica}/{action}")
    async def control_replica(replica: str, action: str) -> JSONResponse:
        try:
            return JSONResponse(dockerctl.control(replica, action))
        except KeyError:
            raise HTTPException(
                status_code=404,
                detail=f"unknown replica '{replica}', known: "
                f"{sorted(config.REPLICA_CONTAINERS)}",
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except dockerctl.DockerUnavailable as exc:
            return JSONResponse(
                status_code=503,
                content={"detail": f"docker control unavailable: {exc}"},
            )

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    app.mount(
        "/static",
        StaticFiles(directory=str(STATIC_DIR)),
        name="static",
    )
    return app


app = create_app()
