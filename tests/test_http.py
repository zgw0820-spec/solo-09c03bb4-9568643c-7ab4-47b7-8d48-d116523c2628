"""End-to-end HTTP tests against the FastAPI app (in-process ASGI client)."""
from __future__ import annotations

import importlib
import os

import pytest

# Configure before the app module is imported.
os.environ["DB_PATH"] = ":memory-test:"  # replaced per-test via fixture env below
os.environ["STALE_AFTER_SECONDS"] = "10"
os.environ["REQUIRE_REMOTE_WRITE_HEADER"] = "1"

from fastapi.testclient import TestClient  # noqa: E402

from app import config  # noqa: E402
from app.main import create_app  # noqa: E402
from tests.conftest import BASE_TS, RW_HEADERS, body, series  # noqa: E402


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "http.db"))
    monkeypatch.setattr(config, "STALE_AFTER_SECONDS", 10.0)
    app = create_app()
    with TestClient(app) as c:
        yield c


def post(client, cluster, replica, points, now_offset=0, **kw):
    # HTTP layer uses wall clock for staleness; test points carry absolute ms.
    return client.post(
        "/api/v1/write",
        content=body([series(cluster, replica, points, **kw)]),
        headers=RW_HEADERS,
    )


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}


def test_remote_write_accepted_and_persisted(client):
    resp = post(client, "demo", "A", [(0, 1.0), (1000, 2.0)])
    assert resp.status_code == 204
    assert resp.headers["x-ingest-accepted"] == "1"
    assert resp.headers["x-ingest-role"] == "initial"

    status = client.get("/api/status").json()
    assert status["clusters"][0]["active_replica"] == "A"
    rows = client.get("/api/series").json()["series"]
    assert len(rows) == 1
    sid = rows[0]["id"]
    samples = client.get(f"/api/series/{sid}/samples").json()["samples"]
    assert [s["value"] for s in samples] == [1.0, 2.0]


def test_standby_batch_returns_204_but_marks_rejected(client):
    post(client, "demo", "A", [(0, 1.0)])
    resp = post(client, "demo", "B", [(1000, 9.0)])
    assert resp.status_code == 204
    assert resp.headers["x-ingest-accepted"] == "0"
    assert resp.headers["x-ingest-role"] == "standby"


def test_missing_protocol_headers_rejected(client):
    payload = body([series("demo", "A", [(0, 1.0)])])
    resp = client.post("/api/v1/write", content=payload)
    assert resp.status_code == 400
    resp = client.post(
        "/api/v1/write", content=payload,
        headers={"X-Prometheus-Remote-Write-Version": "0.1"},
    )
    assert resp.status_code == 400


def test_bad_payload_rejected(client):
    resp = client.post(
        "/api/v1/write", content=b"junk", headers=RW_HEADERS
    )
    assert resp.status_code == 400


def test_takeover_over_real_wall_clock(client):
    # Initial active A.
    assert post(client, "demo", "A", [(0, 1.0)]).status_code == 204
    # B immediately -> standby.
    assert post(client, "demo", "B", [(1000, 2.0)]).headers["x-ingest-role"] == "standby"

    import time
    time.sleep(10.2)  # exceed STALE_AFTER_SECONDS without any new point from A

    resp = post(client, "demo", "B", [(2000, 3.0)])
    assert resp.status_code == 204
    assert resp.headers["x-ingest-role"] == "takeover"
    status = client.get("/api/status").json()["clusters"][0]
    assert status["active_replica"] == "B"
    assert status["stale"] is False
    events = client.get("/api/events").json()["events"]
    assert events[0]["event_type"] == "TAKEOVER"


def test_old_retransmit_over_http_does_not_grow_series(client):
    post(client, "demo", "A", [(0, 1.0), (1000, 2.0)])
    resp = post(client, "demo", "A", [(0, 1.0), (1000, 2.0)])
    assert resp.headers["x-ingest-stored"] == "0"
    assert resp.headers["x-ingest-stale"] == "2"
    rows = client.get("/api/series").json()["series"]
    assert rows[0]["sample_count"] == 2


def test_empty_write_request_is_noop(client):
    resp = client.post("/api/v1/write", content=body([]), headers=RW_HEADERS)
    assert resp.status_code == 204
    assert resp.headers["x-ingest-role"] == "noop"
    assert client.get("/api/series").json()["series"] == []


def test_index_page_served(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "Remote Write" in resp.text


def test_replica_endpoints_graceful_without_docker(client, monkeypatch):
    from app import dockerctl

    def boom():
        raise dockerctl.DockerUnavailable("no socket")

    monkeypatch.setattr(dockerctl, "_client", boom)
    assert client.get("/api/replicas").status_code == 503
    assert client.post("/api/replicas/A/pause").status_code == 503
    assert client.post("/api/replicas/ZZ/pause").status_code == 404


# --------------------------------------------------------------------------- #
# active-series limit over HTTP
# --------------------------------------------------------------------------- #
def set_limit(client, cluster, value):
    return client.put(
        f"/api/clusters/{cluster}/limit", json={"max_series": value}
    )


def test_limit_endpoint_roundtrip_and_unset_default(client):
    assert set_limit(client, "demo", 5).json()["series_limit"] == 5
    limits = client.get("/api/limits").json()
    assert limits["lease_ms"] == 30_000
    # A limit can be configured before the cluster has any data.
    assert limits["clusters"][0]["cluster"] == "demo"
    assert limits["clusters"][0]["series_limit"] == 5
    post(client, "demo", "A", [(0, 1.0)])
    limits = client.get("/api/limits").json()
    assert limits["clusters"][0]["series_limit"] == 5
    assert limits["clusters"][0]["active_series"] == 1
    assert set_limit(client, "demo", None).json()["series_limit"] is None
    assert client.get("/api/limits").json()["clusters"][0]["series_limit"] is None
    assert set_limit(client, "demo", -1).status_code == 400
    assert set_limit(client, "demo", 1.5).status_code == 422


def test_limit_headers_and_dropped_count(client):
    set_limit(client, "demo", 1)
    first = post(client, "demo", "A", [(0, 1.0)])
    assert first.headers["x-ingest-active-series"] == "1"
    assert first.headers["x-ingest-limit-dropped"] == "0"

    import time
    now = int(time.time() * 1000)
    payload = body([
        series("demo", "A", [(now - BASE_TS, 2.0)]),
        series("demo", "A", [(now - BASE_TS, 3.0)], name="other_metric"),
    ])
    resp = client.post("/api/v1/write", content=payload, headers=RW_HEADERS)
    assert resp.status_code == 204
    # The first series already holds the slot, the second one is dropped.
    assert resp.headers["x-ingest-limit-dropped"] == "1"
    rows = client.get("/api/series").json()["series"]
    assert {row["name"] for row in rows} == {"demo_requests_total"}
    cluster = client.get("/api/status").json()["clusters"][0]
    assert cluster["series_limit"] == 1
    assert cluster["active_series"] == 1
    assert cluster["limit_dropped_total"] == 1


def test_demo_batch_endpoint_injects_mixed_batch(client):
    set_limit(client, "demo", 2)
    first = client.post(
        "/api/demo/batch", json={"existing_series": 0, "new_series": 2}
    )
    assert first.status_code == 200
    assert first.json()["outcome"]["samples_stored"] == 2

    second = client.post(
        "/api/demo/batch", json={"existing_series": 2, "new_series": 3}
    ).json()
    assert second["existing_included"] == 2
    # 2 live holders pass, the remaining 3 newcomers miss every free slot
    assert second["outcome"]["samples_stored"] == 2
    assert second["outcome"]["samples_limit_dropped"] == 3
    assert second["outcome"]["active_series"] == 2
    assert second["series_limit"] == 2
    limits = client.get("/api/limits").json()["clusters"][0]
    assert limits["limit_dropped_total"] == 3


def test_index_page_exposes_limit_controls(client):
    assert "活跃序列上限" in client.get("/").text
    assert "demo/batch" in client.get("/").text
