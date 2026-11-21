"""Live exercise of the per-cluster active-series limit (run with -I).

Assumes a receiver is already running with a short LEASE_SECONDS, e.g.:

    DB_PATH=/tmp/limit-smoke/receiver.db LEASE_SECONDS=3 STALE_AFTER_SECONDS=5 \
      uvicorn app.main:app --port 8078 --app-dir receiver

    SMOKE_BASE=http://127.0.0.1:8078 python3 -I scripts/limit_smoke.py

Checks: unset limit accepts everything; cap admits holders and drops
newcomers; dropped points never renew a slot; expiry frees a slot after the
lease window; limit changes take effect through the HTTP API.
"""
import json
import os
import site
import sys
import time
import urllib.request

site.addsitedir("/home/node/.local/lib/python3.11/site-packages")
sys.path.insert(0, "/workspace/receiver")
sys.path.insert(0, "/workspace/tests")

from conftest import RW_HEADERS, body, series  # noqa: E402

BASE = os.environ.get("SMOKE_BASE", "http://127.0.0.1:8078")
LEASE = float(os.environ.get("SMOKE_LEASE_SECONDS", "3"))


def request(method, path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"} if data else {}
    req = urllib.request.Request(
        BASE + path, data=data, headers=headers, method=method
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read() or b"{}")


LAST_TIMESTAMP = {"ts": 0}


def write(replica, items, ts_ms=None):
    """items: list of (metric_name, value) all stamped at one wall-clock ms."""
    now = ts_ms if ts_ms is not None else int(time.time() * 1000)
    LAST_TIMESTAMP["ts"] = now
    batch = [
        series("limitdemo", replica, [(now - 2_000_000_000_000, value)], name=name)
        for name, value in items
    ]
    req = urllib.request.Request(
        BASE + "/api/v1/write", data=body(batch), headers=RW_HEADERS, method="POST"
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.status, {k.lower(): v for k, v in resp.headers.items()}


def limits():
    data = request("GET", "/api/limits")
    for row in data["clusters"]:
        if row["cluster"] == "limitdemo":
            return row
    return {"series_limit": None, "active_series": 0, "limit_dropped_total": 0}


print("health:", request("GET", "/healthz"))

# 1. no limit configured -> everything admitted
s, h = write("A", [("m0", 1.0), ("m1", 1.0), ("m2", 1.0), ("m3", 1.0)])
print("unlimited:", s, "stored=", h["x-ingest-stored"],
      "dropped=", h["x-ingest-limit-dropped"])
assert h["x-ingest-limit-dropped"] == "0"

# 2. cap at 2, all 4 series already hold slots -> all keep writing
print("set limit:", request("PUT", "/api/clusters/limitdemo/limit", {"max_series": 2}))
s, h = write("A", [("m0", 2.0), ("m1", 2.0), ("m2", 2.0), ("m3", 2.0)])
print("holders after lowering:", s, "stored=", h["x-ingest-stored"],
      "dropped=", h["x-ingest-limit-dropped"], "active=",
      h["x-ingest-active-series"])
assert h["x-ingest-limit-dropped"] == "0"
assert h["x-ingest-active-series"] == "4"

# 3. newcomers are dropped while every slot is live
s, h = write("A", [("new1", 1.0), ("new2", 1.0)])
print("newcomers full:", s, "stored=", h["x-ingest-stored"],
      "dropped=", h["x-ingest-limit-dropped"])
assert h["x-ingest-stored"] == "0"
assert h["x-ingest-limit-dropped"] == "2"
print("limits view:", limits())

# 4. after the lease window all slots expire; newcomers compete for 2 slots
time.sleep(LEASE + 0.4)
s, h = write("A", [("new1", 2.0), ("new2", 2.0), ("new3", 2.0)])
print("after expiry:", s, "stored=", h["x-ingest-stored"],
      "dropped=", h["x-ingest-limit-dropped"], "active=",
      h["x-ingest-active-series"])
assert h["x-ingest-stored"] == "2", "expired slots must free up"
assert h["x-ingest-limit-dropped"] == "1"
assert h["x-ingest-active-series"] == "2"

# 5. a retransmit never renews a slot; clearing the limit restores unlimited
#    intake after the remaining slots expire.
retransmit_ts = LAST_TIMESTAMP["ts"]
s, h = write("A", [("new1", 2.0)], ts_ms=retransmit_ts)
assert h["x-ingest-stored"] == "0" and h["x-ingest-stale"] == "1"
print("limit removed:", request("PUT", "/api/clusters/limitdemo/limit",
                               {"max_series": None}))
time.sleep(LEASE + 0.4)
s, h = write("A", [("x1", 1.0), ("x2", 1.0), ("x3", 1.0)])
print("unlimited again:", s, "stored=", h["x-ingest-stored"],
      "dropped=", h["x-ingest-limit-dropped"])
assert h["x-ingest-limit-dropped"] == "0"
print("LIMIT SMOKE OK")
