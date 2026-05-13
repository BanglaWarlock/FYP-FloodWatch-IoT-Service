#!/usr/bin/env python3
"""
FloodWatch API v2  —  FastAPI + SSE

REST endpoints:
  GET  /                                  health check
  GET  /api/v1/stats                      global aggregate counters
  GET  /api/v1/villages                   all villages (summary)
  GET  /api/v1/villages/{village_id}      single village with topology
  GET  /api/v1/nodes                      all river nodes (?village_id, ?status)
  GET  /api/v1/nodes/{node_id}            single river node live state
  GET  /api/v1/nodes/{node_id}/readings   paginated sensor history
  GET  /api/v1/alerts                     paginated alerts (?village_id, ?node_id, ?alert_type)
  GET  /api/v1/events/history             paginated event log (?event_type, ?node_id, ?village_id)
  GET  /api/v1/events/stream              SSE live stream (?types=heartbeat,flood_level,...)

SSE event types:
  heartbeat      — every sensor reading
  flood_level    — water level changed
  alert          — any alert (flood, battery, gps_*)
  node_online    — node came online
  node_offline   — node went offline
  master_online  — master connected
  master_offline — master disconnected (LWT)
  node_announce  — node GPS calibration complete
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone

import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient, DESCENDING, ASCENDING
from sse_starlette.sse import EventSourceResponse
from starlette.requests import Request

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("api")

# ── Config ────────────────────────────────────────────────────────────────────

MONGO_URI     = os.getenv("MONGO_URI")
MONGO_DB      = os.getenv("MONGO_DB", "flood_monitor")
REDIS_URL     = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_CHANNEL = "floodwatch:events"

# ── MongoDB ───────────────────────────────────────────────────────────────────

mongo        = MongoClient(MONGO_URI)
db           = mongo[MONGO_DB]
col_global   = db["global_stats"]
col_villages = db["villages"]
col_masters  = db["master_nodes"]
col_rivers   = db["river_nodes"]
col_readings = db["sensor_readings"]
col_alerts   = db["alerts"]
col_events   = db["events"]

# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="FloodWatch IoT API",
    version="2.0.0",
    description="REST + SSE API for FloodWatch flood sensor network.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ── SSE fan-out ───────────────────────────────────────────────────────────────

_sse_clients: set[asyncio.Queue] = set()
_subscriber_task: asyncio.Task | None = None


@app.on_event("startup")
async def start_redis_subscriber():
    global _subscriber_task
    _subscriber_task = asyncio.create_task(_redis_subscriber())


async def _redis_subscriber():
    while True:
        try:
            r      = aioredis.from_url(REDIS_URL, decode_responses=True)
            pubsub = r.pubsub()
            await pubsub.subscribe(REDIS_CHANNEL)
            log.info(f"Redis subscriber ready — {REDIS_CHANNEL}")

            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                raw  = message["data"]
                dead = set()
                for q in _sse_clients:
                    try:
                        q.put_nowait(raw)
                    except asyncio.QueueFull:
                        dead.add(q)
                _sse_clients.difference_update(dead)

        except Exception as e:
            log.error(f"Redis subscriber error: {e} — reconnecting in 5s")
            await asyncio.sleep(5)


# ── Serialisers ───────────────────────────────────────────────────────────────

def _clean(doc: dict) -> dict:
    """Remove _id and convert datetimes to ISO strings."""
    out = {k: v for k, v in doc.items() if k != "_id"}
    for k, v in out.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat()
    return out


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/", tags=["health"])
def health():
    return {"status": "ok", "service": "floodwatch-api", "version": "2.0.0"}


# ── Global stats ──────────────────────────────────────────────────────────────

@app.get("/api/v1/stats", tags=["stats"])
def get_stats():
    """
    Single-document aggregate counters — total nodes, villages, alerts by type, etc.
    Always O(1) — the parser keeps this updated atomically.
    """
    doc = col_global.find_one({"_id": "global"}) or {}
    doc.pop("_id", None)
    if isinstance(doc.get("last_updated"), datetime):
        doc["last_updated"] = doc["last_updated"].isoformat()
    # Augment with live online counts from river_nodes (accurate)
    doc["nodes_online"]  = col_rivers.count_documents({"status": "online"})
    doc["nodes_offline"] = col_rivers.count_documents({"status": "offline"})
    return doc


# ── Villages ──────────────────────────────────────────────────────────────────

@app.get("/api/v1/villages", tags=["villages"])
def list_villages():
    """All villages with summary. Topology excluded from list view."""
    docs = col_villages.find({}, {"topology": 0, "gps_source_nodes": 0})
    return [_clean(d) for d in docs]


@app.get("/api/v1/villages/{village_id}", tags=["villages"])
def get_village(village_id: str):
    """Single village with full topology and node list."""
    doc = col_villages.find_one({"village_id": village_id})
    if not doc:
        raise HTTPException(404, f"Village '{village_id}' not found")
    return _clean(doc)


# ── River nodes ───────────────────────────────────────────────────────────────

@app.get("/api/v1/nodes", tags=["nodes"])
def list_nodes(
    village_id: str = Query(default=None, description="Filter by village"),
    status:     str = Query(default=None, description="online | offline"),
):
    """
    All river nodes with current live state.
    Reads directly from river_nodes collection — O(n nodes), no aggregation.
    """
    query: dict = {}
    if village_id:
        query["village_id"] = village_id
    if status:
        query["status"] = status
    return [_clean(d) for d in col_rivers.find(query)]


@app.get("/api/v1/nodes/{node_id}", tags=["nodes"])
def get_node(node_id: str):
    """Single river node — current live state."""
    doc = col_rivers.find_one({"node_id": node_id})
    if not doc:
        raise HTTPException(404, f"Node '{node_id}' not found")
    return _clean(doc)


@app.get("/api/v1/nodes/{node_id}/readings", tags=["nodes"])
def node_readings(
    node_id:    str,
    page:       int = Query(default=1,  ge=1),
    page_size:  int = Query(default=50, ge=1, le=200),
    gps_only:   bool = Query(default=False, description="Only return readings with a GPS fix"),
):
    """Paginated sensor reading history for a node (newest first)."""
    query: dict = {"node_id": node_id}
    if gps_only:
        query["gps_fix"] = True
    skip   = (page - 1) * page_size
    cursor = col_readings.find(query).sort("timestamp", DESCENDING).skip(skip).limit(page_size)
    data   = [_clean(d) for d in cursor]
    if not data and page == 1:
        raise HTTPException(404, f"Node '{node_id}' not found")
    return {"node_id": node_id, "page": page, "page_size": page_size, "data": data}


# ── Alerts ────────────────────────────────────────────────────────────────────

@app.get("/api/v1/alerts", tags=["alerts"])
def list_alerts(
    village_id: str = Query(default=None),
    node_id:    str = Query(default=None),
    alert_type: str = Query(default=None, description="flood | battery | gps_signal_lost | gps_restored | gps_moved"),
    page:       int = Query(default=1,  ge=1),
    page_size:  int = Query(default=50, ge=1, le=200),
):
    """Paginated alert history across all nodes or filtered. Newest first."""
    query: dict = {}
    if village_id:  query["village_id"]  = village_id
    if node_id:     query["node_id"]     = node_id
    if alert_type:  query["alert_type"]  = alert_type
    skip   = (page - 1) * page_size
    cursor = col_alerts.find(query).sort("timestamp", DESCENDING).skip(skip).limit(page_size)
    return {"page": page, "page_size": page_size, "data": [_clean(d) for d in cursor]}


# ── Event log ─────────────────────────────────────────────────────────────────

@app.get("/api/v1/events/history", tags=["events"])
def event_history(
    event_type: str = Query(default=None, description="node_online | node_offline | master_online | master_offline | announce"),
    node_id:    str = Query(default=None),
    village_id: str = Query(default=None),
    page:       int = Query(default=1,  ge=1),
    page_size:  int = Query(default=50, ge=1, le=200),
):
    """Paginated log of online/offline and announce events (30-day TTL)."""
    query: dict = {}
    if event_type:  query["event_type"] = event_type
    if node_id:     query["node_id"]    = node_id
    if village_id:  query["village_id"] = village_id
    skip   = (page - 1) * page_size
    cursor = col_events.find(query).sort("timestamp", DESCENDING).skip(skip).limit(page_size)
    return {"page": page, "page_size": page_size, "data": [_clean(d) for d in cursor]}


# ── SSE stream ────────────────────────────────────────────────────────────────

@app.get("/api/v1/events/stream", tags=["events"])
async def sse_stream(
    request: Request,
    types: str = Query(
        default="heartbeat,flood_level,alert,node_online,node_offline",
        description="Comma-separated event types. Omit for all.",
    ),
):
    """
    Server-Sent Events stream. Zero-latency fan-out from Redis Pub/Sub.

    ```js
    const es = new EventSource("/api/v1/events/stream");
    es.addEventListener("alert", e => console.log(JSON.parse(e.data)));
    es.addEventListener("flood_level", e => console.log(JSON.parse(e.data)));
    ```

    Available types:
      heartbeat, flood_level, alert, node_online, node_offline,
      master_online, master_offline, node_announce
    """
    wanted = {t.strip() for t in types.split(",")} if types != "all" else None
    q: asyncio.Queue = asyncio.Queue(maxsize=256)
    _sse_clients.add(q)

    async def generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    raw        = await asyncio.wait_for(q.get(), timeout=15.0)
                    event      = json.loads(raw)
                    event_type = event.get("type", "unknown")
                    if wanted and event_type not in wanted:
                        continue
                    yield {"event": event_type, "data": raw}
                except asyncio.TimeoutError:
                    yield {"comment": "keepalive"}
        finally:
            _sse_clients.discard(q)

    return EventSourceResponse(generator())
