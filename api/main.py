# ============================================================
#  FloodWatch API  —  FastAPI
#
#  REST — readings:
#    GET /api/v1/nodes                       all nodes, latest reading each
#    GET /api/v1/nodes/{node_id}             latest reading for one node
#    GET /api/v1/nodes/{node_id}/readings    paginated history
#
#  REST — event log:
#    GET /api/v1/events/history              all logged events (paginated)
#
#  SSE:
#    GET /api/v1/events/stream               live stream (all types)
#    GET /api/v1/events/stream?types=flood_level  filtered
#
#  Event types (published via Redis by any service):
#    heartbeat     — every new reading from any node
#    flood_level   — water_level changed on a node
#    battery_low   — node voltage dropped below threshold
#    node_offline  — node stopped sending (health-checker)
#
#  Event logging:
#    Every Redis message is written to MongoDB "sse_events" collection
#    with a 30-day TTL before being fanned out to SSE clients.
#    This means the history endpoint shows events from ALL services.
# ============================================================

import asyncio
import json
import logging
import os
from datetime import datetime, timezone

import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient, DESCENDING, ASCENDING
from sse_starlette.sse import EventSourceResponse

load_dotenv()

log = logging.getLogger("api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")

MONGO_URI     = os.getenv("MONGO_URI")
MONGO_DB      = os.getenv("MONGO_DB", "flood_monitor")
MONGO_COL     = os.getenv("MONGO_COL", "sensor_readings")
REDIS_URL     = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_CHANNEL = "floodwatch:events"

mongo        = MongoClient(MONGO_URI)
db           = mongo[MONGO_DB]
col          = db[MONGO_COL]
col_events   = db["sse_events"]

# TTL index: auto-delete events older than 30 days
col_events.create_index("timestamp", expireAfterSeconds=30 * 24 * 3600)
col_events.create_index([("type", ASCENDING), ("timestamp", DESCENDING)])
col_events.create_index([("node_id", ASCENDING), ("timestamp", DESCENDING)])

app = FastAPI(
    title="FloodWatch IoT API",
    version="1.0.0",
    description=(
        "REST + SSE API for FloodWatch flood sensor network. "
        "Events are pushed in real time via Redis Pub/Sub — no polling. "
        "All events are also persisted to MongoDB for historical queries."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ── SSE fan-out + event logging ───────────────────────────────
# Background task subscribes to Redis. For every message:
#   1. Write to MongoDB sse_events (persistent log, 30-day TTL)
#   2. Fan out to all connected SSE client queues

_sse_clients: set[asyncio.Queue] = set()


@app.on_event("startup")
async def start_redis_subscriber():
    asyncio.create_task(_redis_subscriber())


async def _redis_subscriber():
    redis  = aioredis.from_url(REDIS_URL, decode_responses=True)
    pubsub = redis.pubsub()
    await pubsub.subscribe(REDIS_CHANNEL)
    log.info(f"Redis subscriber ready — channel: {REDIS_CHANNEL}")

    async for message in pubsub.listen():
        if message["type"] != "message":
            continue

        raw = message["data"]

        # 1. Persist to MongoDB (run in thread to avoid blocking the loop)
        try:
            event = json.loads(raw)
            doc   = {**event, "timestamp": datetime.now(timezone.utc)}
            doc.pop("type", None)           # stored in "type" field separately
            doc["type"] = event.get("type", "unknown")
            await asyncio.to_thread(col_events.insert_one, doc)
        except Exception as e:
            log.warning(f"Event log write failed: {e}")

        # 2. Fan out to all SSE clients
        dead = set()
        for q in _sse_clients:
            try:
                q.put_nowait(raw)
            except asyncio.QueueFull:
                dead.add(q)
        _sse_clients.difference_update(dead)


# ── Helpers ───────────────────────────────────────────────────

ALL_FIELDS = {
    "node_id", "lat", "lng", "float_bits", "water_level",
    "voltage", "raw_adc", "rssi", "timestamp",
}

def _serialize_reading(doc: dict) -> dict:
    out = {k: v for k, v in doc.items() if k in ALL_FIELDS}
    if isinstance(out.get("timestamp"), datetime):
        out["timestamp"] = out["timestamp"].isoformat()
    return out

def _serialize_event(doc: dict) -> dict:
    out = {k: v for k, v in doc.items() if k != "_id"}
    if isinstance(out.get("timestamp"), datetime):
        out["timestamp"] = out["timestamp"].isoformat()
    return out


# ── REST — readings ───────────────────────────────────────────

@app.get("/", tags=["health"])
def health():
    return {"status": "ok", "service": "floodwatch-api", "version": "1.0.0"}


@app.get("/api/v1/nodes", tags=["nodes"])
def list_nodes():
    """Return every known node with its most recent reading."""
    pipeline = [
        {"$sort": {"timestamp": -1}},
        {"$group": {"_id": "$node_id", "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}},
        {"$limit": 200},
    ]
    return [_serialize_reading(d) for d in col.aggregate(pipeline)]


@app.get("/api/v1/nodes/{node_id}", tags=["nodes"])
def get_node(node_id: str):
    """Latest reading for a single node."""
    doc = col.find_one({"node_id": node_id}, sort=[("timestamp", DESCENDING)])
    if not doc:
        raise HTTPException(404, f"Node '{node_id}' not found")
    return _serialize_reading(doc)


@app.get("/api/v1/nodes/{node_id}/readings", tags=["nodes"])
def node_readings(
    node_id: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    """Paginated reading history for a node."""
    skip   = (page - 1) * page_size
    cursor = (
        col.find({"node_id": node_id})
           .sort("timestamp", DESCENDING)
           .skip(skip)
           .limit(page_size)
    )
    data = [_serialize_reading(d) for d in cursor]
    if not data and page == 1:
        raise HTTPException(404, f"Node '{node_id}' not found")
    return {"node_id": node_id, "page": page, "page_size": page_size, "data": data}


# ── REST — event log ──────────────────────────────────────────

@app.get("/api/v1/events/history", tags=["events"])
def event_history(
    type: str    = Query(default=None, description="Filter by event type, e.g. flood_level"),
    node_id: str = Query(default=None, description="Filter by node ID"),
    page: int    = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    """
    Paginated log of all events that have been emitted, newest first.
    Events are retained for 30 days then auto-deleted by MongoDB TTL.
    """
    query: dict = {}
    if type:
        query["type"] = type
    if node_id:
        query["node_id"] = node_id

    skip   = (page - 1) * page_size
    cursor = (
        col_events.find(query)
                  .sort("timestamp", DESCENDING)
                  .skip(skip)
                  .limit(page_size)
    )
    data = [_serialize_event(d) for d in cursor]
    return {
        "page":      page,
        "page_size": page_size,
        "filters":   {"type": type, "node_id": node_id},
        "data":      data,
    }


# ── SSE stream ────────────────────────────────────────────────

@app.get("/api/v1/events/stream", tags=["events"])
async def sse_stream(
    request: Request,
    types: str = Query(
        default="heartbeat,flood_level,battery_low,node_offline",
        description="Comma-separated event types to receive. Omit for all.",
    ),
):
    """
    Server-Sent Events stream. Events arrive the moment any service publishes
    to the Redis channel — no polling delay.

    ```js
    const es = new EventSource("/api/v1/events/stream");
    es.addEventListener("flood_level", e => console.log(JSON.parse(e.data)));
    ```
    """
    wanted = {t.strip() for t in types.split(",")}
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
                    if event_type not in wanted:
                        continue
                    yield {"event": event_type, "data": raw}
                except asyncio.TimeoutError:
                    yield {"comment": "keepalive"}
        finally:
            _sse_clients.discard(q)

    return EventSourceResponse(generator())
