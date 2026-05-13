#!/usr/bin/env python3
"""
FloodWatch API v2  —  FastAPI + SSE

All data endpoints accept ?dataset=real|sample|all (default: real).
  real   — live sensor data only (default)
  sample — pre-generated demo data only (is_sample: true)
  all    — both together (for side-by-side comparison)

REST endpoints:
  GET  /                                    health check
  GET  /api/v1/stats                        global aggregate counters
  GET  /api/v1/villages                     all villages (summary, no topology)
  GET  /api/v1/villages/{village_id}        single village with full topology + weather
  GET  /api/v1/nodes                        all river nodes (?village_id, ?status)
  GET  /api/v1/nodes/{node_id}              single river node live state
  GET  /api/v1/nodes/{node_id}/readings     paginated heartbeat history (?from, ?to, ?gps_only)
  GET  /api/v1/masters                      all master nodes
  GET  /api/v1/alerts                       paginated alerts (?village_id, ?node_id, ?alert_type, ?from, ?to)
  GET  /api/v1/weather/{village_id}         paginated weather history (?from, ?to)
  GET  /api/v1/weather/{village_id}/at      weather valid at a specific moment (?t=<iso>)
  GET  /api/v1/nodes/{node_id}/summary      avg/min/max water level + alerts (?period, ?from, ?to)
  GET  /api/v1/villages/{village_id}/summary per-node breakdown + village totals (?period, ?from, ?to)
  GET  /api/v1/stats/summary                global totals, top nodes, top villages (?period, ?from, ?to)
  GET  /api/v1/events/history               paginated event log (?event_type, ?node_id, ?village_id)
  GET  /api/v1/events/stream                SSE live stream (?types=..., ?dataset=real|sample|all)

SSE event types:
  heartbeat      — every sensor reading
  flood_level    — water level changed (water_level, water_level_prev)
  alert          — any alert (flood, battery, gps_signal_lost, gps_restored, gps_moved)
  node_online    — node came online
  node_offline   — node went offline
  master_online  — master connected
  master_offline — master disconnected (LWT)
  node_announce  — node GPS calibration complete
  weather_update — village weather changed (from weather poller)

Alert fields for gps_moved:
  dist_m      — metres from install position
  lat/lng     — current position
  home_lat/home_lng — original install position
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import redis.asyncio as aioredis
from bson import ObjectId
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient, DESCENDING
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

mongo          = MongoClient(MONGO_URI)
db             = mongo[MONGO_DB]
col_global     = db["global_stats"]
col_villages   = db["villages"]
col_masters    = db["master_nodes"]
col_rivers     = db["river_nodes"]
col_heartbeats = db["heartbeats"]
col_alerts     = db["alerts"]
col_events     = db["events"]
col_weather    = db["weather_history"]

# ── SSE fan-out ───────────────────────────────────────────────────────────────

_sse_clients: set[asyncio.Queue] = set()
_subscriber_task: asyncio.Task | None = None


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _subscriber_task
    _subscriber_task = asyncio.create_task(_redis_subscriber())
    yield
    if _subscriber_task:
        _subscriber_task.cancel()


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="FloodWatch IoT API",
    version="2.0.0",
    description="REST + SSE API for FloodWatch flood sensor network.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ── Helpers ───────────────────────────────────────────────────────────────────

_DATASET_DESC = (
    "real (default) — live sensor data only  |  "
    "sample — pre-generated demo data only  |  "
    "all — both together"
)


def _dataset_filter(dataset: str) -> dict:
    """
    Return a MongoDB filter fragment based on the requested dataset.
    Real documents never have is_sample set; sample documents always have is_sample: true.
    """
    if dataset == "real":
        return {"is_sample": {"$ne": True}}
    if dataset == "sample":
        return {"is_sample": True}
    if dataset == "all":
        return {}
    raise HTTPException(400, f"Invalid dataset '{dataset}'. Use: real | sample | all")


def _clean(value):
    """Recursively strip _id, convert datetimes to ISO strings, ObjectIds to str."""
    if isinstance(value, dict):
        return {k: _clean(v) for k, v in value.items() if k != "_id"}
    if isinstance(value, list):
        return [_clean(v) for v in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, ObjectId):
        return str(value)
    return value


def _period_range(
    period: str | None, from_: str | None, to: str | None
) -> tuple[datetime | None, datetime | None]:
    """
    Resolve a (start, end) UTC datetime pair from a named shorthand or explicit strings.
    Returns (None, None) if no filter is requested.
      today — midnight UTC today → now
      week  — 7 days ago → now
      month — 30 days ago → now
    """
    if period:
        now   = datetime.now(timezone.utc)
        delta = {"today": timedelta(days=1), "week": timedelta(weeks=1), "month": timedelta(days=30)}
        if period not in delta:
            raise HTTPException(400, f"Invalid period '{period}'. Use: today | week | month")
        start = (now.replace(hour=0, minute=0, second=0, microsecond=0)
                 if period == "today" else now - delta[period])
        return start, now
    if from_ or to:
        return (
            _parse_dt(from_, "from") if from_ else None,
            _parse_dt(to,    "to")   if to    else None,
        )
    return None, None


def _parse_dt(s: str, param: str) -> datetime:
    """Parse an ISO 8601 datetime string into a timezone-aware UTC datetime."""
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        raise HTTPException(
            400,
            f"Invalid datetime for '{param}': '{s}'. Use ISO 8601, e.g. 2026-05-14T06:00:00Z",
        )


_PERIOD_DESC = "Shorthand: today | week | month. Overrides from/to if both supplied."


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/", tags=["health"])
def health():
    return {"status": "ok", "service": "floodwatch-api", "version": "2.0.0"}


# ── Global stats ──────────────────────────────────────────────────────────────

@app.get("/api/v1/stats", tags=["stats"])
def get_stats(
    dataset: str = Query(default="real", description=_DATASET_DESC),
):
    """
    Aggregate counters — total nodes, villages, alerts by type, etc.
    For dataset=real the pre-computed global_stats document is used (O(1)).
    For dataset=sample or all, counts are computed live from the collections.
    """
    df = _dataset_filter(dataset)

    if dataset == "real":
        doc = col_global.find_one({"_id": "global"}) or {}
        doc.pop("_id", None)
    else:
        doc = {
            "total_villages":   col_villages.count_documents(df),
            "total_river_nodes": col_rivers.count_documents(df),
            "total_alerts":     col_alerts.count_documents(df),
        }

    doc["nodes_online"]  = col_rivers.count_documents({**df, "status": "online"})
    doc["nodes_offline"] = col_rivers.count_documents({**df, "status": "offline"})
    return _clean(doc)


# ── Villages ──────────────────────────────────────────────────────────────────

@app.get("/api/v1/villages", tags=["villages"])
def list_villages(
    dataset: str = Query(default="real", description=_DATASET_DESC),
):
    """All villages with summary fields. Topology and weather forecast excluded."""
    df   = _dataset_filter(dataset)
    docs = col_villages.find(df, {"topology": 0, "weather_forecast": 0})
    return [_clean(d) for d in docs]


@app.get("/api/v1/villages/{village_id}", tags=["villages"])
def get_village(
    village_id: str,
    dataset:    str = Query(default="real", description=_DATASET_DESC),
):
    """Single village with full topology, current weather, and 24h forecast."""
    df  = _dataset_filter(dataset)
    doc = col_villages.find_one({"village_id": village_id, **df})
    if not doc:
        raise HTTPException(404, f"Village '{village_id}' not found")
    return _clean(doc)


# ── Master nodes ──────────────────────────────────────────────────────────────

@app.get("/api/v1/masters", tags=["masters"])
def list_masters(
    village_id: str = Query(default=None, description="Filter by village"),
    dataset:    str = Query(default="real", description=_DATASET_DESC),
):
    """All master nodes with current status."""
    df    = _dataset_filter(dataset)
    query = {**df}
    if village_id:
        query["village_id"] = village_id
    return [_clean(d) for d in col_masters.find(query)]


# ── River nodes ───────────────────────────────────────────────────────────────

@app.get("/api/v1/nodes", tags=["nodes"])
def list_nodes(
    village_id: str = Query(default=None, description="Filter by village"),
    status:     str = Query(default=None, description="online | offline"),
    dataset:    str = Query(default="real", description=_DATASET_DESC),
):
    """All river nodes with current live state."""
    df    = _dataset_filter(dataset)
    query = {**df}
    if village_id:
        query["village_id"] = village_id
    if status:
        query["status"] = status
    return [_clean(d) for d in col_rivers.find(query)]


@app.get("/api/v1/nodes/{node_id}", tags=["nodes"])
def get_node(
    node_id: str,
    dataset: str = Query(default="real", description=_DATASET_DESC),
):
    """Single river node — current live state."""
    df  = _dataset_filter(dataset)
    doc = col_rivers.find_one({"node_id": node_id, **df})
    if not doc:
        raise HTTPException(404, f"Node '{node_id}' not found")
    return _clean(doc)


@app.get("/api/v1/nodes/{node_id}/readings", tags=["nodes"])
def node_readings(
    node_id:   str,
    page:      int  = Query(default=1,  ge=1),
    page_size: int  = Query(default=50, ge=1, le=200),
    gps_only:  bool = Query(default=False, description="Only return readings with a GPS fix"),
    from_:     str  = Query(default=None, alias="from", description="ISO 8601 start time (inclusive)"),
    to:        str  = Query(default=None,               description="ISO 8601 end time (inclusive)"),
    dataset:   str  = Query(default="real", description=_DATASET_DESC),
):
    """
    Paginated heartbeat history for a node (newest first).
    Optionally filter by time range with `from` and `to` (ISO 8601).
    """
    df    = _dataset_filter(dataset)
    query = {"node_id": node_id, **df}
    if gps_only:
        query["gps_fix"] = True
    if from_ or to:
        ts_filter: dict = {}
        if from_: ts_filter["$gte"] = _parse_dt(from_, "from")
        if to:    ts_filter["$lte"] = _parse_dt(to,    "to")
        query["timestamp"] = ts_filter
    skip   = (page - 1) * page_size
    cursor = col_heartbeats.find(query).sort("timestamp", DESCENDING).skip(skip).limit(page_size)
    data   = [_clean(d) for d in cursor]
    if not data and page == 1:
        if not col_rivers.find_one({"node_id": node_id, **df}, {"_id": 1}):
            raise HTTPException(404, f"Node '{node_id}' not found")
    return {"node_id": node_id, "page": page, "page_size": page_size, "data": data}


# ── Alerts ────────────────────────────────────────────────────────────────────

@app.get("/api/v1/alerts", tags=["alerts"])
def list_alerts(
    village_id: str = Query(default=None),
    node_id:    str = Query(default=None),
    alert_type: str = Query(default=None, description="flood | battery | gps_signal_lost | gps_restored | gps_moved"),
    from_:      str = Query(default=None, alias="from", description="ISO 8601 start time (inclusive)"),
    to:         str = Query(default=None,               description="ISO 8601 end time (inclusive)"),
    page:       int = Query(default=1,  ge=1),
    page_size:  int = Query(default=50, ge=1, le=200),
    dataset:    str = Query(default="real", description=_DATASET_DESC),
):
    """
    Paginated alert history. Newest first.
    Optionally filter by time range with `from` and `to` (ISO 8601).
    gps_moved alerts include dist_m, lat/lng (current), home_lat/home_lng (install position).
    """
    df    = _dataset_filter(dataset)
    query = {**df}
    if village_id:  query["village_id"]  = village_id
    if node_id:     query["node_id"]     = node_id
    if alert_type:  query["alert_type"]  = alert_type
    if from_ or to:
        ts_filter: dict = {}
        if from_: ts_filter["$gte"] = _parse_dt(from_, "from")
        if to:    ts_filter["$lte"] = _parse_dt(to,    "to")
        query["timestamp"] = ts_filter
    skip   = (page - 1) * page_size
    cursor = col_alerts.find(query).sort("timestamp", DESCENDING).skip(skip).limit(page_size)
    return {"page": page, "page_size": page_size, "data": [_clean(d) for d in cursor]}


# ── Weather history ───────────────────────────────────────────────────────────

@app.get("/api/v1/weather/{village_id}/at", tags=["weather"])
def get_weather_at(
    village_id: str,
    t:       str = Query(..., description="ISO 8601 datetime — returns the weather record valid at that moment"),
    dataset: str = Query(default="real", description=_DATASET_DESC),
):
    """
    Returns the single weather record valid at time `t` for a village.
    Uses last-known-value semantics: finds the most recent record with timestamp <= t.
    Useful for correlating historical sensor readings or alerts with weather conditions.
    """
    df  = _dataset_filter(dataset)
    at  = _parse_dt(t, "t")
    doc = col_weather.find_one(
        {"village_id": village_id, "timestamp": {"$lte": at}, **df},
        sort=[("timestamp", DESCENDING)],
    )
    if not doc:
        raise HTTPException(404, f"No weather record found for village '{village_id}' at or before {t}")
    return _clean(doc)


@app.get("/api/v1/weather/{village_id}", tags=["weather"])
def get_weather_history(
    village_id: str,
    from_:      str = Query(default=None, alias="from", description="ISO 8601 start time (inclusive)"),
    to:         str = Query(default=None,               description="ISO 8601 end time (inclusive)"),
    page:       int = Query(default=1,  ge=1),
    page_size:  int = Query(default=48, ge=1, le=200),
    dataset:    str = Query(default="real", description=_DATASET_DESC),
):
    """
    Paginated weather history for a village (newest first).
    Only records where at least one field changed from the previous poll are stored —
    the last record before any gap represents conditions during that gap.
    Optionally filter by time range with `from` and `to` (ISO 8601).
    Use villages/{village_id} for the latest snapshot + 24h forecast.
    """
    df    = _dataset_filter(dataset)
    query = {"village_id": village_id, **df}
    if from_ or to:
        ts_filter: dict = {}
        if from_: ts_filter["$gte"] = _parse_dt(from_, "from")
        if to:    ts_filter["$lte"] = _parse_dt(to,    "to")
        query["timestamp"] = ts_filter
    skip   = (page - 1) * page_size
    cursor = col_weather.find(query).sort("timestamp", DESCENDING).skip(skip).limit(page_size)
    data   = [_clean(d) for d in cursor]
    if not data and page == 1:
        raise HTTPException(404, f"No weather history for village '{village_id}'")
    return {"village_id": village_id, "page": page, "page_size": page_size, "data": data}


# ── Summaries ─────────────────────────────────────────────────────────────────

@app.get("/api/v1/nodes/{node_id}/summary", tags=["summary"])
def node_summary(
    node_id: str,
    period:  str = Query(default=None, description=_PERIOD_DESC),
    from_:   str = Query(default=None, alias="from", description="ISO 8601 start time"),
    to:      str = Query(default=None,               description="ISO 8601 end time"),
    dataset: str = Query(default="real", description=_DATASET_DESC),
):
    """
    Aggregate summary for a single river node over a time period.
    Returns avg/min/max water level, avg battery voltage, reading count, and alert breakdown.
    Use ?period=today|week|month or explicit ?from=...&to=... for custom ranges.
    If no period is given, summarises all-time data.
    """
    df = _dataset_filter(dataset)
    if not col_rivers.find_one({"node_id": node_id, **df}, {"_id": 1}):
        raise HTTPException(404, f"Node '{node_id}' not found")

    start, end = _period_range(period, from_, to)
    hb_match: dict = {"node_id": node_id, **df}
    if start or end:
        ts: dict = {}
        if start: ts["$gte"] = start
        if end:   ts["$lte"] = end
        hb_match["timestamp"] = ts

    pipeline = [
        {"$match": hb_match},
        {"$group": {
            "_id":             None,
            "reading_count":   {"$sum": 1},
            "avg_water_level": {"$avg": "$water_level"},
            "min_water_level": {"$min": "$water_level"},
            "max_water_level": {"$max": "$water_level"},
            "avg_battery_v":   {"$avg": "$battery_voltage"},
            "min_battery_v":   {"$min": "$battery_voltage"},
            "first_reading":   {"$min": "$timestamp"},
            "last_reading":    {"$max": "$timestamp"},
        }},
    ]
    result = next(col_heartbeats.aggregate(pipeline), {})
    result.pop("_id", None)

    alt_match: dict = {"node_id": node_id, **df}
    if "timestamp" in hb_match:
        alt_match["timestamp"] = hb_match["timestamp"]
    alert_pipeline = [
        {"$match": alt_match},
        {"$group": {"_id": "$alert_type", "count": {"$sum": 1}}},
    ]
    alerts_by_type = {r["_id"]: r["count"] for r in col_alerts.aggregate(alert_pipeline)}

    return {
        "node_id":        node_id,
        "period":         period or "all_time",
        "from":           start.isoformat() if start else None,
        "to":             end.isoformat()   if end   else None,
        "dataset":        dataset,
        **_clean(result),
        "total_alerts":   sum(alerts_by_type.values()),
        "alerts_by_type": alerts_by_type,
    }


@app.get("/api/v1/villages/{village_id}/summary", tags=["summary"])
def village_summary(
    village_id: str,
    period:     str = Query(default=None, description=_PERIOD_DESC),
    from_:      str = Query(default=None, alias="from", description="ISO 8601 start time"),
    to:         str = Query(default=None,               description="ISO 8601 end time"),
    dataset:    str = Query(default="real", description=_DATASET_DESC),
):
    """
    Aggregate summary for all nodes in a village over a time period.
    Returns per-node and village-wide avg/min/max water level, battery, reading count,
    and alert breakdown by type.
    """
    df = _dataset_filter(dataset)
    if not col_villages.find_one({"village_id": village_id, **df}, {"_id": 1}):
        raise HTTPException(404, f"Village '{village_id}' not found")

    start, end = _period_range(period, from_, to)
    hb_match: dict = {"village_id": village_id, **df}
    if start or end:
        ts: dict = {}
        if start: ts["$gte"] = start
        if end:   ts["$lte"] = end
        hb_match["timestamp"] = ts

    pipeline = [
        {"$match": hb_match},
        {"$group": {
            "_id":             "$node_id",
            "reading_count":   {"$sum": 1},
            "avg_water_level": {"$avg": "$water_level"},
            "min_water_level": {"$min": "$water_level"},
            "max_water_level": {"$max": "$water_level"},
            "avg_battery_v":   {"$avg": "$battery_voltage"},
            "min_battery_v":   {"$min": "$battery_voltage"},
            "first_reading":   {"$min": "$timestamp"},
            "last_reading":    {"$max": "$timestamp"},
        }},
        {"$sort": {"_id": 1}},
    ]
    nodes          = []
    total_readings = 0
    for r in col_heartbeats.aggregate(pipeline):
        node_id = r.pop("_id")
        total_readings += r.get("reading_count", 0)
        nodes.append({"node_id": node_id, **_clean(r)})

    alt_match: dict = {"village_id": village_id, **df}
    if "timestamp" in hb_match:
        alt_match["timestamp"] = hb_match["timestamp"]
    alert_pipeline = [
        {"$match": alt_match},
        {"$group": {"_id": "$alert_type", "count": {"$sum": 1}}},
    ]
    alerts_by_type = {r["_id"]: r["count"] for r in col_alerts.aggregate(alert_pipeline)}

    return {
        "village_id":     village_id,
        "period":         period or "all_time",
        "from":           start.isoformat() if start else None,
        "to":             end.isoformat()   if end   else None,
        "dataset":        dataset,
        "total_readings": total_readings,
        "total_alerts":   sum(alerts_by_type.values()),
        "alerts_by_type": alerts_by_type,
        "nodes":          nodes,
    }


@app.get("/api/v1/stats/summary", tags=["summary"])
def global_summary(
    period:  str = Query(default=None, description=_PERIOD_DESC),
    from_:   str = Query(default=None, alias="from", description="ISO 8601 start time"),
    to:      str = Query(default=None,               description="ISO 8601 end time"),
    dataset: str = Query(default="real", description=_DATASET_DESC),
):
    """
    Global aggregate summary across all villages and nodes.
    Returns total readings, alert breakdown by type, most active nodes and villages,
    and peak water level recorded.
    """
    df         = _dataset_filter(dataset)
    start, end = _period_range(period, from_, to)
    ts: dict   = {}
    if start: ts["$gte"] = start
    if end:   ts["$lte"] = end
    hb_match  = {**df, "timestamp": ts} if ts else {**df}
    alt_match = {**df, "timestamp": ts} if ts else {**df}

    hb_pipeline = [
        {"$match": hb_match},
        {"$group": {
            "_id":              None,
            "total_readings":   {"$sum": 1},
            "peak_water_level": {"$max": "$water_level"},
        }},
    ]
    hb_result = next(col_heartbeats.aggregate(hb_pipeline), {})
    hb_result.pop("_id", None)

    alert_pipeline = [
        {"$match": alt_match},
        {"$group": {"_id": "$alert_type", "count": {"$sum": 1}}},
    ]
    alerts_by_type = {r["_id"]: r["count"] for r in col_alerts.aggregate(alert_pipeline)}

    top_nodes_pipeline = [
        {"$match": hb_match},
        {"$group": {"_id": "$node_id", "readings": {"$sum": 1}}},
        {"$sort": {"readings": -1}},
        {"$limit": 5},
    ]
    top_nodes = [{"node_id": r["_id"], "readings": r["readings"]}
                 for r in col_heartbeats.aggregate(top_nodes_pipeline)]

    top_villages_pipeline = [
        {"$match": alt_match},
        {"$group": {"_id": "$village_id", "alerts": {"$sum": 1}}},
        {"$sort": {"alerts": -1}},
        {"$limit": 5},
    ]
    top_villages = [{"village_id": r["_id"], "alerts": r["alerts"]}
                    for r in col_alerts.aggregate(top_villages_pipeline)]

    return {
        "period":               period or "all_time",
        "from":                 start.isoformat() if start else None,
        "to":                   end.isoformat()   if end   else None,
        "dataset":              dataset,
        **_clean(hb_result),
        "total_alerts":         sum(alerts_by_type.values()),
        "alerts_by_type":       alerts_by_type,
        "top_active_nodes":     top_nodes,
        "top_alerted_villages": top_villages,
    }


# ── Event log ─────────────────────────────────────────────────────────────────

@app.get("/api/v1/events/history", tags=["events"])
def event_history(
    event_type: str = Query(default=None, description="node_online | node_offline | master_online | master_offline | announce"),
    node_id:    str = Query(default=None),
    village_id: str = Query(default=None),
    page:       int = Query(default=1,  ge=1),
    page_size:  int = Query(default=50, ge=1, le=200),
    dataset:    str = Query(default="real", description=_DATASET_DESC),
):
    """Paginated log of online/offline and announce events (30-day TTL)."""
    df    = _dataset_filter(dataset)
    query = {**df}
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
    types:   str = Query(
        default="heartbeat,flood_level,alert,node_online,node_offline,weather_update",
        description="Comma-separated event types, or 'all'.",
    ),
    dataset: str = Query(default="real", description=_DATASET_DESC),
):
    """
    Server-Sent Events stream. Zero-latency fan-out from Redis Pub/Sub.
    Use ?dataset=sample to stream simulation events, or ?dataset=all for both.

    ```js
    const es = new EventSource("/api/v1/events/stream");
    es.addEventListener("alert", e => console.log(JSON.parse(e.data)));
    es.addEventListener("flood_level", e => console.log(JSON.parse(e.data)));
    ```

    Available types:
      heartbeat, flood_level, alert, node_online, node_offline,
      master_online, master_offline, node_announce, weather_update
    """
    # Validate dataset early so the error surfaces before the stream opens
    _dataset_filter(dataset)

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
                    is_sample  = event.get("is_sample", False)

                    # Dataset filter
                    if dataset == "real"   and is_sample:
                        continue
                    if dataset == "sample" and not is_sample:
                        continue

                    if wanted and event_type not in wanted:
                        continue
                    yield {"event": event_type, "data": raw}
                except asyncio.TimeoutError:
                    yield {"comment": "keepalive"}
        finally:
            _sse_clients.discard(q)

    return EventSourceResponse(generator())
