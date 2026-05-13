#!/usr/bin/env python3
"""
FloodWatch MQTT Parser v2

Topic → Handler:
  floodwatch/+/master/status   → handle_master_status
  floodwatch/+/sensor/+        → handle_sensor
  floodwatch/+/alert/+         → handle_alert
  floodwatch/+/announce/+      → handle_announce
  floodwatch/+/nodes/+/status  → handle_node_status
  floodwatch/+/topology        → handle_topology

Collections written:
  global_stats    — one document, aggregate counters across everything
  villages        — one per village, includes derived GPS and per-village counts
  master_nodes    — one per master node
  river_nodes     — one per river node, current live state
  sensor_readings — time-series heartbeats (TTL 30 days)
  alerts          — deduplicated alert events (TTL 90 days)
  events          — online/offline/announce log (TTL 30 days)
  failed_messages — malformed or unroutable MQTT messages
"""

import json
import logging
import os
import queue
import threading
import time
from collections import OrderedDict
from datetime import datetime, timezone, timedelta

import paho.mqtt.client as mqtt
import redis
from dotenv import load_dotenv
from pymongo import MongoClient, DESCENDING, ASCENDING, UpdateOne

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("parser")

# ── Config ────────────────────────────────────────────────────────────────────

MQTT_BROKER          = os.getenv("MQTT_BROKER", "suts-fyp-floodwatch-mqtt.fly.dev")
MQTT_PORT            = int(os.getenv("MQTT_PORT", 1883))
MONGO_URI            = os.getenv("MONGO_URI")
MONGO_DB             = os.getenv("MONGO_DB", "flood_monitor")
REDIS_URL            = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_CHANNEL        = "floodwatch:events"
NODE_OFFLINE_TIMEOUT   = int(os.getenv("NODE_OFFLINE_TIMEOUT",   60))    # seconds
ALERT_DEDUP_WINDOW     = int(os.getenv("ALERT_DEDUP_WINDOW",     60))    # seconds
ALERT_DEDUP_MAX        = int(os.getenv("ALERT_DEDUP_MAX",        50000)) # max entries in dedup cache
MQTT_SHARE_GROUP       = os.getenv("MQTT_SHARE_GROUP", "parsers")        # shared subscription group

# Shared subscriptions: $share/{group}/{filter}
# Multiple parser instances with the same group receive each message exactly once,
# round-robin — horizontal scaling with no duplicate processing.
_S = f"$share/{MQTT_SHARE_GROUP}"
TOPICS = [
    (f"{_S}/floodwatch/+/master/status",  1),
    (f"{_S}/floodwatch/+/sensor/+",       1),
    (f"{_S}/floodwatch/+/alert/+",        1),
    (f"{_S}/floodwatch/+/announce/+",     1),
    (f"{_S}/floodwatch/+/nodes/+/status", 1),
    (f"{_S}/floodwatch/+/topology",       0),
]

# ── MongoDB ───────────────────────────────────────────────────────────────────

mongo = MongoClient(MONGO_URI)
db    = mongo[MONGO_DB]

col_global   = db["global_stats"]
col_villages = db["villages"]
col_masters  = db["master_nodes"]
col_rivers   = db["river_nodes"]
col_readings = db["sensor_readings"]
col_alerts   = db["alerts"]
col_events   = db["events"]
col_failed   = db["failed_messages"]

def _setup_indexes():
    # global_stats: queried by _id only
    # villages
    col_villages.create_index("village_id", unique=True)
    # master_nodes
    col_masters.create_index("node_id", unique=True)
    # river_nodes
    col_rivers.create_index("node_id", unique=True)
    col_rivers.create_index([("village_id", ASCENDING), ("status", ASCENDING)])
    col_rivers.create_index([("village_id", ASCENDING), ("depth", ASCENDING)])
    # sensor_readings (TTL 30 days)
    col_readings.create_index("timestamp", expireAfterSeconds=30 * 24 * 3600)
    col_readings.create_index([("node_id", ASCENDING), ("timestamp", DESCENDING)])
    col_readings.create_index([("village_id", ASCENDING), ("timestamp", DESCENDING)])
    # alerts (TTL 90 days)
    col_alerts.create_index("timestamp", expireAfterSeconds=90 * 24 * 3600)
    col_alerts.create_index([("node_id", ASCENDING), ("timestamp", DESCENDING)])
    col_alerts.create_index([("village_id", ASCENDING), ("alert_type", ASCENDING), ("timestamp", DESCENDING)])
    # events (TTL 30 days)
    col_events.create_index("timestamp", expireAfterSeconds=30 * 24 * 3600)
    col_events.create_index([("village_id", ASCENDING), ("timestamp", DESCENDING)])
    col_events.create_index([("node_id", ASCENDING), ("timestamp", DESCENDING)])
    col_events.create_index([("event_type", ASCENDING), ("timestamp", DESCENDING)])
    # failed_messages
    col_failed.create_index("timestamp")
    log.info("Indexes verified")

# ── Redis ─────────────────────────────────────────────────────────────────────

_redis = redis.from_url(REDIS_URL, decode_responses=True)

def publish_redis(event_type: str, data: dict):
    data["type"] = event_type
    _redis.publish(REDIS_CHANNEL, json.dumps(data))
    log.debug(f"  → Redis [{event_type}]")

# ── In-memory state ───────────────────────────────────────────────────────────

# last known water level per node — seeded from DB on startup
_last_water_level: dict[str, int] = {}

# alert dedup: (node_id, alert_type) → last published datetime
# Bounded OrderedDict — oldest entries evicted when ALERT_DEDUP_MAX is reached.
# At country scale (100k nodes × 5 alert types = 500k potential keys) this caps
# memory at roughly ALERT_DEDUP_MAX × ~120 bytes ≈ 6 MB at the default 50,000.
_alert_last_seen: OrderedDict[tuple, datetime] = OrderedDict()

# ── Helpers ───────────────────────────────────────────────────────────────────

def compute_water_level(float_bits: int) -> int:
    if float_bits & 0x04: return 3
    if float_bits & 0x02: return 2
    if float_bits & 0x01: return 1
    return 0

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _update_village_gps(village_id: str):
    """
    Recalculate village GPS from online river nodes that have a GPS fix.
    Uses nodes at the minimum depth (closest to master) to represent the village.
    If only one node, that node represents the village.
    """
    candidates = list(col_rivers.find(
        {"village_id": village_id, "gps_fix": True, "status": "online"},
        {"node_id": 1, "depth": 1, "lat": 1, "lng": 1}
    ))
    if not candidates:
        return
    min_depth = min(n["depth"] for n in candidates)
    closest   = [n for n in candidates if n["depth"] == min_depth]
    avg_lat   = sum(n["lat"] for n in closest) / len(closest)
    avg_lng   = sum(n["lng"] for n in closest) / len(closest)
    col_villages.update_one(
        {"village_id": village_id},
        {"$set": {
            "lat":              avg_lat,
            "lng":              avg_lng,
            "gps_source_nodes": [n["node_id"] for n in closest],
        }}
    )

def _inc_global(fields: dict, now: datetime):
    """Atomically increment global_stats counters. Creates the doc if missing."""
    col_global.update_one(
        {"_id": "global"},
        {"$inc": fields, "$set": {"last_updated": now}},
        upsert=True
    )

def _alert_is_duplicate(node_id: str, alert_type: str) -> bool:
    """
    Returns True if the same alert from this node was published within ALERT_DEDUP_WINDOW.
    Uses a bounded OrderedDict (LRU-style): when ALERT_DEDUP_MAX entries are reached,
    the oldest entry is evicted. Evicted entries are forgotten — a very old node that
    resurfaces will get one fresh alert publication, which is correct behaviour.
    """
    key = (node_id, alert_type)
    now = now_utc()

    if key in _alert_last_seen:
        last = _alert_last_seen[key]
        _alert_last_seen.move_to_end(key)   # refresh recency
        if (now - last).total_seconds() < ALERT_DEDUP_WINDOW:
            return True

    # Record this publication (insert or update)
    _alert_last_seen[key] = now
    _alert_last_seen.move_to_end(key)

    # Evict oldest entry if over capacity
    while len(_alert_last_seen) > ALERT_DEDUP_MAX:
        _alert_last_seen.popitem(last=False)

    return False

def _seed_water_levels():
    """Seed last_water_level from the most recent sensor reading per node."""
    pipeline = [
        {"$sort": {"timestamp": -1}},
        {"$group": {"_id": "$node_id", "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}},
    ]
    for doc in col_readings.aggregate(pipeline):
        wl = doc.get("water_level")
        if wl is not None:
            _last_water_level[doc["node_id"]] = wl
    log.info(f"Seeded water levels: {_last_water_level}")

# ── Handlers ─────────────────────────────────────────────────────────────────

def handle_sensor(village: str, node_id: str, payload: dict, now: datetime):
    """
    floodwatch/{village}/sensor/{node_id}
    Heartbeat from river node — update live state and write time-series.
    """
    bat        = payload.get("bat", 0.0)
    float_bits = payload.get("float_bits", 0)
    water_lvl  = compute_water_level(float_bits)
    lat        = payload.get("lat", 0.0)
    lng        = payload.get("lng", 0.0)
    gps_fix    = bool(payload.get("gps_fix", False))
    depth      = payload.get("depth", 0)
    parent     = payload.get("parent", "")
    rssi       = payload.get("rssi")
    snr        = payload.get("snr")

    # Update river_nodes live state
    river_update = {
        "village_id":       village,
        "parent_id":        parent,
        "depth":            depth,
        "status":           "online",
        "last_seen":        now,
        "battery_voltage":  round(bat, 2),
        "float_bits":       float_bits,
        "water_level":      water_lvl,
        "gps_fix":          gps_fix,
        "rssi":             rssi,
        "snr":              snr,
    }
    if gps_fix:
        river_update["lat"] = lat
        river_update["lng"] = lng

    col_rivers.update_one(
        {"node_id": node_id},
        {
            "$set":         river_update,
            "$inc":         {"total_messages": 1},
            "$setOnInsert": {"first_seen": now},
        },
        upsert=True
    )

    # Insert time-series reading
    col_readings.insert_one({
        "node_id":         node_id,
        "village_id":      village,
        "timestamp":       now,
        "battery_voltage": round(bat, 2),
        "float_bits":      float_bits,
        "water_level":     water_lvl,
        "lat":             lat if gps_fix else None,
        "lng":             lng if gps_fix else None,
        "gps_fix":         gps_fix,
        "depth":           depth,
        "parent_id":       parent,
        "rssi":            rssi,
        "snr":             snr,
    })

    # Update village: add node, update last_seen, update GPS
    col_villages.update_one(
        {"village_id": village},
        {
            "$set":      {"last_seen": now},
            "$addToSet": {"node_ids": node_id},
        },
        upsert=True
    )
    if gps_fix:
        _update_village_gps(village)

    # Global counter
    _inc_global({"total_messages_received": 1}, now)

    ts = now.isoformat()

    # SSE: heartbeat every reading
    publish_redis("heartbeat", {
        "node_id":     node_id,
        "village_id":  village,
        "water_level": water_lvl,
        "float_bits":  float_bits,
        "bat":         round(bat, 2),
        "gps_fix":     gps_fix,
        "lat":         lat if gps_fix else None,
        "lng":         lng if gps_fix else None,
        "depth":       depth,
        "parent":      parent,
        "rssi":        rssi,
        "snr":         snr,
        "timestamp":   ts,
    })

    # SSE: flood_level when water level changes
    prev = _last_water_level.get(node_id)
    if prev is None or water_lvl != prev:
        publish_redis("flood_level", {
            "node_id":           node_id,
            "village_id":        village,
            "water_level":       water_lvl,
            "water_level_prev":  prev,
            "float_bits":        float_bits,
            "lat":               lat if gps_fix else None,
            "lng":               lng if gps_fix else None,
            "gps_fix":           gps_fix,
            "timestamp":         ts,
        })
    _last_water_level[node_id] = water_lvl

    log.info(f"sensor  {node_id}  level={water_lvl}  bat={bat:.2f}V  gps={'✓' if gps_fix else '✗'}  rssi={rssi}")


def handle_alert(village: str, node_id: str, payload: dict, now: datetime):
    """
    floodwatch/{village}/alert/{node_id}
    Alert from river node. Firmware retries until ACKed — deduplicate per
    (node_id, alert_type) within ALERT_DEDUP_WINDOW seconds.
    """
    alert_type = payload.get("type", "unknown")
    level      = payload.get("level", 0)
    float_bits = payload.get("float_bits", 0)
    water_lvl  = compute_water_level(float_bits)
    bat        = payload.get("bat", 0.0)
    lat        = payload.get("lat", 0.0)
    lng        = payload.get("lng", 0.0)
    gps_fix    = bool(payload.get("gps_fix", False))
    rssi       = payload.get("rssi")
    snr        = payload.get("snr")

    if _alert_is_duplicate(node_id, alert_type):
        log.debug(f"alert  {node_id}  type={alert_type}  (dedup — skipped)")
        return

    ts = now.isoformat()

    # Insert alert record
    col_alerts.insert_one({
        "node_id":         node_id,
        "village_id":      village,
        "timestamp":       now,
        "alert_type":      alert_type,
        "level":           level if alert_type == "flood" else None,
        "float_bits":      float_bits if alert_type == "flood" else None,
        "water_level":     water_lvl  if alert_type == "flood" else None,
        "battery_voltage": round(bat, 2),
        "lat":             lat if (lat or lng) else None,
        "lng":             lng if (lat or lng) else None,
        "gps_fix":         gps_fix,
        "rssi":            rssi,
        "snr":             snr,
    })

    # Update river_nodes: last_alert + per-type counter
    col_rivers.update_one(
        {"node_id": node_id},
        {
            "$set": {
                "last_seen":  now,
                "status":     "online",
                "last_alert": {"type": alert_type, "level": level, "timestamp": now},
            },
            "$inc":         {f"alert_counts.{alert_type}": 1},
            "$setOnInsert": {"first_seen": now, "village_id": village},
        },
        upsert=True
    )

    # Village + global alert counters
    col_villages.update_one(
        {"village_id": village},
        {
            "$set":      {"last_seen": now},
            "$inc":      {f"alerts_by_type.{alert_type}": 1, "total_alerts": 1},
            "$addToSet": {"node_ids": node_id},
        },
        upsert=True
    )
    _inc_global({f"alerts_by_type.{alert_type}": 1, "total_alerts": 1}, now)

    # SSE publish
    publish_redis("alert", {
        "node_id":    node_id,
        "village_id": village,
        "alert_type": alert_type,
        "level":      level,
        "float_bits": float_bits,
        "bat":        round(bat, 2),
        "lat":        lat,
        "lng":        lng,
        "gps_fix":    gps_fix,
        "rssi":       rssi,
        "timestamp":  ts,
    })

    log.info(f"alert   {node_id}  type={alert_type}  level={level}  bat={bat:.2f}V")


def handle_announce(village: str, node_id: str, payload: dict, now: datetime):
    """
    floodwatch/{village}/announce/{node_id}
    Sent after GPS calibration completes. lat/lng here are the calibrated
    install position — store as install_lat/lng on the river node.
    """
    lat    = payload.get("lat")
    lng    = payload.get("lng")
    depth  = payload.get("depth", 0)
    parent = payload.get("parent", "")
    rssi   = payload.get("rssi")
    snr    = payload.get("snr")

    col_rivers.update_one(
        {"node_id": node_id},
        {
            "$set": {
                "village_id":  village,
                "parent_id":   parent,
                "depth":       depth,
                "status":      "online",
                "last_seen":   now,
                "install_lat": lat,
                "install_lng": lng,
                "rssi":        rssi,
                "snr":         snr,
            },
            "$setOnInsert": {"first_seen": now},
        },
        upsert=True
    )

    col_villages.update_one(
        {"village_id": village},
        {"$set": {"last_seen": now}, "$addToSet": {"node_ids": node_id}},
        upsert=True
    )

    # Register new river node in global counter (only on first announce)
    existing = col_rivers.find_one({"node_id": node_id}, {"first_seen": 1})
    if not existing:
        _inc_global({"total_river_nodes": 1}, now)

    # Log event
    col_events.insert_one({
        "event_type": "announce",
        "node_id":    node_id,
        "village_id": village,
        "timestamp":  now,
        "data": {"depth": depth, "parent": parent, "lat": lat, "lng": lng, "rssi": rssi, "snr": snr},
    })

    publish_redis("node_announce", {
        "node_id":    node_id,
        "village_id": village,
        "depth":      depth,
        "parent":     parent,
        "lat":        lat,
        "lng":        lng,
        "timestamp":  now.isoformat(),
    })

    log.info(f"announce  {node_id}  depth={depth}  parent={parent}  pos=({lat},{lng})")


def handle_node_status(village: str, node_id: str, payload: dict, now: datetime):
    """
    floodwatch/{village}/nodes/{node_id}/status
    Online/offline events fired by the master node.
    """
    online     = bool(payload.get("online", False))
    new_status = "online" if online else "offline"
    event_type = "node_online" if online else "node_offline"

    # Read current status to detect actual transitions
    existing = col_rivers.find_one({"node_id": node_id}, {"status": 1, "village_id": 1})
    old_status = existing.get("status") if existing else None

    col_rivers.update_one(
        {"node_id": node_id},
        {
            "$set":         {"status": new_status, "last_seen": now, "village_id": village},
            "$setOnInsert": {"first_seen": now},
        },
        upsert=True
    )

    # Only adjust online/offline counters on a real transition
    if old_status != new_status:
        if online:
            _inc_global({"nodes_online": 1, "nodes_offline": -1}, now)
            col_villages.update_one(
                {"village_id": village},
                {"$inc": {"nodes_online": 1, "nodes_offline": -1}, "$set": {"last_seen": now}},
                upsert=True
            )
        else:
            _inc_global({"nodes_online": -1, "nodes_offline": 1}, now)
            col_villages.update_one(
                {"village_id": village},
                {"$inc": {"nodes_online": -1, "nodes_offline": 1}, "$set": {"last_seen": now}},
                upsert=True
            )

        col_events.insert_one({
            "event_type": event_type,
            "node_id":    node_id,
            "village_id": village,
            "timestamp":  now,
            "data":       {},
        })

        publish_redis(event_type, {
            "node_id":    node_id,
            "village_id": village,
            "timestamp":  now.isoformat(),
        })

    log.info(f"status  {node_id}  {new_status}" + (" (transition)" if old_status != new_status else ""))


def handle_master_status(village: str, payload: dict, now: datetime):
    """
    floodwatch/{village}/master/status
    Master online/offline — LWT fires on unexpected disconnect.
    """
    # Firmware may send {"online": true} or {"status": "online"}
    if "online" in payload:
        online = bool(payload["online"])
    else:
        online = payload.get("status", "offline") == "online"

    master_id  = payload.get("node_id", village)
    new_status = "online" if online else "offline"
    event_type = "master_online" if online else "master_offline"

    col_masters.update_one(
        {"node_id": master_id},
        {
            "$set":         {"village_id": village, "status": new_status, "last_seen": now},
            "$setOnInsert": {"first_seen": now},
        },
        upsert=True
    )

    existing_village = col_villages.find_one({"village_id": village})
    col_villages.update_one(
        {"village_id": village},
        {
            "$set":         {"master_id": master_id, "status": new_status, "last_seen": now},
            "$setOnInsert": {"first_seen": now, "node_ids": [], "topology": {}, "total_alerts": 0,
                             "alerts_by_type": {}, "nodes_online": 0, "nodes_offline": 0},
        },
        upsert=True
    )

    if not existing_village:
        _inc_global({"total_villages": 1, "total_master_nodes": 1}, now)
        log.info(f"NEW village registered: {village}")

    col_events.insert_one({
        "event_type": event_type,
        "node_id":    master_id,
        "village_id": village,
        "timestamp":  now,
        "data":       {},
    })

    publish_redis(event_type, {
        "village_id":     village,
        "master_node_id": master_id,
        "timestamp":      now.isoformat(),
    })

    log.info(f"master  {master_id}  {new_status}")


def handle_topology(village: str, payload: dict, now: datetime):
    """
    floodwatch/{village}/topology
    Full mesh tree published by master on demand or when a node announces.
    Update village topology and master_nodes.
    """
    # Derive master_id: the top-level key in the topology JSON
    master_id = next(iter(payload), village)

    col_villages.update_one(
        {"village_id": village},
        {"$set": {"topology": payload, "last_seen": now}},
        upsert=True
    )

    # Flatten the tree to count total registered nodes
    def _count_nodes(tree: dict) -> int:
        count = 0
        for children in tree.values():
            count += 1 + _count_nodes(children)
        return count

    total = _count_nodes(payload.get(master_id, {}))

    col_masters.update_one(
        {"node_id": master_id},
        {"$set": {"topology": payload, "total_nodes_registered": total, "last_seen": now}},
        upsert=True
    )

    log.info(f"topology  {village}  nodes={total}")


# ── Health checker ────────────────────────────────────────────────────────────

def _health_checker():
    """
    Background thread. Marks river nodes offline if last_seen exceeds
    NODE_OFFLINE_TIMEOUT. Runs every 30 seconds.
    This is a safety net — the master also sends node_status events.
    """
    while True:
        time.sleep(30)
        try:
            cutoff = now_utc() - timedelta(seconds=NODE_OFFLINE_TIMEOUT)
            gone = list(col_rivers.find(
                {"status": "online", "last_seen": {"$lt": cutoff}},
                {"node_id": 1, "village_id": 1}
            ))
            for node in gone:
                nid = node["node_id"]
                vid = node.get("village_id", "")
                _ts = now_utc()
                col_rivers.update_one({"_id": node["_id"]}, {"$set": {"status": "offline"}})
                col_villages.update_one(
                    {"village_id": vid},
                    {"$inc": {"nodes_online": -1, "nodes_offline": 1}}
                )
                _inc_global({"nodes_online": -1, "nodes_offline": 1}, _ts)
                col_events.insert_one({
                    "event_type": "node_offline",
                    "node_id":    nid,
                    "village_id": vid,
                    "timestamp":  _ts,
                    "data":       {"reason": "timeout"},
                })
                publish_redis("node_offline", {
                    "node_id":    nid,
                    "village_id": vid,
                    "timestamp":  _ts.isoformat(),
                })
                log.info(f"health  {nid}  → offline (timeout)")
        except Exception as e:
            log.error(f"Health checker error: {e}", exc_info=True)


# ── Worker / router ───────────────────────────────────────────────────────────

_msg_queue: queue.Queue = queue.Queue(maxsize=1024)


def _route(topic: str, raw: str):
    """Parse topic, decode JSON, dispatch to handler."""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as e:
        col_failed.insert_one({
            "topic":     topic,
            "raw":       raw,
            "reason":    f"json_parse_error: {e}",
            "timestamp": now_utc(),
        })
        _inc_global({"failed_messages": 1}, now_utc())
        log.warning(f"JSON parse error on {topic}: {e}")
        return

    # Broker delivers shared-subscription topics with the original filter path,
    # not the $share/group prefix, so parts are always: floodwatch/village/type/...
    parts = topic.split("/")
    # parts: floodwatch / {village} / {msg_type} [/ {node_id} [/ status]]
    if len(parts) < 3:
        log.warning(f"Unrecognised topic: {topic}")
        return

    village  = parts[1]
    msg_type = parts[2]
    now      = now_utc()

    try:
        if msg_type == "sensor" and len(parts) == 4:
            handle_sensor(village, parts[3], payload, now)

        elif msg_type == "alert" and len(parts) == 4:
            handle_alert(village, parts[3], payload, now)

        elif msg_type == "announce" and len(parts) == 4:
            handle_announce(village, parts[3], payload, now)

        elif msg_type == "nodes" and len(parts) == 5 and parts[4] == "status":
            handle_node_status(village, parts[3], payload, now)

        elif msg_type == "master" and len(parts) == 4 and parts[3] == "status":
            handle_master_status(village, payload, now)

        elif msg_type == "topology" and len(parts) == 3:
            handle_topology(village, payload, now)

        else:
            log.debug(f"Unhandled topic: {topic}")

    except Exception as e:
        log.error(f"Handler error [{topic}]: {e}", exc_info=True)
        col_failed.insert_one({
            "topic":     topic,
            "raw":       raw,
            "reason":    f"handler_error: {e}",
            "timestamp": now_utc(),
        })
        _inc_global({"failed_messages": 1}, now_utc())


def _worker():
    while True:
        item = _msg_queue.get()
        if item is None:
            break
        topic, raw = item
        try:
            _route(topic, raw)
        except Exception as e:
            log.error(f"Worker error: {e}", exc_info=True)
        finally:
            _msg_queue.task_done()


# ── MQTT callbacks ────────────────────────────────────────────────────────────

def on_connect(client, userdata, flags, rc, props=None):
    if rc == 0:
        for topic, qos in TOPICS:
            client.subscribe(topic, qos=qos)
        log.info(f"MQTT connected — {MQTT_BROKER}:{MQTT_PORT}")
        log.info(f"Subscribed to {len(TOPICS)} topic patterns")
    else:
        log.error(f"MQTT connect failed rc={rc}")


def on_message(client, userdata, msg):
    raw = msg.payload.decode("utf-8", errors="replace").strip()
    if not raw:
        return
    try:
        _msg_queue.put_nowait((msg.topic, raw))
    except queue.Full:
        log.error("Message queue full — dropping message")


def on_disconnect(client, userdata, rc, props=None):
    if rc != 0:
        log.warning(f"Unexpected disconnect rc={rc} — paho will reconnect")


# ── Startup ───────────────────────────────────────────────────────────────────

log.info(f"MongoDB: {MONGO_DB}")
_setup_indexes()
_seed_water_levels()

log.info(f"Redis: {REDIS_URL}")
log.info(f"Node offline timeout: {NODE_OFFLINE_TIMEOUT}s")
log.info(f"Alert dedup window: {ALERT_DEDUP_WINDOW}s")

threading.Thread(target=_health_checker, name="health-checker", daemon=True).start()
threading.Thread(target=_worker,         name="msg-worker",     daemon=True).start()

client = mqtt.Client(protocol=mqtt.MQTTv5)
client.on_connect    = on_connect
client.on_message    = on_message
client.on_disconnect = on_disconnect

log.info(f"Connecting to {MQTT_BROKER}:{MQTT_PORT} ...")
client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
client.loop_forever()
