#!/usr/bin/env python3
"""
FloodWatch MQTT Parser v2

Topic → Handler:
  floodwatch/+/master/status   → handle_master_status
  floodwatch/+/heartbeat/+     → handle_heartbeat
  floodwatch/+/alert/+         → handle_alert
  floodwatch/+/announce/+      → handle_announce
  floodwatch/+/nodes/+/status  → handle_node_status
  floodwatch/+/topology        → handle_topology

Collections written:
  global_stats    — one document, aggregate counters across everything
  villages        — one per village, includes derived GPS and per-village counts
  master_nodes    — one per master node
  river_nodes     — one per river node, current live state
  heartbeats      — time-series heartbeat data (TTL 30 days)
  alerts          — deduplicated alert events (TTL 90 days)
  events          — online/offline/announce log (TTL 30 days)
  failed_messages — malformed or unroutable MQTT messages (debug)
"""

import json
import logging
import os
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

import paho.mqtt.client as mqtt
import redis
from dotenv import load_dotenv
from pymongo import MongoClient, DESCENDING, ASCENDING

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
NODE_OFFLINE_TIMEOUT = int(os.getenv("NODE_OFFLINE_TIMEOUT",   60))    # seconds
ALERT_DEDUP_WINDOW   = int(os.getenv("ALERT_DEDUP_WINDOW",     60))    # seconds
ALERT_DEDUP_MAX      = int(os.getenv("ALERT_DEDUP_MAX",        50000)) # max entries in dedup cache
MQTT_SHARE_GROUP     = os.getenv("MQTT_SHARE_GROUP", "parsers")        # shared subscription group
WORKER_THREADS       = int(os.getenv("WORKER_THREADS", 4))             # parallel message processors

# Shared subscriptions: $share/{group}/{filter}
# Multiple parser instances with the same group receive each message exactly once,
# round-robin — horizontal scaling with no duplicate processing.
_S = f"$share/{MQTT_SHARE_GROUP}"
TOPICS = [
    (f"{_S}/floodwatch/+/master/status",   1),
    (f"{_S}/floodwatch/+/master/topology", 0),
    (f"{_S}/floodwatch/+/heartbeat/+",     1),
    (f"{_S}/floodwatch/+/alert/+",         1),
    (f"{_S}/floodwatch/+/announce/+",      1),
    (f"{_S}/floodwatch/+/nodes/+/status",  1),
    (f"{_S}/floodwatch/+/topology",        0),
]

SERVICE_STATUS_TOPIC = "floodwatch/system/listener/status"

# ── MongoDB ───────────────────────────────────────────────────────────────────

mongo = MongoClient(MONGO_URI)
db    = mongo[MONGO_DB]

col_global     = db["global_stats"]
col_villages   = db["villages"]
col_masters    = db["master_nodes"]
col_rivers     = db["river_nodes"]
col_heartbeats = db["heartbeats"]
col_alerts     = db["alerts"]
col_events     = db["events"]
col_failed     = db["failed_messages"]


def _setup_indexes():
    col_villages.create_index("village_id", unique=True)
    col_masters.create_index("node_id", unique=True)
    col_rivers.create_index("node_id", unique=True)
    col_rivers.create_index([("village_id", ASCENDING), ("status", ASCENDING)])
    col_rivers.create_index([("village_id", ASCENDING), ("depth", ASCENDING)])
    # heartbeats TTL 30 days
    col_heartbeats.create_index("timestamp", expireAfterSeconds=30 * 24 * 3600)
    col_heartbeats.create_index([("node_id", ASCENDING), ("timestamp", DESCENDING)])
    col_heartbeats.create_index([("village_id", ASCENDING), ("timestamp", DESCENDING)])
    # alerts TTL 90 days
    col_alerts.create_index("timestamp", expireAfterSeconds=90 * 24 * 3600)
    col_alerts.create_index([("node_id", ASCENDING), ("timestamp", DESCENDING)])
    col_alerts.create_index([("village_id", ASCENDING), ("alert_type", ASCENDING), ("timestamp", DESCENDING)])
    # events TTL 30 days
    col_events.create_index("timestamp", expireAfterSeconds=30 * 24 * 3600)
    col_events.create_index([("village_id", ASCENDING), ("timestamp", DESCENDING)])
    col_events.create_index([("node_id", ASCENDING), ("timestamp", DESCENDING)])
    col_events.create_index([("event_type", ASCENDING), ("timestamp", DESCENDING)])
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
_lock_water_level = threading.Lock()

# alert dedup: (node_id, alert_type) → last published datetime
# Bounded OrderedDict — oldest entries evicted when ALERT_DEDUP_MAX is reached.
# At country scale (100k nodes × 5 alert types = 500k potential keys) this caps
# memory at roughly ALERT_DEDUP_MAX × ~120 bytes ≈ 6 MB at the default 50,000.
_alert_last_seen: OrderedDict[tuple, datetime] = OrderedDict()
_lock_dedup = threading.Lock()

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
    Uses nodes at minimum depth (closest to master) to represent the village.
    """
    candidates = list(col_rivers.find(
        {"village_id": village_id, "gps_fix": True, "status": "online"},
        {"node_id": 1, "depth": 1, "lat": 1, "lng": 1}
    ))
    if not candidates:
        return
    min_depth = min(n.get("depth") or 99 for n in candidates)
    closest   = [n for n in candidates if n["depth"] == min_depth]
    avg_lat   = sum(n["lat"] for n in closest) / len(closest)
    avg_lng   = sum(n["lng"] for n in closest) / len(closest)
    col_villages.update_one(
        {"village_id": village_id},
        {"$set": {"lat": avg_lat, "lng": avg_lng}}
    )


def _inc_global(fields: dict, now: datetime):
    col_global.update_one(
        {"_id": "global"},
        {"$inc": fields, "$set": {"last_updated": now}},
        upsert=True
    )


def _alert_is_duplicate(node_id: str, alert_type: str) -> bool:
    """
    Returns True if the same alert from this node was published within ALERT_DEDUP_WINDOW.
    Thread-safe via _lock_dedup. Uses a bounded LRU OrderedDict.
    """
    key = (node_id, alert_type)
    now = now_utc()
    with _lock_dedup:
        if key in _alert_last_seen:
            last = _alert_last_seen[key]
            _alert_last_seen.move_to_end(key)
            if (now - last).total_seconds() < ALERT_DEDUP_WINDOW:
                return True
        _alert_last_seen[key] = now
        _alert_last_seen.move_to_end(key)
        while len(_alert_last_seen) > ALERT_DEDUP_MAX:
            _alert_last_seen.popitem(last=False)
    return False


def _seed_water_levels():
    """Seed _last_water_level from the most recent heartbeat per node."""
    pipeline = [
        {"$sort": {"timestamp": -1}},
        {"$group": {"_id": "$node_id", "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}},
    ]
    for doc in col_heartbeats.aggregate(pipeline):
        wl = doc.get("water_level")
        if wl is not None:
            _last_water_level[doc["node_id"]] = wl
    log.info(f"Seeded water levels: {_last_water_level}")


# ── Handlers ─────────────────────────────────────────────────────────────────

def handle_heartbeat(village: str, node_id: str, payload: dict, now: datetime):
    """
    floodwatch/{village}/heartbeat/{node_id}
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

    river_update = {
        "village_id":      village,
        "parent_id":       parent,
        "depth":           depth,
        "status":          "online",
        "last_seen":       now,
        "battery_voltage": round(bat, 2),
        "float_bits":      float_bits,
        "water_level":     water_lvl,
        "gps_fix":         gps_fix,
        "rssi":            rssi,
        "snr":             snr,
    }
    if gps_fix:
        river_update["lat"] = lat
        river_update["lng"] = lng

    result = col_rivers.update_one(
        {"node_id": node_id},
        {
            "$set":         river_update,
            "$setOnInsert": {"first_seen": now},
        },
        upsert=True
    )

    col_heartbeats.insert_one({
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

    # Update village last_seen; increment total_nodes only on first insert of this node
    village_update: dict = {"$set": {"last_seen": now}}
    if result.upserted_id:
        village_update["$inc"] = {"total_nodes": 1}
    col_villages.update_one({"village_id": village}, village_update, upsert=True)

    if gps_fix:
        _update_village_gps(village)

    _inc_global({"total_messages_received": 1}, now)

    ts = now.isoformat()

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

    # flood_level event on water level change (thread-safe read-modify)
    with _lock_water_level:
        prev = _last_water_level.get(node_id)
        level_changed = (prev is None or water_lvl != prev)
        if level_changed:
            _last_water_level[node_id] = water_lvl

    if level_changed:
        publish_redis("flood_level", {
            "node_id":          node_id,
            "village_id":       village,
            "water_level":      water_lvl,
            "water_level_prev": prev,
            "float_bits":       float_bits,
            "lat":              lat if gps_fix else None,
            "lng":              lng if gps_fix else None,
            "gps_fix":          gps_fix,
            "timestamp":        ts,
        })

    log.info(f"heartbeat  {node_id}  level={water_lvl}  bat={bat:.2f}V  gps={'✓' if gps_fix else '✗'}  rssi={rssi}")


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
    dist       = payload.get("dist")      # metres moved; only present for gps_moved
    lat        = payload.get("lat", 0.0)
    lng        = payload.get("lng", 0.0)
    home_lat   = payload.get("home_lat")  # install position; only present for gps_moved
    home_lng   = payload.get("home_lng")
    gps_fix    = bool(payload.get("gps_fix", False))
    rssi       = payload.get("rssi")
    snr        = payload.get("snr")

    if _alert_is_duplicate(node_id, alert_type):
        log.debug(f"alert  {node_id}  type={alert_type}  (dedup — skipped)")
        return

    ts = now.isoformat()

    # Snapshot village weather at alert time for historical correlation
    village_doc    = col_villages.find_one({"village_id": village}, {"weather": 1})
    weather_snapshot = village_doc.get("weather") if village_doc else None

    # Insert alert record; store its ObjectId on river_nodes as last_alert_id
    alert_result = col_alerts.insert_one({
        "node_id":         node_id,
        "village_id":      village,
        "timestamp":       now,
        "alert_type":      alert_type,
        "level":           level if alert_type == "flood" else None,
        "float_bits":      float_bits if alert_type == "flood" else None,
        "water_level":     water_lvl  if alert_type == "flood" else None,
        "battery_voltage": round(bat, 2),
        "dist_m":          dist if alert_type == "gps_moved" else None,
        "lat":             lat if (lat or lng) else None,
        "lng":             lng if (lat or lng) else None,
        "home_lat":        home_lat if alert_type == "gps_moved" else None,
        "home_lng":        home_lng if alert_type == "gps_moved" else None,
        "gps_fix":         gps_fix,
        "rssi":            rssi,
        "snr":             snr,
        "weather_at_alert": weather_snapshot,
    })

    col_rivers.update_one(
        {"node_id": node_id},
        {
            "$set": {
                "last_seen":     now,
                "status":        "online",
                "last_alert_id": alert_result.inserted_id,
            },
            "$inc":         {f"alert_counts.{alert_type}": 1},
            "$setOnInsert": {"first_seen": now, "village_id": village},
        },
        upsert=True
    )

    col_villages.update_one(
        {"village_id": village},
        {
            "$set": {"last_seen": now},
            "$inc": {f"alerts_by_type.{alert_type}": 1, "total_alerts": 1},
        },
        upsert=True
    )
    _inc_global({f"alerts_by_type.{alert_type}": 1, "total_alerts": 1}, now)

    publish_redis("alert", {
        "node_id":    node_id,
        "village_id": village,
        "alert_type": alert_type,
        "level":      level,
        "float_bits": float_bits,
        "bat":        round(bat, 2),
        "dist_m":     dist,
        "lat":        lat,
        "lng":        lng,
        "home_lat":   home_lat,
        "home_lng":   home_lng,
        "gps_fix":    gps_fix,
        "rssi":       rssi,
        "timestamp":  ts,
    })

    log.info(f"alert   {node_id}  type={alert_type}  level={level}  bat={bat:.2f}V")


def handle_announce(village: str, node_id: str, payload: dict, now: datetime):
    """
    floodwatch/{village}/announce/{node_id}
    Sent after GPS calibration completes. lat/lng are the calibrated install
    position — stored as install_lat/lng on the river node.
    """
    lat    = payload.get("lat")
    lng    = payload.get("lng")
    depth  = payload.get("depth", 0)
    parent = payload.get("parent", "")
    rssi   = payload.get("rssi")
    snr    = payload.get("snr")

    result = col_rivers.update_one(
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

    # Increment village total_nodes and global counter only on first insert
    village_update: dict = {"$set": {"last_seen": now}}
    if result.upserted_id:
        village_update["$inc"] = {"total_nodes": 1}
        _inc_global({"total_river_nodes": 1}, now)
    col_villages.update_one({"village_id": village}, village_update, upsert=True)

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

    existing   = col_rivers.find_one({"node_id": node_id}, {"status": 1, "village_id": 1})
    old_status = existing.get("status") if existing else None

    col_rivers.update_one(
        {"node_id": node_id},
        {
            "$set":         {"status": new_status, "last_seen": now, "village_id": village},
            "$setOnInsert": {"first_seen": now},
        },
        upsert=True
    )

    if old_status != new_status:
        delta = (1, -1) if online else (-1, 1)
        _inc_global({"nodes_online": delta[0], "nodes_offline": delta[1]}, now)
        col_villages.update_one(
            {"village_id": village},
            {"$inc": {"nodes_online": delta[0], "nodes_offline": delta[1]}, "$set": {"last_seen": now}},
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
            "$setOnInsert": {
                "first_seen": now, "topology": {}, "total_alerts": 0,
                "alerts_by_type": {}, "nodes_online": 0, "nodes_offline": 0, "total_nodes": 0,
            },
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
    Full mesh tree published by master. Update village topology and node count.
    total_nodes in villages is authoritative; master_nodes no longer stores topology.
    """
    master_id = next(iter(payload), village)

    def _count_nodes(tree: dict) -> int:
        count = 0
        for children in tree.values():
            count += 1 + _count_nodes(children)
        return count

    total = _count_nodes(payload.get(master_id, {}))

    col_villages.update_one(
        {"village_id": village},
        {"$set": {"topology": payload, "last_seen": now, "total_nodes": total}},
        upsert=True
    )

    col_masters.update_one(
        {"node_id": master_id},
        {"$set": {"last_seen": now}},
        upsert=True
    )

    log.info(f"topology  {village}  nodes={total}")


def handle_full_topology(village: str, payload: dict, now: datetime):
    """
    floodwatch/{village}/master/topology  (flat array format)
    Published by master periodically and on every topology change.
    Bulk-upserts each node's parent/depth/online state into river_nodes.
    """
    nodes = payload.get("nodes", [])
    if not isinstance(nodes, list) or not nodes:
        return

    for n in nodes:
        node_id = n.get("node_id", "")
        if not node_id:
            continue
        online  = bool(n.get("online", False))
        parent  = n.get("parent", "")
        depth   = n.get("depth", 0)
        bat     = n.get("bat")
        fb      = n.get("float_bits")
        lat     = n.get("lat", 0.0)
        lng     = n.get("lng", 0.0)
        gps_fix = bool(n.get("gps_fix", False))
        rssi    = n.get("rssi")
        snr     = n.get("snr")

        update: dict = {
            "village_id": village,
            "parent_id":  parent,
            "depth":      depth,
            "status":     "online" if online else "offline",
            "last_seen":  now,
        }
        if bat is not None:
            update["battery_voltage"] = round(float(bat), 2)
        if fb is not None:
            update["float_bits"] = fb
        if gps_fix and (lat or lng):
            update["lat"]     = lat
            update["lng"]     = lng
            update["gps_fix"] = True

        col_rivers.update_one(
            {"node_id": node_id},
            {"$set": update, "$setOnInsert": {"first_seen": now}},
            upsert=True
        )

    online_count = sum(1 for n in nodes if n.get("online", False))
    col_villages.update_one(
        {"village_id": village},
        {"$set": {"last_seen": now, "nodes_online": online_count, "total_nodes": len(nodes)}},
        upsert=True
    )
    log.info(f"full_topology  {village}  nodes={len(nodes)}  online={online_count}")


# ── Health checker ────────────────────────────────────────────────────────────

def _health_checker():
    """
    Background thread. Marks river nodes offline if last_seen exceeds
    NODE_OFFLINE_TIMEOUT. Runs every 30 seconds — safety net alongside
    the master's explicit node_status events.
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
        log.warning(f"JSON parse error on {topic}: {e}")
        return

    # Broker strips $share/group/ prefix before delivering, so parts always start
    # with: floodwatch / {village} / {msg_type} [/ {node_id} [/ status]]
    parts = topic.split("/")
    if len(parts) < 3:
        log.warning(f"Unrecognised topic: {topic}")
        return

    village  = parts[1]
    msg_type = parts[2]
    now      = now_utc()

    try:
        if msg_type == "heartbeat" and len(parts) == 4:
            handle_heartbeat(village, parts[3], payload, now)

        elif msg_type == "alert" and len(parts) == 4:
            handle_alert(village, parts[3], payload, now)

        elif msg_type == "announce" and len(parts) == 4:
            handle_announce(village, parts[3], payload, now)

        elif msg_type == "nodes" and len(parts) == 5 and parts[4] == "status":
            handle_node_status(village, parts[3], payload, now)

        elif msg_type == "master" and len(parts) == 4 and parts[3] == "status":
            handle_master_status(village, payload, now)

        elif msg_type == "master" and len(parts) == 4 and parts[3] == "topology":
            handle_full_topology(village, payload, now)

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


# ── MQTT callbacks ────────────────────────────────────────────────────────────

def on_connect(client, userdata, flags, rc, props=None):
    if rc == 0:
        for topic, qos in TOPICS:
            client.subscribe(topic, qos=qos)
        client.publish(
            SERVICE_STATUS_TOPIC,
            json.dumps({"status": "online", "service": "parser", "broker": MQTT_BROKER}),
            qos=1, retain=True,
        )
        log.info(f"MQTT connected — {MQTT_BROKER}:{MQTT_PORT}")
        log.info(f"Subscribed to {len(TOPICS)} topic patterns")
    else:
        log.error(f"MQTT connect failed rc={rc}")


def on_message(client, userdata, msg):
    raw = msg.payload.decode("utf-8", errors="replace").strip()
    if not raw:
        return
    _executor.submit(_route, msg.topic, raw)


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
log.info(f"Worker threads: {WORKER_THREADS}")

threading.Thread(target=_health_checker, name="health-checker", daemon=True).start()
_executor = ThreadPoolExecutor(max_workers=WORKER_THREADS, thread_name_prefix="msg-worker")

client = mqtt.Client(protocol=mqtt.MQTTv5)
client.on_connect    = on_connect
client.on_message    = on_message
client.on_disconnect = on_disconnect

client.will_set(
    SERVICE_STATUS_TOPIC,
    json.dumps({"status": "offline", "service": "parser"}),
    qos=1, retain=True,
)

log.info(f"Connecting to {MQTT_BROKER}:{MQTT_PORT} ...")
client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
client.loop_forever()
