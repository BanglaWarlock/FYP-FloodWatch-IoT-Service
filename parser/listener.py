# ============================================================
#  listener.py  —  FloodWatch MQTT listener + event publisher
#
#  Payload from master (pipe-delimited, no wrapping):
#    White node:  NODE_ID|voltage|raw_adc|lat|lng|floatBits|rssi  (7 fields)
#    White node:  NODE_ID|voltage|raw_adc|lat|lng|floatBits        (6 fields)
#    Black node:  NODE_ID|voltage|raw_adc|lat|lng|rssi             (6 fields)
#    Black node:  NODE_ID|voltage|raw_adc|lat|lng                  (5 fields)
#
#  MongoDB collections:
#    sensor_readings        — valid readings
#    sensor_readings_failed — malformed packets
#
#  Redis channel: floodwatch:events
#    Events published:
#      heartbeat         — every new reading (node alive)
#      flood_level       — water_level changed (includes old→new)
#      battery_low       — voltage dropped below BATTERY_LOW_V
# ============================================================

import json
import logging
import os
import queue
import threading
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import redis
from dotenv import load_dotenv
from pymongo import MongoClient, DESCENDING

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("listener")

# ── Config ────────────────────────────────────────────────────
MQTT_BROKER = os.getenv("MQTT_BROKER", "suts-fyp-floodwatch-mqtt.fly.dev")
MQTT_PORT   = int(os.getenv("MQTT_PORT", 1883))

TOPIC_NODES  = "$share/listeners/suts/fyp/nodes"
TOPIC_FAILED = "suts/fyp/nodes_failed"

MONGO_URI        = os.getenv("MONGO_URI")
MONGO_DB         = os.getenv("MONGO_DB", "flood_monitor")
MONGO_COL        = os.getenv("MONGO_COL", "sensor_readings")
MONGO_COL_FAILED = "sensor_readings_failed"

REDIS_URL       = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_CHANNEL   = "floodwatch:events"
BATTERY_LOW_V   = float(os.getenv("BATTERY_LOW_V", "11.5"))

NODES_WITHOUT_FLOAT = set(
    os.getenv("NODES_WITHOUT_FLOAT", "SUTS_Black").split(",")
)

# ── MongoDB ───────────────────────────────────────────────────
mongo = MongoClient(MONGO_URI)
db    = mongo[MONGO_DB]
col        = db[MONGO_COL]
col_failed = db[MONGO_COL_FAILED]

col.create_index([("node_id", 1), ("timestamp", -1)])
col.create_index("timestamp")
col_failed.create_index("timestamp")

# ── Redis ─────────────────────────────────────────────────────
r = redis.from_url(REDIS_URL, decode_responses=True)

def publish(event_type: str, payload: dict):
    """Publish an event to the Redis channel consumed by the API's SSE."""
    payload["type"] = event_type
    r.publish(REDIS_CHANNEL, json.dumps(payload))
    log.info(f"  → Redis [{event_type}] {payload}")

# ── Water level change tracking ───────────────────────────────
# Initialised from MongoDB on startup so a parser restart doesn't
# falsely re-emit flood events for nodes that were already flooding.
last_water_level: dict[str, int] = {}

def init_water_levels():
    """Seed last known water level per node from most recent DB readings."""
    pipeline = [
        {"$sort": {"timestamp": -1}},
        {"$group": {"_id": "$node_id", "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}},
    ]
    for doc in col.aggregate(pipeline):
        wl = doc.get("water_level")
        if wl is not None:
            last_water_level[doc["node_id"]] = wl
    log.info(f"Seeded water levels from DB: {last_water_level}")

# ── Parser ────────────────────────────────────────────────────

def parse_message(raw: str) -> dict | None:
    parts = raw.strip().split("|")
    n     = len(parts)

    if n < 5 or n > 7:
        log.warning(f"Wrong field count ({n}): {raw!r}")
        return None

    node_id = parts[0].strip()
    if not node_id:
        log.warning(f"Empty node_id: {raw!r}")
        return None

    try:
        has_float = node_id not in NODES_WITHOUT_FLOAT

        if not has_float:
            return {
                "node_id":     node_id,
                "voltage":     float(parts[1]),
                "raw_adc":     int(parts[2]),
                "lat":         float(parts[3]),
                "lng":         float(parts[4]),
                "float_bits":  None,
                "water_level": None,
                "rssi":        int(parts[5]) if n >= 6 else None,
            }

        if n < 6:
            log.warning(f"Too few fields for float node: {raw!r}")
            return None

        bits = parts[5]
        if len(bits) != 3 or not all(c in "01" for c in bits):
            log.warning(f"Invalid floatBits {bits!r}: {raw!r}")
            return None

        return {
            "node_id":     node_id,
            "voltage":     float(parts[1]),
            "raw_adc":     int(parts[2]),
            "lat":         float(parts[3]),
            "lng":         float(parts[4]),
            "float_bits":  bits,
            "water_level": (
                3 if bits[0] == "1" else
                2 if bits[1] == "1" else
                1 if bits[2] == "1" else 0
            ),
            "rssi": int(parts[6]) if n == 7 else None,
        }

    except ValueError as e:
        log.warning(f"Parse error — {e} | raw: {raw!r}")
        return None


# ── Event detection ───────────────────────────────────────────

def emit_events(data: dict, ts: str):
    node_id = data["node_id"]
    wl      = data.get("water_level")
    v       = data.get("voltage")

    # heartbeat — every reading
    publish("heartbeat", {"node_id": node_id, "timestamp": ts})

    # flood_level — emit when water level changes (or first reading for this node)
    if wl is not None:
        prev = last_water_level.get(node_id)
        if prev is None or wl != prev:
            publish("flood_level", {
                "node_id":        node_id,
                "water_level":    wl,
                "water_level_prev": prev,
                "float_bits":     data.get("float_bits"),
                "lat":            data.get("lat"),
                "lng":            data.get("lng"),
                "timestamp":      ts,
            })
        last_water_level[node_id] = wl

    # battery_low — emit whenever voltage is below threshold
    if v is not None and v < BATTERY_LOW_V:
        publish("battery_low", {
            "node_id":   node_id,
            "voltage":   round(v, 2),
            "threshold": BATTERY_LOW_V,
            "timestamp": ts,
        })


# ── Worker queue ──────────────────────────────────────────────
# on_message runs on paho's network thread. DB and Redis calls can
# take tens of milliseconds, which would delay the next message.
# Instead we push raw (topic, payload) tuples onto a bounded queue
# and a dedicated worker thread does all the slow I/O.

_msg_queue: queue.Queue = queue.Queue(maxsize=512)


def _worker():
    while True:
        item = _msg_queue.get()
        if item is None:
            break
        topic, raw = item
        try:
            _process(topic, raw)
        except Exception as e:
            log.error(f"Worker error: {e}", exc_info=True)
        finally:
            _msg_queue.task_done()


def _process(topic: str, raw: str):
    if topic == TOPIC_FAILED:
        col_failed.insert_one({
            "raw":       raw,
            "source":    "master_parse_fail",
            "timestamp": datetime.now(timezone.utc),
        })
        log.warning(f"✗ master bad packet: {raw}")
        return

    log.info(f"← {raw}")

    data = parse_message(raw)
    if data is None:
        col_failed.insert_one({
            "raw":       raw,
            "source":    "listener_parse_fail",
            "timestamp": datetime.now(timezone.utc),
        })
        return

    now = datetime.now(timezone.utc)
    col.insert_one({**data, "timestamp": now})

    ts = now.isoformat()
    emit_events(data, ts)

    float_str = (
        f"  level={data['water_level']} bits={data['float_bits']}"
        if data["float_bits"] is not None else "  (no float)"
    )
    rssi_str = f"  RSSI:{data['rssi']}dBm" if data["rssi"] is not None else ""
    log.info(
        f"  ✓ {data['node_id']:<16}"
        f"  {data['voltage']:.2f}V"
        f"  raw={data['raw_adc']}"
        f"  ({data['lat']:.4f}, {data['lng']:.4f})"
        f"{float_str}{rssi_str}"
    )


# ── MQTT callbacks ────────────────────────────────────────────

def on_connect(client, userdata, flags, rc, props=None):
    if rc == 0:
        client.subscribe(TOPIC_NODES,  qos=1)
        client.subscribe(TOPIC_FAILED, qos=0)
        log.info(f"MQTT connected — {MQTT_BROKER}:{MQTT_PORT}")
    else:
        log.error(f"MQTT connect failed rc={rc}")


def on_message(client, userdata, msg):
    raw = msg.payload.decode("utf-8", errors="replace").strip()
    try:
        _msg_queue.put_nowait((msg.topic, raw))
    except queue.Full:
        log.error("Message queue full — dropping message")


def on_disconnect(client, userdata, rc, props=None):
    if rc != 0:
        log.warning(f"Unexpected disconnect rc={rc} — paho will reconnect")


# ── Main ──────────────────────────────────────────────────────

log.info(f"MongoDB connected — db: {MONGO_DB}")
init_water_levels()

log.info(f"Redis connected — {REDIS_URL}")

_worker_thread = threading.Thread(target=_worker, name="msg-worker", daemon=True)
_worker_thread.start()

client = mqtt.Client(protocol=mqtt.MQTTv5)
client.on_connect    = on_connect
client.on_message    = on_message
client.on_disconnect = on_disconnect

log.info(f"Connecting to MQTT {MQTT_BROKER}:{MQTT_PORT} ...")
client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)

log.info("Listener running — Ctrl+C to stop")
client.loop_forever()
