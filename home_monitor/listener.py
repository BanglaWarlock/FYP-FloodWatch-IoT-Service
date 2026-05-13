#!/usr/bin/env python3
"""
FloodWatch Home Monitor Listener

Subscribes to:  floodwatch/home/+/battery
Collections:    home_nodes     — current state per device (upserted, one doc per device_id)
                home_readings  — time-series battery readings (TTL 90 days)
"""

import json
import logging
import os
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING, DESCENDING

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("home-monitor")

# ── Config ────────────────────────────────────────────────────────────────────

MQTT_BROKER = os.getenv("MQTT_BROKER", "suts-fyp-floodwatch-mqtt.fly.dev")
MQTT_PORT   = int(os.getenv("MQTT_PORT", 1883))
MONGO_URI   = os.getenv("MONGO_URI")
MONGO_DB    = os.getenv("MONGO_DB", "flood_monitor")

TOPIC                = "floodwatch/home/+/battery"
SERVICE_STATUS_TOPIC = "floodwatch/system/home_monitor/status"

# ── MongoDB ───────────────────────────────────────────────────────────────────

mongo = MongoClient(MONGO_URI)
db    = mongo[MONGO_DB]

col_home_nodes    = db["home_nodes"]     # current state — one doc per device_id
col_home_readings = db["home_readings"]  # time-series


def _setup_indexes():
    col_home_nodes.create_index("device_id", unique=True)
    col_home_readings.create_index("timestamp", expireAfterSeconds=90 * 24 * 3600)
    col_home_readings.create_index([("device_id", ASCENDING), ("timestamp", DESCENDING)])
    log.info("Indexes verified")


# ── Handler ───────────────────────────────────────────────────────────────────

def handle_battery(device_id: str, payload: dict):
    bat = round(float(payload.get("bat", 0.0)), 2)
    now = datetime.now(timezone.utc)

    col_home_nodes.update_one(
        {"device_id": device_id},
        {
            "$set":         {"battery_voltage": bat, "last_seen": now},
            "$setOnInsert": {"first_seen": now},
        },
        upsert=True,
    )

    col_home_readings.insert_one({
        "device_id":       device_id,
        "battery_voltage": bat,
        "timestamp":       now,
    })

    log.info(f"[{device_id}]  bat={bat:.2f}V")


# ── MQTT callbacks ────────────────────────────────────────────────────────────

def on_connect(client, userdata, flags, rc, props=None):
    if rc == 0:
        client.subscribe(TOPIC, qos=1)
        client.publish(
            SERVICE_STATUS_TOPIC,
            json.dumps({"status": "online", "service": "home_monitor", "broker": MQTT_BROKER}),
            qos=1, retain=True,
        )
        log.info(f"Connected to {MQTT_BROKER}:{MQTT_PORT} — subscribed to {TOPIC}")
    else:
        log.error(f"Connect failed rc={rc}")


def on_message(client, userdata, msg):
    topic = msg.topic
    try:
        payload = json.loads(msg.payload.decode("utf-8", errors="replace"))
    except Exception as e:
        log.warning(f"Bad JSON on {topic}: {e}")
        return

    # floodwatch / home / {device_id} / battery
    parts = topic.split("/")
    if len(parts) != 4:
        log.warning(f"Unexpected topic: {topic}")
        return

    try:
        handle_battery(parts[2], payload)
    except Exception as e:
        log.error(f"Handler error [{topic}]: {e}", exc_info=True)


def on_disconnect(client, userdata, rc, props=None):
    if rc != 0:
        log.warning(f"Unexpected disconnect rc={rc} — paho will reconnect")


# ── Startup ───────────────────────────────────────────────────────────────────

_setup_indexes()

client = mqtt.Client(protocol=mqtt.MQTTv5)
client.on_connect    = on_connect
client.on_message    = on_message
client.on_disconnect = on_disconnect

client.will_set(
    SERVICE_STATUS_TOPIC,
    json.dumps({"status": "offline", "service": "home_monitor"}),
    qos=1, retain=True,
)

log.info(f"Connecting to {MQTT_BROKER}:{MQTT_PORT} ...")
client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
client.loop_forever()
