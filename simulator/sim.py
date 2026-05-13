#!/usr/bin/env python3
"""
FloodWatch IoT Simulator

Persistent program intended for EC2. Simulates two real Pitas, Sabah villages
(Kg. Sosop and Kampung Mandamai Bai) with 22 sensor nodes, each running in its
own thread with a personality-driven water-level and battery state machine.

Writes directly to MongoDB (bypasses MQTT/parser) and publishes SSE events
to Redis with is_sample=True so the API can filter them independently.

Usage:
  python sim.py                # normal mode — 30 s tick
  python sim.py --demo         # demo mode   — 5 s tick, verbose logging
  python sim.py --reset        # clear all sample data then restart
  python sim.py --reset --demo # both
"""

import json
import logging
import os
import random
import sys
import threading
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
import redis as _redis_lib
from pymongo import MongoClient, ASCENDING, DESCENDING

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

MONGO_URI      = os.getenv("MONGO_URI")
MONGO_DB       = os.getenv("MONGO_DB", "flood_monitor")
REDIS_URL      = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_CHANNEL  = "floodwatch:events"

DEMO_MODE  = "--demo"  in sys.argv
RESET_MODE = "--reset" in sys.argv

TICK_INTERVAL    = 5  if DEMO_MODE else 30   # seconds between node heartbeats
RAIN_TICK_EVERY  = 6                          # rain state updates every N ticks
ALERT_DEDUP_SECS = 120                        # minimum seconds between same alert type

logging.basicConfig(
    level=logging.DEBUG if DEMO_MODE else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sim")

# ── MongoDB / Redis ───────────────────────────────────────────────────────────

_mongo        = MongoClient(MONGO_URI)
_db           = _mongo[MONGO_DB]
col_villages  = _db["villages"]
col_masters   = _db["master_nodes"]
col_rivers    = _db["river_nodes"]
col_heartbeats = _db["heartbeats"]
col_alerts    = _db["alerts"]
col_events    = _db["events"]

_redis = _redis_lib.from_url(REDIS_URL, decode_responses=True)


def _publish(event_type: str, data: dict):
    data["type"] = event_type
    _redis.publish(REDIS_CHANNEL, json.dumps(data, default=str))

# ── Village / Node Topology ───────────────────────────────────────────────────

SOSOP_VID    = "SIM-PITAS-SOSOP"
MANDAMAI_VID = "SIM-PITAS-MANDAMAI"

VILLAGES = [
    {
        "village_id": SOSOP_VID,
        "name":       "Kg. Sosop Pitas",
        "lat":         6.6490674,
        "lng":         117.0712744,
        "district":   "Pitas",
        "state":      "Sabah",
        "is_sample":  True,
    },
    {
        "village_id": MANDAMAI_VID,
        "name":       "Kampung Mandamai Bai",
        "lat":         6.6364879,
        "lng":         117.0549326,
        "district":   "Pitas",
        "state":      "Sabah",
        "is_sample":  True,
    },
]

MASTERS = [
    {
        "node_id":    "SIM-SOS-MASTER",
        "village_id": SOSOP_VID,
        "lat":         6.6490674,
        "lng":         117.0712744,
        "is_sample":  True,
    },
    {
        "node_id":    "SIM-MAN-MASTER",
        "village_id": MANDAMAI_VID,
        "lat":         6.6364879,
        "lng":         117.0549326,
        "is_sample":  True,
    },
]

# Each entry: node_id, village_id, parent_id, depth, lat, lng (install position),
# has_gps, personality (drives water-level and battery state machine).
#
# personality keys:
#   base            — water level the node gravitates toward (0-3)
#   max             — maximum water level this node can reach
#   rise            — base probability per tick to rise one level
#   fall            — base probability per tick to fall one level
#   bat_start       — initial battery voltage (V)
#   bat_drain       — voltage drop per tick when not flooded
#   bat_drain_flood — voltage drop per tick when water_level >= 2
#   bat_low         — threshold for battery_low alert
#   bat_critical    — threshold for battery_critical alert
#   bat_recharge_rate  — voltage gain per tick during solar recharge
#   bat_recharge_start — recharge kicks in at this voltage
#   bat_recharge_end   — recharge stops at this voltage
NODES = [
    # ── SOSOP Chain A — upstream west, moderate flooding ─────────────────────
    {
        "node_id": "SIM-SOS-A1", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-MASTER", "depth": 1,
        "lat": 6.6492, "lng": 117.0700, "has_gps": True,
        "personality": {
            "base": 1, "max": 3, "rise": 0.20, "fall": 0.30,
            "bat_start": 4.10, "bat_drain": 0.0010, "bat_drain_flood": 0.0015,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
    {
        "node_id": "SIM-SOS-A2", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-A1", "depth": 2,
        "lat": 6.6494, "lng": 117.0690, "has_gps": True,
        "personality": {
            "base": 1, "max": 3, "rise": 0.25, "fall": 0.28,
            "bat_start": 3.90, "bat_drain": 0.0012, "bat_drain_flood": 0.0018,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
    {
        "node_id": "SIM-SOS-A3", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-A2", "depth": 3,
        "lat": 6.6496, "lng": 117.0680, "has_gps": True,
        "personality": {
            "base": 1, "max": 3, "rise": 0.30, "fall": 0.25,
            "bat_start": 4.00, "bat_drain": 0.0011, "bat_drain_flood": 0.0017,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
    {
        "node_id": "SIM-SOS-A4", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-A3", "depth": 4,
        "lat": 6.6498, "lng": 117.0670, "has_gps": False,
        "personality": {
            "base": 2, "max": 3, "rise": 0.35, "fall": 0.20,
            "bat_start": 3.80, "bat_drain": 0.0015, "bat_drain_flood": 0.0020,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
    # ── SOSOP Chain B — NW tributary, usually calm ────────────────────────────
    {
        "node_id": "SIM-SOS-B1", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-MASTER", "depth": 1,
        "lat": 6.6500, "lng": 117.0710, "has_gps": True,
        "personality": {
            "base": 0, "max": 3, "rise": 0.15, "fall": 0.40,
            "bat_start": 4.20, "bat_drain": 0.0009, "bat_drain_flood": 0.0013,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
    {
        "node_id": "SIM-SOS-B2", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-B1", "depth": 2,
        "lat": 6.6510, "lng": 117.0705, "has_gps": True,
        "personality": {
            "base": 0, "max": 3, "rise": 0.18, "fall": 0.35,
            "bat_start": 4.00, "bat_drain": 0.0010, "bat_drain_flood": 0.0015,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
    {
        "node_id": "SIM-SOS-B3", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-B2", "depth": 3,
        "lat": 6.6518, "lng": 117.0698, "has_gps": False,
        "personality": {
            "base": 1, "max": 3, "rise": 0.22, "fall": 0.30,
            "bat_start": 3.90, "bat_drain": 0.0011, "bat_drain_flood": 0.0016,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
    # ── SOSOP Chain C — downstream east, most persistent flooding ─────────────
    {
        "node_id": "SIM-SOS-C1", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-MASTER", "depth": 1,
        "lat": 6.6488, "lng": 117.0725, "has_gps": True,
        "personality": {
            "base": 2, "max": 3, "rise": 0.35, "fall": 0.15,
            "bat_start": 4.10, "bat_drain": 0.0012, "bat_drain_flood": 0.0018,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
    {
        "node_id": "SIM-SOS-C2", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-C1", "depth": 2,
        "lat": 6.6485, "lng": 117.0738, "has_gps": True,
        "personality": {
            "base": 2, "max": 3, "rise": 0.40, "fall": 0.12,
            "bat_start": 3.90, "bat_drain": 0.0013, "bat_drain_flood": 0.0019,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
    {
        "node_id": "SIM-SOS-C3", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-C2", "depth": 3,
        "lat": 6.6482, "lng": 117.0750, "has_gps": True,
        "personality": {
            "base": 2, "max": 3, "rise": 0.45, "fall": 0.10,
            "bat_start": 3.70, "bat_drain": 0.0014, "bat_drain_flood": 0.0020,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
    # ── SOSOP Chain D — SW tributary, gentle river ────────────────────────────
    {
        "node_id": "SIM-SOS-D1", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-MASTER", "depth": 1,
        "lat": 6.6480, "lng": 117.0700, "has_gps": True,
        "personality": {
            "base": 1, "max": 3, "rise": 0.20, "fall": 0.30,
            "bat_start": 4.00, "bat_drain": 0.0010, "bat_drain_flood": 0.0015,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
    {
        "node_id": "SIM-SOS-D2", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-D1", "depth": 2,
        "lat": 6.6470, "lng": 117.0692, "has_gps": False,
        "personality": {
            "base": 1, "max": 3, "rise": 0.22, "fall": 0.28,
            "bat_start": 3.80, "bat_drain": 0.0012, "bat_drain_flood": 0.0017,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },

    # ── MANDAMAI Chain A — upstream west, moderate ────────────────────────────
    {
        "node_id": "SIM-MAN-A1", "village_id": MANDAMAI_VID,
        "parent_id": "SIM-MAN-MASTER", "depth": 1,
        "lat": 6.6366, "lng": 117.0537, "has_gps": True,
        "personality": {
            "base": 1, "max": 3, "rise": 0.20, "fall": 0.30,
            "bat_start": 4.20, "bat_drain": 0.0009, "bat_drain_flood": 0.0014,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
    {
        "node_id": "SIM-MAN-A2", "village_id": MANDAMAI_VID,
        "parent_id": "SIM-MAN-A1", "depth": 2,
        "lat": 6.6368, "lng": 117.0526, "has_gps": True,
        "personality": {
            "base": 1, "max": 3, "rise": 0.25, "fall": 0.27,
            "bat_start": 4.00, "bat_drain": 0.0010, "bat_drain_flood": 0.0015,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
    {
        "node_id": "SIM-MAN-A3", "village_id": MANDAMAI_VID,
        "parent_id": "SIM-MAN-A2", "depth": 3,
        "lat": 6.6370, "lng": 117.0515, "has_gps": False,
        "personality": {
            "base": 1, "max": 3, "rise": 0.28, "fall": 0.24,
            "bat_start": 3.90, "bat_drain": 0.0011, "bat_drain_flood": 0.0016,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
    # ── MANDAMAI Chain B — SW tributary, mostly dry ───────────────────────────
    {
        "node_id": "SIM-MAN-B1", "village_id": MANDAMAI_VID,
        "parent_id": "SIM-MAN-MASTER", "depth": 1,
        "lat": 6.6354, "lng": 117.0538, "has_gps": True,
        "personality": {
            "base": 0, "max": 3, "rise": 0.12, "fall": 0.40,
            "bat_start": 4.10, "bat_drain": 0.0009, "bat_drain_flood": 0.0013,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
    {
        "node_id": "SIM-MAN-B2", "village_id": MANDAMAI_VID,
        "parent_id": "SIM-MAN-B1", "depth": 2,
        "lat": 6.6344, "lng": 117.0530, "has_gps": False,
        "personality": {
            "base": 0, "max": 3, "rise": 0.15, "fall": 0.35,
            "bat_start": 3.80, "bat_drain": 0.0010, "bat_drain_flood": 0.0015,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
    # ── MANDAMAI Chain C — NW tributary ───────────────────────────────────────
    {
        "node_id": "SIM-MAN-C1", "village_id": MANDAMAI_VID,
        "parent_id": "SIM-MAN-MASTER", "depth": 1,
        "lat": 6.6376, "lng": 117.0548, "has_gps": True,
        "personality": {
            "base": 1, "max": 3, "rise": 0.18, "fall": 0.32,
            "bat_start": 4.00, "bat_drain": 0.0010, "bat_drain_flood": 0.0015,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
    {
        "node_id": "SIM-MAN-C2", "village_id": MANDAMAI_VID,
        "parent_id": "SIM-MAN-C1", "depth": 2,
        "lat": 6.6386, "lng": 117.0547, "has_gps": True,
        "personality": {
            "base": 1, "max": 3, "rise": 0.22, "fall": 0.28,
            "bat_start": 3.90, "bat_drain": 0.0011, "bat_drain_flood": 0.0016,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
    # ── MANDAMAI Chain D — downstream east, most persistent flooding ──────────
    {
        "node_id": "SIM-MAN-D1", "village_id": MANDAMAI_VID,
        "parent_id": "SIM-MAN-MASTER", "depth": 1,
        "lat": 6.6362, "lng": 117.0561, "has_gps": True,
        "personality": {
            "base": 2, "max": 3, "rise": 0.35, "fall": 0.15,
            "bat_start": 4.00, "bat_drain": 0.0012, "bat_drain_flood": 0.0018,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
    {
        "node_id": "SIM-MAN-D2", "village_id": MANDAMAI_VID,
        "parent_id": "SIM-MAN-D1", "depth": 2,
        "lat": 6.6358, "lng": 117.0574, "has_gps": True,
        "personality": {
            "base": 2, "max": 3, "rise": 0.38, "fall": 0.12,
            "bat_start": 3.90, "bat_drain": 0.0013, "bat_drain_flood": 0.0019,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
    {
        "node_id": "SIM-MAN-D3", "village_id": MANDAMAI_VID,
        "parent_id": "SIM-MAN-D2", "depth": 3,
        "lat": 6.6354, "lng": 117.0586, "has_gps": True,
        "personality": {
            "base": 2, "max": 3, "rise": 0.42, "fall": 0.10,
            "bat_start": 3.70, "bat_drain": 0.0015, "bat_drain_flood": 0.0022,
            "bat_low": 3.50, "bat_critical": 3.30,
            "bat_recharge_rate": 0.003, "bat_recharge_start": 3.30, "bat_recharge_end": 4.00,
        },
    },
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _wl_to_fb(water_level: int) -> int:
    """Water level integer → float_bits bitmask (matches firmware encoding)."""
    if water_level >= 3: return 7   # 0b111 — high + mid + low sensors
    if water_level >= 2: return 3   # 0b011 — mid + low
    if water_level >= 1: return 1   # 0b001 — low only
    return 0


def _fake_signal() -> tuple[int, float]:
    """Generate plausible RSSI and SNR for a simulated LoRa hop."""
    rssi = random.randint(-105, -55)
    snr  = round(random.uniform(4.0, 12.0), 1)
    return rssi, snr


# ── Village Rain State Machine ────────────────────────────────────────────────

class VillageRain:
    """
    Shared, thread-safe rainfall state for a village.
    Drives water-level rise/fall probabilities for all nodes in that village.

    Markov transitions run every RAIN_TICK_EVERY node ticks (roughly every few
    minutes in normal mode, ~30 s in demo mode).
    """

    _TRANSITIONS: dict[str, list[tuple[str, float]]] = {
        "dry":      [("dry", 0.94), ("light", 0.06)],
        "light":    [("dry", 0.08), ("light", 0.80), ("moderate", 0.12)],
        "moderate": [("light", 0.12), ("moderate", 0.72), ("heavy", 0.16)],
        "heavy":    [("moderate", 0.25), ("heavy", 0.75)],
    }

    # Multiplier applied to a node's rise probability
    _RISE_BOOST: dict[str, float] = {
        "dry": 0.0, "light": 0.30, "moderate": 0.80, "heavy": 1.80,
    }

    # Multiplier applied to a node's fall probability
    _FALL_REDUCE: dict[str, float] = {
        "dry": 1.0, "light": 0.80, "moderate": 0.50, "heavy": 0.25,
    }

    def __init__(self, village_id: str):
        self.village_id = village_id
        self._state     = "dry"
        self._lock      = threading.Lock()

    def step(self):
        with self._lock:
            transitions = self._TRANSITIONS[self._state]
            r = random.random()
            total = 0.0
            for next_state, prob in transitions:
                total += prob
                if r < total:
                    if next_state != self._state:
                        log.info(f"Rain [{self.village_id}]: {self._state} → {next_state}")
                    self._state = next_state
                    return

    def get_state(self) -> str:
        with self._lock:
            return self._state

    def rise_boost(self) -> float:
        with self._lock:
            return self._RISE_BOOST[self._state]

    def fall_reduce(self) -> float:
        with self._lock:
            return self._FALL_REDUCE[self._state]

    def run_loop(self):
        """Called from a dedicated rain thread."""
        while True:
            time.sleep(TICK_INTERVAL * RAIN_TICK_EVERY)
            self.step()


# ── SimNode Thread ────────────────────────────────────────────────────────────

class SimNode(threading.Thread):
    """
    One thread per river node. Each tick it:
      1. Steps water level (personality + village rain)
      2. Steps battery (drain or solar recharge)
      3. Writes a heartbeat to MongoDB
      4. Updates river_nodes doc
      5. Publishes SSE heartbeat (and flood_level if level changed)
      6. Fires alerts on state transitions (flood start/end, battery thresholds)
    """

    def __init__(self, node_def: dict, rain: VillageRain):
        super().__init__(daemon=True, name=node_def["node_id"])
        self.node_id    = node_def["node_id"]
        self.village_id = node_def["village_id"]
        self.parent_id  = node_def["parent_id"]
        self.depth      = node_def["depth"]
        self.lat        = node_def.get("lat")
        self.lng        = node_def.get("lng")
        self.has_gps    = node_def.get("has_gps", False)
        self.p          = node_def["personality"]
        self.rain       = rain

        # Mutable state
        self._water     = self.p["base"]
        self._battery   = self.p["bat_start"]
        self._recharging = False
        self._tick_count = 0

        # Alert dedup: maps alert_type → last sent timestamp (monotonic)
        self._last_alert: dict[str, float] = {}

        # Battery alert state machine: avoid repeated low/critical alerts
        self._bat_state = "normal"  # normal | low | critical | recharging

        # Stagger start times so 22 nodes don't hammer the DB simultaneously
        self._jitter = random.uniform(0.0, TICK_INTERVAL)

    def run(self):
        time.sleep(self._jitter)
        while True:
            try:
                self._tick()
            except Exception as e:
                log.error(f"{self.node_id}: tick error: {e}", exc_info=True)
            time.sleep(TICK_INTERVAL)

    # ── Per-tick logic ────────────────────────────────────────────────────────

    def _tick(self):
        self._tick_count += 1
        now = datetime.now(timezone.utc)
        ts  = now.isoformat()

        prev_water = self._water
        self._step_water()
        self._step_battery()

        wl         = self._water
        fb         = _wl_to_fb(wl)
        bat        = round(self._battery, 2)
        gps_fix    = self.has_gps
        lat        = self.lat if gps_fix else None
        lng        = self.lng if gps_fix else None
        rssi, snr  = _fake_signal()

        # Heartbeat → MongoDB
        col_heartbeats.insert_one({
            "node_id":         self.node_id,
            "village_id":      self.village_id,
            "timestamp":       now,
            "battery_voltage": bat,
            "float_bits":      fb,
            "water_level":     wl,
            "lat":             lat,
            "lng":             lng,
            "gps_fix":         gps_fix,
            "depth":           self.depth,
            "parent_id":       self.parent_id,
            "rssi":            rssi,
            "snr":             snr,
            "is_sample":       True,
        })

        # River node live state
        river_set: dict = {
            "status":          "online",
            "last_seen":       now,
            "battery_voltage": bat,
            "float_bits":      fb,
            "water_level":     wl,
            "gps_fix":         gps_fix,
            "rssi":            rssi,
            "snr":             snr,
        }
        if gps_fix:
            river_set["lat"] = lat
            river_set["lng"] = lng

        col_rivers.update_one(
            {"node_id": self.node_id},
            {"$set": river_set},
            upsert=False,
        )

        col_villages.update_one(
            {"village_id": self.village_id},
            {"$set": {"last_seen": now}},
        )

        # SSE heartbeat
        _publish("heartbeat", {
            "node_id":     self.node_id,
            "village_id":  self.village_id,
            "water_level": wl,
            "float_bits":  fb,
            "bat":         bat,
            "gps_fix":     gps_fix,
            "lat":         lat,
            "lng":         lng,
            "depth":       self.depth,
            "parent":      self.parent_id,
            "rssi":        rssi,
            "snr":         snr,
            "timestamp":   ts,
            "is_sample":   True,
        })

        # SSE flood_level if water level changed
        if wl != prev_water:
            _publish("flood_level", {
                "node_id":          self.node_id,
                "village_id":       self.village_id,
                "water_level":      wl,
                "water_level_prev": prev_water,
                "float_bits":       fb,
                "lat":              lat,
                "lng":              lng,
                "gps_fix":          gps_fix,
                "timestamp":        ts,
                "is_sample":        True,
            })

        # Alerts
        self._check_alerts(now, prev_water, wl, fb, bat, lat, lng, gps_fix, rssi, snr)

        log.debug(
            f"{self.node_id}  wl={wl}  bat={bat:.2f}V  "
            f"rain={self.rain.get_state()}  tick={self._tick_count}"
        )

    # ── State machine steps ───────────────────────────────────────────────────

    def _step_water(self):
        p = self.p
        rise_p = min(0.90, p["rise"] * (1.0 + self.rain.rise_boost()))
        fall_p = min(0.90, p["fall"] * self.rain.fall_reduce())

        r = random.random()
        if r < rise_p and self._water < p["max"]:
            self._water += 1
        elif r < rise_p + fall_p and self._water > 0:
            self._water -= 1

    def _step_battery(self):
        p = self.p
        if self._recharging:
            self._battery = min(p["bat_recharge_end"], self._battery + p["bat_recharge_rate"])
            if self._battery >= p["bat_recharge_end"]:
                self._recharging = False
                self._bat_state  = "normal"
                log.debug(f"{self.node_id}: battery recharged to {self._battery:.2f}V")
        else:
            drain = p["bat_drain_flood"] if self._water >= 2 else p["bat_drain"]
            self._battery -= drain
            if self._battery <= p["bat_recharge_start"]:
                self._battery    = p["bat_recharge_start"]
                self._recharging = True

    # ── Alert logic ───────────────────────────────────────────────────────────

    def _can_alert(self, alert_type: str) -> bool:
        now  = time.monotonic()
        last = self._last_alert.get(alert_type, 0.0)
        if now - last >= ALERT_DEDUP_SECS:
            self._last_alert[alert_type] = now
            return True
        return False

    def _check_alerts(self, now, prev_wl, wl, fb, bat, lat, lng, gps_fix, rssi, snr):
        p = self.p

        # Flood starts: water level just reached maximum
        if wl >= p["max"] and prev_wl < p["max"]:
            if self._can_alert("flood"):
                self._send_alert(now, "flood", wl, fb, bat, lat, lng, gps_fix, rssi, snr)

        # Flood recedes: coming down from maximum
        if prev_wl >= p["max"] and wl < p["max"]:
            if self._can_alert("water_fall"):
                self._send_alert(now, "water_fall", wl, fb, bat, lat, lng, gps_fix, rssi, snr)

        # Battery state transitions (avoid re-alerting on same state)
        if bat <= p["bat_critical"] and self._bat_state not in ("critical", "recharging"):
            self._bat_state = "critical"
            if self._can_alert("battery_critical"):
                self._send_alert(now, "battery_critical", wl, fb, bat, lat, lng, gps_fix, rssi, snr)
        elif p["bat_critical"] < bat <= p["bat_low"] and self._bat_state == "normal":
            self._bat_state = "low"
            if self._can_alert("battery_low"):
                self._send_alert(now, "battery_low", wl, fb, bat, lat, lng, gps_fix, rssi, snr)

        if self._recharging:
            self._bat_state = "recharging"

    def _send_alert(self, now, alert_type, wl, fb, bat, lat, lng, gps_fix, rssi, snr):
        village_doc      = col_villages.find_one({"village_id": self.village_id}, {"weather": 1})
        weather_snapshot = village_doc.get("weather") if village_doc else None
        ts = now.isoformat()

        alert_result = col_alerts.insert_one({
            "node_id":          self.node_id,
            "village_id":       self.village_id,
            "timestamp":        now,
            "alert_type":       alert_type,
            "level":            wl  if alert_type == "flood"      else None,
            "float_bits":       fb  if alert_type == "flood"      else None,
            "water_level":      wl  if alert_type == "flood"      else None,
            "battery_voltage":  bat,
            "dist_m":           None,
            "lat":              lat,
            "lng":              lng,
            "home_lat":         None,
            "home_lng":         None,
            "gps_fix":          gps_fix,
            "rssi":             rssi,
            "snr":              snr,
            "weather_at_alert": weather_snapshot,
            "is_sample":        True,
        })

        col_rivers.update_one(
            {"node_id": self.node_id},
            {
                "$set": {"last_alert_id": alert_result.inserted_id, "last_seen": now},
                "$inc": {f"alert_counts.{alert_type}": 1},
            },
        )

        col_villages.update_one(
            {"village_id": self.village_id},
            {
                "$set": {"last_seen": now},
                "$inc": {f"alerts_by_type.{alert_type}": 1, "total_alerts": 1},
            },
        )

        _publish("alert", {
            "node_id":    self.node_id,
            "village_id": self.village_id,
            "alert_type": alert_type,
            "level":      wl,
            "float_bits": fb,
            "bat":        bat,
            "dist_m":     None,
            "lat":        lat,
            "lng":        lng,
            "gps_fix":    gps_fix,
            "rssi":       rssi,
            "timestamp":  ts,
            "is_sample":  True,
        })

        log.info(f"ALERT  {self.node_id}  type={alert_type}  wl={wl}  bat={bat:.2f}V")


# ── DB Topology Setup ─────────────────────────────────────────────────────────

def _build_chains(village_id: str) -> list[dict]:
    chains: dict[str, list[str]] = {}
    for n in NODES:
        if n["village_id"] != village_id:
            continue
        letter = n["node_id"].split("-")[-1][0]   # e.g. "SIM-SOS-A1" → "A"
        chains.setdefault(letter, []).append(n["node_id"])
    return [{"id": k, "nodes": v} for k, v in sorted(chains.items())]


def _setup_topology():
    """Upsert villages, masters, and river nodes. Safe to call on every startup."""
    log.info("Setting up DB topology ...")
    now = datetime.now(timezone.utc)

    for v in VILLAGES:
        vid          = v["village_id"]
        village_nodes = [n for n in NODES if n["village_id"] == vid]
        master        = next(m for m in MASTERS if m["village_id"] == vid)

        col_villages.update_one(
            {"village_id": vid},
            {
                "$set": {
                    **v,
                    "topology": {
                        "master":     master["node_id"],
                        "node_count": len(village_nodes),
                        "chains":     _build_chains(vid),
                    },
                    "total_nodes":         len(village_nodes),
                    "alerts_by_type":      {},
                    "active_alerts_count": 0,
                },
                "$setOnInsert": {
                    "total_alerts": 0,
                    "first_seen":   now,
                    "last_seen":    now,
                },
            },
            upsert=True,
        )
        log.info(f"  Village {vid} ({len(village_nodes)} nodes)")

    for m in MASTERS:
        col_masters.update_one(
            {"node_id": m["node_id"]},
            {
                "$set":         {**m, "status": "online", "last_seen": now},
                "$setOnInsert": {"first_seen": now},
            },
            upsert=True,
        )

    for n in NODES:
        gps_fix  = n.get("has_gps", False)
        base_wl  = n["personality"]["base"]
        node_set = {
            "village_id":      n["village_id"],
            "parent_id":       n["parent_id"],
            "depth":           n["depth"],
            "status":          "online",
            "last_seen":       now,
            "battery_voltage": n["personality"]["bat_start"],
            "float_bits":      _wl_to_fb(base_wl),
            "water_level":     base_wl,
            "gps_fix":         gps_fix,
            "is_sample":       True,
        }
        if gps_fix:
            node_set["lat"]         = n["lat"]
            node_set["lng"]         = n["lng"]
            node_set["install_lat"] = n["lat"]
            node_set["install_lng"] = n["lng"]

        col_rivers.update_one(
            {"node_id": n["node_id"]},
            {
                "$set":         node_set,
                "$setOnInsert": {"first_seen": now, "alert_counts": {}},
            },
            upsert=True,
        )

    log.info("Topology ready.")


def _clear_sample_data():
    log.info("Clearing sample data ...")
    for col in [col_villages, col_masters, col_rivers, col_heartbeats, col_alerts, col_events]:
        r = col.delete_many({"is_sample": True})
        log.info(f"  {col.name}: deleted {r.deleted_count}")
    log.info("Sample data cleared.")


# ── Entrypoint ────────────────────────────────────────────────────────────────

def main():
    log.info(f"FloodWatch Simulator starting — mode={'DEMO' if DEMO_MODE else 'normal'}, tick={TICK_INTERVAL}s")

    if RESET_MODE:
        _clear_sample_data()

    _setup_topology()

    # One rain-state machine per village
    rain_map: dict[str, VillageRain] = {
        SOSOP_VID:    VillageRain(SOSOP_VID),
        MANDAMAI_VID: VillageRain(MANDAMAI_VID),
    }

    for rain in rain_map.values():
        t = threading.Thread(
            target=rain.run_loop,
            daemon=True,
            name=f"rain-{rain.village_id}",
        )
        t.start()

    # One thread per river node
    node_threads: list[SimNode] = []
    for node_def in NODES:
        rain = rain_map[node_def["village_id"]]
        t    = SimNode(node_def, rain)
        t.start()
        node_threads.append(t)

    log.info(f"Simulator running: {len(node_threads)} nodes across {len(rain_map)} villages")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("Simulator stopped by user.")


if __name__ == "__main__":
    main()
