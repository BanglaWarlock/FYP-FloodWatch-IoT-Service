#!/usr/bin/env python3
"""
FloodWatch IoT Simulator

Persistent program for EC2. Simulates two real Pitas, Sabah villages with
22 personality-driven sensor nodes writing directly to MongoDB and publishing
SSE events to Redis (is_sample=True).

Architecture:
  One VillageCoordinator thread per village — every TICK_INTERVAL seconds it:
    1. Steps all nodes' water-level and battery state machines
    2. Selects ~1/3 of nodes to "transmit" (write heartbeat + publish SSE)
    3. Steps the shared rain Markov chain every RAIN_TICK_EVERY ticks
    4. Refreshes predetermined village weather every WEATHER_TICK_EVERY ticks

Battery state machine:  draining → critical_hold → charging → draining
  - Drains at bat_drain V/tick (faster when flooding)
  - When critical: sits at that voltage for HOLD_TICKS_MIN..MAX ticks
  - Then charges at BAT_CHARGE_RATE V/tick toward a random goal (+0.5..+0.9 V)
  - On charge complete: resets to draining, alerts re-arm for next cycle

Weather: predetermined from time-of-day + rain state (no API calls).

Usage:
  python sim.py                # normal mode, 10 s tick
  python sim.py --demo         # faster battery/water/rain cycles, verbose log
  python sim.py --reset        # wipe all is_sample docs then restart
  python sim.py --reset --demo
"""

import json
import logging
import math
import os
import random
import sys
import threading
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
import redis as _redis_lib
from pymongo import MongoClient

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

MONGO_URI     = os.getenv("MONGO_URI")
MONGO_DB      = os.getenv("MONGO_DB", "flood_monitor")
REDIS_URL     = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_CHANNEL = "floodwatch:events"

DEMO_MODE  = "--demo"  in sys.argv
RESET_MODE = "--reset" in sys.argv

TICK_INTERVAL = 10  # seconds — fixed; 1/3 node selection gives ~30 s per node

if DEMO_MODE:
    WATER_STEPS        = 3       # water state steps per coordinator tick
    DRAIN_MULT         = 12.0    # battery drain/charge multiplier
    RAIN_TICK_EVERY    = 6       # rain step every 6 × 10 s = 60 s
    WEATHER_TICK_EVERY = 12      # weather refresh every 12 × 10 s = 120 s
    BAT_HOLD_MIN       = 10      # ticks at critical before charging starts
    BAT_HOLD_MAX       = 30
else:
    WATER_STEPS        = 1
    DRAIN_MULT         = 1.0
    RAIN_TICK_EVERY    = 18      # 180 s
    WEATHER_TICK_EVERY = 36      # 360 s
    BAT_HOLD_MIN       = 30      # 300 s
    BAT_HOLD_MAX       = 120     # 1200 s

# Solar panel charges at this rate (scaled by DRAIN_MULT so demo stays proportional)
BAT_CHARGE_RATE = 0.004 * DRAIN_MULT   # V/tick
BAT_CHARGE_MIN  = 0.5                  # minimum charge goal increment (V)
BAT_CHARGE_MAX  = 0.9                  # maximum charge goal increment
BAT_MAX_VOLTAGE = 4.20

ALERT_DEDUP_SECS = 120

logging.basicConfig(
    level=logging.DEBUG if DEMO_MODE else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sim")

# ── MongoDB / Redis ───────────────────────────────────────────────────────────

_mongo         = MongoClient(MONGO_URI)
_db            = _mongo[MONGO_DB]
col_villages   = _db["villages"]
col_masters    = _db["master_nodes"]
col_rivers     = _db["river_nodes"]
col_heartbeats = _db["heartbeats"]
col_alerts     = _db["alerts"]
col_events     = _db["events"]

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
    {"node_id": "SIM-SOS-MASTER",  "village_id": SOSOP_VID,    "lat": 6.6490674, "lng": 117.0712744, "is_sample": True},
    {"node_id": "SIM-MAN-MASTER",  "village_id": MANDAMAI_VID, "lat": 6.6364879, "lng": 117.0549326, "is_sample": True},
]

# personality keys:
#   base, max   — water level the node gravitates toward and its ceiling (0-3)
#   rise, fall  — per-tick probability to go up / down one level
#   bat_start   — initial voltage (varied across nodes for diverse starting states)
#   bat_drain   — V/tick normal drain (multiplied by DRAIN_MULT at runtime)
#   bat_drain_flood — V/tick when water_level >= 2
#   bat_low     — voltage threshold for battery_low alert
#   bat_critical — threshold for battery_critical; node sits here during hold
NODES = [
    # ── SOSOP Chain A — upstream west ─────────────────────────────────────────
    {
        "node_id": "SIM-SOS-A1", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-MASTER", "depth": 1,
        "lat": 6.6492, "lng": 117.0700, "has_gps": True,
        "personality": {"base": 1, "max": 3, "rise": 0.20, "fall": 0.30,
                        "bat_start": 4.10, "bat_drain": 0.0010, "bat_drain_flood": 0.0015,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
    {
        "node_id": "SIM-SOS-A2", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-A1", "depth": 2,
        "lat": 6.6494, "lng": 117.0690, "has_gps": True,
        "personality": {"base": 1, "max": 3, "rise": 0.25, "fall": 0.28,
                        "bat_start": 3.90, "bat_drain": 0.0012, "bat_drain_flood": 0.0018,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
    {
        "node_id": "SIM-SOS-A3", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-A2", "depth": 3,
        "lat": 6.6496, "lng": 117.0680, "has_gps": True,
        "personality": {"base": 1, "max": 3, "rise": 0.30, "fall": 0.25,
                        "bat_start": 4.00, "bat_drain": 0.0011, "bat_drain_flood": 0.0017,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
    {
        "node_id": "SIM-SOS-A4", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-A3", "depth": 4,
        "lat": 6.6498, "lng": 117.0670, "has_gps": False,
        "personality": {"base": 2, "max": 3, "rise": 0.35, "fall": 0.20,
                        "bat_start": 3.80, "bat_drain": 0.0015, "bat_drain_flood": 0.0020,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
    # ── SOSOP Chain B — NW tributary, usually calm ────────────────────────────
    {
        "node_id": "SIM-SOS-B1", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-MASTER", "depth": 1,
        "lat": 6.6500, "lng": 117.0710, "has_gps": True,
        "personality": {"base": 0, "max": 3, "rise": 0.15, "fall": 0.40,
                        "bat_start": 4.20, "bat_drain": 0.0009, "bat_drain_flood": 0.0013,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
    {
        "node_id": "SIM-SOS-B2", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-B1", "depth": 2,
        "lat": 6.6510, "lng": 117.0705, "has_gps": True,
        "personality": {"base": 0, "max": 3, "rise": 0.18, "fall": 0.35,
                        "bat_start": 4.00, "bat_drain": 0.0010, "bat_drain_flood": 0.0015,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
    {
        "node_id": "SIM-SOS-B3", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-B2", "depth": 3,
        "lat": 6.6518, "lng": 117.0698, "has_gps": False,
        "personality": {"base": 1, "max": 3, "rise": 0.22, "fall": 0.30,
                        "bat_start": 3.70, "bat_drain": 0.0011, "bat_drain_flood": 0.0016,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
    # ── SOSOP Chain C — downstream east, most persistent flooding ─────────────
    {
        "node_id": "SIM-SOS-C1", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-MASTER", "depth": 1,
        "lat": 6.6488, "lng": 117.0725, "has_gps": True,
        "personality": {"base": 2, "max": 3, "rise": 0.35, "fall": 0.15,
                        "bat_start": 4.10, "bat_drain": 0.0012, "bat_drain_flood": 0.0018,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
    {
        "node_id": "SIM-SOS-C2", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-C1", "depth": 2,
        "lat": 6.6485, "lng": 117.0738, "has_gps": True,
        "personality": {"base": 2, "max": 3, "rise": 0.40, "fall": 0.12,
                        "bat_start": 3.50, "bat_drain": 0.0013, "bat_drain_flood": 0.0019,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
    {
        "node_id": "SIM-SOS-C3", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-C2", "depth": 3,
        "lat": 6.6482, "lng": 117.0750, "has_gps": True,
        "personality": {"base": 2, "max": 3, "rise": 0.45, "fall": 0.10,
                        "bat_start": 3.30, "bat_drain": 0.0014, "bat_drain_flood": 0.0020,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
    # ── SOSOP Chain D — SW tributary ──────────────────────────────────────────
    {
        "node_id": "SIM-SOS-D1", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-MASTER", "depth": 1,
        "lat": 6.6480, "lng": 117.0700, "has_gps": True,
        "personality": {"base": 1, "max": 3, "rise": 0.20, "fall": 0.30,
                        "bat_start": 3.60, "bat_drain": 0.0010, "bat_drain_flood": 0.0015,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
    {
        "node_id": "SIM-SOS-D2", "village_id": SOSOP_VID,
        "parent_id": "SIM-SOS-D1", "depth": 2,
        "lat": 6.6470, "lng": 117.0692, "has_gps": False,
        "personality": {"base": 1, "max": 3, "rise": 0.22, "fall": 0.28,
                        "bat_start": 4.05, "bat_drain": 0.0012, "bat_drain_flood": 0.0017,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },

    # ── MANDAMAI Chain A — upstream west ──────────────────────────────────────
    {
        "node_id": "SIM-MAN-A1", "village_id": MANDAMAI_VID,
        "parent_id": "SIM-MAN-MASTER", "depth": 1,
        "lat": 6.6366, "lng": 117.0537, "has_gps": True,
        "personality": {"base": 1, "max": 3, "rise": 0.20, "fall": 0.30,
                        "bat_start": 4.20, "bat_drain": 0.0009, "bat_drain_flood": 0.0014,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
    {
        "node_id": "SIM-MAN-A2", "village_id": MANDAMAI_VID,
        "parent_id": "SIM-MAN-A1", "depth": 2,
        "lat": 6.6368, "lng": 117.0526, "has_gps": True,
        "personality": {"base": 1, "max": 3, "rise": 0.25, "fall": 0.27,
                        "bat_start": 3.85, "bat_drain": 0.0010, "bat_drain_flood": 0.0015,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
    {
        "node_id": "SIM-MAN-A3", "village_id": MANDAMAI_VID,
        "parent_id": "SIM-MAN-A2", "depth": 3,
        "lat": 6.6370, "lng": 117.0515, "has_gps": False,
        "personality": {"base": 1, "max": 3, "rise": 0.28, "fall": 0.24,
                        "bat_start": 3.40, "bat_drain": 0.0011, "bat_drain_flood": 0.0016,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
    # ── MANDAMAI Chain B — SW tributary, mostly dry ───────────────────────────
    {
        "node_id": "SIM-MAN-B1", "village_id": MANDAMAI_VID,
        "parent_id": "SIM-MAN-MASTER", "depth": 1,
        "lat": 6.6354, "lng": 117.0538, "has_gps": True,
        "personality": {"base": 0, "max": 3, "rise": 0.12, "fall": 0.40,
                        "bat_start": 4.15, "bat_drain": 0.0009, "bat_drain_flood": 0.0013,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
    {
        "node_id": "SIM-MAN-B2", "village_id": MANDAMAI_VID,
        "parent_id": "SIM-MAN-B1", "depth": 2,
        "lat": 6.6344, "lng": 117.0530, "has_gps": False,
        "personality": {"base": 0, "max": 3, "rise": 0.15, "fall": 0.35,
                        "bat_start": 3.75, "bat_drain": 0.0010, "bat_drain_flood": 0.0015,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
    # ── MANDAMAI Chain C — NW tributary ───────────────────────────────────────
    {
        "node_id": "SIM-MAN-C1", "village_id": MANDAMAI_VID,
        "parent_id": "SIM-MAN-MASTER", "depth": 1,
        "lat": 6.6376, "lng": 117.0548, "has_gps": True,
        "personality": {"base": 1, "max": 3, "rise": 0.18, "fall": 0.32,
                        "bat_start": 3.55, "bat_drain": 0.0010, "bat_drain_flood": 0.0015,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
    {
        "node_id": "SIM-MAN-C2", "village_id": MANDAMAI_VID,
        "parent_id": "SIM-MAN-C1", "depth": 2,
        "lat": 6.6386, "lng": 117.0547, "has_gps": True,
        "personality": {"base": 1, "max": 3, "rise": 0.22, "fall": 0.28,
                        "bat_start": 4.00, "bat_drain": 0.0011, "bat_drain_flood": 0.0016,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
    # ── MANDAMAI Chain D — downstream east, most persistent flooding ──────────
    {
        "node_id": "SIM-MAN-D1", "village_id": MANDAMAI_VID,
        "parent_id": "SIM-MAN-MASTER", "depth": 1,
        "lat": 6.6362, "lng": 117.0561, "has_gps": True,
        "personality": {"base": 2, "max": 3, "rise": 0.35, "fall": 0.15,
                        "bat_start": 3.95, "bat_drain": 0.0012, "bat_drain_flood": 0.0018,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
    {
        "node_id": "SIM-MAN-D2", "village_id": MANDAMAI_VID,
        "parent_id": "SIM-MAN-D1", "depth": 2,
        "lat": 6.6358, "lng": 117.0574, "has_gps": True,
        "personality": {"base": 2, "max": 3, "rise": 0.38, "fall": 0.12,
                        "bat_start": 3.65, "bat_drain": 0.0013, "bat_drain_flood": 0.0019,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
    {
        "node_id": "SIM-MAN-D3", "village_id": MANDAMAI_VID,
        "parent_id": "SIM-MAN-D2", "depth": 3,
        "lat": 6.6354, "lng": 117.0586, "has_gps": True,
        "personality": {"base": 2, "max": 3, "rise": 0.42, "fall": 0.10,
                        "bat_start": 3.30, "bat_drain": 0.0015, "bat_drain_flood": 0.0022,
                        "bat_low": 3.50, "bat_critical": 3.30},
    },
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _wl_to_fb(water_level: int) -> int:
    """Water level integer → float_bits bitmask (matches firmware encoding)."""
    if water_level >= 3: return 7   # 0b111
    if water_level >= 2: return 3   # 0b011
    if water_level >= 1: return 1   # 0b001
    return 0


def _fake_signal() -> tuple[int, float]:
    return random.randint(-105, -55), round(random.uniform(4.0, 12.0), 1)


def _sim_weather(rain_state: str) -> dict:
    """
    Predetermined weather for Pitas, Sabah (UTC+8).
    Simulates Malaysian tropical diurnal cycle, no API calls required.
    """
    now        = datetime.now(timezone.utc)
    local_hour = (now.hour + 8) % 24

    # Diurnal temperature: peak ~14:00, trough ~05:00 local
    temp_c = round(
        29.0 + 4.5 * math.sin(math.pi * max(0, local_hour - 5) / 14.0)
        + random.uniform(-0.5, 0.5), 1
    )
    temp_c = max(24.0, min(36.0, temp_c))

    humidity_boost = {"dry": 0, "light": 5, "moderate": 10, "heavy": 18}[rain_state]
    humidity = int(78 - (temp_c - 29) * 1.5 + humidity_boost + random.uniform(-2, 2))
    humidity = max(60, min(99, humidity))

    precip_ranges = {
        "dry":      (0.0,  0.0),
        "light":    (0.1,  2.5),
        "moderate": (2.5,  9.0),
        "heavy":    (9.0, 22.0),
    }
    lo, hi   = precip_ranges[rain_state]
    precip   = round(random.uniform(lo, hi), 1)
    rain_mm  = precip if rain_state in ("light", "moderate") else 0.0
    shower_mm = precip if rain_state == "heavy" else 0.0

    wcode_map = {"dry": random.choice([0, 1, 2]), "light": 51, "moderate": 63, "heavy": 80}
    wcode = wcode_map[rain_state]
    if rain_state == "dry" and not (6 <= local_hour < 19):
        wcode = 0

    cloud = {"dry": random.randint(5, 30), "light": random.randint(35, 60),
             "moderate": random.randint(65, 85), "heavy": random.randint(85, 99)}[rain_state]

    pressure = round(1010 + random.uniform(-3, 3), 1)

    return {
        "temperature_c":          temp_c,
        "humidity_pct":           humidity,
        "apparent_temperature_c": round(temp_c + random.uniform(0, 3), 1),
        "is_day":                 6 <= local_hour < 19,
        "precipitation_mm":       precip,
        "rain_mm":                round(rain_mm, 1),
        "showers_mm":             round(shower_mm, 1),
        "snowfall_cm":            0.0,
        "weather_code":           wcode,
        "cloud_cover_pct":        cloud,
        "pressure_msl_hpa":       pressure,
        "surface_pressure_hpa":   round(pressure - 5 + random.uniform(-1, 1), 1),
        "wind_speed_kmh":         round(random.uniform(2, 18), 1),
        "wind_direction_deg":     random.randint(0, 359),
        "wind_gusts_kmh":         round(random.uniform(8, 30), 1),
        "fetched_at":             now,
    }


# ── Village Rain State Machine ────────────────────────────────────────────────

class VillageRain:
    """
    Simple Markov-chain rainfall state for one village.
    Not a thread — owned and stepped by VillageCoordinator.
    """
    _TRANSITIONS: dict[str, list[tuple[str, float]]] = {
        "dry":      [("dry", 0.94), ("light", 0.06)],
        "light":    [("dry", 0.08), ("light", 0.80), ("moderate", 0.12)],
        "moderate": [("light", 0.12), ("moderate", 0.72), ("heavy", 0.16)],
        "heavy":    [("moderate", 0.25), ("heavy", 0.75)],
    }
    _RISE_BOOST:  dict[str, float] = {"dry": 0.0, "light": 0.30, "moderate": 0.80, "heavy": 1.80}
    _FALL_REDUCE: dict[str, float] = {"dry": 1.0, "light": 0.80, "moderate": 0.50, "heavy": 0.25}

    def __init__(self):
        self.state = "dry"

    def step(self, village_id: str):
        transitions = self._TRANSITIONS[self.state]
        r, total = random.random(), 0.0
        for next_state, prob in transitions:
            total += prob
            if r < total:
                if next_state != self.state:
                    log.info(f"Rain [{village_id}]: {self.state} → {next_state}")
                self.state = next_state
                return

    def rise_boost(self)  -> float: return self._RISE_BOOST[self.state]
    def fall_reduce(self) -> float: return self._FALL_REDUCE[self.state]


# ── SimNode (state machine, not a Thread) ────────────────────────────────────

class SimNode:
    """
    Holds per-node water-level and battery state.
    step_state() is called every coordinator tick for ALL nodes.
    transmit()   is called only for the selected ~1/3 that "report" this tick.
    """

    def __init__(self, node_def: dict):
        self.node_id    = node_def["node_id"]
        self.village_id = node_def["village_id"]
        self.parent_id  = node_def["parent_id"]
        self.depth      = node_def["depth"]
        self.lat        = node_def.get("lat")
        self.lng        = node_def.get("lng")
        self.has_gps    = node_def.get("has_gps", False)
        self.p          = node_def["personality"]

        # Water state
        self._water              = self.p["base"]
        self._prev_reported_water = self.p["base"]

        # Battery state machine
        # States: "draining" | "critical_hold" | "charging"
        self._battery     = self.p["bat_start"]
        self._bat_state   = "draining"
        self._hold_ticks  = 0       # countdown during critical_hold
        self._charge_goal = BAT_MAX_VOLTAGE

        # Alert flags — reset when a charge cycle completes
        self._bat_low_alerted  = False
        self._bat_crit_alerted = False

        # Per-type dedup: alert_type → monotonic timestamp of last send
        self._last_alert: dict[str, float] = {}

    # ── State machine ─────────────────────────────────────────────────────────

    def step_state(self, rain: VillageRain):
        """Advance water and battery state. Called every tick regardless of selection."""
        for _ in range(WATER_STEPS):
            self._step_water(rain)
        self._step_battery()

    def _step_water(self, rain: VillageRain):
        p      = self.p
        rise_p = min(0.90, p["rise"] * (1.0 + rain.rise_boost()))
        fall_p = min(0.90, p["fall"] * rain.fall_reduce())

        r = random.random()
        if   r < rise_p               and self._water < p["max"]: self._water += 1
        elif r < rise_p + fall_p      and self._water > 0:        self._water -= 1

    def _step_battery(self):
        p = self.p

        if self._bat_state == "draining":
            drain = (p["bat_drain_flood"] if self._water >= 2 else p["bat_drain"]) * DRAIN_MULT
            self._battery -= drain
            if self._battery <= p["bat_critical"]:
                self._battery    = p["bat_critical"]
                self._bat_state  = "critical_hold"
                self._hold_ticks = random.randint(BAT_HOLD_MIN, BAT_HOLD_MAX)
                log.debug(f"{self.node_id}: battery critical, hold={self._hold_ticks} ticks")

        elif self._bat_state == "critical_hold":
            self._hold_ticks -= 1
            if self._hold_ticks <= 0:
                increment         = random.uniform(BAT_CHARGE_MIN, BAT_CHARGE_MAX)
                self._charge_goal = min(BAT_MAX_VOLTAGE, self._battery + increment)
                self._bat_state   = "charging"
                log.debug(f"{self.node_id}: charging → goal={self._charge_goal:.2f}V")

        elif self._bat_state == "charging":
            self._battery = min(self._charge_goal, self._battery + BAT_CHARGE_RATE)
            if self._battery >= self._charge_goal:
                self._bat_state        = "draining"
                self._bat_low_alerted  = False   # re-arm for next drain cycle
                self._bat_crit_alerted = False
                log.debug(f"{self.node_id}: charge complete at {self._battery:.2f}V")

    # ── Transmission ──────────────────────────────────────────────────────────

    def transmit(self, now: datetime):
        """Write heartbeat to DB, publish SSE, fire any pending alerts."""
        prev_water = self._prev_reported_water
        curr_water = self._water
        bat        = round(self._battery, 2)
        fb         = _wl_to_fb(curr_water)
        gps_fix    = self.has_gps
        lat        = self.lat if gps_fix else None
        lng        = self.lng if gps_fix else None
        rssi, snr  = _fake_signal()
        ts         = now.isoformat()

        # ── MongoDB heartbeat ─────────────────────────────────────────────────
        col_heartbeats.insert_one({
            "node_id":         self.node_id,
            "village_id":      self.village_id,
            "timestamp":       now,
            "battery_voltage": bat,
            "float_bits":      fb,
            "water_level":     curr_water,
            "lat":             lat,
            "lng":             lng,
            "gps_fix":         gps_fix,
            "depth":           self.depth,
            "parent_id":       self.parent_id,
            "rssi":            rssi,
            "snr":             snr,
            "is_sample":       True,
        })

        # ── River node live state ─────────────────────────────────────────────
        river_set: dict = {
            "status":          "online",
            "last_seen":       now,
            "battery_voltage": bat,
            "float_bits":      fb,
            "water_level":     curr_water,
            "gps_fix":         gps_fix,
            "rssi":            rssi,
            "snr":             snr,
        }
        if gps_fix:
            river_set["lat"] = lat
            river_set["lng"] = lng
        col_rivers.update_one({"node_id": self.node_id}, {"$set": river_set})

        col_villages.update_one(
            {"village_id": self.village_id},
            {"$set": {"last_seen": now}},
        )

        # ── SSE heartbeat (matches real parser format exactly) ────────────────
        _publish("heartbeat", {
            "node_id":     self.node_id,
            "village_id":  self.village_id,
            "water_level": curr_water,
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

        # ── SSE flood_level when water level changed since last transmission ──
        if curr_water != prev_water:
            _publish("flood_level", {
                "node_id":          self.node_id,
                "village_id":       self.village_id,
                "water_level":      curr_water,
                "water_level_prev": prev_water,
                "float_bits":       fb,
                "lat":              lat,
                "lng":              lng,
                "gps_fix":          gps_fix,
                "timestamp":        ts,
                "is_sample":        True,
            })
            self._prev_reported_water = curr_water

        # ── Alert checks ──────────────────────────────────────────────────────
        self._check_alerts(now, curr_water, prev_water, fb, bat, lat, lng, gps_fix, rssi, snr, ts)

        log.debug(
            f"{self.node_id}  wl={curr_water}  bat={bat:.2f}V  [{self._bat_state}]"
        )

    def _can_alert(self, alert_type: str) -> bool:
        now  = time.monotonic()
        last = self._last_alert.get(alert_type, 0.0)
        if now - last >= ALERT_DEDUP_SECS:
            self._last_alert[alert_type] = now
            return True
        return False

    def _check_alerts(self, now, wl, prev_wl, fb, bat, lat, lng, gps_fix, rssi, snr, ts):
        p = self.p

        # Flood start: water just reached maximum
        if wl >= p["max"] and prev_wl < p["max"]:
            if self._can_alert("flood"):
                self._send_alert(now, "flood", wl, fb, bat, lat, lng, gps_fix, rssi, snr, ts)

        # Flood receding: coming down from maximum
        if prev_wl >= p["max"] and wl < p["max"]:
            if self._can_alert("water_fall"):
                self._send_alert(now, "water_fall", wl, fb, bat, lat, lng, gps_fix, rssi, snr, ts)

        # Battery alerts fire once per drain cycle (flags reset on charge complete)
        if not self._bat_crit_alerted and bat <= p["bat_critical"]:
            self._bat_crit_alerted = True
            if self._can_alert("battery_critical"):
                self._send_alert(now, "battery_critical", wl, fb, bat, lat, lng, gps_fix, rssi, snr, ts)
        elif not self._bat_low_alerted and bat <= p["bat_low"]:
            self._bat_low_alerted = True
            if self._can_alert("battery_low"):
                self._send_alert(now, "battery_low", wl, fb, bat, lat, lng, gps_fix, rssi, snr, ts)

    def _send_alert(self, now, alert_type, wl, fb, bat, lat, lng, gps_fix, rssi, snr, ts):
        village_doc      = col_villages.find_one({"village_id": self.village_id}, {"weather": 1})
        weather_snapshot = village_doc.get("weather") if village_doc else None

        alert_result = col_alerts.insert_one({
            "node_id":          self.node_id,
            "village_id":       self.village_id,
            "timestamp":        now,
            "alert_type":       alert_type,
            "level":            wl   if alert_type == "flood" else None,
            "float_bits":       fb   if alert_type == "flood" else None,
            "water_level":      wl   if alert_type == "flood" else None,
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

        # SSE alert — matches real parser format exactly
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
            "home_lat":   None,
            "home_lng":   None,
            "gps_fix":    gps_fix,
            "rssi":       rssi,
            "timestamp":  ts,
            "is_sample":  True,
        })

        log.info(f"ALERT  {self.node_id}  type={alert_type}  wl={wl}  bat={bat:.2f}V")


# ── Village Coordinator Thread ────────────────────────────────────────────────

class VillageCoordinator(threading.Thread):
    """
    One coordinator per village. Every TICK_INTERVAL seconds:
      - Steps all nodes' state machines
      - Selects ~1/3 to transmit (heartbeat + SSE)
      - Advances rain state every RAIN_TICK_EVERY ticks
      - Refreshes village weather every WEATHER_TICK_EVERY ticks
    """

    def __init__(self, village_id: str, nodes: list, rain: VillageRain):
        super().__init__(daemon=True, name=f"coord-{village_id}")
        self.village_id = village_id
        self.nodes      = nodes
        self.rain       = rain
        self._tick      = 0

    def run(self):
        # Push initial weather so village.weather is populated immediately
        self._update_weather()
        while True:
            time.sleep(TICK_INTERVAL)
            self._tick += 1
            try:
                self._cycle()
            except Exception as e:
                log.error(f"Coordinator [{self.village_id}] tick error: {e}", exc_info=True)

    def _cycle(self):
        # Step all nodes (water + battery)
        for node in self.nodes:
            node.step_state(self.rain)

        # Rain step
        if self._tick % RAIN_TICK_EVERY == 0:
            self.rain.step(self.village_id)

        # Weather refresh
        if self._tick % WEATHER_TICK_EVERY == 0:
            self._update_weather()

        # Select ~1/3 of nodes to transmit this tick
        n_select = max(1, len(self.nodes) // 3)
        selected = random.sample(self.nodes, n_select)

        now = datetime.now(timezone.utc)
        for node in selected:
            node.transmit(now)

        log.debug(
            f"[{self.village_id}] tick={self._tick}  "
            f"transmitted={len(selected)}/{len(self.nodes)}  rain={self.rain.state}"
        )

    def _update_weather(self):
        now     = datetime.now(timezone.utc)
        weather = _sim_weather(self.rain.state)

        col_villages.update_one(
            {"village_id": self.village_id},
            {"$set": {"weather": weather}},
        )

        _publish("weather_update", {
            "village_id": self.village_id,
            "timestamp":  now.isoformat(),
            "is_sample":  True,
            **{k: v for k, v in weather.items() if k != "fetched_at"},
        })

        log.debug(
            f"Weather [{self.village_id}]: code={weather['weather_code']}  "
            f"{weather['temperature_c']}°C  rain={self.rain.state}"
        )


# ── DB Topology Setup ─────────────────────────────────────────────────────────

def _build_chains(village_id: str) -> list[dict]:
    chains: dict[str, list[str]] = {}
    for n in NODES:
        if n["village_id"] != village_id:
            continue
        letter = n["node_id"].split("-")[-1][0]   # "SIM-SOS-A1" → "A"
        chains.setdefault(letter, []).append(n["node_id"])
    return [{"id": k, "nodes": v} for k, v in sorted(chains.items())]


def _setup_topology():
    log.info("Setting up DB topology ...")
    now = datetime.now(timezone.utc)

    for v in VILLAGES:
        vid          = v["village_id"]
        vnodes       = [n for n in NODES if n["village_id"] == vid]
        master       = next(m for m in MASTERS if m["village_id"] == vid)

        col_villages.update_one(
            {"village_id": vid},
            {
                "$set": {
                    **v,
                    "topology": {
                        "master":     master["node_id"],
                        "node_count": len(vnodes),
                        "chains":     _build_chains(vid),
                    },
                    "total_nodes":         len(vnodes),
                    "alerts_by_type":      {},
                    "active_alerts_count": 0,
                },
                "$setOnInsert": {"total_alerts": 0, "first_seen": now, "last_seen": now},
            },
            upsert=True,
        )
        log.info(f"  Village {vid} ({len(vnodes)} nodes)")

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
            node_set["lat"] = node_set["install_lat"] = n["lat"]
            node_set["lng"] = node_set["install_lng"] = n["lng"]

        col_rivers.update_one(
            {"node_id": n["node_id"]},
            {
                "$set":         node_set,
                "$setOnInsert": {"first_seen": now, "alert_counts": {}},
            },
            upsert=True,
        )

        # node_announce SSE so the API/clients see the node come online
        col_events.insert_one({
            "event_type": "announce",
            "node_id":    n["node_id"],
            "village_id": n["village_id"],
            "timestamp":  now,
            "is_sample":  True,
            "data": {
                "depth":  n["depth"],
                "parent": n["parent_id"],
                "lat":    n["lat"] if gps_fix else None,
                "lng":    n["lng"] if gps_fix else None,
            },
        })
        _publish("node_announce", {
            "node_id":    n["node_id"],
            "village_id": n["village_id"],
            "depth":      n["depth"],
            "parent":     n["parent_id"],
            "lat":        n["lat"] if gps_fix else None,
            "lng":        n["lng"] if gps_fix else None,
            "timestamp":  now.isoformat(),
            "is_sample":  True,
        })

    log.info("Topology ready.")


def _clear_sample_data():
    log.info("Clearing sample data ...")
    for col in [col_villages, col_masters, col_rivers, col_heartbeats, col_alerts, col_events]:
        r = col.delete_many({"is_sample": True})
        log.info(f"  {col.name}: deleted {r.deleted_count}")


# ── Entrypoint ────────────────────────────────────────────────────────────────

def main():
    log.info(
        f"FloodWatch Simulator — mode={'DEMO' if DEMO_MODE else 'normal'}  "
        f"tick={TICK_INTERVAL}s  drain_mult={DRAIN_MULT}x  water_steps={WATER_STEPS}"
    )

    if RESET_MODE:
        _clear_sample_data()

    _setup_topology()

    coordinators: list[VillageCoordinator] = []
    for v in VILLAGES:
        vid   = v["village_id"]
        nodes = [SimNode(n) for n in NODES if n["village_id"] == vid]
        rain  = VillageRain()
        coord = VillageCoordinator(vid, nodes, rain)
        coord.start()
        coordinators.append(coord)
        log.info(f"Coordinator started: {vid} ({len(nodes)} nodes)")

    log.info(f"Simulator running — {sum(len(c.nodes) for c in coordinators)} nodes total")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("Simulator stopped.")


if __name__ == "__main__":
    main()
