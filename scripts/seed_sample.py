#!/usr/bin/env python3
"""
FloodWatch — Sample Data Seed Script

Inserts pre-generated demo documents with is_sample=True into the live database.
All real collections (villages, river_nodes, heartbeats, alerts, events,
weather_history) are populated. Real data is never touched.

Run:
    python scripts/seed_sample.py            # insert sample data
    python scripts/seed_sample.py --clear    # remove all sample data only

Safe to re-run: --clear first, then re-inserts. Use --clear alone to clean up.

The "stuck in time" design: nodes appear online with a status of "online" and
have 30 days of heartbeat history, but their last_seen is frozen at FROZEN_AT.
The dashboard will show them as online with a rich history — but nothing new arrives.
"""

import argparse
import os
import random
import sys
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING, DESCENDING

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB  = os.getenv("MONGO_DB", "flood_monitor")
RNG_SEED  = 42                                                   # reproducible output

# The moment in time at which all sample nodes are "frozen"
FROZEN_AT = datetime(2026, 5, 10, 8, 30, 0, tzinfo=timezone.utc)
HISTORY_DAYS       = 30          # how many days of heartbeat history to generate
HB_INTERVAL_MIN    = 30          # minutes between heartbeats (matches real firmware)
BATTERY_DRAIN_DAYS = 5           # days before battery "recharges"

# ── Village + node topology ───────────────────────────────────────────────────
# lat/lng are real Malaysian coordinates but villages are fictional

SAMPLE_VILLAGES = [
    {
        "village_id": "DEMO-SUNGAI-BATU",
        "master_id":  "M-DEMO-SB",
        "lat": 3.1390, "lng": 101.6869,   # Kuala Lumpur
        "nodes": [
            {"node_id": "DEMO-SB-001", "depth": 1, "parent": "M-DEMO-SB",  "has_gps": True},
            {"node_id": "DEMO-SB-002", "depth": 1, "parent": "M-DEMO-SB",  "has_gps": True},
            {"node_id": "DEMO-SB-003", "depth": 2, "parent": "DEMO-SB-001","has_gps": False},
        ],
    },
    {
        "village_id": "DEMO-KAMPUNG-RAJA",
        "master_id":  "M-DEMO-KR",
        "lat": 3.2115, "lng": 101.5975,   # Petaling Jaya
        "nodes": [
            {"node_id": "DEMO-KR-001", "depth": 1, "parent": "M-DEMO-KR",  "has_gps": True},
            {"node_id": "DEMO-KR-002", "depth": 1, "parent": "M-DEMO-KR",  "has_gps": True},
            {"node_id": "DEMO-KR-003", "depth": 2, "parent": "DEMO-KR-001","has_gps": True},
            {"node_id": "DEMO-KR-004", "depth": 2, "parent": "DEMO-KR-001","has_gps": False},
        ],
    },
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def compute_water_level(float_bits: int) -> int:
    if float_bits & 0x04: return 3
    if float_bits & 0x02: return 2
    if float_bits & 0x01: return 1
    return 0


def water_level_to_float_bits(level: int) -> int:
    return {0: 0x00, 1: 0x01, 2: 0x03, 3: 0x07}[level]


def _jitter(base_lat, base_lng, rng, radius=0.002):
    """Scatter node GPS slightly around the village centre."""
    return (
        round(base_lat + rng.uniform(-radius, radius), 6),
        round(base_lng + rng.uniform(-radius, radius), 6),
    )


def _battery_at(t: datetime, first_seen: datetime, rng) -> float:
    """Simulate a slow drain + recharge cycle."""
    elapsed_days = (t - first_seen).total_seconds() / 86400
    cycle = elapsed_days % BATTERY_DRAIN_DAYS
    fraction = cycle / BATTERY_DRAIN_DAYS          # 0.0 → fully charged, 1.0 → depleted
    base = 12.6 - fraction * 0.9                   # 12.6V → 11.7V over a cycle
    return round(base + rng.uniform(-0.05, 0.05), 2)


def _water_sequence(n: int, rng) -> list[int]:
    """
    Generate n water level values with realistic behaviour:
    mostly 0-1, occasional multi-step rises to 3 (flood events), then gradual retreat.
    """
    levels = []
    level  = rng.choice([0, 0, 0, 1])   # start calm
    flood_countdown = 0

    for _ in range(n):
        if flood_countdown > 0:
            # Rising or staying at flood level
            if flood_countdown > n // 6 and level < 3:
                level = min(3, level + 1)
            elif flood_countdown < 4:
                level = max(0, level - 1)
            flood_countdown -= 1
        else:
            # Random walk with strong mean-reversion toward 0-1
            if level == 0:
                level = rng.choice([0, 0, 0, 0, 1])
            elif level == 1:
                level = rng.choice([0, 1, 1, 1, 2])
            elif level == 2:
                level = rng.choice([1, 1, 2, 2, 3])
            elif level == 3:
                level = rng.choice([2, 2, 3])

            # Occasionally trigger a flood event (roughly once per 10 days)
            if rng.random() < (HB_INTERVAL_MIN / (10 * 24 * 60)):
                flood_countdown = rng.randint(8, 20)

        levels.append(level)
    return levels


# ── Weather generation ────────────────────────────────────────────────────────

_WMO_CLEAR    = [0, 1]
_WMO_CLOUDY   = [2, 3]
_WMO_DRIZZLE  = [51, 53]
_WMO_RAIN     = [61, 63, 80, 81]
_WMO_TSTORM   = [95, 96]


def _fake_weather(t: datetime, rng) -> dict:
    """Generate plausible Malaysian tropical weather for a given UTC time."""
    hour  = t.hour
    month = t.month

    # Malaysia: hotter Mar-Apr, wetter Oct-Jan, humid year-round
    base_temp = 29.0 + (1.5 if month in (3, 4) else -1.0 if month in (11, 12, 1) else 0)
    is_day    = 6 <= hour <= 18

    rain_prob = 0.35 if month in (10, 11, 12, 1) else 0.20
    rain_mm   = 0.0
    code      = rng.choice(_WMO_CLEAR + _WMO_CLOUDY)

    if is_day and rng.random() < rain_prob:
        code    = rng.choice(_WMO_RAIN + _WMO_TSTORM)
        rain_mm = round(rng.uniform(1.0, 25.0), 1)

    temp = round(base_temp + (2.0 if is_day else -3.0) + rng.uniform(-1.0, 1.0), 1)

    return {
        "temperature_c":          temp,
        "humidity_pct":           round(rng.uniform(72, 92), 0),
        "apparent_temperature_c": round(temp + rng.uniform(1.5, 4.5), 1),
        "is_day":                 is_day,
        "precipitation_mm":       round(rain_mm + rng.uniform(0, 0.5), 1),
        "rain_mm":                round(rain_mm, 1),
        "showers_mm":             0.0,
        "snowfall_cm":            0.0,
        "weather_code":           code,
        "cloud_cover_pct":        rng.randint(10, 95) if rain_mm > 0 else rng.randint(5, 50),
        "pressure_msl_hpa":       round(rng.uniform(1009.0, 1014.0), 1),
        "surface_pressure_hpa":   round(rng.uniform(1007.0, 1012.0), 1),
        "wind_speed_kmh":         round(rng.uniform(5.0, 25.0), 1),
        "wind_direction_deg":     rng.randint(0, 359),
        "wind_gusts_kmh":         round(rng.uniform(10.0, 40.0), 1),
        "fetched_at":             t,
    }


def _fake_forecast(from_t: datetime, rng) -> list[dict]:
    """Generate 24 hours of hourly forecast entries."""
    entries = []
    for h in range(24):
        t     = from_t.replace(minute=0, second=0, microsecond=0) + timedelta(hours=h)
        w     = _fake_weather(t, rng)
        entry = {"hour": t}
        entry.update({k: v for k, v in w.items() if k != "fetched_at"})
        entry["dew_point_c"]                   = round(w["temperature_c"] - rng.uniform(2, 5), 1)
        entry["precip_prob_pct"]               = rng.randint(10, 80) if w["rain_mm"] > 0 else rng.randint(0, 25)
        entry["snow_depth_m"]                  = 0.0
        entry["cloud_cover_low_pct"]           = rng.randint(0, 40)
        entry["cloud_cover_mid_pct"]           = rng.randint(0, 40)
        entry["cloud_cover_high_pct"]          = rng.randint(0, 30)
        entry["visibility_m"]                  = rng.randint(5000, 10000)
        entry["vapour_pressure_deficit_kpa"]   = round(rng.uniform(0.3, 1.8), 2)
        entries.append(entry)
    return entries


# ── Seeding ───────────────────────────────────────────────────────────────────

def seed(db, rng: random.Random):
    print("Seeding sample data ...")
    S = True   # shorthand for is_sample flag

    history_start = FROZEN_AT - timedelta(days=HISTORY_DAYS)
    heartbeat_times = []
    t = history_start
    while t <= FROZEN_AT:
        heartbeat_times.append(t)
        t += timedelta(minutes=HB_INTERVAL_MIN)

    total_hb      = 0
    total_alerts  = 0
    total_events  = 0
    total_weather = 0

    for village in SAMPLE_VILLAGES:
        vid        = village["village_id"]
        mid        = village["master_id"]
        vlat       = village["lat"]
        vlng       = village["lng"]
        nodes      = village["nodes"]
        node_count = len(nodes)

        print(f"  {vid}  ({node_count} nodes) ...")

        # ── Master node ───────────────────────────────────────────────────────
        db["master_nodes"].insert_one({
            "node_id":    mid,
            "village_id": vid,
            "status":     "online",
            "first_seen": history_start,
            "last_seen":  FROZEN_AT,
            "is_sample":  S,
        })

        # ── Events: master_online ─────────────────────────────────────────────
        db["events"].insert_one({
            "event_type": "master_online",
            "node_id":    mid,
            "village_id": vid,
            "timestamp":  history_start,
            "data":       {},
            "is_sample":  S,
        })
        total_events += 1

        # ── River nodes + per-node history ────────────────────────────────────
        node_states      = {}   # node_id → current live state
        alerts_by_type   = {}
        total_nodes_online = 0

        for node in nodes:
            nid     = node["node_id"]
            has_gps = node["has_gps"]
            nlat, nlng = _jitter(vlat, vlng, rng) if has_gps else (0.0, 0.0)

            water_levels = _water_sequence(len(heartbeat_times), rng)
            prev_level   = water_levels[0]
            alert_counts: dict = {}
            last_alert_id = None
            bat_low_sent  = False

            hb_docs     = []
            alert_docs  = []
            event_docs  = []

            # node_online event
            event_docs.append({
                "event_type": "node_online",
                "node_id":    nid,
                "village_id": vid,
                "timestamp":  history_start + timedelta(minutes=rng.randint(1, 10)),
                "data":       {},
                "is_sample":  S,
            })
            # announce event
            event_docs.append({
                "event_type": "announce",
                "node_id":    nid,
                "village_id": vid,
                "timestamp":  history_start + timedelta(minutes=rng.randint(11, 20)),
                "data":       {
                    "depth":  node["depth"],
                    "parent": node["parent"],
                    "lat":    nlat,
                    "lng":    nlng,
                    "rssi":   rng.randint(-80, -30),
                    "snr":    round(rng.uniform(3.0, 12.0), 1),
                },
                "is_sample":  S,
            })

            for i, ts in enumerate(heartbeat_times):
                level    = water_levels[i]
                fb       = water_level_to_float_bits(level)
                bat      = _battery_at(ts, history_start, rng)
                rssi     = rng.randint(-90, -25)
                snr      = round(rng.uniform(2.0, 12.0), 1)

                hb_docs.append({
                    "node_id":         nid,
                    "village_id":      vid,
                    "timestamp":       ts,
                    "battery_voltage": bat,
                    "float_bits":      fb,
                    "water_level":     level,
                    "lat":             nlat if has_gps else None,
                    "lng":             nlng if has_gps else None,
                    "gps_fix":         has_gps,
                    "depth":           node["depth"],
                    "parent_id":       node["parent"],
                    "rssi":            rssi,
                    "snr":             snr,
                    "is_sample":       S,
                })

                # Flood alert on rising edge to level 3
                if level == 3 and prev_level < 3:
                    doc = {
                        "node_id":         nid,
                        "village_id":      vid,
                        "timestamp":       ts,
                        "alert_type":      "flood",
                        "level":           3,
                        "float_bits":      fb,
                        "water_level":     3,
                        "battery_voltage": bat,
                        "dist_m":          None,
                        "lat":             nlat if has_gps else None,
                        "lng":             nlng if has_gps else None,
                        "home_lat":        None,
                        "home_lng":        None,
                        "gps_fix":         has_gps,
                        "rssi":            rssi,
                        "snr":             snr,
                        "weather_at_alert": None,
                        "is_sample":       S,
                    }
                    alert_docs.append(doc)
                    alert_counts["flood"] = alert_counts.get("flood", 0) + 1

                # Battery alert (once per drain cycle, around 11.8V)
                if bat <= 11.85 and not bat_low_sent:
                    doc = {
                        "node_id":         nid,
                        "village_id":      vid,
                        "timestamp":       ts,
                        "alert_type":      "battery",
                        "level":           None,
                        "float_bits":      None,
                        "water_level":     None,
                        "battery_voltage": bat,
                        "dist_m":          None,
                        "lat":             nlat if has_gps else None,
                        "lng":             nlng if has_gps else None,
                        "home_lat":        None,
                        "home_lng":        None,
                        "gps_fix":         has_gps,
                        "rssi":            rssi,
                        "snr":             snr,
                        "weather_at_alert": None,
                        "is_sample":       S,
                    }
                    alert_docs.append(doc)
                    alert_counts["battery"] = alert_counts.get("battery", 0) + 1
                    bat_low_sent = True
                if bat > 12.3:
                    bat_low_sent = False   # reset after recharge

                prev_level = level

            if hb_docs:
                db["heartbeats"].insert_many(hb_docs)
                total_hb += len(hb_docs)

            if alert_docs:
                result = db["alerts"].insert_many(alert_docs)
                last_alert_id = result.inserted_ids[-1]
                total_alerts += len(alert_docs)
                for k, v in alert_counts.items():
                    alerts_by_type[k] = alerts_by_type.get(k, 0) + v

            if event_docs:
                db["events"].insert_many(event_docs)
                total_events += len(event_docs)

            final_level = water_levels[-1]
            final_fb    = water_level_to_float_bits(final_level)
            final_bat   = _battery_at(FROZEN_AT, history_start, rng)

            node_states[nid] = {
                "level": final_level,
                "fb":    final_fb,
                "bat":   final_bat,
            }

            db["river_nodes"].insert_one({
                "node_id":         nid,
                "village_id":      vid,
                "parent_id":       node["parent"],
                "depth":           node["depth"],
                "status":          "online",
                "first_seen":      history_start,
                "last_seen":       FROZEN_AT,
                "battery_voltage": final_bat,
                "float_bits":      final_fb,
                "water_level":     final_level,
                "gps_fix":         has_gps,
                "lat":             nlat if has_gps else None,
                "lng":             nlng if has_gps else None,
                "install_lat":     nlat if has_gps else None,
                "install_lng":     nlng if has_gps else None,
                "rssi":            rng.randint(-80, -30),
                "snr":             round(rng.uniform(3.0, 12.0), 1),
                "last_alert_id":   last_alert_id,
                "alert_counts":    alert_counts,
                "is_sample":       S,
            })
            total_nodes_online += 1

        # ── Weather history for village ───────────────────────────────────────
        weather_docs = []
        last_w: dict = {}
        wt = history_start
        while wt <= FROZEN_AT:
            w = _fake_weather(wt, rng)
            # Change-only storage: write if any field differs
            if any(w.get(k) != last_w.get(k) for k in w if k != "fetched_at"):
                weather_docs.append({
                    "village_id": vid,
                    "timestamp":  wt,
                    "lat":        vlat,
                    "lng":        vlng,
                    **{k: v for k, v in w.items() if k != "fetched_at"},
                    "is_sample":  S,
                })
                last_w = dict(w)
            wt += timedelta(hours=1)

        if weather_docs:
            db["weather_history"].insert_many(weather_docs)
            total_weather += len(weather_docs)

        current_weather = _fake_weather(FROZEN_AT, rng)
        forecast        = _fake_forecast(FROZEN_AT, rng)

        # ── Village document ──────────────────────────────────────────────────
        topology = {mid: {n["node_id"]: {} for n in nodes if n["depth"] == 1}}
        for n in nodes:
            if n["depth"] == 2:
                parent = n["parent"]
                if parent in topology[mid]:
                    topology[mid][parent][n["node_id"]] = {}

        db["villages"].insert_one({
            "village_id":       vid,
            "master_id":        mid,
            "status":           "online",
            "first_seen":       history_start,
            "last_seen":        FROZEN_AT,
            "lat":              vlat,
            "lng":              vlng,
            "topology":         topology,
            "total_nodes":      node_count,
            "nodes_online":     total_nodes_online,
            "nodes_offline":    0,
            "total_alerts":     total_alerts,
            "alerts_by_type":   alerts_by_type,
            "weather":          {k: v for k, v in current_weather.items()},
            "weather_forecast": forecast,
            "is_sample":        S,
        })

        print(f"    heartbeats={len(heartbeat_times) * node_count}  "
              f"alerts={total_alerts}  weather_records={total_weather}")

    print(f"\nDone — {total_hb} heartbeats, {total_alerts} alerts, "
          f"{total_events} events, {total_weather} weather records inserted.")


def clear(db):
    print("Clearing existing sample data ...")
    for col_name in ["villages", "master_nodes", "river_nodes",
                     "heartbeats", "alerts", "events", "weather_history"]:
        result = db[col_name].delete_many({"is_sample": True})
        print(f"  {col_name}: removed {result.deleted_count} documents")
    print("Done.")


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed or clear FloodWatch sample data")
    parser.add_argument("--clear", action="store_true", help="Remove all sample data and exit")
    args = parser.parse_args()

    if not MONGO_URI:
        print("ERROR: MONGO_URI not set. Add it to .env or export it.", file=sys.stderr)
        sys.exit(1)

    mongo = MongoClient(MONGO_URI)
    db    = mongo[MONGO_DB]
    rng   = random.Random(RNG_SEED)

    clear(db)
    if not args.clear:
        seed(db, rng)
