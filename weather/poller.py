#!/usr/bin/env python3
"""
FloodWatch Weather Poller

Polls Open-Meteo (free, no API key) every WEATHER_POLL_INTERVAL seconds for
every village that has lat/lng set. Writes:

  villages.weather          — latest current conditions snapshot (overwritten each poll)
  villages.weather_forecast — next WEATHER_FORECAST_HOURS hourly forecast (overwritten)
  weather_history           — point-in-time current conditions only, TTL 90 days

Publishes Redis event "weather_update" per village so the API can push SSE.

The parser reads villages.weather when writing alerts to embed weather_at_alert.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
import redis as _redis_lib
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING, DESCENDING

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("weather")

# ── Config ────────────────────────────────────────────────────────────────────

MONGO_URI      = os.getenv("MONGO_URI")
MONGO_DB       = os.getenv("MONGO_DB", "flood_monitor")
REDIS_URL      = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_CHANNEL  = "floodwatch:events"
POLL_INTERVAL  = int(os.getenv("WEATHER_POLL_INTERVAL",  1800))  # seconds (30 min)
FORECAST_HOURS = int(os.getenv("WEATHER_FORECAST_HOURS", 24))    # hours of forecast to keep
CALL_SPACING   = float(os.getenv("WEATHER_CALL_SPACING", 1.0))   # seconds between API calls

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# All current-condition variables we want (skipping soil and upper-alt wind/temp)
_CURRENT_PARAMS = ",".join([
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "is_day",
    "precipitation",
    "rain",
    "showers",
    "snowfall",
    "weather_code",
    "cloud_cover",
    "pressure_msl",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
])

# Hourly forecast variables (same exclusions)
_HOURLY_PARAMS = ",".join([
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "precipitation_probability",
    "precipitation",
    "rain",
    "showers",
    "snowfall",
    "snow_depth",
    "weather_code",
    "pressure_msl",
    "surface_pressure",
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "visibility",
    "vapour_pressure_deficit",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
])

# Open-Meteo API name → clean DB field name
_CURRENT_MAP = {
    "temperature_2m":       "temperature_c",
    "relative_humidity_2m": "humidity_pct",
    "apparent_temperature": "apparent_temperature_c",
    "is_day":               "is_day",
    "precipitation":        "precipitation_mm",
    "rain":                 "rain_mm",
    "showers":              "showers_mm",
    "snowfall":             "snowfall_cm",
    "weather_code":         "weather_code",
    "cloud_cover":          "cloud_cover_pct",
    "pressure_msl":         "pressure_msl_hpa",
    "surface_pressure":     "surface_pressure_hpa",
    "wind_speed_10m":       "wind_speed_kmh",
    "wind_direction_10m":   "wind_direction_deg",
    "wind_gusts_10m":       "wind_gusts_kmh",
}

_HOURLY_MAP = {
    "temperature_2m":            "temperature_c",
    "relative_humidity_2m":      "humidity_pct",
    "dew_point_2m":              "dew_point_c",
    "apparent_temperature":      "apparent_temperature_c",
    "precipitation_probability": "precip_prob_pct",
    "precipitation":             "precipitation_mm",
    "rain":                      "rain_mm",
    "showers":                   "showers_mm",
    "snowfall":                  "snowfall_cm",
    "snow_depth":                "snow_depth_m",
    "weather_code":              "weather_code",
    "pressure_msl":              "pressure_msl_hpa",
    "surface_pressure":          "surface_pressure_hpa",
    "cloud_cover":               "cloud_cover_pct",
    "cloud_cover_low":           "cloud_cover_low_pct",
    "cloud_cover_mid":           "cloud_cover_mid_pct",
    "cloud_cover_high":          "cloud_cover_high_pct",
    "visibility":                "visibility_m",
    "vapour_pressure_deficit":   "vapour_pressure_deficit_kpa",
    "wind_speed_10m":            "wind_speed_kmh",
    "wind_direction_10m":        "wind_direction_deg",
    "wind_gusts_10m":            "wind_gusts_kmh",
}

# ── MongoDB ───────────────────────────────────────────────────────────────────

mongo        = MongoClient(MONGO_URI)
db           = mongo[MONGO_DB]
col_villages = db["villages"]
col_weather  = db["weather_history"]


def _setup_indexes():
    col_weather.create_index([("village_id", ASCENDING), ("timestamp", DESCENDING)])
    col_weather.create_index("timestamp")
    log.info("Indexes verified")


# ── Redis ─────────────────────────────────────────────────────────────────────

_redis = _redis_lib.from_url(REDIS_URL, decode_responses=True)


def _publish(event_type: str, data: dict):
    data["type"] = event_type
    _redis.publish(REDIS_CHANNEL, json.dumps(data, default=str))


# ── Open-Meteo fetch ──────────────────────────────────────────────────────────

def _parse_current(raw: dict, now: datetime) -> dict:
    result = {}
    for api_key, clean_key in _CURRENT_MAP.items():
        val = raw.get(api_key)
        # is_day comes back as 0/1 integer
        if api_key == "is_day":
            val = bool(val)
        result[clean_key] = val
    result["fetched_at"] = now
    return result


def _parse_forecast(hourly: dict, from_utc: datetime) -> list[dict]:
    """
    Extract the next FORECAST_HOURS hourly entries starting from from_utc
    (rounded down to the current hour). forecast_days=2 ensures we always
    have enough future entries even when polling late in the day.
    """
    times  = hourly.get("time", [])
    cutoff = from_utc.replace(minute=0, second=0, microsecond=0)

    entries = []
    for i, t_str in enumerate(times):
        t = datetime.fromisoformat(t_str).replace(tzinfo=timezone.utc)
        if t < cutoff:
            continue
        if len(entries) >= FORECAST_HOURS:
            break
        entry: dict = {"hour": t}
        for api_key, clean_key in _HOURLY_MAP.items():
            col_data = hourly.get(api_key)
            entry[clean_key] = col_data[i] if col_data else None
        entries.append(entry)

    return entries


def fetch_weather(lat: float, lng: float) -> tuple[dict, list] | None:
    """
    Call Open-Meteo and return (current_dict, forecast_list).
    Returns None on any error — caller should skip and retry next cycle.
    """
    try:
        resp = requests.get(OPEN_METEO_URL, params={
            "latitude":      lat,
            "longitude":     lng,
            "current":       _CURRENT_PARAMS,
            "hourly":        _HOURLY_PARAMS,
            "forecast_days": 2,      # ensures 24h ahead even late in the day
            "timezone":      "UTC",
        }, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error(f"Open-Meteo request error: {e}")
        return None

    now      = datetime.now(timezone.utc)
    current  = _parse_current(data.get("current", {}), now)
    forecast = _parse_forecast(data.get("hourly", {}), now)
    return current, forecast


# ── Poll cycle ────────────────────────────────────────────────────────────────

def _poll_all():
    villages = list(col_villages.find(
        {"lat": {"$exists": True}, "lng": {"$exists": True}},
        {"village_id": 1, "lat": 1, "lng": 1}
    ))

    if not villages:
        log.info("No villages with GPS coordinates yet — skipping poll")
        return

    log.info(f"Polling weather for {len(villages)} village(s) ...")

    for v in villages:
        vid = v["village_id"]
        lat = v["lat"]
        lng = v["lng"]

        result = fetch_weather(lat, lng)
        if result is None:
            log.warning(f"  {vid}  skipped (API error)")
            time.sleep(CALL_SPACING)
            continue

        current, forecast = result
        now = current["fetched_at"]

        # Update village: latest snapshot + forecast (overwrite each poll)
        col_villages.update_one(
            {"village_id": vid},
            {"$set": {
                "weather":          current,
                "weather_forecast": forecast,
            }}
        )

        # Historical record: current conditions only (forecast is transient)
        col_weather.insert_one({
            "village_id": vid,
            "timestamp":  now,
            "lat":        lat,
            "lng":        lng,
            # Spread current fields at top level for easy range queries
            # e.g. find all readings where rain_mm > 5
            **{k: v for k, v in current.items() if k != "fetched_at"},
        })

        _publish("weather_update", {
            "village_id": vid,
            "timestamp":  now.isoformat(),
            **{k: v for k, v in current.items() if k != "fetched_at"},
        })

        log.info(
            f"  {vid}  {current.get('temperature_c')}°C  "
            f"rain={current.get('rain_mm')}mm  "
            f"humidity={current.get('humidity_pct')}%  "
            f"code={current.get('weather_code')}  "
            f"is_day={current.get('is_day')}"
        )

        time.sleep(CALL_SPACING)


# ── Entrypoint ────────────────────────────────────────────────────────────────

log.info(f"MongoDB: {MONGO_DB}")
_setup_indexes()
log.info(f"Redis: {REDIS_URL}")
log.info(f"Poll interval: {POLL_INTERVAL}s  Forecast: {FORECAST_HOURS}h  Spacing: {CALL_SPACING}s")

while True:
    cycle_start = time.time()
    try:
        _poll_all()
    except Exception as e:
        log.error(f"Poll cycle error: {e}", exc_info=True)

    elapsed = time.time() - cycle_start
    wait    = max(0, POLL_INTERVAL - elapsed)
    log.info(f"Cycle done in {elapsed:.1f}s — next poll in {wait:.0f}s")
    time.sleep(wait)
