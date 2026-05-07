#!/usr/bin/env python3
"""
MQTT region latency + stress tester — FloodWatch FYP.

Run from Malaysia (or wherever sensors are deployed) for representative results.
Each run is one round, auto-numbered. Run 3 times then generate charts.

Usage:
    pip install paho-mqtt requests
    python scripts/test-region-latency.py            # one round, auto-numbered
    python scripts/test-region-latency.py --cleanup  # destroy test brokers after
"""

import argparse
import glob
import json
import os
import socket
import statistics
import sys
import threading
import time
import uuid
import requests

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("Missing dependency. Run: pip install paho-mqtt")
    sys.exit(1)


# ============================================================
#  LATENCY_REGIONS — tested every round for all 17 regions.
#  Comment out regions whose test broker you haven't deployed.
#  Unreachable brokers are automatically skipped with a note.
# ============================================================
LATENCY_REGIONS = [
    "sin",   # Singapore     ← uses main broker, always available
    "nrt",   # Tokyo, Japan
    "syd",   # Sydney, Australia
    "bom",   # Mumbai, India
    "lhr",   # London, UK
    "fra",   # Frankfurt, Germany
    "arn",   # Stockholm, Sweden
    "cdg",   # Paris, France
    "iad",   # Ashburn, Virginia (US)
    "ewr",   # Secaucus, NJ (US)
    "dfw",   # Dallas, Texas (US)
    "lax",   # Los Angeles, California (US)
    "sjc",   # San Jose, California (US)
    "ord",   # Chicago, Illinois (US)
    "yyz",   # Toronto, Canada
    "gru",   # São Paulo, Brazil
    "jnb",   # Johannesburg, South Africa
]

# ============================================================
#  STRESS_REGIONS — subset for throughput testing.
#  Stress tests broker capacity (same hardware everywhere) so
#  a few representative regions is enough.
# ============================================================
STRESS_REGIONS = [
    "sin",   # Singapore — primary candidate
    "nrt",   # Tokyo — nearest Asia alternative
    "lhr",   # London — Europe baseline
    "iad",   # Ashburn — US East baseline
    "lax",   # Los Angeles — US West baseline
]

LATENCY_COUNT   = 30    # messages per latency test (paced, 50 ms apart)
STRESS_COUNT    = 200   # messages per stress test (blasted all at once)
PORT            = 1883
TEST_TOPIC_BASE = "flood/latency-test"
RESULTS_DIR     = "results"

# ── Region metadata ───────────────────────────────────────────────────────────
REGION_INFO = {
    "sin": "Singapore",
    "nrt": "Tokyo, Japan",
    "syd": "Sydney, Australia",
    "bom": "Mumbai, India",
    "lhr": "London, UK",
    "fra": "Frankfurt, Germany",
    "arn": "Stockholm, Sweden",
    "cdg": "Paris, France",
    "iad": "Ashburn, VA (US)",
    "ewr": "Secaucus, NJ (US)",
    "dfw": "Dallas, TX (US)",
    "lax": "Los Angeles, CA (US)",
    "sjc": "San Jose, CA (US)",
    "ord": "Chicago, IL (US)",
    "yyz": "Toronto, Canada",
    "gru": "São Paulo, Brazil",
    "jnb": "Johannesburg, South Africa",
}

def broker_for(region: str) -> str:
    if region == "sin":
        return "suts-fyp-floodwatch-mqtt.fly.dev"
    return f"flood-mqtt-test-{region}.fly.dev"

def label_for(region: str) -> str:
    return f"{region} — {REGION_INFO.get(region, region)}"


# ── Round number ──────────────────────────────────────────────────────────────

def next_round_number() -> int:
    existing = glob.glob(os.path.join(RESULTS_DIR, "round-*.json"))
    nums = []
    for f in existing:
        try:
            nums.append(int(os.path.basename(f).replace("round-", "").replace(".json", "")))
        except ValueError:
            pass
    return max(nums) + 1 if nums else 1


# ── TCP connect time ──────────────────────────────────────────────────────────

def measure_tcp_ms(host: str, port: int, timeout: float = 10.0) -> float | None:
    try:
        t0 = time.perf_counter()
        with socket.create_connection((host, port), timeout=timeout):
            return round((time.perf_counter() - t0) * 1000, 2)
    except Exception:
        return None


# ── Latency test (paced) ──────────────────────────────────────────────────────

def measure_latency(host: str, port: int, count: int) -> dict:
    latencies = []
    errors    = 0
    conn_ms   = None

    run_id       = uuid.uuid4().hex[:8]
    topic        = f"{TEST_TOPIC_BASE}/{run_id}"
    sent_times: dict[str, float] = {}
    connect_t0   = time.perf_counter()
    connect_done = threading.Event()
    subscribed   = threading.Event()
    msg_received = threading.Event()

    def on_connect(client, userdata, flags, rc, props=None):
        nonlocal conn_ms
        if rc == 0:
            conn_ms = round((time.perf_counter() - connect_t0) * 1000, 2)
            client.subscribe(topic, qos=0)
        connect_done.set()

    def on_subscribe(client, userdata, mid, granted_qos, props=None):
        subscribed.set()

    def on_message(client, userdata, msg):
        recv_t = time.perf_counter()
        seq    = msg.payload.decode()
        if seq in sent_times:
            latencies.append((recv_t - sent_times[seq]) * 1000)
        msg_received.set()

    client = mqtt.Client(protocol=mqtt.MQTTv5)
    client.on_connect   = on_connect
    client.on_subscribe = on_subscribe
    client.on_message   = on_message

    try:
        client.connect(host, port, keepalive=30)
        client.loop_start()
        if not connect_done.wait(timeout=10): return {"error": "connect timeout"}
        if conn_ms is None:                  return {"error": "connect refused"}
        if not subscribed.wait(timeout=10):  return {"error": "subscribe timeout"}

        for i in range(count):
            seq = str(i)
            msg_received.clear()
            sent_times[seq] = time.perf_counter()
            client.publish(topic, seq, qos=0)
            if not msg_received.wait(timeout=5):
                errors += 1
            time.sleep(0.05)
    except Exception as e:
        return {"error": str(e)}
    finally:
        client.loop_stop()
        client.disconnect()

    if not latencies:
        return {"error": "no messages received"}

    s = sorted(latencies)
    return {
        "count":   len(latencies),
        "errors":  errors,
        "conn_ms": conn_ms,
        "min":     round(min(latencies),               2),
        "avg":     round(statistics.mean(latencies),   2),
        "median":  round(statistics.median(latencies), 2),
        "p95":     round(s[max(0, int(len(s) * 0.95) - 1)], 2),
        "p99":     round(s[max(0, int(len(s) * 0.99) - 1)], 2),
        "max":     round(max(latencies),               2),
        "stdev":   round(statistics.stdev(latencies),  2) if len(latencies) > 1 else 0.0,
    }


# ── Stress test (blast) ───────────────────────────────────────────────────────

def measure_stress(host: str, port: int, count: int) -> dict:
    latencies: dict[str, float] = {}
    conn_ms        = None
    received_count = [0]
    all_received   = threading.Event()

    run_id       = uuid.uuid4().hex[:8]
    topic        = f"{TEST_TOPIC_BASE}/{run_id}"
    sent_times: dict[str, float] = {}
    connect_t0   = time.perf_counter()
    connect_done = threading.Event()
    subscribed   = threading.Event()

    def on_connect(client, userdata, flags, rc, props=None):
        nonlocal conn_ms
        if rc == 0:
            conn_ms = round((time.perf_counter() - connect_t0) * 1000, 2)
            client.subscribe(topic, qos=0)
        connect_done.set()

    def on_subscribe(client, userdata, mid, granted_qos, props=None):
        subscribed.set()

    def on_message(client, userdata, msg):
        recv_t = time.perf_counter()
        seq    = msg.payload.decode()
        if seq in sent_times:
            latencies[seq] = (recv_t - sent_times[seq]) * 1000
        received_count[0] += 1
        if received_count[0] >= count:
            all_received.set()

    client = mqtt.Client(protocol=mqtt.MQTTv5)
    client.on_connect   = on_connect
    client.on_subscribe = on_subscribe
    client.on_message   = on_message

    try:
        client.connect(host, port, keepalive=30)
        client.loop_start()
        if not connect_done.wait(timeout=10): return {"error": "connect timeout"}
        if conn_ms is None:                  return {"error": "connect refused"}
        if not subscribed.wait(timeout=10):  return {"error": "subscribe timeout"}

        send_start = time.perf_counter()
        for i in range(count):
            seq = str(i)
            sent_times[seq] = time.perf_counter()
            client.publish(topic, seq, qos=0)
        send_end = time.perf_counter()

        all_received.wait(timeout=30)
        total_time = time.perf_counter() - send_start
    except Exception as e:
        return {"error": str(e)}
    finally:
        client.loop_stop()
        client.disconnect()

    lat_list = list(latencies.values())
    if not lat_list:
        return {"error": "no messages received"}

    errors = count - received_count[0]
    s = sorted(lat_list)
    return {
        "count":          received_count[0],
        "errors":         errors,
        "conn_ms":        conn_ms,
        "min":            round(min(lat_list),               2),
        "avg":            round(statistics.mean(lat_list),   2),
        "median":         round(statistics.median(lat_list), 2),
        "p95":            round(s[max(0, int(len(s) * 0.95) - 1)], 2),
        "p99":            round(s[max(0, int(len(s) * 0.99) - 1)], 2),
        "max":            round(max(lat_list),               2),
        "stdev":          round(statistics.stdev(lat_list),  2) if len(lat_list) > 1 else 0.0,
        "throughput_mps": round(received_count[0] / total_time, 1) if total_time > 0 else 0.0,
        "send_rate_mps":  round(count / (send_end - send_start), 1) if (send_end - send_start) > 0 else 0.0,
    }


# ── Console report ────────────────────────────────────────────────────────────

def print_section(title: str, results: dict[str, dict], stress: bool = False) -> None:
    print(f"\n  {'─' * 80}")
    print(f"  {title}")
    print(f"  {'─' * 80}")
    if stress:
        print(f"  {'Region':<32} {'TCP':>7} {'Avg':>7} {'p95':>7} {'p99':>7} {'Loss':>6} {'msg/s':>7}")
    else:
        print(f"  {'Region':<32} {'TCP':>7} {'Avg':>7} {'p95':>7} {'p99':>7} {'Max':>7} {'Loss':>6}")
    print(f"  {'·' * 80}")

    ok  = [(k, v) for k, v in results.items() if "error" not in v]
    err = [(k, v) for k, v in results.items() if "error" in v]
    ok.sort(key=lambda x: x[1].get("avg", 9999))

    for i, (region, r) in enumerate(ok):
        star = "★ " if i == 0 else "  "
        tcp  = f"{r.get('tcp_ms','?')}ms" if r.get("tcp_ms") else "n/a"
        loss = f"{r['errors']}/{r['count']}" if r.get("errors") else "0"
        name = label_for(region)
        if stress:
            print(f"  {star}{name:<30} {tcp:>7} {r['avg']:>6.1f}ms "
                  f"{r['p95']:>6.1f}ms {r['p99']:>6.1f}ms {loss:>6} "
                  f"{r.get('throughput_mps', 0):>6.0f}/s")
        else:
            print(f"  {star}{name:<30} {tcp:>7} {r['avg']:>6.1f}ms "
                  f"{r['p95']:>6.1f}ms {r['p99']:>6.1f}ms {r['max']:>6.1f}ms {loss:>6}")

    for region, r in err:
        print(f"  ✗  {label_for(region):<30}  SKIPPED — {r['error']}")


# ── Cleanup ───────────────────────────────────────────────────────────────────
GITHUB_REPO = "BanglaWarlock/FYP-FloodWatch-IoT-Service"

def trigger_cleanup(region: str) -> None:
    token = os.environ.get("GITHUB_PAT")
    if not token:
        print(f"  [cleanup] GITHUB_PAT not set — skipping {region}")
        print(f"  [cleanup] Run manually: Actions → Destroy test region → {region}")
        return

    resp = requests.post(
        f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/cleanup-region.yml/dispatches",
        headers={
            "Authorization":     f"Bearer {token}",
            "Accept":            "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        json={"ref": "main", "inputs": {"region": region}},
        timeout=10,
    )
    if resp.status_code == 204:
        print(f"  [cleanup] ✓ Triggered destroy for flood-mqtt-test-{region}")
    else:
        print(f"  [cleanup] ✗ Failed ({resp.status_code}) for {region} — destroy manually")

def cleanup_test_regions() -> None:
    print("\n  Triggering cleanup for test regions (skipping Singapore main broker)...")
    for region in set(LATENCY_REGIONS + STRESS_REGIONS):
        if region != "sin":
            trigger_cleanup(region)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="MQTT region latency + stress tester")
    parser.add_argument("--cleanup", action="store_true",
                        help="Trigger cleanup-region.yml on GitHub after tests complete")
    parser.add_argument("--round", type=int, default=None,
                        help="Force a specific round number (default: auto-detect)")
    args = parser.parse_args()

    round_n = args.round or next_round_number()
    out_path = os.path.join(RESULTS_DIR, f"round-{round_n}.json")

    print(f"\n  FloodWatch MQTT Region Tester  —  Round {round_n}")
    print(f"  Latency: {len(LATENCY_REGIONS)} regions  ·  {LATENCY_COUNT} msgs each (50ms paced)")
    print(f"  Stress:  {len(STRESS_REGIONS)} regions   ·  {STRESS_COUNT} msgs each (blasted)")
    print(f"  Output:  {out_path}\n")

    latency_results: dict[str, dict] = {}
    stress_results:  dict[str, dict] = {}

    try:
        # ── Latency pass ──
        print("  [ LATENCY TEST ]")
        for region in LATENCY_REGIONS:
            host = broker_for(region)
            print(f"  {label_for(region):<34}", end=" ... ", flush=True)

            tcp_ms = measure_tcp_ms(host, PORT)
            if tcp_ms is None:
                print("UNREACHABLE  (deploy with test-region.yml first)")
                latency_results[region] = {"error": "TCP unreachable"}
                continue

            r = measure_latency(host, PORT, LATENCY_COUNT)
            if "error" in r:
                print(f"FAILED: {r['error']}")
            else:
                r["tcp_ms"] = tcp_ms
                print(f"avg={r['avg']}ms  p95={r['p95']}ms  p99={r['p99']}ms")
            latency_results[region] = r
            time.sleep(1)

        # ── Stress pass ──
        print("\n  [ STRESS TEST ]")
        for region in STRESS_REGIONS:
            host = broker_for(region)
            print(f"  {label_for(region):<34}", end=" ... ", flush=True)

            tcp_ms = measure_tcp_ms(host, PORT)
            if tcp_ms is None:
                print("UNREACHABLE")
                stress_results[region] = {"error": "TCP unreachable"}
                continue

            r = measure_stress(host, PORT, STRESS_COUNT)
            if "error" in r:
                print(f"FAILED: {r['error']}")
            else:
                r["tcp_ms"] = tcp_ms
                print(f"avg={r['avg']}ms  throughput={r['throughput_mps']}msg/s")
            stress_results[region] = r
            time.sleep(1)

        # ── Console report ──
        print_section(f"LATENCY  ({LATENCY_COUNT} msgs, 50ms paced)", latency_results)
        print_section(f"STRESS   ({STRESS_COUNT} msgs, blasted)", stress_results, stress=True)

        # ── Save ──
        os.makedirs(RESULTS_DIR, exist_ok=True)
        payload = {
            "round":          round_n,
            "timestamp":      time.strftime("%Y-%m-%dT%H:%M:%S"),
            "latency_count":  LATENCY_COUNT,
            "stress_count":   STRESS_COUNT,
            "latency":        latency_results,
            "stress":         stress_results,
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print(f"\n  Saved → {out_path}")
        print(f"  Run again to collect more rounds, then: python scripts/generate-charts.py")

    finally:
        if args.cleanup:
            cleanup_test_regions()


if __name__ == "__main__":
    main()
