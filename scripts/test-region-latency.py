#!/usr/bin/env python3
"""
MQTT region latency tester for flood monitoring FYP.

Tests broker response time across Fly.io regions. Run from Malaysia
(or your field test location) for representative results.

Usage:
    pip install paho-mqtt
    python scripts/test-region-latency.py               # all brokers in list
    python scripts/test-region-latency.py --broker flood-mqtt-test-hkg.fly.dev
    python scripts/test-region-latency.py --count 50    # more messages = more accurate

Engineering report note:
    Run this test from the same network as your deployment environment.
    Results from your laptop in Kuching are more representative than
    results from a GitHub Actions runner in the US.
"""

import argparse
import socket
import statistics
import sys
import threading
import time
import uuid
import os
import requests

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("Missing dependency. Run: pip install paho-mqtt")
    sys.exit(1)

# ── Edit these to match your deployed test brokers ──────────────────────────
BROKERS = {
    "sin (Singapore) [main]": "fsuts-fyp-floodwatch-mqtt.fly.dev",
    "hkg (Hong Kong)":        "flood-mqtt-test-hkg.fly.dev",
    "bom (Mumbai)":           "flood-mqtt-test-bom.fly.dev",
    "nrt (Tokyo)":            "flood-mqtt-test-nrt.fly.dev",
    "syd (Sydney)":           "flood-mqtt-test-syd.fly.dev",
}
PORT           = 1883
DEFAULT_COUNT  = 30
TEST_TOPIC_BASE = "flood/latency-test"


# ── TCP connect time ─────────────────────────────────────────────────────────

def measure_tcp_ms(host: str, port: int, timeout: float = 10.0) -> float | None:
    try:
        t0 = time.perf_counter()
        with socket.create_connection((host, port), timeout=timeout):
            return round((time.perf_counter() - t0) * 1000, 2)
    except Exception:
        return None


# ── MQTT round-trip latency ──────────────────────────────────────────────────

def measure_mqtt(host: str, port: int, count: int) -> dict:
    """
    Connects to broker, subscribes to a unique topic, publishes N messages,
    and records the time from publish() call to on_message() callback.

    This measures the full round-trip: client → broker → client.
    """
    latencies = []
    errors    = 0
    conn_ms   = None

    run_id  = uuid.uuid4().hex[:8]
    topic   = f"{TEST_TOPIC_BASE}/{run_id}"

    # Shared state between callbacks and main thread
    sent_times  : dict[str, float] = {}
    connect_t0  = time.perf_counter()
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

        if not connect_done.wait(timeout=10):
            return {"error": "connect timeout"}
        if conn_ms is None:
            return {"error": "connect refused"}

        if not subscribed.wait(timeout=10):
            return {"error": "subscribe timeout"}

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
        return {"error": "no messages received — broker may not echo back"}

    s = sorted(latencies)
    return {
        "count":   len(latencies),
        "errors":  errors,
        "conn_ms": conn_ms,
        "min":     round(min(latencies),              2),
        "avg":     round(statistics.mean(latencies),  2),
        "median":  round(statistics.median(latencies),2),
        "p95":     round(s[max(0, int(len(s) * 0.95) - 1)], 2),
        "max":     round(max(latencies),              2),
        "stdev":   round(statistics.stdev(latencies), 2) if len(latencies) > 1 else 0.0,
    }


# ── Report ───────────────────────────────────────────────────────────────────

def print_report(results: dict[str, dict], count: int) -> None:
    print()
    print("=" * 78)
    print(f"  MQTT REGION LATENCY REPORT   {count} messages/broker   port {PORT}")
    print("=" * 78)
    print(f"  {'Region':<28} {'TCP':>7} {'MQTT':>7} {'Avg':>7} {'p50':>7} {'p95':>7} {'Max':>7} {'Loss':>6}")
    print("  " + "-" * 74)

    ok  = [(n, r) for n, r in results.items() if "error" not in r]
    err = [(n, r) for n, r in results.items() if "error" in r]

    ok.sort(key=lambda x: x[1]["median"])

    for i, (name, r) in enumerate(ok):
        marker = " ★" if i == 0 else "  "
        tcp    = f"{r.get('tcp_ms', '?')}ms"   if r.get('tcp_ms')   else "n/a"
        conn   = f"{r['conn_ms']}ms"             if r.get('conn_ms') else "n/a"
        loss   = f"{r['errors']}/{r['count']}"   if r["errors"]      else "  0"
        print(
            f"{marker}{name:<28} {tcp:>7} {conn:>7} "
            f"{r['avg']:>6.1f}ms {r['median']:>6.1f}ms "
            f"{r['p95']:>6.1f}ms {r['max']:>6.1f}ms {loss:>6}"
        )

    for name, r in err:
        print(f"  {name:<28}  SKIPPED — {r['error']}")

    print()
    if ok:
        best = ok[0]
        print(f"  ★ Best region by median latency: {best[0]}")
        print(f"    Use in fly.toml: primary_region = \"{best[1].get('region_code', '?')}\"")
    print()
    
    
# Cleanup 
GITHUB_REPO = "BanglaWarlock/FYP-FloodWatch-IoT-Service"

def trigger_cleanup(region: str) -> None:
    """
    Calls the GitHub API to trigger cleanup-region.yml for a given region.
    Requires GITHUB_PAT environment variable with 'workflow' scope.
    Fails silently if token is missing — cleanup just won't happen automatically.
    """
    if requests is None:
        print(f"  [cleanup] requests not installed — skipping {region}")
        return

    token = os.environ.get("GITHUB_PAT")
    if not token:
        print(f"  [cleanup] GITHUB_PAT not set — skipping {region}")
        print(f"  [cleanup] Run manually: Actions → Destroy test region → {region}")
        return

    url = (
        f"https://api.github.com/repos/{GITHUB_REPO}"
        f"/actions/workflows/cleanup-region.yml/dispatches"
    )

    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept":        "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        json={
            "ref":    "main",
            "inputs": {"region": region},
        },
        timeout=10,
    )

    if resp.status_code == 204:
        print(f"  [cleanup] ✓ Triggered destroy for flood-mqtt-test-{region}")
    else:
        print(f"  [cleanup] ✗ Failed ({resp.status_code}) for {region} — destroy manually")


def cleanup_all_test_regions(brokers: dict) -> None:
    """
    Triggers cleanup for every broker in the list that is a test region.
    Skips the main broker (flood-mqtt-fyp) — never destroys production.
    """
    print("\n  Triggering cleanup for test regions...")
    for label, host in brokers.items():
        # Only destroy test brokers — never touch the main one
        if "flood-mqtt-test-" not in host:
            print(f"  [cleanup] Skipping {label} — this is your main broker")
            continue

        # Extract region code from hostname: flood-mqtt-test-hkg.fly.dev → hkg
        try:
            region = host.split("flood-mqtt-test-")[1].split(".")[0]
            trigger_cleanup(region)
        except IndexError:
            print(f"  [cleanup] Could not parse region from {host}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="MQTT region latency tester")
    parser.add_argument("--broker",  help="Test a single broker hostname only")
    parser.add_argument("--count",   type=int, default=DEFAULT_COUNT)
    parser.add_argument("--port",    type=int, default=PORT)
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Trigger cleanup-region.yml on GitHub after tests complete (or on error)"
    )
    args = parser.parse_args()

    brokers = {args.broker: args.broker} if args.broker else BROKERS

    print(f"\n  Testing {len(brokers)} broker(s) — {args.count} msgs each")
    if args.cleanup:
        print(f"  Auto-cleanup: ON (will destroy test brokers when done)\n")
    else:
        print(f"  Auto-cleanup: OFF (pass --cleanup to auto-destroy test brokers)\n")

    all_results: dict[str, dict] = {}

    try:
        for label, host in brokers.items():
            print(f"  {label}  ({host})", end=" ... ", flush=True)

            tcp_ms = measure_tcp_ms(host, args.port)
            if tcp_ms is None:
                print("UNREACHABLE")
                all_results[label] = {"error": "TCP unreachable"}
                continue

            r = measure_mqtt(host, args.port, args.count)
            if "error" in r:
                print(f"FAILED: {r['error']}")
            else:
                r["tcp_ms"] = tcp_ms
                print(f"avg={r['avg']}ms  p95={r['p95']}ms")

            all_results[label] = r
            time.sleep(1)

        print_report(all_results, args.count)

    finally:
        # Runs whether tests passed, failed, or crashed midway
        if args.cleanup:
            cleanup_all_test_regions(brokers)


if __name__ == "__main__":
    main()