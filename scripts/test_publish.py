"""
Continuous sensor simulator — publishes random fake readings to MQTT.
The parser picks them up, writes to MongoDB, and fires SSE events.

Run:
    pip install paho-mqtt
    python scripts/test_publish.py
    Ctrl+C to stop.
"""

import random
import time
import paho.mqtt.client as mqtt

BROKER = "suts-fyp-floodwatch-mqtt.fly.dev"
PORT   = 1883
TOPIC  = "suts/fyp/nodes"

# Fixed GPS base positions per node (near SUTS campus area, Malaysia)
# Small jitter added each reading to simulate real GPS noise
NODE_GPS = {
    'SUTS_Black': (3.0682, 101.5832),
    'SUTS_White': (3.0695, 101.5849),
}

# Mutable per-node state — drifts gradually each reading
state = {
    'SUTS_Black': {'voltage': 12.4},
    'SUTS_White': {'voltage': 12.1, 'level': 0},
}


def _drift_voltage(node_id: str) -> float:
    """Drift voltage slowly up or down, clamped to realistic range."""
    v = state[node_id]['voltage']
    v += random.uniform(-0.08, 0.06)
    v = round(max(10.0, min(13.2, v)), 2)
    state[node_id]['voltage'] = v
    return v


def _next_level(node_id: str) -> int:
    """Randomly step water level up or down (weighted toward calm)."""
    cur = state[node_id]['level']
    # 70% stay, 15% step up, 15% step down
    roll = random.random()
    if roll < 0.15 and cur < 3:
        cur += 1
    elif roll < 0.30 and cur > 0:
        cur -= 1
    state[node_id]['level'] = cur
    return cur


def _level_to_bits(level: int) -> str:
    return {0: '000', 1: '001', 2: '011', 3: '111'}[level]


def _gps(node_id: str):
    lat, lng = NODE_GPS[node_id]
    return (
        round(lat + random.uniform(-0.0002, 0.0002), 6),
        round(lng + random.uniform(-0.0002, 0.0002), 6),
    )


def make_message(node_id: str) -> str:
    v    = _drift_voltage(node_id)
    adc  = int(v / 14.4 * 1800)
    rssi = random.randint(-58, -32)
    lat, lng = _gps(node_id)

    if node_id == 'SUTS_Black':
        # Format: NODE_ID|voltage|adc|lat|lng|rssi
        return f'{node_id}|{v}|{adc}|{lat}|{lng}|{rssi}'
    else:
        # Format: NODE_ID|voltage|adc|lat|lng|floatBits|rssi
        bits = _level_to_bits(_next_level(node_id))
        return f'{node_id}|{v}|{adc}|{lat}|{lng}|{bits}|{rssi}'


# ── MQTT setup ────────────────────────────────────────────────

client = mqtt.Client(protocol=mqtt.MQTTv5)
client.connect(BROKER, PORT, keepalive=60)
client.loop_start()

print(f"Simulator running → {BROKER}/{TOPIC}")
print("Ctrl+C to stop\n")

try:
    while True:
        node_id = random.choice(list(NODE_GPS.keys()))
        msg     = make_message(node_id)

        info = client.publish(TOPIC, msg, qos=1)
        info.wait_for_publish()
        print(f"→ {msg}")

        time.sleep(random.uniform(2, 3))

except KeyboardInterrupt:
    print("\nStopped.")
finally:
    client.loop_stop()
    client.disconnect()
