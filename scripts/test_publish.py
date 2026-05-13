"""
Continuous sensor simulator.

- After a 5-second grace period a new node spawns every 20 seconds,
  placed at a random location anywhere on earth, on its own thread.
- SUTS_Black is commented out (real hardware takes over).
- SUTS_White removed.

Run:
    pip install paho-mqtt
    python scripts/test_publish.py
    Ctrl+C to stop.
"""

import random
import threading
import time
import paho.mqtt.client as mqtt

BROKER = "suts-fyp-floodwatch-mqtt.fly.dev"
PORT   = 1883
TOPIC  = "suts/fyp/nodes"

LEVEL_BITS = {0: '000', 1: '001', 2: '011', 3: '111'}

# Keyed by node_id. Spawner adds entries; node threads read/write only their own key.
_state: dict[str, dict] = {
    # 'SUTS_Black': {'voltage': 12.4, 'lat': 3.0682, 'lng': 101.5832, 'has_level': False},
}


# ── Per-node helpers ──────────────────────────────────────────

def _update_voltage(node_id: str) -> float:
    v = _state[node_id]['voltage']
    if random.random() < 0.30:
        v += random.uniform(-0.5, 0.4)
    else:
        v += random.uniform(-0.1, 0.1)
    v = round(max(10.0, min(13.2, v)), 2)
    _state[node_id]['voltage'] = v
    return v


def _update_level(node_id: str) -> int:
    cur  = _state[node_id]['level']
    roll = random.random()
    if roll < 0.20 and cur < 3:
        cur += 1
    elif roll < 0.35 and cur > 0:
        cur -= 1
    _state[node_id]['level'] = cur
    return cur


def _jitter_gps(node_id: str):
    lat = _state[node_id]['lat']
    lng = _state[node_id]['lng']
    return (
        round(lat + random.uniform(-0.0002, 0.0002), 6),
        round(lng + random.uniform(-0.0002, 0.0002), 6),
    )


def make_message(node_id: str) -> str:
    v    = _update_voltage(node_id)
    adc  = int(v / 14.4 * 1800)
    rssi = random.randint(-58, -32)
    lat, lng = _jitter_gps(node_id)

    if not _state[node_id]['has_level']:
        return f'{node_id}|{v}|{adc}|{lat}|{lng}|{rssi}'

    bits = LEVEL_BITS[_update_level(node_id)]
    return f'{node_id}|{v}|{adc}|{lat}|{lng}|{bits}|{rssi}'


# ── Thread targets ────────────────────────────────────────────

def node_loop(node_id: str, client: mqtt.Client, stop: threading.Event):
    print(f"[{node_id}] started")
    while not stop.is_set():
        msg  = make_message(node_id)
        info = client.publish(TOPIC, msg, qos=1)
        info.wait_for_publish()
        print(f"→ {msg}")
        stop.wait(random.uniform(2, 5))
    print(f"[{node_id}] stopped")


def spawner_loop(client: mqtt.Client, stop: threading.Event):
    """Wait 5 s then drop a new node every 20 s."""
    stop.wait(5)
    counter = 1
    while not stop.is_set():
        node_id   = f'NODE_{counter:03d}'
        counter  += 1
        lat       = round(random.uniform(-55.0, 70.0), 4)
        lng       = round(random.uniform(-179.0, 179.0), 4)
        has_level = random.random() > 0.3   # 70 % get a water sensor

        _state[node_id] = {
            'voltage':   round(random.uniform(10.5, 13.0), 2),
            'lat':       lat,
            'lng':       lng,
            'has_level': has_level,
        }
        if has_level:
            _state[node_id]['level'] = 0

        print(f"[SPAWNER] +{node_id}  ({lat}, {lng})  sensor={'yes' if has_level else 'no'}")
        t = threading.Thread(
            target=node_loop,
            args=(node_id, client, stop),
            daemon=True,
        )
        t.start()
        stop.wait(5)


# ── MQTT setup ────────────────────────────────────────────────

client = mqtt.Client(protocol=mqtt.MQTTv5)
client.connect(BROKER, PORT, keepalive=60)
client.loop_start()

print(f"Simulator running → {BROKER}/{TOPIC}")
print("New nodes spawn every 20 s after a 5 s grace period\n")

stop_event = threading.Event()
threads = [
    # threading.Thread(target=node_loop, args=('SUTS_Black', client, stop_event), daemon=True),
    threading.Thread(target=spawner_loop, args=(client, stop_event), daemon=True),
]

for t in threads:
    t.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nStopping...")
    stop_event.set()
    for t in threads:
        t.join()
finally:
    client.loop_stop()
    client.disconnect()
    print("Done.")
