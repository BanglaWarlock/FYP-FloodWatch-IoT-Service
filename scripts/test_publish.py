"""
Quick test — publishes fake sensor readings to the MQTT broker.
The parser picks them up, writes to MongoDB, and fires SSE events.

Run:
    pip install paho-mqtt
    python scripts/test_publish.py
"""

import time
import paho.mqtt.client as mqtt

BROKER  = "suts-fyp-floodwatch-mqtt.fly.dev"
PORT    = 1883
TOPIC   = "suts/fyp/nodes"

# Fake payloads — same format the real nodes send.
# lat/lng match real node output (0|0 until GPS is configured).
MESSAGES = [
    "SUTS_Black|12.02|1425|0|0|-43",        # black node, no float
    "SUTS_White|11.75|1392|0|0|000|-51",     # white node, level 0
    "SUTS_White|11.74|1390|0|0|001|-51",     # white node, level 1 (flood_level event)
    "SUTS_White|11.20|1350|0|0|011|-51",     # level 2 + battery_low (voltage < 11.5)
]

client = mqtt.Client(protocol=mqtt.MQTTv5)
client.connect(BROKER, PORT, keepalive=60)
client.loop_start()

for msg in MESSAGES:
    info = client.publish(TOPIC, msg, qos=1)
    info.wait_for_publish()
    print(f"published: {msg}")
    time.sleep(2)

client.loop_stop()
client.disconnect()
print("done")
