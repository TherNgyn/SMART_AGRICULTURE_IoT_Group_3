#!/usr/bin/env python3
import os, csv, json, time, random, ssl
from dotenv import load_dotenv
import paho.mqtt.client as mqtt

# Load environment variables
load_dotenv()

MQTT_BROKER   = os.environ["MQTT_BROKER"]
MQTT_PORT     = int(os.environ.get("MQTT_PORT", "8883"))
MQTT_USERNAME = os.environ["MQTT_USERNAME"]
MQTT_PASSWORD = os.environ["MQTT_PASSWORD"]
MQTT_TOPIC    = os.environ.get("MQTT_TOPIC", "sensor-data")

print(f"Testing MQTT publish to {MQTT_BROKER}:{MQTT_PORT}")
print(f"Topic: {MQTT_TOPIC}")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("MQTT connected successfully.")
    else:
        print(f"MQTT connection failed with code {rc}")

def on_publish(client, userdata, mid):
    print(f"Message {mid} published successfully.")

def on_disconnect(client, userdata, rc):
    print(f"MQTT disconnected with code {rc}")

# Create MQTT client
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

# TLS for HiveMQ Cloud
ssl_context = ssl.create_default_context()
client.tls_set_context(ssl_context)

# Assign callbacks
client.on_connect = on_connect
client.on_publish = on_publish
client.on_disconnect = on_disconnect

print("Connecting to MQTT broker...")
client.connect(MQTT_BROKER, MQTT_PORT, 60)

# Start the loop in a separate thread
client.loop_start()

# Wait for connection
time.sleep(3)

# Test publish message
test_message = {
    "device_id": "test-device",
    "temperature": 25.5,
    "humidity": 60.0,
    "ts": int(time.time() * 1000)
}

msg_info = client.publish(MQTT_TOPIC, json.dumps(test_message), qos=1)
print(f"Published message with mid: {msg_info.mid}")

# Wait for publish to complete
time.sleep(2)

client.loop_stop()
client.disconnect()
print("MQTT publish test completed.")
