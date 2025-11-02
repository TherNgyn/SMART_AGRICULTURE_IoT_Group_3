#!/usr/bin/env python3
import os
import json
import ssl
from dotenv import load_dotenv
import paho.mqtt.client as mqtt

load_dotenv()

MQTT_BROKER = os.environ["MQTT_BROKER"]
MQTT_PORT = int(os.environ.get("MQTT_PORT", "8883"))
MQTT_USERNAME = os.environ["MQTT_USERNAME"]
MQTT_PASSWORD = os.environ["MQTT_PASSWORD"]
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "sensor-data")

print(f"Testing MQTT connection to {MQTT_BROKER}:{MQTT_PORT}")
print(f"Topic: {MQTT_TOPIC}")

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    if rc == 0:
        print(f"Subscribing to {MQTT_TOPIC}")
        client.subscribe(MQTT_TOPIC, qos=1)
    else:
        print(f"Connection failed with code {rc}")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode('utf-8')
        data = json.loads(payload)
        device_id = data.get('device_id', 'unknown')
        zone_id = data.get('Zone_ID', 'N/A')
        action = data.get('Action_Suggested', 'N/A')
        print(f"Received from {device_id} | Zone: {zone_id} | Action: {action}")
        print(f"  Temperature: {data.get('Temperature')}°C, Humidity: {data.get('Humidity')}%")
        print(f"  NPK: N={data.get('N')}, P={data.get('P')}, K={data.get('K')}")
        print(f"  Full payload: {payload[:200]}...")
        print("-" * 80)
    except Exception as e:
        print(f"Error processing message: {e}")

def on_disconnect(client, userdata, rc):
    print(f"Disconnected with code {rc}")

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

ssl_context = ssl.create_default_context()
client.tls_set_context(ssl_context)

client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

try:
    print("Connecting...")
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    print("Starting loop...")
    client.loop_forever()
except KeyboardInterrupt:
    print("\nStopping...")
    client.disconnect()