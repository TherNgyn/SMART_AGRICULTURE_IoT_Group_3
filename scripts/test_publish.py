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

# Test publish message with new data structure
test_message = {
    "Zone_ID": "Z1",
    "Image_Source_ID": "IMG_Z1_202504010600.jpg",
    "Image_Type": "Multispectral",
    "NDVI": 0.65,
    "NDRE": 0.45,
    "RGB_Damage_Score": None,
    "UAV_Timestamp": "04/01/2025 6:00",
    "N": 45.0,
    "P": 32.0,
    "K": 78.0,
    "Moisture": 25.4,
    "pH": 6.8,
    "Temperature": 65,
    "Humidity": 12,
    "NDI_Label": "Medium",
    "PDI_Label": "Low",
    "Semantic_Tag": "N-deficiency, Healthy",
    "Action_Suggested": "Apply Fertilizer",
    "Energy_Consumed_mAh": 5.2,
    "Latency_ms": 45.3,
    "Current_Node": "Edge-Node-A",
    "Migrated_To": "Edge-Node-A",
    "Migration_Required": "No",
    "Migration_Timestamp": "",
    "device_id": "agri-sensor-test",
    "ts": int(time.time() * 1000),
    "data_source": "smart_agriculture_iot"
}

msg_info = client.publish(MQTT_TOPIC, json.dumps(test_message, ensure_ascii=False), qos=1)
print(f"Published test message with mid: {msg_info.mid}")
print(f"Message content: {json.dumps(test_message, ensure_ascii=False, indent=2)}")

# Wait for publish to complete
time.sleep(2)

client.loop_stop()
client.disconnect()
print("MQTT publish test completed.")
