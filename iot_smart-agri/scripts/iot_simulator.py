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
DELAY_SEC     = float(os.environ.get("SENDER_DELAY_SEC", "2"))
NUM_DEVICES   = int(os.environ.get("NUM_DEVICES", "5"))
STREAM_CSV    = os.environ.get("STREAM_CSV", "./data/stream_15000.csv")
QOS           = int(os.environ.get("MQTT_QOS", "1"))

# Cột tính năng (theo cấu trúc dữ liệu mới)
FEATURES = [
    "Zone_ID", "Image_Source_ID", "Image_Type", "NDVI", "NDRE", 
    "RGB_Damage_Score", "UAV_Timestamp", "N", "P", "K",
    "Moisture", "pH", "Temperature", "Humidity", "NDI_Label", "PDI_Label",
    "Semantic_Tag", "Action_Suggested", "Energy_Consumed_mAh", "Latency_ms",
    "Current_Node", "Migrated_To", "Migration_Required", "Migration_Timestamp"
]

# MQTT callbacks for better connection handling
connected = False

def on_connect(client, userdata, flags, rc):
    global connected
    if rc == 0:
        print(f"MQTT connected successfully to {MQTT_BROKER}:{MQTT_PORT}")
        connected = True
    else:
        print(f"MQTT connection failed with code {rc}")
        connected = False

def on_disconnect(client, userdata, rc):
    global connected
    print(f"MQTT disconnected with code {rc}")
    connected = False

def on_publish(client, userdata, mid):
    pass  # Không in log mỗi lần publish để tránh tràn terminal

# Tạo MQTT client
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

# Thiết lập TLS cho HiveMQ Cloud
ssl_context = ssl.create_default_context()
client.tls_set_context(ssl_context)

# Set callbacks
client.on_connect = on_connect
client.on_disconnect = on_disconnect  
client.on_publish = on_publish

print(f"Connecting MQTT TLS to {MQTT_BROKER}:{MQTT_PORT} ...")
client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)

# Start network loop
client.loop_start()

# Wait for connection
timeout = 10
start_time = time.time()
while not connected and (time.time() - start_time) < timeout:
    print("Waiting for MQTT connection...")
    time.sleep(1)

if not connected:
    print("Failed to connect to MQTT broker within timeout")
    exit(1)

def read_rows(path):
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            yield row

i = 0
print(f"Starting to send data every {DELAY_SEC} seconds...")
for row in read_rows(STREAM_CSV):
    if not connected:
        print("MQTT connection lost, stopping...")
        break
        
    i += 1
    payload = {}

    # Xử lý các cột đặc biệt
    for col in FEATURES:
        v = row.get(col, None)
        if v is None or v == "":
            payload[col] = None
        else:
            # Các cột số: NDVI, NDRE, RGB_Damage_Score, N, P, K, Moisture, pH, Temperature, Humidity, Energy_Consumed_mAh, Latency_ms
            if col in ["NDVI", "NDRE", "RGB_Damage_Score", "N", "P", "K", 
                      "Moisture", "pH", "Temperature", "Humidity", "Energy_Consumed_mAh", "Latency_ms"]:
                try:
                    payload[col] = float(v) if v != "" else None
                except:
                    payload[col] = None
            # Các cột text/categorical giữ nguyên
            else:
                payload[col] = v

    # Thêm metadata cho IoT device
    payload["device_id"] = f"agri-sensor-{random.randint(1, NUM_DEVICES)}"
    payload["ts"] = int(time.time() * 1000)
    payload["data_source"] = "smart_agriculture_iot"

    msg = json.dumps(payload, ensure_ascii=False)
    
    # Publish with error handling
    try:
        result = client.publish(MQTT_TOPIC, msg, qos=QOS)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"[{i}] Published to MQTT {MQTT_TOPIC}: {msg}")
        else:
            print(f"[{i}] Publish failed with code {result.rc}")
    except Exception as e:
        print(f"[{i}] Publish error: {e}")

    time.sleep(DELAY_SEC)

client.loop_stop()
client.disconnect()
print("Simulator finished.")
