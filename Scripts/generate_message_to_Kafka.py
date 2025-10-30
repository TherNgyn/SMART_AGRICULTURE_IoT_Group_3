import json
import time
from kafka import KafkaProducer

# Cấu hình producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],  # kết nối tới Kafka container
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Dữ liệu cần gửi
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
    "Temperature": 28.5,
    "Humidity": 65.2,
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

# Topic Kafka
topic_name = "sensor-data"
while True:
# Gửi message
    producer.send(topic_name, test_message)
    producer.flush()

    print(f"✅ Message sent to topic '{topic_name}' successfully!")
    time.sleep(5)