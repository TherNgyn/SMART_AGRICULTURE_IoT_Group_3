import os
import redis
import json
import time
import threading
from kafka import KafkaConsumer

KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'kafka:29092')
KAFKA_SENSOR_TOPIC = os.getenv('KAFKA_TOPIC_SENSOR', 'sensor-data')
REDIS_HOST = 'redis'
REDIS_PORT = 6379

print("GRAFANA DATA FORWARDER")
print(f"Kafka: {KAFKA_SERVER}")
print(f"Sensor Topic: {KAFKA_SENSOR_TOPIC}")
print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")

def kafka_to_redis_forwarder():
    """
    Đọc trực tiếp từ Kafka và đẩy lên Redis không qua Spark
    """
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        redis_client.ping()
        print("Redis connection OK")
    except Exception as e:
        print(f"Redis connection failed: {e}")
        return

    try:
        consumer = KafkaConsumer(
            KAFKA_SENSOR_TOPIC,
            bootstrap_servers=[KAFKA_SERVER],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='grafana_forwarder',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"Kafka consumer connected to topic: {KAFKA_SENSOR_TOPIC}")
    except Exception as e:
        print(f"Kafka connection failed: {e}")
        return

    record_count = 0
    
    for message in consumer:
        try:
            data = message.value
            record_count += 1
            
            current_time_ms = int(time.time() * 1000)
            
            raw_data = {
                "device_id": data.get("device_id"),
                "temperature": float(data.get("Temperature", 0)),
                "humidity": float(data.get("Humidity", 0)),
                "moisture": float(data.get("Moisture", 0)),
                "n": float(data.get("N", 0)),
                "p": float(data.get("P", 0)),
                "k": float(data.get("K", 0)),
                "timestamp": int(time.time()),
                "event_timestamp": current_time_ms,
                "original_ts": data.get("ts")
            }
            
            redis_client.lpush("raw_sensor_data", json.dumps(raw_data))
            redis_client.ltrim("raw_sensor_data", 0, 99)
            
            temp_chart_data = {
                "timestamp": int(time.time()),
                "event_timestamp": current_time_ms,
                "value": float(data.get("Temperature", 0))
            }
            redis_client.lpush("chart_realtime_temp", json.dumps(temp_chart_data))
            redis_client.ltrim("chart_realtime_temp", 0, 49)
            
            moisture_chart_data = {
                "timestamp": int(time.time()),
                "event_timestamp": current_time_ms,
                "value": float(data.get("Moisture", 0))
            }
            redis_client.lpush("chart_realtime_moisture", json.dumps(moisture_chart_data))
            redis_client.ltrim("chart_realtime_moisture", 0, 49)
            
            if record_count % 10 == 0:
                print(f"Processed {record_count} records from Kafka")
            
        except Exception as e:
            print(f"Error processing message: {e}")
            continue

if __name__ == "__main__":
    print("Starting Kafka to Redis forwarder...")
    print("- Reading raw data from Kafka")
    print("- Writing to Redis for Grafana API")
    print("- No Spark processing - direct forwarding")
    
    try:
        kafka_to_redis_forwarder()
    except KeyboardInterrupt:
        print("\nStopping forwarder...")
    except Exception as e:
        print(f"Fatal error: {e}")