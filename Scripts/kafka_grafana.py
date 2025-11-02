import os
import redis 
import json  
import shutil 
import time 
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, lit, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "local[4]")

print("Dang khoi tao Spark Session...")
spark = SparkSession \
    .builder \
    .appName("SmartAgri-Grafana-Kafka") \
    .master(SPARK_MASTER_URL) \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session da khoi tao thanh cong.")

CHECKPOINT_BASE_DIR = "/tmp/spark_grafana_forwarder"
print(f"Dang xoa checkpoint cu ({CHECKPOINT_BASE_DIR})...")
try:
    shutil.rmtree(CHECKPOINT_BASE_DIR)
    print("Xoa checkpoint cu thanh cong.")
except FileNotFoundError:
    print("Khong tim thay checkpoint cu.")
except Exception as e:
    print(f"Loi khi xoa checkpoint: {e}")

KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'kafka:29092')
KAFKA_SENSOR_TOPIC = os.getenv('KAFKA_TOPIC_SENSOR', 'sensor-data')
REDIS_HOST = 'redis'
REDIS_PORT = 6379

schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("Temperature", DoubleType(), True),
    StructField("Humidity", DoubleType(), True),
    StructField("Moisture", DoubleType(), True),
    StructField("N", DoubleType(), True),
    StructField("P", DoubleType(), True),
    StructField("K", DoubleType(), True),
    StructField("ts", LongType(), True)
])

print(f"Bat dau doc tu Kafka: {KAFKA_SENSOR_TOPIC}")
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", KAFKA_SENSOR_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

cleaned_df = parsed_df \
    .withColumn("EventTimestamp", (col("ts") / 1000).cast("timestamp")) \
    .withColumn("Temperature", coalesce(col("Temperature"), lit(25.0))) \
    .withColumn("Humidity", coalesce(col("Humidity"), lit(60.0))) \
    .withColumn("Moisture", coalesce(col("Moisture"), lit(50.0))) \
    .withColumn("N", coalesce(col("N"), lit(0.0))) \
    .withColumn("P", coalesce(col("P"), lit(0.0))) \
    .withColumn("K", coalesce(col("K"), lit(0.0)))

watermarked_df = cleaned_df.withWatermark("EventTimestamp", "30 seconds")
print("Lam sach du lieu thanh cong.")

def write_raw_data_to_redis(df, epoch_id):
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        redis_client.ping()
    except Exception as e:
        print(f"[Raw Data] Batch {epoch_id}: Redis error {e}")
        return
        
    if df.count() == 0:
        return
        
    record_count = 0
    current_time_ms = int(time.time() * 1000)
    
    for row in df.collect():
        record_count += 1
        
        raw_data = {
            "device_id": row.device_id,
            "temperature": float(row.Temperature),
            "humidity": float(row.Humidity),
            "moisture": float(row.Moisture),
            "n": float(row.N),
            "p": float(row.P),
            "k": float(row.K),
            "timestamp": int(time.time()),
            "event_timestamp": current_time_ms
        }
        
        redis_client.lpush("raw_sensor_data", json.dumps(raw_data))
        redis_client.ltrim("raw_sensor_data", 0, 99)
        
        temp_chart_data = {
            "timestamp": int(time.time()),
            "event_timestamp": current_time_ms,
            "value": float(row.Temperature)
        }
        redis_client.lpush("chart_realtime_temp", json.dumps(temp_chart_data))
        redis_client.ltrim("chart_realtime_temp", 0, 49)
        
        moisture_chart_data = {
            "timestamp": int(time.time()),
            "event_timestamp": current_time_ms,
            "value": float(row.Moisture)
        }
        redis_client.lpush("chart_realtime_moisture", json.dumps(moisture_chart_data))
        redis_client.ltrim("chart_realtime_moisture", 0, 49)
        
        current_time_ms += 100
    
    print(f"[Raw Data] Batch {epoch_id}: Ghi {record_count} records vao Redis")

print("GRAFANA DASHBOARD")
print(f"Kafka: {KAFKA_SERVER}")
print(f"Sensor Topic: {KAFKA_SENSOR_TOPIC}")
print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")

stream_raw_data = watermarked_df.writeStream \
    .foreachBatch(write_raw_data_to_redis) \
    .trigger(processingTime='5 seconds') \
    .option("checkpointLocation", f"{CHECKPOINT_BASE_DIR}/raw_data") \
    .start()

print(" Stream Raw Data started (trigger: 5s)")

print("DATA FORWARDER ĐANG CHẠY...")
print("- Raw Data: Đọc từ Kafka -> Ghi vào Redis")
print("- Grafana: Đọc từ Redis qua API")

spark.streams.awaitAnyTermination()