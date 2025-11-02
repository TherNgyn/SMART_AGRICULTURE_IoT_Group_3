import os
import redis 
import json  
import shutil 
import time 
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, when, max, min, count, stddev,
    lit, round, coalesce, concat, sha2, current_timestamp, approx_count_distinct
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType

SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "local[4]")

print("Dang khoi tao Spark Session...")
spark = SparkSession \
    .builder \
    .appName("SmartAgriAnalytics") \
    .master(SPARK_MASTER_URL) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session da khoi tao thanh cong.")

CHECKPOINT_BASE_DIR = "/tmp/spark_state"
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
KAFKA_ALERTS_TOPIC = os.getenv('KAFKA_TOPIC_ALERTS', 'alerts')
KAFKA_HOURLY_REPORT_TOPIC = os.getenv('KAFKA_TOPIC_HOURLY', 'hourly-reports')

REDIS_HOST = 'redis'
REDIS_PORT = 6379

schema = StructType([
    StructField("Zone_ID", StringType(), True),
    StructField("NDVI", DoubleType(), True),
    StructField("NDRE", DoubleType(), True),
    StructField("RGB_Damage_Score", DoubleType(), True),
    StructField("N", DoubleType(), True),
    StructField("P", DoubleType(), True),
    StructField("K", DoubleType(), True),
    StructField("Moisture", DoubleType(), True),
    StructField("pH", DoubleType(), True),
    StructField("Temperature", DoubleType(), True),
    StructField("Humidity", DoubleType(), True),
    StructField("device_id", StringType(), True),
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
    .withColumn("Moisture", coalesce(col("Moisture"), lit(50.0))) \
    .withColumn("pH", coalesce(col("pH"), lit(7.0))) \
    .withColumn("N", coalesce(col("N"), lit(50.0))) \
    .withColumn("P", coalesce(col("P"), lit(40.0))) \
    .withColumn("K", coalesce(col("K"), lit(40.0))) \
    .withColumn("Humidity", coalesce(col("Humidity"), lit(60.0))) \
    .withColumn("NDVI", coalesce(col("NDVI"), lit(0.5))) \
    .withColumn("NDRE", coalesce(col("NDRE"), lit(0.3))) \
    .withColumn("RGB_Damage_Score", coalesce(col("RGB_Damage_Score"), lit(0.0)))

watermarked_df = cleaned_df.withWatermark("EventTimestamp", "30 seconds")
print("Lam sach du lieu thanh cong.")

def write_dashboard_data_to_redis(df, epoch_id):
    try:
        redis_client_batch = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        redis_client_batch.ping()
    except Exception as e:
        print(f"[Redis] Batch {epoch_id}: Loi ket noi: {e}")
        redis_client_batch = None
        
    if df.count() == 0:
        return
        
    print(f"[Redis] Xu ly Batch {epoch_id}...")
    
    df.createOrReplaceTempView("sensor_view")
    spark_local = df.sparkSession
    
    dashboard_data_df = spark_local.sql("""
        SELECT 
            COUNT(DISTINCT device_id) as active_devices,
            ROUND(AVG(Temperature), 2) as overall_avg_temp,
            ROUND(AVG(Moisture), 2) as overall_avg_moisture,
            ROUND(AVG(Temperature), 2) as chart_avg_temp,
            ROUND(AVG(Moisture), 2) as chart_avg_moisture
        FROM sensor_view
    """)
    
    results = dashboard_data_df.collect()[0]
    
    if redis_client_batch:
        try:
            current_timestamp = int(time.time())

            metrics_dict = {
                "active_devices": results.active_devices,
                "overall_avg_temp": results.overall_avg_temp,
                "overall_avg_moisture": results.overall_avg_moisture
            }
            redis_client_batch.set("dashboard_realtime_metrics", json.dumps(metrics_dict))
            
            def push_to_redis_list(key, value):
                point = json.dumps({"timestamp": current_timestamp, "value": value})
                redis_client_batch.lpush(key, point)
                redis_client_batch.ltrim(key, 0, 49)

            push_to_redis_list("chart_realtime_temp", results.chart_avg_temp)
            push_to_redis_list("chart_realtime_moisture", results.chart_avg_moisture)

            print(f"[Redis] Batch {epoch_id}: Ghi thanh cong")
            
        except Exception as e:
            print(f"[Redis] Batch {epoch_id}: Loi: {e}")

print("HE THONG CANH BAO")
print("TIER 2: HIGH - Canh bao quan trong (5 phut)")
print("TIER 3: INFO - Bao cao tong hop (1 gio)")

high_temp_tier2 = watermarked_df \
    .groupBy(window(col("EventTimestamp"), "5 minutes"), col("device_id")) \
    .agg(
        avg("Temperature").alias("avg_temp"),
        max("Temperature").alias("max_temp"),
        min("Temperature").alias("min_temp"),
        count("*").alias("record_count")
    ) \
    .filter(col("avg_temp") > 32) \
    .withColumn("Alert_Priority", lit("TIER_2_HIGH")) \
    .withColumn("Alert_Type", lit("High_Temperature_Alert")) \
    .withColumn("Severity_Level", when(col("avg_temp") > 34, "HIGH").otherwise("MEDIUM")) \
    .withColumn("Temp_Range", round(col("max_temp") - col("min_temp"), 2)) \
    .withColumn("alert_timestamp", current_timestamp()) \
    .withColumn("alert_key", sha2(concat(col("device_id"), lit("HighTemp"), col("Severity_Level")), 256))

low_moisture_tier2 = watermarked_df \
    .groupBy(window(col("EventTimestamp"), "5 minutes"), col("device_id")) \
    .agg(
        avg("Moisture").alias("avg_moisture"),
        min("Moisture").alias("min_moisture"),
        stddev("Moisture").alias("moisture_stddev"),
        count("*").alias("record_count")
    ) \
    .filter(col("avg_moisture") < 25) \
    .withColumn("Alert_Priority", lit("TIER_2_HIGH")) \
    .withColumn("Alert_Type", lit("Low_Moisture_Alert")) \
    .withColumn("Severity_Level",
        when(col("avg_moisture") < 15, "CRITICAL")
        .when(col("avg_moisture") < 20, "HIGH")
        .otherwise("MEDIUM")
    ) \
    .withColumn("Moisture_Variability", round(coalesce(col("moisture_stddev"), lit(0.0)), 2)) \
    .withColumn("alert_timestamp", current_timestamp()) \
    .withColumn("alert_key", sha2(concat(col("device_id"), lit("LowMoisture"), col("Severity_Level")), 256))

plant_health_report = watermarked_df \
    .groupBy(window(col("EventTimestamp"), "1 hour"), col("device_id")) \
    .agg(
        avg("NDVI").alias("avg_ndvi"),
        avg("NDRE").alias("avg_ndre"),
        avg("RGB_Damage_Score").alias("avg_damage"),
        avg("Temperature").alias("avg_temp"),
        avg("Moisture").alias("avg_moisture"),
        count("*").alias("record_count")
    ) \
    .withColumn("Plant_Health_Score",
        round((col("avg_ndvi") * 0.5 + col("avg_ndre") * 0.3 + (1 - col("avg_damage")/100) * 0.2) * 100, 2)
    ) \
    .withColumn("Health_Status",
        when(col("Plant_Health_Score") > 75, "EXCELLENT")
        .when(col("Plant_Health_Score") > 60, "GOOD")
        .when(col("Plant_Health_Score") > 40, "FAIR")
        .otherwise("POOR")
    ) \
    .withColumn("report_type", lit("Plant_Health_Report"))

comprehensive_report = watermarked_df \
    .groupBy(window(col("EventTimestamp"), "1 hour"), col("device_id")) \
    .agg(
        avg("Temperature").alias("avg_temp"),
        max("Temperature").alias("max_temp"),
        min("Temperature").alias("min_temp"),
        avg("Moisture").alias("avg_moisture"),
        avg("pH").alias("avg_pH"),
        avg("N").alias("avg_N"),
        avg("P").alias("avg_P"),
        avg("K").alias("avg_K"),
        avg("NDVI").alias("avg_ndvi"),
        count("*").alias("total_readings"),
        approx_count_distinct("Zone_ID")
    ) \
    .withColumn("Environmental_Score",
        round(
            (when(col("avg_temp").between(20, 30), 1.0).otherwise(0.5) * 25 +
             when(col("avg_moisture").between(40, 70), 1.0).otherwise(0.5) * 25 +
             when(col("avg_pH").between(6.0, 7.5), 1.0).otherwise(0.5) * 25 +
             when(col("avg_ndvi") > 0.5, 1.0).otherwise(0.5) * 25), 2
        )
    ) \
    .withColumn("Overall_Health_Grade",
        when(col("Environmental_Score") > 80, "A")
        .when(col("Environmental_Score") > 70, "B")
        .when(col("Environmental_Score") > 60, "C")
        .otherwise("D")
    ) \
    .withColumn("report_type", lit("Comprehensive_Report")) \
    .withColumn("report_timestamp", current_timestamp())

print("Dinh nghia cac muc canh bao thanh cong.")
def write_alerts_to_redis(alert_data):
    """Ghi cảnh báo lên Redis cho Streamlit"""
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        
        # Thêm cảnh báo vào list (giới hạn 20 cảnh báo gần nhất)
        alert_json = json.dumps({
            "timestamp": int(time.time()),
            "device_id": alert_data.get("device_id"),
            "alert_type": alert_data.get("Alert_Type"),
            "severity": alert_data.get("Severity_Level"),
            "message": f"{alert_data.get('Alert_Type')} - {alert_data.get('device_id')}",
            "value": alert_data.get("avg_temp") or alert_data.get("avg_moisture")
        })
        
        redis_client.lpush("realtime_alert_history", alert_json)
        redis_client.ltrim("realtime_alert_history", 0, 19)  # Giữ 20 cảnh báo gần nhất
        
        # Cập nhật số lượng cảnh báo trong metrics
        update_alert_counts(alert_data)
        
    except Exception as e:
        print(f"[Redis Alerts] Loi: {e}")

def update_alert_counts(alert_data):
    """Cập nhật số lượng cảnh báo trong metrics"""
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        
        # Lấy metrics hiện tại
        metrics_json = redis_client.get("dashboard_realtime_metrics")
        if metrics_json:
            metrics = json.loads(metrics_json)
        else:
            metrics = {"high_temp_alerts": 0, "low_moisture_alerts": 0}
        
        # Cập nhật số lượng cảnh báo
        alert_type = alert_data.get("Alert_Type", "")
        if "High_Temperature" in alert_type:
            metrics["high_temp_alerts"] = metrics.get("high_temp_alerts", 0) + 1
        elif "Low_Moisture" in alert_type:
            metrics["low_moisture_alerts"] = metrics.get("low_moisture_alerts", 0) + 1
        
        # Lưu lại metrics
        redis_client.set("dashboard_realtime_metrics", json.dumps(metrics))
        
    except Exception as e:
        print(f"[Redis Alert Counts] Loi: {e}")

def write_alerts_with_throttling(df, epoch_id, alert_tier):
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=1, decode_responses=True)
        redis_client.ping()
    except Exception as e:
        print(f"[{alert_tier}] Batch {epoch_id}: Loi Redis: {e}")
        redis_client = None
    
    if df.count() == 0:
        return
    
    print(f"[{alert_tier}] Batch {epoch_id} - Total: {df.count()}")
    
    alerts_to_send = []
    throttled = 0
    
    for row in df.collect():
        device_id = row.device_id
        alert_type = row.Alert_Type if hasattr(row, 'Alert_Type') else row.report_type
        severity = row.Severity_Level if hasattr(row, 'Severity_Level') else "INFO"
        
        if alert_tier == "TIER_2" and redis_client:
            redis_key = f"throttle:{device_id}:{alert_type}:{severity}"
            last_sent = redis_client.get(redis_key)
            
            if last_sent is None:
                alerts_to_send.append(row.asDict())
                redis_client.setex(redis_key, 300, str(int(time.time())))
                print(f"  NEW: {device_id} | {alert_type} | {severity}")
                
                # THÊM: Ghi cảnh báo lên Redis cho Streamlit
                write_alerts_to_redis(row.asDict())
                
            else:
                throttled += 1
                print(f"  SKIP: {device_id} | {alert_type}")
        
        elif alert_tier == "TIER_3":
            alerts_to_send.append(row.asDict())
            print(f"  REPORT: {device_id} | {alert_type}")
    
    print(f"[{alert_tier}] Sent: {len(alerts_to_send)} | Throttled: {throttled}")
    
    if len(alerts_to_send) > 0:
        from kafka import KafkaProducer
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
            )
            
            topic = KAFKA_ALERTS_TOPIC if alert_tier != "TIER_3" else KAFKA_HOURLY_REPORT_TOPIC
            
            for alert in alerts_to_send:
                producer.send(topic, value=alert)
            
            producer.flush()
            producer.close()
            print(f"[{alert_tier}] Gui thanh cong {len(alerts_to_send)} alerts vao '{topic}'")
        except Exception as e:
            print(f"[{alert_tier}] Loi gui Kafka: {e}")

print("SPARK STREAMING STARTED")
print(f"Kafka: {KAFKA_SERVER}")
print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")

query_redis = watermarked_df.writeStream \
    .foreachBatch(write_dashboard_data_to_redis) \
    .trigger(processingTime='5 seconds') \
    .option("checkpointLocation", f"{CHECKPOINT_BASE_DIR}/redis") \
    .start()

query_temp = high_temp_tier2.writeStream \
    .foreachBatch(lambda df, eid: write_alerts_with_throttling(df, eid, "TIER_2")) \
    .trigger(processingTime='5 minutes') \
    .option("checkpointLocation", f"{CHECKPOINT_BASE_DIR}/tier2_temp") \
    .start()

query_moisture = low_moisture_tier2.writeStream \
    .foreachBatch(lambda df, eid: write_alerts_with_throttling(df, eid, "TIER_2")) \
    .trigger(processingTime='5 minutes') \
    .option("checkpointLocation", f"{CHECKPOINT_BASE_DIR}/tier2_moisture") \
    .start()

query_plant = plant_health_report.writeStream \
    .foreachBatch(lambda df, eid: write_alerts_with_throttling(df, eid, "TIER_3")) \
    .trigger(processingTime='1 hour') \
    .option("checkpointLocation", f"{CHECKPOINT_BASE_DIR}/tier3_plant") \
    .start()

query_comprehensive = comprehensive_report.writeStream \
    .foreachBatch(lambda df, eid: write_alerts_with_throttling(df, eid, "TIER_3")) \
    .trigger(processingTime='1 hour') \
    .option("checkpointLocation", f"{CHECKPOINT_BASE_DIR}/tier3_comp") \
    .start()

print("ALL STREAMS STARTED")

spark.streams.awaitAnyTermination()