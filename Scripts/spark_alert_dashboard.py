import os
import redis 
import json  
import shutil 
import time 
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, when, max, min, count, sum, stddev,
    expr, lit, round, coalesce, lag, lead, percentile_approx, countDistinct, concat,
    struct, to_json, udf
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType, IntegerType
from pyspark.sql.window import Window
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "local[4]")
# === 1. KHỞI TẠO SPARK SESSION ===
print("Dang khoi tao Spark Session (Spark 3.4.1)...")
spark = SparkSession \
    .builder \
    .appName("SmartAgriAnalytics-Pipeline") \
    .master(SPARK_MASTER_URL) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session da khoi tao thanh cong.")

# === 1.1. XÓA THƯ MỤC CHECKPOINT CŨ ĐỂ TRÁNH LỖI ===
CHECKPOINT_BASE_DIR = "/tmp/spark_state_v2"
print(f"Dang xoa thu muc checkpoint cu ({CHECKPOINT_BASE_DIR})...")
try:
    shutil.rmtree(CHECKPOINT_BASE_DIR)
    print("Xoa checkpoint cu thanh cong.")
except FileNotFoundError:
    print("Khong tim thay thu muc checkpoint cu, bo qua.")
except Exception as e:
    print(f"Loi khi xoa checkpoint cu: {e}")

# === 1.5. LẤY BIẾN MÔI TRƯỜNG ===
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'kafka:29092')
KAFKA_SENSOR_TOPIC = os.getenv('KAFKA_TOPIC_SENSOR', 'sensor-data')
KAFKA_ALERTS_TOPIC = os.getenv('KAFKA_TOPIC_ALERTS', 'alerts')

# --- Biến Redis (KHÔNG KẾT NỐI Ở ĐÂY) ---
REDIS_HOST = 'redis'
REDIS_PORT = 6379

# === 2. ĐỊNH NGHĨA SCHEMA DỮ LIỆU ===
schema = StructType([
    StructField("Zone_ID", StringType(), True),
    StructField("Image_Source_ID", StringType(), True),
    StructField("Image_Type", StringType(), True),
    StructField("NDVI", DoubleType(), True),
    StructField("NDRE", DoubleType(), True),
    StructField("RGB_Damage_Score", DoubleType(), True),
    StructField("UAV_Timestamp", StringType(), True),
    StructField("N", DoubleType(), True),
    StructField("P", DoubleType(), True),
    StructField("K", DoubleType(), True),
    StructField("Moisture", DoubleType(), True),
    StructField("pH", DoubleType(), True),
    StructField("Temperature", DoubleType(), True),
    StructField("Humidity", DoubleType(), True),
    StructField("NDI_Label", StringType(), True),
    StructField("PDI_Label", StringType(), True),
    StructField("Semantic_Tag", StringType(), True),
    StructField("Action_Suggested", StringType(), True),
    StructField("Energy_Consumed_mAh", DoubleType(), True),
    StructField("Latency_ms", DoubleType(), True),
    StructField("Current_Node", StringType(), True),
    StructField("Migrated_To", StringType(), True),
    StructField("Migration_Required", StringType(), True),
    StructField("Migration_Timestamp", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("ts", LongType(), True),
    StructField("data_source", StringType(), True)
])

# === 3. NHẬN DỮ LIỆU TỪ KAFKA ===
print(f"Bat dau doc tu Kafka topic: {KAFKA_SENSOR_TOPIC}")
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", KAFKA_SENSOR_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# === 4. XỬ LÝ, CHUẨN HÓA, LÀM SẠCH DỮ LIỆU (ETL) ===
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

def label_to_int(label):
    if label == "Low": return 0
    if label == "Medium": return 1
    if label == "High": return 2
    return -1

label_to_int_udf = udf(label_to_int, IntegerType())

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
    .withColumn("RGB_Damage_Score", coalesce(col("RGB_Damage_Score"), lit(0.0))) \
    .withColumn("NDI_Label_Int", label_to_int_udf(col("NDI_Label"))) \
    .withColumn("PDI_Label_Int", label_to_int_udf(col("PDI_Label")))

watermarked_df = cleaned_df.withWatermark("EventTimestamp", "30 seconds")
print("Lam sach va them watermark thanh cong.")


# === 5. HÀM GHI DỮ LIỆU DASHBOARD VÀO REDIS ===
def write_dashboard_data_to_redis(df, epoch_id):
    # Tạo kết nối Redis BÊN TRONG hàm (quan trọng cho executor)
    try:
        redis_client_batch = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        redis_client_batch.ping()
    except Exception as e:
        print(f"[Redis Sink] Batch {epoch_id}: LỖI ket noi Redis: {e}")
        redis_client_batch = None
        
    if df.count() == 0:
        print(f"[Redis Sink] Batch {epoch_id}: Khong co du lieu.")
        return
        
    print(f"\n[Redis Sink] Bat dau xu ly Batch {epoch_id}...")
    
    df.createOrReplaceTempView("sensor_view")
    spark_local = df.sparkSession
    
    # === A. TÍNH TOÁN DỮ LIỆU CHO DASHBOARD ===
    dashboard_data_df = spark_local.sql("""
        SELECT 
            -- 1. Cho Metrics
            COUNT(DISTINCT device_id) as active_devices,
            ROUND(AVG(Temperature), 2) as overall_avg_temp,
            ROUND(AVG(Moisture), 2) as overall_avg_moisture,
            COUNT(CASE WHEN Temperature > 35 THEN 1 END) as high_temp_alerts,
            COUNT(CASE WHEN Moisture < 20 THEN 1 END) as low_moisture_alerts,
            
            -- 2. Cho Biểu đồ
            ROUND(AVG(Temperature), 2) as chart_avg_temp,
            ROUND(AVG(Moisture), 2) as chart_avg_moisture,
            ROUND(AVG(NDI_Label_Int), 2) as chart_avg_ndi,
            ROUND(AVG(PDI_Label_Int), 2) as chart_avg_pdi,
            ROUND(AVG(RGB_Damage_Score), 2) as chart_avg_rgb
        FROM sensor_view
    """)
    
    results = dashboard_data_df.collect()[0]
    
    # === B. GHI VÀO REDIS ===
    if redis_client_batch:
        try:
            current_timestamp = int(time.time())

            # 1. Ghi Metrics (dạng single key)
            metrics_dict = {
                "active_devices": results.active_devices,
                "overall_avg_temp": results.overall_avg_temp,
                "overall_avg_moisture": results.overall_avg_moisture,
                "high_temp_alerts": results.high_temp_alerts,
                "low_moisture_alerts": results.low_moisture_alerts
            }
            redis_client_batch.set("dashboard_realtime_metrics", json.dumps(metrics_dict))
            
            # 2. Ghi Dữ liệu Biểu đồ (dạng List)
            def push_to_redis_list(key, value):
                point = json.dumps({"timestamp": current_timestamp, "value": value})
                redis_client_batch.lpush(key, point)
                redis_client_batch.ltrim(key, 0, 49) # Giữ 50 điểm

            push_to_redis_list("chart_realtime_temp", results.chart_avg_temp)
            push_to_redis_list("chart_realtime_moisture", results.chart_avg_moisture)
            push_to_redis_list("chart_realtime_ndi", results.chart_avg_ndi)
            push_to_redis_list("chart_realtime_pdi", results.chart_avg_pdi)
            push_to_redis_list("chart_realtime_rgb", results.chart_avg_rgb)

            print(f"[Redis Sink] Batch {epoch_id}: Ghi metrics va 5 bieu do vao Redis thanh cong!")
            
        except Exception as e:
            print(f"[Redis Sink] Batch {epoch_id}: LỖI khi ghi vao Redis: {e}")
    
    print(f"\n[Redis Sink] Batch {epoch_id}: Hoan thanh.\n{'='*80}\n")


# === 6. TÍNH TOÁN CÁC CỬA SỔ CẢNH BÁO (CHO SINK 3 - ALERT CONSUMER) ===
print("Dinh nghia 7 luong windowing cho Kafka Alerts...")

# === WINDOW 1: Cảnh báo nhiệt độ cao (60s window, 20s slide) ===
high_temp_window = watermarked_df \
    .groupBy(
        window(col("EventTimestamp"), "60 seconds", "20 seconds"), 
        col("device_id")
    ) \
    .agg(
        avg("Temperature").alias("avg_temp"),
        max("Temperature").alias("max_temp"),
        min("Temperature").alias("min_temp"),
        count("*").alias("record_count")
    ) \
    .filter(col("avg_temp") > 32) \
    .withColumn("Alert_Type", lit("High_Temperature_Alert")) \
    .withColumn("Alert_Level", 
        when(col("avg_temp") > 38, "CRITICAL")
        .when(col("avg_temp") > 35, "HIGH")
        .otherwise("MEDIUM")
    ) \
    .withColumn("Temp_Range", 
        round(col("max_temp") - col("min_temp"), 2)
    )

# === WINDOW 2: Cảnh báo độ ẩm thấp (90s window, 30s slide) ===
low_moisture_window = watermarked_df \
    .groupBy(
        window(col("EventTimestamp"), "90 seconds", "30 seconds"), 
        col("device_id")
    ) \
    .agg(
        avg("Moisture").alias("avg_moisture"),
        min("Moisture").alias("min_moisture"),
        stddev("Moisture").alias("moisture_stddev"),
        count("*").alias("record_count")
    ) \
    .filter(col("avg_moisture") < 25) \
    .withColumn("Alert_Type", lit("Low_Moisture_Alert")) \
    .withColumn("Alert_Level",
        when(col("avg_moisture") < 15, "CRITICAL")
        .when(col("avg_moisture") < 20, "HIGH")
        .otherwise("MEDIUM")
    ) \
    .withColumn("Moisture_Variability",
        round(coalesce(col("moisture_stddev"), lit(0.0)), 2)
    )

# === WINDOW 3: Cảnh báo dinh dưỡng thấp (2 minutes window, 40s slide) ===
low_nutrient_window = watermarked_df \
    .groupBy(
        window(col("EventTimestamp"), "2 minutes", "40 seconds"), 
        col("device_id")
    ) \
    .agg(
        avg("N").alias("avg_N"),
        avg("P").alias("avg_P"),
        avg("K").alias("avg_K"),
        count("*").alias("record_count")
    ) \
    .filter((col("avg_N") < 50) | (col("avg_P") < 30) | (col("avg_K") < 30)) \
    .withColumn("Alert_Type", lit("Low_Nutrients_Alert")) \
    .withColumn("Deficient_Nutrients",
        when(col("avg_N") < 50, "N,")
        .otherwise("") +
        when(col("avg_P") < 30, "P,")
        .otherwise("") +
        when(col("avg_K") < 30, "K")
        .otherwise("")
    ) \
    .withColumn("NPK_Total", 
        round(col("avg_N") + col("avg_P") + col("avg_K"), 2)
    ) \
    .withColumn("Alert_Level",
        when((col("avg_N") < 30) | (col("avg_P") < 20) | (col("avg_K") < 20), "CRITICAL")
        .otherwise("MEDIUM")
    )

# === WINDOW 4: Cảnh báo sốc nhiệt (90s window, 30s slide) ===
heat_stress_window = watermarked_df \
    .groupBy(
        window(col("EventTimestamp"), "90 seconds", "30 seconds"), 
        col("device_id")
    ) \
    .agg(
        avg("Temperature").alias("avg_temp"),
        avg("Moisture").alias("avg_moisture"),
        avg("Humidity").alias("avg_humidity"),
        count("*").alias("record_count")
    ) \
    .filter((col("avg_temp") > 35) & (col("avg_moisture") < 30)) \
    .withColumn("Alert_Type", lit("Heat_Stress_Alert")) \
    .withColumn("Alert_Level", lit("HIGH")) \
    .withColumn("Stress_Index",
        round((col("avg_temp") - 25) * (100 - col("avg_moisture")) / 100, 2)
    ) \
    .withColumn("Recommended_Action",
        when(col("Stress_Index") > 15, "IMMEDIATE_IRRIGATION_REQUIRED")
        .otherwise("INCREASE_IRRIGATION")
    )

# === WINDOW 5: Phân tích pH bất thường (2 minutes window, 40s slide) ===
ph_abnormal_window = watermarked_df \
    .groupBy(
        window(col("EventTimestamp"), "2 minutes", "40 seconds"), 
        col("device_id")
    ) \
    .agg(
        avg("pH").alias("avg_pH"),
        min("pH").alias("min_pH"),
        max("pH").alias("max_pH"),
        count("*").alias("record_count")
    ) \
    .filter((col("avg_pH") < 5.5) | (col("avg_pH") > 8.0)) \
    .withColumn("Alert_Type", lit("pH_Abnormal_Alert")) \
    .withColumn("Alert_Level",
        when((col("avg_pH") < 5.0) | (col("avg_pH") > 8.5), "CRITICAL")
        .otherwise("MEDIUM")
    ) \
    .withColumn("pH_Status",
        when(col("avg_pH") < 5.0, "Strongly Acidic")
        .when(col("avg_pH") < 5.5, "Acidic")
        .when(col("avg_pH") > 8.5, "Strongly Alkaline")
        .when(col("avg_pH") > 8.0, "Alkaline")
        .otherwise("Normal")
    )

# === WINDOW 6: Phân tích sức khỏe cây trồng (3 minutes window) ===
plant_health_window = watermarked_df \
    .groupBy(
        window(col("EventTimestamp"), "3 minutes", "1 minute"), 
        col("device_id")
    ) \
    .agg(
        avg("NDVI").alias("avg_ndvi"),
        avg("NDRE").alias("avg_ndre"),
        avg("RGB_Damage_Score").alias("avg_damage"),
        avg("Temperature").alias("avg_temp"),
        avg("Moisture").alias("avg_moisture"),
        count("*").alias("record_count")
    ) \
    .withColumn("Plant_Health_Score",
        round((col("avg_ndvi") * 0.5 + col("avg_ndre") * 0.3 + 
               (1 - col("avg_damage")/100) * 0.2) * 100, 2)
    ) \
    .withColumn("Health_Status",
        when(col("Plant_Health_Score") > 75, "EXCELLENT")
        .when(col("Plant_Health_Score") > 60, "GOOD")
        .when(col("Plant_Health_Score") > 40, "FAIR")
        .otherwise("POOR")
    ) \
    .withColumn("Requires_Attention",
        when(col("Plant_Health_Score") < 50, "YES").otherwise("NO")
    )

# === WINDOW 7: Phân tích tổng hợp đa chỉ số (5 minutes window) ===
comprehensive_analysis_window = watermarked_df \
    .groupBy(
        window(col("EventTimestamp"), "5 minutes", "2 minutes"), 
        col("device_id")
    ) \
    .agg(
        avg("Temperature").alias("avg_temp"),
        avg("Moisture").alias("avg_moisture"),
        avg("pH").alias("avg_pH"),
        avg("N").alias("avg_N"),
        avg("P").alias("avg_P"),
        avg("K").alias("avg_K"),
        avg("NDVI").alias("avg_ndvi"),
        avg("Energy_Consumed_mAh").alias("avg_energy"),
        count("*").alias("total_readings")
    ) \
    .withColumn("Environmental_Score",
        round(
            (when(col("avg_temp").between(20, 30), 1.0).otherwise(0.5) * 25 +
             when(col("avg_moisture").between(40, 70), 1.0).otherwise(0.5) * 25 +
             when(col("avg_pH").between(6.0, 7.5), 1.0).otherwise(0.5) * 25 +
             when(col("avg_ndvi") > 0.5, 1.0).otherwise(0.5) * 25), 2
        )
    ) \
    .withColumn("Nutrition_Score",
        round((col("avg_N")/150 * 33.33 + col("avg_P")/100 * 33.33 + 
               col("avg_K")/100 * 33.33), 2)
    ) \
    .withColumn("Overall_Health_Grade",
        when(col("Environmental_Score") > 80, "A")
        .when(col("Environmental_Score") > 70, "B")
        .when(col("Environmental_Score") > 60, "C")
        .otherwise("D")
    ) \
    .withColumn("Energy_Efficiency_Rating",
        when(col("avg_energy") < 50, "EXCELLENT")
        .when(col("avg_energy") < 100, "GOOD")
        .otherwise("NEEDS_OPTIMIZATION")
    )
print("Dinh nghia 7 luong windowing thanh cong.")


# === 7. XUẤT KẾT QUẢ RA CÁC SINKS ===

print("\n" + "="*80)
print(" SPARK STREAMING APPLICATION STARTED")
print("="*80)
print(f" Connected to Kafka: {KAFKA_SERVER}")
print(f" Reading from Topic: {KAFKA_SENSOR_TOPIC}")
print(f" Writing Alerts to Topic: {KAFKA_ALERTS_TOPIC}")
print(f" Writing Metrics to Redis: {REDIS_HOST}:{REDIS_PORT}")
print("="*80 + "\n")

# === SINK 1: GHI LOG EDA RA CONSOLE VÀ GHI METRICS/CHARTS VÀO REDIS ===
print("✓ Starting EDA Console Log & Redis Realtime Stream...")
query_eda_redis = watermarked_df.writeStream \
    .foreachBatch(write_dashboard_data_to_redis) \
    .trigger(processingTime='5 seconds') \
    .option("checkpointLocation", f"{CHECKPOINT_BASE_DIR}/eda_redis_console") \
    .start()

# === SINK 2: GHI CẢNH BÁO VÀO KAFKA (Cho Alert Consumer) ===
print("✓ Starting ALL 7 Alert Streams to Kafka 'alerts' Topic...")
def write_alerts_to_kafka(df, checkpoint_suffix):
    """Chuyen dataframe thanh JSON va ghi vao Kafka"""
    return df.select(to_json(struct("*")).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("topic", KAFKA_ALERTS_TOPIC) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE_DIR}/kafka_{checkpoint_suffix}") \
        .trigger(processingTime='30 seconds') \
        .start()

# Bat dau 7 luong ghi canh bao
query_temp_alerts_kafka = write_alerts_to_kafka(high_temp_window, "temp")
query_moisture_alerts_kafka = write_alerts_to_kafka(low_moisture_window, "moist")
query_nutrient_alerts_kafka = write_alerts_to_kafka(low_nutrient_window, "nutri")
query_heat_stress_alerts_kafka = write_alerts_to_kafka(heat_stress_window, "stress")
query_ph_alerts_kafka = write_alerts_to_kafka(ph_abnormal_window, "ph")
query_plant_health_kafka = write_alerts_to_kafka(plant_health_window, "plant")
query_comprehensive_kafka = write_alerts_to_kafka(comprehensive_analysis_window, "comp")


print("\n" + "="*80)
print(" ALL 8 STREAMS STARTED SUCCESSFULLY!")
print("="*80)
print("  • Sink 1: Ghi metrics/charts vao Redis (Moi 5s)")
print("  • Sink 2: Ghi 7 luong canh bao vao Kafka (Moi 30s)")
print("="*80 + "\n")

# Chờ tất cả các stream kết thúc
spark.streams.awaitAnyTermination()