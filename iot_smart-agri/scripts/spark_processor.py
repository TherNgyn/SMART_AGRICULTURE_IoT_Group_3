import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, when, max, min, count, sum, stddev,
    expr, lit, round, coalesce, lag, lead, percentile_approx, countDistinct, concat
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType
from pyspark.sql.window import Window

# === 1. KHỞI TẠO SPARK SESSION ===
spark = SparkSession \
    .builder \
    .appName("SmartAgriAnalytics-WindowBased") \
    .master("local[4]") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

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
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "sensor-data") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# === 4. XỬ LÝ, CHUẨN HÓA, LÀM SẠCH DỮ LIỆU (ETL) ===
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Chuẩn hóa và làm sạch dữ liệu
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

# Thêm watermark
watermarked_df = cleaned_df.withWatermark("EventTimestamp", "30 seconds")

# === 5. KHAI PHÁ DỮ LIỆU (EDA) VỚI SPARK SQL ===
def perform_eda_on_batch(df, epoch_id):
    if df.count() == 0:
        print(f"[Batch {epoch_id}] No data in this batch")
        return
        
    print(f"\n{'='*80}")
    print(f" EDA ANALYSIS - BATCH ID: {epoch_id}")
    print(f"{'='*80}\n")
    
    df.createOrReplaceTempView("sensor_view")
    spark_local = df.sparkSession
    
    # === QUERY 1: Thống kê cơ bản theo thiết bị ===
    print(" 1. BASIC STATISTICS PER DEVICE:")
    print("-" * 80)
    stats_per_device = spark_local.sql("""
        SELECT 
            device_id,
            COUNT(*) as total_records,
            ROUND(AVG(Temperature), 2) as avg_temp,
            ROUND(MIN(Temperature), 2) as min_temp,
            ROUND(MAX(Temperature), 2) as max_temp,
            ROUND(AVG(Moisture), 2) as avg_moisture,
            ROUND(AVG(Humidity), 2) as avg_humidity,
            ROUND(AVG(pH), 2) as avg_pH
        FROM sensor_view 
        GROUP BY device_id
        ORDER BY device_id
    """)
    stats_per_device.show(truncate=False)

    # === QUERY 2: Phân tích hành động được đề xuất ===
    print("\n 2. SUGGESTED ACTIONS DISTRIBUTION:")
    print("-" * 80)
    action_counts = spark_local.sql("""
        SELECT 
            Action_Suggested, 
            COUNT(*) as action_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
        FROM sensor_view
        WHERE Action_Suggested IS NOT NULL
        GROUP BY Action_Suggested
        ORDER BY action_count DESC
    """)
    action_counts.show(truncate=False)

    # === QUERY 3: Phân tích dinh dưỡng đất (NPK) ===
    print("\n 3. SOIL NUTRITION ANALYSIS (NPK):")
    print("-" * 80)
    nutrition_analysis = spark_local.sql("""
        SELECT 
            device_id,
            ROUND(AVG(N), 2) as avg_nitrogen,
            ROUND(AVG(P), 2) as avg_phosphorus,
            ROUND(AVG(K), 2) as avg_potassium,
            CASE 
                WHEN AVG(N) < 50 THEN 'Low N'
                WHEN AVG(N) > 100 THEN 'High N'
                ELSE 'Normal N'
            END as nitrogen_status,
            CASE 
                WHEN AVG(P) < 30 THEN 'Low P'
                WHEN AVG(P) > 80 THEN 'High P'
                ELSE 'Normal P'
            END as phosphorus_status
        FROM sensor_view
        GROUP BY device_id
        ORDER BY device_id
    """)
    nutrition_analysis.show(truncate=False)

    # === QUERY 4: Phân tích chỉ số thực vật (NDVI, NDRE) ===
    print("\n 4. VEGETATION INDEX ANALYSIS:")
    print("-" * 80)
    vegetation_analysis = spark_local.sql("""
        SELECT 
            device_id,
            ROUND(AVG(NDVI), 3) as avg_ndvi,
            ROUND(AVG(NDRE), 3) as avg_ndre,
            ROUND(AVG(RGB_Damage_Score), 2) as avg_damage_score,
            CASE 
                WHEN AVG(NDVI) < 0.3 THEN 'Poor Health'
                WHEN AVG(NDVI) BETWEEN 0.3 AND 0.6 THEN 'Moderate Health'
                ELSE 'Good Health'
            END as plant_health_status
        FROM sensor_view
        WHERE NDVI IS NOT NULL
        GROUP BY device_id
        ORDER BY avg_ndvi DESC
    """)
    vegetation_analysis.show(truncate=False)

    # === QUERY 5: Phân tích hiệu suất năng lượng ===
    print("\n 5. ENERGY CONSUMPTION ANALYSIS:")
    print("-" * 80)
    energy_analysis = spark_local.sql("""
        SELECT 
            device_id,
            COUNT(*) as operations,
            ROUND(AVG(Energy_Consumed_mAh), 2) as avg_energy_mAh,
            ROUND(SUM(Energy_Consumed_mAh), 2) as total_energy_mAh,
            ROUND(AVG(Latency_ms), 2) as avg_latency_ms
        FROM sensor_view
        WHERE Energy_Consumed_mAh IS NOT NULL
        GROUP BY device_id
        ORDER BY total_energy_mAh DESC
    """)
    energy_analysis.show(truncate=False)

    # === QUERY 6: Migration Pattern Analysis ===
    print("\n 6. MIGRATION PATTERN ANALYSIS:")
    print("-" * 80)
    migration_analysis = spark_local.sql("""
        SELECT 
            Current_Node,
            Migration_Required,
            COUNT(*) as occurrence_count,
            COUNT(DISTINCT device_id) as affected_devices
        FROM sensor_view
        WHERE Current_Node IS NOT NULL
        GROUP BY Current_Node, Migration_Required
        ORDER BY occurrence_count DESC
    """)
    migration_analysis.show(truncate=False)

    print(f"\n{'='*80}\n")


# === 6. TÍNH TOÁN WINDOWING VÀ TẠO CỘT MỚI ===

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


# === 7. FUNCTION ĐỂ HIỂN THỊ ALERTS TỪ WINDOWING RESULTS ===
def display_window_alerts(batch_df, epoch_id):
    """Hiển thị alerts từ kết quả windowing streams"""
    
    spark_local = batch_df.sparkSession
    
    print(f"\n{'='*80}")
    print(f" WINDOW-BASED ALERTS REPORT - BATCH ID: {epoch_id}")
    print(f"{'='*80}\n")
    
    # === 1. HIGH TEMPERATURE ALERTS (từ memory table) ===
    try:
        high_temp_alerts = spark_local.sql("""
            SELECT * FROM high_temp_alerts 
            ORDER BY avg_temp DESC
            LIMIT 10
        """)
        
        high_temp_count = high_temp_alerts.count()
        
        if high_temp_count > 0:
            print("🌡️  HIGH TEMPERATURE ALERTS (from 60s window aggregation)")
            print("-" * 80)
            print(f"Total alerts: {high_temp_count}")
            
            # Thống kê
            max_avg_temp = high_temp_alerts.agg(max("avg_temp")).collect()[0][0]
            critical_count = high_temp_alerts.filter(col("Alert_Level") == "CRITICAL").count()
            
            print(f"  • Maximum average temperature: {max_avg_temp:.2f}°C")
            print(f"  • Critical alerts: {critical_count}")
            print(f"  • Status: {'🔴 CRITICAL' if critical_count > 0 else '🟠 WARNING'}")
            print("\nDetailed window aggregations:")
            
            high_temp_alerts.select(
                col("window.start").alias("Window_Start"),
                col("window.end").alias("Window_End"),
                "device_id",
                "avg_temp",
                "max_temp",
                "min_temp",
                "Temp_Range",
                "record_count",
                "Alert_Level"
            ).show(10, truncate=False)
        else:
            print("🌡️  Temperature: ✅ NORMAL (no high temperature windows detected)\n")
    except Exception as e:
        print(f"⚠️  High temperature alerts not yet available: {e}\n")
    
    # === 2. LOW MOISTURE ALERTS (từ memory table) ===
    try:
        low_moisture_alerts = spark_local.sql("""
            SELECT * FROM low_moisture_alerts 
            ORDER BY avg_moisture ASC
            LIMIT 10
        """)
        
        low_moisture_count = low_moisture_alerts.count()
        
        if low_moisture_count > 0:
            print("\n💧 LOW SOIL MOISTURE ALERTS (from 90s window aggregation)")
            print("-" * 80)
            print(f"Total alerts: {low_moisture_count}")
            
            min_avg_moisture = low_moisture_alerts.agg(min("avg_moisture")).collect()[0][0]
            critical_count = low_moisture_alerts.filter(col("Alert_Level") == "CRITICAL").count()
            
            print(f"  • Minimum average moisture: {min_avg_moisture:.2f}%")
            print(f"  • Critical alerts: {critical_count}")
            print(f"  • Status: {'🔴 CRITICAL' if critical_count > 0 else '🟠 WARNING'}")
            print("\nDetailed window aggregations:")
            
            low_moisture_alerts.select(
                col("window.start").alias("Window_Start"),
                col("window.end").alias("Window_End"),
                "device_id",
                "avg_moisture",
                "min_moisture",
                "Moisture_Variability",
                "record_count",
                "Alert_Level"
            ).show(10, truncate=False)
        else:
            print("\n💧 Soil Moisture: ✅ NORMAL (no low moisture windows detected)\n")
    except Exception as e:
        print(f"⚠️  Low moisture alerts not yet available: {e}\n")
    
    # === 3. HEAT STRESS ALERTS (từ memory table) ===
    try:
        heat_stress_alerts = spark_local.sql("""
            SELECT * FROM heat_stress_alerts 
            ORDER BY Stress_Index DESC
            LIMIT 10
        """)
        
        heat_stress_count = heat_stress_alerts.count()
        
        if heat_stress_count > 0:
            print("\n🔥 HEAT STRESS ALERTS (from 90s window aggregation)")
            print("-" * 80)
            print(f"Total alerts: {heat_stress_count}")
            print(f"  • Status: 🔴 CRITICAL - Combined high temperature + low moisture")
            
            max_stress = heat_stress_alerts.agg(max("Stress_Index")).collect()[0][0]
            print(f"  • Maximum stress index: {max_stress:.2f}")
            print("\nDetailed window aggregations:")
            
            heat_stress_alerts.select(
                col("window.start").alias("Window_Start"),
                col("window.end").alias("Window_End"),
                "device_id",
                "avg_temp",
                "avg_moisture",
                "avg_humidity",
                "Stress_Index",
                "Recommended_Action",
                "record_count"
            ).show(10, truncate=False)
        else:
            print("\n🔥 Heat Stress: ✅ NO CRITICAL CONDITIONS (no heat stress windows detected)\n")
    except Exception as e:
        print(f"⚠️  Heat stress alerts not yet available: {e}\n")
    
    # === 4. LOW NUTRIENTS ALERTS (từ memory table) ===
    try:
        nutrient_alerts = spark_local.sql("""
            SELECT * FROM low_nutrient_alerts 
            ORDER BY NPK_Total ASC
            LIMIT 10
        """)
        
        nutrient_count = nutrient_alerts.count()
        
        if nutrient_count > 0:
            print("\n🌱 NUTRIENT DEFICIENCY ALERTS (from 2-minute window aggregation)")
            print("-" * 80)
            print(f"Total alerts: {nutrient_count}")
            
            critical_count = nutrient_alerts.filter(col("Alert_Level") == "CRITICAL").count()
            print(f"  • Critical nutrient deficiencies: {critical_count}")
            print("\nDetailed window aggregations:")
            
            nutrient_alerts.select(
                col("window.start").alias("Window_Start"),
                col("window.end").alias("Window_End"),
                "device_id",
                "avg_N",
                "avg_P",
                "avg_K",
                "NPK_Total",
                "Deficient_Nutrients",
                "Alert_Level",
                "record_count"
            ).show(10, truncate=False)
        else:
            print("\n🌱 Soil Nutrients: ✅ ADEQUATE (no nutrient deficiency windows detected)\n")
    except Exception as e:
        print(f"⚠️  Nutrient alerts not yet available: {e}\n")
    
    # === 5. pH ABNORMAL ALERTS (từ memory table) ===
    try:
        ph_alerts = spark_local.sql("""
            SELECT * FROM ph_abnormal_alerts 
            ORDER BY Alert_Level DESC, avg_pH
            LIMIT 10
        """)
        
        ph_count = ph_alerts.count()
        
        if ph_count > 0:
            print("\n⚗️  SOIL pH ALERTS (from 2-minute window aggregation)")
            print("-" * 80)
            print(f"Total alerts: {ph_count}")
            
            critical_count = ph_alerts.filter(col("Alert_Level") == "CRITICAL").count()
            print(f"  • Critical pH issues: {critical_count}")
            print("\nDetailed window aggregations:")
            
            ph_alerts.select(
                col("window.start").alias("Window_Start"),
                col("window.end").alias("Window_End"),
                "device_id",
                "avg_pH",
                "min_pH",
                "max_pH",
                "pH_Status",
                "Alert_Level",
                "record_count"
            ).show(10, truncate=False)
        else:
            print("\n⚗️  Soil pH: ✅ OPTIMAL (no pH abnormality windows detected)\n")
    except Exception as e:
        print(f"⚠️  pH alerts not yet available: {e}\n")
    
    # === 6. PLANT HEALTH ANALYSIS (từ memory table) ===
    try:
        plant_health = spark_local.sql("""
            SELECT * FROM plant_health_analysis 
            WHERE Requires_Attention = 'YES'
            ORDER BY Plant_Health_Score ASC
            LIMIT 10
        """)
        
        poor_health_count = plant_health.count()
        
        if poor_health_count > 0:
            print("\n🌿 PLANT HEALTH CONCERNS (from 3-minute window aggregation)")
            print("-" * 80)
            print(f"Plants requiring attention: {poor_health_count}")
            print("\nDetailed window aggregations:")
            
            plant_health.select(
                col("window.start").alias("Window_Start"),
                col("window.end").alias("Window_End"),
                "device_id",
                "avg_ndvi",
                "avg_ndre",
                "avg_damage",
                "Plant_Health_Score",
                "Health_Status",
                "record_count"
            ).show(10, truncate=False)
        else:
            print("\n🌿 Plant Health: ✅ GOOD (all plants healthy)\n")
    except Exception as e:
        print(f"⚠️  Plant health data not yet available: {e}\n")
    
    # === 7. COMPREHENSIVE ANALYSIS SUMMARY (từ memory table) ===
    try:
        comprehensive = spark_local.sql("""
            SELECT * FROM comprehensive_analysis 
            ORDER BY Environmental_Score ASC
            LIMIT 5
        """)
        
        comp_count = comprehensive.count()
        
        if comp_count > 0:
            print("\n📊 COMPREHENSIVE HEALTH ANALYSIS (from 5-minute window aggregation)")
            print("-" * 80)
            print("Zones with lowest environmental scores:")
            
            comprehensive.select(
                col("window.start").alias("Window_Start"),
                col("window.end").alias("Window_End"),
                "device_id",
                "Environmental_Score",
                "Nutrition_Score",
                "Overall_Health_Grade",
                "Energy_Efficiency_Rating",
                "total_readings"
            ).show(5, truncate=False)
        else:
            print("\n📊 Comprehensive Analysis: ⏳ Aggregating data...\n")
    except Exception as e:
        print(f"⚠️  Comprehensive analysis not yet available: {e}\n")
    
    # === 8. OVERALL ALERT SUMMARY ===
    print("\n" + "="*80)
    print("📋 OVERALL ALERT SUMMARY (Based on Window Aggregations)")
    print("="*80)
    
    # Đếm tổng số alerts từ các memory tables
    total_alerts = 0
    alert_counts = {}
    
    try:
        alert_counts['high_temp'] = spark_local.sql("SELECT COUNT(*) as cnt FROM high_temp_alerts").collect()[0][0]
        total_alerts += alert_counts['high_temp']
    except:
        alert_counts['high_temp'] = 0
    
    try:
        alert_counts['low_moisture'] = spark_local.sql("SELECT COUNT(*) as cnt FROM low_moisture_alerts").collect()[0][0]
        total_alerts += alert_counts['low_moisture']
    except:
        alert_counts['low_moisture'] = 0
    
    try:
        alert_counts['heat_stress'] = spark_local.sql("SELECT COUNT(*) as cnt FROM heat_stress_alerts").collect()[0][0]
        total_alerts += alert_counts['heat_stress']
    except:
        alert_counts['heat_stress'] = 0
    
    try:
        alert_counts['nutrients'] = spark_local.sql("SELECT COUNT(*) as cnt FROM low_nutrient_alerts").collect()[0][0]
        total_alerts += alert_counts['nutrients']
    except:
        alert_counts['nutrients'] = 0
    
    try:
        alert_counts['ph'] = spark_local.sql("SELECT COUNT(*) as cnt FROM ph_abnormal_alerts").collect()[0][0]
        total_alerts += alert_counts['ph']
    except:
        alert_counts['ph'] = 0
    
    if total_alerts > 0:
        print(f"  Total Active Alerts: {total_alerts}")
        print(f"    • High Temperature Windows: {alert_counts['high_temp']}")
        print(f"    • Low Moisture Windows: {alert_counts['low_moisture']}")
        print(f"    • Heat Stress Windows: {alert_counts['heat_stress']}")
        print(f"    • Nutrient Deficiency Windows: {alert_counts['nutrients']}")
        print(f"    • pH Abnormal Windows: {alert_counts['ph']}")
        print(f"\n🎯 PRIORITY ACTIONS (based on window aggregations):")
        
        priority = 1
        if alert_counts['heat_stress'] > 0:
            print(f"    {priority}. 🔴 IMMEDIATE: Address {alert_counts['heat_stress']} heat stress window(s)")
            priority += 1
        if alert_counts['low_moisture'] > 0:
            print(f"    {priority}. 🟠 URGENT: Irrigate {alert_counts['low_moisture']} low moisture zone(s)")
            priority += 1
        if alert_counts['high_temp'] > 0 and alert_counts['heat_stress'] == 0:
            print(f"    {priority}. 🟡 HIGH: Monitor {alert_counts['high_temp']} high temperature zone(s)")
            priority += 1
        if alert_counts['nutrients'] > 0:
            print(f"    {priority}. 🟢 MEDIUM: Schedule fertilization for {alert_counts['nutrients']} zone(s)")
            priority += 1
        if alert_counts['ph'] > 0:
            print(f"    {priority}. 🔵 LOW: Plan pH correction for {alert_counts['ph']} zone(s)")
    else:
        print("✅ ALL PARAMETERS NORMAL - NO ACTIVE ALERTS!")
        print("    • Temperature: Within safe range")
        print("    • Soil Moisture: Adequate")
        print("    • Nutrients: Sufficient")
        print("    • pH: Optimal")
        print("    • Continue regular monitoring")
    
    print(f"\n{'='*80}\n")


# === 8. XUẤT KẾT QUẢ RA CONSOLE ===

print("\n" + "="*80)
print(" SPARK STREAMING APPLICATION STARTED")
print("="*80)
print(" Connected to Kafka: kafka:29092")
print(" Topic: sensor-data")
print(" Watermark: 30 seconds")
print("="*80 + "\n")

# Stream 1: Chạy EDA trên từng batch
print("✓ Starting EDA Stream...")
query_eda = watermarked_df.writeStream \
    .foreachBatch(perform_eda_on_batch) \
    .trigger(processingTime='30 seconds') \
    .start()

# Stream 2: Cảnh báo nhiệt độ cao - lưu vào memory
print("✓ Starting High Temperature Alert Stream (60s window)...")
query_temp = high_temp_window.writeStream \
    .outputMode("update") \
    .format("memory") \
    .queryName("high_temp_alerts") \
    .trigger(processingTime='60 seconds') \
    .start()

# Stream 3: Cảnh báo độ ẩm thấp - lưu vào memory
print("✓ Starting Low Moisture Alert Stream (90s window)...")
query_moisture = low_moisture_window.writeStream \
    .outputMode("update") \
    .format("memory") \
    .queryName("low_moisture_alerts") \
    .trigger(processingTime='60 seconds') \
    .start()

# Stream 4: Cảnh báo dinh dưỡng - lưu vào memory
print("✓ Starting Low Nutrients Alert Stream (2min window)...")
query_nutrient = low_nutrient_window.writeStream \
    .outputMode("update") \
    .format("memory") \
    .queryName("low_nutrient_alerts") \
    .trigger(processingTime='60 seconds') \
    .start()

# Stream 5: Cảnh báo sốc nhiệt - lưu vào memory
print("✓ Starting Heat Stress Alert Stream (90s window)...")
query_heat_stress = heat_stress_window.writeStream \
    .outputMode("update") \
    .format("memory") \
    .queryName("heat_stress_alerts") \
    .trigger(processingTime='60 seconds') \
    .start()

# Stream 6: Cảnh báo pH bất thường - lưu vào memory
print("✓ Starting pH Abnormal Alert Stream (2min window)...")
query_ph = ph_abnormal_window.writeStream \
    .outputMode("update") \
    .format("memory") \
    .queryName("ph_abnormal_alerts") \
    .trigger(processingTime='60 seconds') \
    .start()

# Stream 7: Phân tích sức khỏe cây trồng - lưu vào memory
print("✓ Starting Plant Health Analysis Stream (3min window)...")
query_plant_health = plant_health_window.writeStream \
    .outputMode("update") \
    .format("memory") \
    .queryName("plant_health_analysis") \
    .trigger(processingTime='60 seconds') \
    .start()

# Stream 8: Phân tích tổng hợp - lưu vào memory
print("✓ Starting Comprehensive Analysis Stream (5min window)...")
query_comprehensive = comprehensive_analysis_window.writeStream \
    .outputMode("update") \
    .format("memory") \
    .queryName("comprehensive_analysis") \
    .trigger(processingTime='60 seconds') \
    .start()

# Stream 9: Hiển thị Window-based Alerts từ memory tables
print("✓ Starting Window-Based Alerts Display Stream...")
query_alerts_display = watermarked_df \
    .writeStream \
    .foreachBatch(display_window_alerts) \
    .trigger(processingTime='45 seconds') \
    .start()

print("\n" + "="*80)
print(" ALL 9 STREAMS STARTED SUCCESSFULLY!")
print("="*80)
print(" Stream 1: EDA Analysis (every 30s)")
print(" Streams 2-8: Window Aggregations → Memory Tables")
print(" Stream 9: Window-Based Alerts Display (every 45s)")
print("="*80)
print("\n📊 Reports Structure:")
print("  • EDA Report: Basic statistics and data exploration")
print("  • Alerts Report: Window-aggregated alerts with detailed analysis")
print("="*80 + "\n")

# Chờ tất cả các stream kết thúc
spark.streams.awaitAnyTermination()