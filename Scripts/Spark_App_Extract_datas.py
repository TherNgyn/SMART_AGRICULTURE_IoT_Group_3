from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType
from pymongo import MongoClient
from pyspark.sql import DataFrame

# --- 1. Tạo SparkSession với MongoDB connector ---
spark = SparkSession.builder \
    .appName("KafkaToMongoRaw") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/Realtime-project.raw_data") \
    .getOrCreate()

# --- Schema ---
schema = StructType([
    StructField("RGB_Damage_Score", DoubleType(), True),
    StructField("N", DoubleType(), True),
    StructField("P", DoubleType(), True),
    StructField("K", DoubleType(), True),
    StructField("Moisture", DoubleType(), True),
    StructField("pH", DoubleType(), True),
    StructField("Temperature", DoubleType(), True),
    StructField("Humidity", DoubleType(), True),
    StructField("ts", LongType(), True)
])

# --- Đọc dữ liệu từ Kafka ---
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor-data") \
    .option("startingOffsets", "latest") \
    .load()

json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# --- Cột delta thresholds ---
threshold = {
    "RGB_Damage_Score": 0.05,
    "N": 5,
    "P": 5,
    "K": 5,
    "Moisture": 2,
    "pH": 0.5,
    "Temperature": 2,
    "Humidity": 2
}

def filter_and_save(batch_df, batch_id):
    client = MongoClient("mongodb://localhost:27017/")
    collection = client["Realtime-project"]["raw_data"]

    # Lấy bản ghi cuối cùng từ DB để làm reference đầu tiên
    last_ref = {}
    for col_name in threshold.keys():
        last_record = collection.find_one(sort=[("ts", -1)])
        last_ref[col_name] = last_record[col_name] if last_record and last_record.get(col_name) is not None else None

    rows_to_insert = []

    # Duyệt từng dòng batch theo thứ tự
    for row in batch_df.collect():
        save = False
        for col_name, th in threshold.items():
            val = row[col_name]
            ref = last_ref[col_name]
            if ref is None or val is None or abs(val - ref) >= th:
                save = True
                break
        if save:
            # Lưu dòng này vào MongoDB
            doc = row.asDict()
            collection.insert_one(doc)
            rows_to_insert.append(doc)

            # Update last_ref để dòng tiếp theo trong batch so sánh với dòng này
            for col_name in threshold.keys():
                last_ref[col_name] = row[col_name]

    if rows_to_insert:
        print(f"Batch {batch_id}: inserted {len(rows_to_insert)} rows")

# --- WriteStream với foreachBatch ---
query = json_df.writeStream \
    .foreachBatch(filter_and_save) \
    .option("checkpointLocation", "/tmp/spark_checkpoints/raw_data_delta") \
    .start()

query.awaitTermination()