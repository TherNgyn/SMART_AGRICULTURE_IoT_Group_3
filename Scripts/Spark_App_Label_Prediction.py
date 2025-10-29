from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, rand, randn, mean, stddev
from pyspark.sql.types import StructType, StructField, DoubleType, LongType
from pyspark.ml import PipelineModel, Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pymongo import MongoClient
from datetime import datetime
import torch
import torch.nn as nn
import numpy as np
import collections
import os

spark = SparkSession.builder \
    .appName("KafkaToMongoRaw") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/Realtime-project.predictions") \
    .getOrCreate()

# --- 2. Schema Kafka ---
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

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor-data") \
    .option("startingOffsets", "latest") \
    .load()

json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

def process_batch(df, epoch_id):
    if df.isEmpty():
        return

    df = df.withColumn(
        "RGB_Damage_Score",
        when(col("RGB_Damage_Score").isNull(), (rand() * 0.8) + 0.1).otherwise(col("RGB_Damage_Score"))
    )

    pipeline_path = "/opt/bitnami/spark/models/common_preprocess_pipeline"
    if os.path.exists(pipeline_path):
        pipeline_model = PipelineModel.load(pipeline_path)
    else:
        feature_cols = ["N", "P", "K", "Moisture", "pH", "Temperature", "Humidity", "RGB_Damage_Score"]
        train_data = spark.read.csv("/opt/bitnami/spark/data/train_45000.csv", header=True, inferSchema=True)

        stats = train_data.select(mean(col("RGB_Damage_Score")).alias("mean"),
                                  stddev(col("RGB_Damage_Score")).alias("std")).collect()[0]
        mean_damescore = stats["mean"]
        std_dame_score = stats["std"]

        train_data = train_data.withColumn(
            "RGB_Damage_Score",
            when(col("RGB_Damage_Score").isNull(),
                 mean_damescore + std_dame_score * randn()).otherwise(col("RGB_Damage_Score"))
        )

        indexer_NDI = StringIndexer(inputCol="NDI_Label", outputCol="NDI_Label_Indexed")
        indexer_PDI = StringIndexer(inputCol="PDI_Label", outputCol="PDI_Label_Indexed")
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_unscaled")
        scaler = StandardScaler(inputCol="features_unscaled", outputCol="features", withMean=True, withStd=True)
        pipeline = Pipeline(stages=[indexer_NDI, indexer_PDI, assembler, scaler])
        pipeline_model = pipeline.fit(train_data)
        pipeline_model.save(pipeline_path)

    # --- 5. Transform dữ liệu ---
    processed_df = pipeline_model.transform(df)

    features = np.array(processed_df.select("features").rdd.map(lambda x: x[0]).collect())
    X_tensor = torch.tensor(features, dtype=torch.float32)

    # --- 6. Load mô hình ---
    class MLPClassifier(nn.Module):
        def __init__(self, input_dim, hidden_dims, output_dim, activation):
            super().__init__()
            layers = []
            prev_dim = input_dim
            for h in hidden_dims:
                layers.append(nn.Linear(prev_dim, h))
                layers.append(activation())
                prev_dim = h
            layers.append(nn.Linear(prev_dim, output_dim))
            layers.append(nn.Softmax(dim=1))
            self.model = nn.Sequential(*layers)
        def forward(self, x):
            return self.model(x)

    # Load 2 model PyTorch
    NDI_hidden_layers = [224, 192, 96, 224, 224, 192, 224, 64, 128, 192, 224, 160, 128, 129, 96]
    activation = getattr(nn, "Tanh")
    NDI_model = MLPClassifier(8, NDI_hidden_layers, 1, activation)
    NDI_model.load_state_dict(torch.load("../models/best_modelNDI.pth"))

    PDI_hidden_layers = [128, 128, 192, 256, 224, 64, 256, 224, 96]
    activation = getattr(nn, "LeakyReLU")
    PDI_model = MLPClassifier(8, PDI_hidden_layers, 1, activation)
    PDI_model.load_state_dict(torch.load("../models/best_modePNDI.pth"))

    # --- 7. Dự đoán ---
    with torch.no_grad():
        preds_NDI = torch.argmax(NDI_model(X_tensor), 1).numpy()
        preds_PDI = torch.argmax(PDI_model(X_tensor), 1).numpy()

    # --- 8. Decode ---
    NDI_decoder = pipeline_model.stages[0]
    PDI_decoder = pipeline_model.stages[1]
    NDI_decoded = [NDI_decoder.labels[i] for i in preds_NDI]
    PDI_decoded = [PDI_decoder.labels[i] for i in preds_PDI]

    NDI_mode = collections.Counter(NDI_decoded).most_common(1)[0]
    PDI_mode = collections.Counter(PDI_decoded).most_common(1)[0]

    # --- 9. Ghi vào MongoDB ---
    client = MongoClient("mongodb://mongodb:27017/")
    db = client["Realtime-project"]
    col = db["predictions"]

    result = {
        "timestamp": datetime.now().isoformat(),
        "NDI_Prediction": NDI_mode[0],
        "PDI_Prediction": PDI_mode[0]
    }

    col.insert_one(result)


# --- 10. Chạy streaming ---
query = json_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .start()

query.awaitTermination()
