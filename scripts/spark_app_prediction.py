from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, rand, randn, mean, stddev
from pyspark.sql.types import StructType, StructField, DoubleType, LongType
from pyspark.ml import PipelineModel, Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pymongo import MongoClient
from datetime import datetime
import numpy as np
import torch
import torch.nn as nn
import collections
import os
import redis
import json
import pandas as pd
import pickle

spark = SparkSession.builder \
    .appName("KafkaToMongoClassification") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/Realtime-project.sensor_predicted") \
    .getOrCreate()

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

def load_action_models():
    try:
        with open("/opt/bitnami/spark/models/action_model.pkl", 'rb') as f:
            action_model = pickle.load(f)
        with open("/opt/bitnami/spark/models/action_scaler.pkl", 'rb') as f:
            action_scaler = pickle.load(f)
        with open("/opt/bitnami/spark/models/action_encoder.pkl", 'rb') as f:
            action_encoder = pickle.load(f)
        print("Loaded Action models from mounted directory")
        return action_model, action_scaler, action_encoder
    except Exception as e:
        print(f"Failed to load Action models: {e}")
        return None, None, None

def create_action_features_with_predictions(df_pandas, ndi_pred, pdi_pred):
    features = pd.DataFrame()
    
    features['N_log'] = np.log1p(df_pandas['N'])
    features['P_log'] = np.log1p(df_pandas['P'])
    features['K_log'] = np.log1p(df_pandas['K'])
    features['Moisture_sqrt'] = np.sqrt(df_pandas['Moisture'])
    features['pH_square'] = df_pandas['pH'] ** 2
    features['Temperature_square'] = df_pandas['Temperature'] ** 2 
    features['Humidity_sqrt'] = np.sqrt(df_pandas['Humidity'])
    features['NP_interaction'] = features['N_log'] * features['P_log']
    features['Moisture_Temp'] = features['Moisture_sqrt'] * df_pandas['Temperature']

    ndi_map = {"High": 0, "Low": 1, "Medium": 2}
    pdi_map = {"High": 0, "Low": 1, "Medium": 2}
    
    features['NDI_encoded'] = ndi_map.get(ndi_pred, 2)
    features['PDI_encoded'] = pdi_map.get(pdi_pred, 2)
    
    return features.values

ACTION_MODEL, ACTION_SCALER, ACTION_ENCODER = load_action_models()

def process_batch(df, epoch_id):
    if df.isEmpty():
        return
    from pyspark.sql.functions import col, when, rand
    import torch

    df = df.withColumn(
        "RGB_Damage_Score",
        when(col("RGB_Damage_Score").isNull(), (rand() * 0.8) + 0.1).otherwise(col("RGB_Damage_Score"))
    )
    current_timestamp = datetime.now().isoformat()
    current_ts_ms = int(datetime.now().timestamp() * 1000)
    pipeline_path = "/opt/bitnami/spark/models/common_preprocess_pipeline"
    pipeline_model = None
    try:
        if os.path.exists(pipeline_path):
            pipeline_model = PipelineModel.load(pipeline_path)
            print(f"Loaded existing pipeline from {pipeline_path}")
    except Exception as e:
        print(f"Failed to load pipeline: {e}")
        print("Creating new pipeline...")
        pipeline_model = None

    if pipeline_model is None:
        feature_cols = ["N", "P", "K", "Moisture", "pH", "Temperature", "Humidity", "RGB_Damage_Score"]
        train_data = spark.read.csv("/app/data/train_45000.csv", header=True, inferSchema=True)

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
        
        import shutil
        if os.path.exists(pipeline_path):
            shutil.rmtree(pipeline_path)
        
        pipeline_model.save(pipeline_path)
        print(f"Created and saved new pipeline to {pipeline_path}")

    processed_df = pipeline_model.transform(df)
    features = np.array(processed_df.select("features").rdd.map(lambda x: x[0]).collect())
    X_tensor = torch.tensor(features, dtype=torch.float32)

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

    NDI_hidden_layers = [224, 192, 96, 224, 224, 192, 224, 64, 128, 192, 224, 160, 128, 192, 96]
    activation = getattr(nn, "Tanh")
    NDI_model = MLPClassifier(8, NDI_hidden_layers, 3, activation)
    NDI_model.load_state_dict(torch.load("/opt/bitnami/spark/models/best_modelNDI.pth"))

    PDI_hidden_layers = [128, 128, 192, 256, 224, 64, 256, 224, 96]
    activation = getattr(nn, "LeakyReLU")
    PDI_model = MLPClassifier(8, PDI_hidden_layers, 3, activation)
    PDI_model.load_state_dict(torch.load("/opt/bitnami/spark/models/best_modelPDI.pth"))

    with torch.no_grad():
        preds_NDI = torch.argmax(NDI_model(X_tensor), 1).numpy()
        preds_PDI = torch.argmax(PDI_model(X_tensor), 1).numpy()

    NDI_decoder = pipeline_model.stages[0]
    PDI_decoder = pipeline_model.stages[1]
    NDI_decoded = [NDI_decoder.labels[i] for i in preds_NDI]
    PDI_decoded = [PDI_decoder.labels[i] for i in preds_PDI]

    NDI_mode = collections.Counter(NDI_decoded).most_common(1)[0]
    PDI_mode = collections.Counter(PDI_decoded).most_common(1)[0]

    action_prediction = "Monitor"  
    action_confidence = 0.0
    
    if ACTION_MODEL is not None and ACTION_SCALER is not None and ACTION_ENCODER is not None:
        try:
            df_pandas = df.select("N", "P", "K", "Moisture", "pH", "Temperature", "Humidity").toPandas()
            
            predicted_ndi = NDI_mode[0]  
            predicted_pdi = PDI_mode[0]  
            
            print(f" Using predicted labels for Action: NDI={predicted_ndi}, PDI={predicted_pdi}")
            
            action_features = create_action_features_with_predictions(
                df_pandas, predicted_ndi, predicted_pdi
            )
            
            action_features_scaled = ACTION_SCALER.transform(action_features)

            action_pred = ACTION_MODEL.predict(action_features_scaled)
            action_decoded = ACTION_ENCODER.inverse_transform(action_pred)
            action_mode = collections.Counter(action_decoded).most_common(1)[0]
            
            action_prediction = action_mode[0]
            action_confidence = round(action_mode[1] / len(action_decoded) * 100, 2)
            
            print(f" Action predicted: {action_prediction} ({action_confidence}% confidence)")
            
        except Exception as e:
            print(f" Error in Action prediction: {e}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")

    try:
        client = MongoClient("mongodb://mongodb:27017/")
        db = client["Realtime-project"]
        col = db["sensor_predicted"]  

        complete_result = {
            "timestamp": datetime.now().isoformat(),
            "event_timestamp": current_ts_ms,
            "epoch_id": epoch_id,
            "NDI_Prediction": NDI_mode[0],
            "PDI_Prediction": PDI_mode[0],
            "Action_Suggestion": action_prediction,
            "total_samples": len(NDI_decoded),
            "model_pipeline": "NDI_PDI_Action_Complete"
        }

        col.insert_one(complete_result)
        print(f"Saved ALL predictions to MongoDB: NDI={NDI_mode[0]}, PDI={PDI_mode[0]}, Action={action_prediction}")
        
    except Exception as e:
        print(f" MongoDB error: {e}")

    try:
        r = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)
        redis_prediction_data = {
            "timestamp": current_timestamp,
            "event_timestamp": current_ts_ms,
            "NDI_Prediction": NDI_mode[0],
            "PDI_Prediction": PDI_mode[0],
            "Action_Suggestion": action_prediction
        }

        r.set("dashboard_model_predictions", json.dumps(redis_prediction_data))

        r.lpush("chart_realtime_ndi_prediction", json.dumps({
            "timestamp": current_timestamp,
            "event_timestamp": current_ts_ms,
            "prediction": NDI_mode[0]
        }))
        r.ltrim("chart_realtime_ndi_prediction", 0, 49)

        r.lpush("chart_realtime_pdi_prediction", json.dumps({
            "timestamp": current_timestamp,
            "event_timestamp": current_ts_ms,
            "prediction": PDI_mode[0]
        }))
        r.ltrim("chart_realtime_pdi_prediction", 0, 49)

        r.lpush("chart_realtime_action_prediction", json.dumps({
            "timestamp": current_timestamp,
            "event_timestamp": current_ts_ms,
            "prediction": action_prediction
        }))
        r.ltrim("chart_realtime_action_prediction", 0, 49)

        print(f" Redis updated: NDI={NDI_mode[0]}, PDI={PDI_mode[0]}, Action={action_prediction}")

    except Exception as e:
        print(f"Redis error: {e}")

query = json_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .start()

query.awaitTermination()