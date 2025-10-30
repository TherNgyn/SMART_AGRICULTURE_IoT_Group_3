#!/usr/bin/env python3
import os
import sys
import time
import json
import ssl
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
from confluent_kafka import Producer

if __name__ == "__main__":
    try:
        print("=== MQTT-Kafka Bridge Starting ===", flush=True)
        
        # Load environment variables
        load_dotenv()
        print("Environment variables loaded", flush=True)
        
        # Get configuration from environment
        MQTT_BROKER = os.environ["MQTT_BROKER"]
        MQTT_PORT = int(os.environ.get("MQTT_PORT", "8883"))
        MQTT_USERNAME = os.environ["MQTT_USERNAME"]
        MQTT_PASSWORD = os.environ["MQTT_PASSWORD"]
        MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "sensor-data")
        QOS = int(os.environ.get("MQTT_QOS", "1"))

        KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'kafka:29092')
        KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
        KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_SENSOR", "sensor-data")
        KAFKA_ALERTS_TOPIC = os.environ.get("KAFKA_TOPIC_ALERTS", "alerts")
        print("Configuration loaded:", flush=True)
        print(f"  MQTT: {MQTT_BROKER}:{MQTT_PORT}/{MQTT_TOPIC}", flush=True)
        print(f"  Kafka: {KAFKA_BOOTSTRAP}/{KAFKA_TOPIC}", flush=True)
        
        # Wait for Kafka to be ready and create producer
        print("Waiting for Kafka to be ready...", flush=True)
        for i in range(30):
            try:
                producer_config = {
                    'bootstrap.servers': KAFKA_BOOTSTRAP,
                    'client.id': 'mqtt-kafka-bridge',
                    'api.version.request': False,
                    'broker.version.fallback': '2.0.0',
                    'enable.idempotence': False,
                    'acks': '1'
                }
                
                producer = Producer(producer_config)
                metadata = producer.list_topics(timeout=5)
                if metadata:
                    print("Successfully connected to Kafka!", flush=True)
                    break
            except Exception as e:
                print(f"Attempt {i+1}/30: Kafka not ready ({str(e)[:50]}...)", flush=True)
                time.sleep(2)
        else:
            raise Exception("Could not connect to Kafka after 30 attempts")
        
        # MQTT callbacks
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                print(f"MQTT connected successfully! Subscribing to {MQTT_TOPIC}", flush=True)
                client.subscribe(MQTT_TOPIC, qos=QOS)
            else:
                print(f"MQTT connection failed with code {rc}", flush=True)
        
        def on_message(client, userdata, msg):
            try:
                payload_str = msg.payload.decode('utf-8')
                payload = json.loads(payload_str)
                
                producer.produce(
                    topic=KAFKA_TOPIC,
                    value=json.dumps(payload),
                    callback=delivery_callback
                )
                producer.flush()
                
                device_id = payload.get('device_id', 'unknown')
                timestamp = payload.get('ts', 'no-ts')
                print(f"MQTT→Kafka: {device_id} at {timestamp}", flush=True)
                
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e}", flush=True)
            except Exception as e:
                print(f"Error processing message: {e}", flush=True)
        
        def on_disconnect(client, userdata, rc):
            print(f"MQTT disconnected with code {rc}", flush=True)
        
        def delivery_callback(err, msg):
            if err:
                print(f"Kafka delivery failed: {err}", flush=True)
        
        # Setup MQTT client
        print("Setting up MQTT client...", flush=True)
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        
        # Setup TLS for HiveMQ Cloud
        ssl_context = ssl.create_default_context()
        client.tls_set_context(ssl_context)
        
        # Set callbacks
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_disconnect = on_disconnect
        
        # Connect to MQTT broker
        print(f"Connecting to MQTT broker {MQTT_BROKER}:{MQTT_PORT}...", flush=True)
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        
        print("Bridge is now running. Waiting for messages...", flush=True)
        print("(Press Ctrl+C to stop)", flush=True)
        
        # Start the loop
        client.loop_forever()
        
    except KeyboardInterrupt:
        print("\nBridge stopped by user", flush=True)
    except Exception as e:
        print(f"Fatal error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if 'producer' in locals() and producer:
            producer.flush()
        if 'client' in locals():
            client.disconnect()
        print("Bridge shutdown complete", flush=True)
