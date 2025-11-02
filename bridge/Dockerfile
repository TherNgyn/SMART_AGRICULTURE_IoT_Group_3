FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    paho-mqtt \
    confluent-kafka \
    python-dotenv

COPY scripts/ /app/scripts/

CMD ["python", "/app/scripts/mqtt_kafka_bridge.py"]