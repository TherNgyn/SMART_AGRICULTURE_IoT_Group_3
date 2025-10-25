FROM apache/spark:3.5.1-python3

USER root

# Tạo thư mục jars
RUN mkdir -p /opt/spark/jars/custom

# Tải các JAR files cần thiết cho Kafka integration
RUN apt-get update && apt-get install -y wget && \
    wget -q -P /opt/spark/jars/custom/ \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip install redis

# Tạo thư mục app và set quyền
RUN mkdir -p /app && chown -R spark:spark /app

USER spark

WORKDIR /app

COPY scripts/spark_processor.py .

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--jars", "/opt/spark/jars/custom/spark-sql-kafka-0-10_2.12-3.5.1.jar,/opt/spark/jars/custom/kafka-clients-3.4.1.jar,/opt/spark/jars/custom/commons-pool2-2.11.1.jar,/opt/spark/jars/custom/spark-token-provider-kafka-0-10_2.12-3.5.1.jar", \
     "spark_processor.py"]