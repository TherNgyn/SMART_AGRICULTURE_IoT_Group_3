FROM bitnami/spark:3.5.0

USER root

RUN pip install --no-cache-dir numpy==1.24.4 \
    && pip install --no-cache-dir torch==2.2.0+cpu -f https://download.pytorch.org/whl/torch_stable.html \
    && pip install --no-cache-dir pandas pymongo