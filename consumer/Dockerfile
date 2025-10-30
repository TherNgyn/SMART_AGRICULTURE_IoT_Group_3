FROM python:3.9-slim

WORKDIR /app

COPY ./consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./consumer/consumer_canh_bao.py .

CMD ["python", "consumer_canh_bao.py"]