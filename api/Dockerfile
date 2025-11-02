FROM python:3.9-slim

WORKDIR /app

COPY ./api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install redis kafka-python fastapi

COPY ./api/api.py .

# Dùng 0.0.0.0 để API có thể được truy cập từ bên ngoài container
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]