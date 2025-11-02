FROM python:3.11-slim

WORKDIR /app

# Cài đặt packages với root
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Tạo thư mục data và cấp quyền
RUN mkdir -p /app/data && chmod 777 /app/data

# Copy scripts
COPY scripts/ ./scripts/

# Chạy với root để tránh permission issues
CMD ["sh", "-c", "python /app/scripts/split_data.py && python /app/scripts/iot_simulator.py"]