FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# copy scripts at runtime via volume (compose), chỉ cần tạo user non-root cho an toàn
# RUN useradd -m appuser
# USER appuser
