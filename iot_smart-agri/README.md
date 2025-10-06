# Smart Agriculture IoT System

Hệ thống IoT nông nghiệp thông minh với khả năng xử lý dữ liệu real-time và edge computing.

## Cấu Trúc Dữ Liệu Mới

Dataset bao gồm 24 cột dữ liệu từ hệ thống IoT nông nghiệp:

### Thông Tin Vùng & Hình Ảnh

- `Zone_ID`: ID vùng canh tác (Z1, Z2, ..., Z10)
- `Image_Source_ID`: ID file ảnh từ UAV
- `Image_Type`: Loại ảnh (RGB, Multispectral)
- `UAV_Timestamp`: Timestamp chụp ảnh từ UAV

### Chỉ Số Thực Vật

- `NDVI`: Normalized Difference Vegetation Index (0-1)
- `NDRE`: Normalized Difference Red Edge Index (0-1)
- `RGB_Damage_Score`: Điểm đánh giá thiệt hại từ ảnh RGB (0-1)

### Thông Số Đất & Môi Trường

- `N`, `P`, `K`: Nồng độ dinh dưỡng NPK
- `Moisture`: Độ ẩm đất (%)
- `pH`: Độ pH của đất
- `Temperature`: Nhiệt độ (°C)
- `Humidity`: Độ ẩm không khí (%)

### Nhãn & Khuyến Nghị

- `NDI_Label`: Nhãn Nutrient Deficiency Index (Low/Medium/High)
- `PDI_Label`: Nhãn Plant Disease Index (Low/Medium/High)
- `Semantic_Tag`: Thẻ ngữ nghĩa mô tả tình trạng
- `Action_Suggested`: Hành động được khuyến nghị

### Edge Computing Metrics

- `Energy_Consumed_mAh`: Năng lượng tiêu thụ (mAh)
- `Latency_ms`: Độ trễ xử lý (ms)
- `Current_Node`: Node hiện tại xử lý
- `Migrated_To`: Node được migrate tới
- `Migration_Required`: Yêu cầu migration (Yes/No)
- `Migration_Timestamp`: Timestamp migration

## Cài Đặt

1. **Clone repository**

```bash
git clone https://github.com/TherNgyn/SMART_AGRICULTURE_IoT_Group_3.git
cd iot_smart-agri
```

2. **Cài đặt dependencies**

```bash
pip install -r requirements.txt
```

3. **Cấu hình environment**

```bash
cp .env.example .env
# Chỉnh sửa .env với thông tin MQTT broker của bạn
```

4. **Chia dữ liệu**

```bash
python scripts/split_data.py
```

## Sử Dụng

### 1. Phân tích dữ liệu

```bash
python scripts/analyze_data.py
```

### 2. Test MQTT connection

```bash
python scripts/test_mqtt.py
```

### 3. Test publish message

```bash
python scripts/test_publish.py
```

### 4. Chạy IoT simulator

```bash
python scripts/iot_simulator.py
```

### 5. Chạy hệ thống complete với Docker

```bash
docker-compose up --build
```

## Kiến Trúc Hệ Thống

```
IoT Sensors → MQTT Broker → Kafka → Data Processing
     ↓
Edge Computing Nodes ← Migration Logic
     ↓
Real-time Analytics & Dashboard
```

### Components

1. **IoT Simulator**: Mô phỏng sensors gửi dữ liệu qua MQTT
2. **MQTT-Kafka Bridge**: Chuyển dữ liệu từ MQTT sang Kafka
3. **Edge Computing**: Xử lý dữ liệu phân tán với migration logic
4. **Data Processing**: Phân tích và xử lý dữ liệu real-time

## Dữ Liệu Mẫu

Dataset chứa 60,000+ records với:

- 10 vùng canh tác (Z1-Z10)
- Dữ liệu từ nhiều loại sensor
- Hình ảnh multispectral và RGB
- Metrics edge computing
- Khuyến nghị hành động

## File Structure

```
iot_smart-agri/
├── data/
│   ├── data.csv              # Dataset gốc
│   ├── train_45000.csv       # Dữ liệu training
│   └── stream_15000.csv      # Dữ liệu streaming
├── scripts/
│   ├── iot_simulator.py      # IoT data simulator
│   ├── mqtt_kafka_bridge.py # MQTT-Kafka bridge
│   ├── split_data.py         # Data splitting
│   ├── test_mqtt.py          # MQTT test
│   └── test_publish.py       # Publish test
├── docker-compose.yml        # Docker services
├── requirements.txt          # Python dependencies
```

## Monitoring & Analytics

Hệ thống cung cấp:

- Real-time monitoring của sensors
- Edge computing performance metrics
- Migration analytics
- Environmental condition tracking
- Crop health assessment

## Contributing

1. Fork repository
2. Tạo feature branch
3. Commit changes
4. Push và tạo Pull Request

## License

MIT License
