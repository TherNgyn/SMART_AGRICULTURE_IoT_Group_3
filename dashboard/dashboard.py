import streamlit as st
import requests
import pandas as pd
import time
import os

# --- CẤU HÌNH ---
API_URL = os.getenv('API_URL', 'http://127.0.0.1:8000')
REFRESH_RATE_SECONDS = 3
# ------------------

st.set_page_config(layout="wide", page_title="Smart Agriculture Dashboard")
st.title("🚜 Dashboard Giám sát Nông nghiệp (100% Real-time)")

# --- Phần Sidebar Điều khiển ---
st.sidebar.header("Bảng điều khiển")
device_id_input = st.sidebar.text_input("Nhập Device ID", "device_001")

if st.sidebar.button("💧 Bật máy tưới"):
    try:
        payload = {"device_id": device_id_input, "action": "irrigation_on"}
        r = requests.post(f"{API_URL}/control/", json=payload, timeout=5)
        r.raise_for_status() 
        st.sidebar.success(f"Đã gửi lệnh tưới cho {device_id_input}")
    except requests.exceptions.RequestException as e:
        st.sidebar.error(f"Lỗi API: {e}")

if st.sidebar.button("💨 Bật quạt làm mát"):
    try:
        payload = {"device_id": device_id_input, "action": "fan_on"}
        r = requests.post(f"{API_URL}/control/", json=payload, timeout=5)
        r.raise_for_status()
        st.sidebar.success(f"Đã gửi lệnh bật quạt cho {device_id_input}")
    except requests.exceptions.RequestException as e:
        st.sidebar.error(f"Lỗi API: {e}")

# --- Hàm gọi API (Không cache) ---
def fetch_realtime_data(endpoint):
    try:
        response = requests.get(f"{API_URL}/{endpoint}", timeout=2)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi lấy dữ liệu {endpoint}: {e}")
        return None # Trả về None nếu lỗi

# --- Hàm vẽ biểu đồ ---
def draw_chart(data, y_col, title, color=None):
    if data and len(data) > 1:
        try:
            df_chart = pd.DataFrame(data)
            df_chart['timestamp'] = pd.to_datetime(df_chart['timestamp'], unit='s')
            df_chart = df_chart.set_index('timestamp')
            
            st.markdown(f"##### {title}")
            st.line_chart(df_chart, y=y_col, color=color)
        except Exception as e:
            st.error(f"Lỗi khi vẽ biểu đồ {title}: {e}")
    else:
        st.info(f"Đang chờ thêm dữ liệu cho biểu đồ {title}...")

# --- Vòng lặp cập nhật chính ---
while True:
    
    # 1. Lấy tất cả dữ liệu từ API (Redis)
    metrics_data = fetch_realtime_data("metrics/realtime")
    alerts_data = fetch_realtime_data("alerts/realtime?limit=10")
    temp_chart_data = fetch_realtime_data("charts/realtime_temp")
    moisture_chart_data = fetch_realtime_data("charts/realtime_moisture")
    ndi_chart_data = fetch_realtime_data("charts/realtime_ndi")
    pdi_chart_data = fetch_realtime_data("charts/realtime_pdi")
    rgb_chart_data = fetch_realtime_data("charts/realtime_rgb")
    
    # --- Khu vực hiển thị chính ---
    
    # Hàng 1: Chỉ số Metrics
    st.subheader("Trạng thái Tổng quan (Real-time)")
    if metrics_data and "overall_avg_temp" in metrics_data:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Nhiệt độ TB", f"{metrics_data.get('overall_avg_temp', 0)} °C")
        col2.metric("Độ ẩm TB", f"{metrics_data.get('overall_avg_moisture', 0)} %")
        col3.metric("Số thiết bị", metrics_data.get('active_devices', 0))
        col4.metric("Cảnh báo Nóng/Khô", f"{metrics_data.get('high_temp_alerts', 0)} / {metrics_data.get('low_moisture_alerts', 0)}")
    elif metrics_data:
        st.info(metrics_data.get("message", "Đang tải chỉ số..."))
    else:
        st.error("Lỗi: Không thể kết nối API để lấy chỉ số.")

    st.divider()

    # Hàng 2: Biểu đồ chính (Nhiệt & Ẩm)
    st.subheader("Biểu đồ Cảm biến (Real-time, 50 điểm dữ liệu)")
    col1, col2 = st.columns(2)
    with col1:
        draw_chart(temp_chart_data, "value", "Nhiệt độ Trung bình (°C)", "#FF8C00")
    with col2:
        draw_chart(moisture_chart_data, "value", "Độ ẩm Trung bình (%)", "#00BFFF")

    st.divider()
    
    # Hàng 3: Biểu đồ phụ (NDI, PDI, RGB)
    st.subheader("Biểu đồ Phân tích Cây trồng (Real-time, 50 điểm dữ liệu)")
    tab1, tab2, tab3 = st.tabs(["Biểu đồ NDI", "Biểu đồ PDI", "Biểu đồ RGB"])
    with tab1:
        draw_chart(ndi_chart_data, "value", "Mức độ NDI (0=Thấp, 1=TB, 2=Cao)", "#32CD32")
    with tab2:
        draw_chart(pdi_chart_data, "value", "Mức độ PDI (0=Thấp, 1=TB, 2=Cao)", "#FF4500")
    with tab3:
        draw_chart(rgb_chart_data, "value", "Chỉ số Hư hại RGB", "#DA70D6")

    st.divider()

    # Hàng 4: Lịch sử Cảnh báo
    st.subheader("Cảnh báo Real-time (10 cảnh báo gần nhất)")
    if alerts_data:
        df_alerts = pd.DataFrame(alerts_data)
        st.dataframe(df_alerts, use_container_width=True)
    else:
        st.info("Không có cảnh báo nào gần đây.")

    # Tần suất làm mới dashboard (giây)
    time.sleep(REFRESH_RATE_SECONDS)
    st.rerun()