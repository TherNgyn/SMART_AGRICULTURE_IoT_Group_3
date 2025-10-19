import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import time
import os

# --- CẤU HÌNH ---
# API_URL được đọc từ biến môi trường do docker-compose cung cấp
# Giá trị này sẽ là 'http://api-backend:8000' bên trong Docker network
API_URL = os.getenv('API_URL', 'http://127.0.0.1:8000')
# ------------------

st.set_page_config(layout="wide", page_title="Smart Agriculture Dashboard")
st.title("🚜 Dashboard Giám sát Nông nghiệp Thông minh")

# --- Phần Sidebar Điều khiển ---
st.sidebar.header("Bảng điều khiển")
device_id_input = st.sidebar.text_input("Nhập Device ID", "device_001")

if st.sidebar.button("💧 Bật máy tưới"):
    try:
        payload = {"device_id": device_id_input, "action": "irrigation_on"}
        # request này được thực hiện từ server của Streamlit (bên trong container)
        r = requests.post(f"{API_URL}/control/", json=payload)
        r.raise_for_status() # Báo lỗi nếu status code là 4xx hoặc 5xx
        st.sidebar.success(f"Đã gửi lệnh tưới cho {device_id_input}")
    except requests.exceptions.RequestException as e:
        st.sidebar.error(f"Lỗi API: {e}")

if st.sidebar.button("💨 Bật quạt làm mát"):
    try:
        payload = {"device_id": device_id_input, "action": "fan_on"}
        r = requests.post(f"{API_URL}/control/", json=payload)
        r.raise_for_status()
        st.sidebar.success(f"Đã gửi lệnh bật quạt cho {device_id_input}")
    except requests.exceptions.RequestException as e:
        st.sidebar.error(f"Lỗi API: {e}")

# --- Hàm gọi API ---
@st.cache_data(ttl=10) # Cache dữ liệu trong 10 giây
def fetch_data(endpoint):
    try:
        response = requests.get(f"{API_URL}/{endpoint}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        # Không hiển thị st.error ở đây vì nó sẽ spam màn hình trong vòng lặp
        print(f"Lỗi khi lấy dữ liệu {endpoint}: {e}")
        return []

# --- Phần hiển thị chính ---
placeholder = st.empty()

# Vòng lặp tự động làm mới
while True:
    with placeholder.container():
        # Lấy dữ liệu
        sensor_data = fetch_data("sensor-data/?limit=100")
        alert_data = fetch_data("alerts/?limit=20")

        if not sensor_data:
            st.warning("Không có dữ liệu cảm biến. API đang chạy hoặc đang chờ dữ liệu...")
            df_sensor = pd.DataFrame()
        else:
            try:
                # Chuyển sang DataFrame
                df_sensor = pd.DataFrame(sensor_data)
                df_sensor['timestamp'] = pd.to_datetime(df_sensor['timestamp'])
                df_sensor = df_sensor.sort_values('timestamp')
            except Exception as e:
                st.error(f"Lỗi xử lý dữ liệu sensor: {e}")
                df_sensor = pd.DataFrame() # Tạo dataframe rỗng
        
        # --- Tab hiển thị ---
        tab1, tab2, tab3 = st.tabs([
            "📊 Biểu đồ Cảm biến", 
            "⚠️ Lịch sử Cảnh báo", 
            "📈 Dữ liệu Dự báo (NDI/PDI)"
        ])

        with tab1:
            st.subheader("Dữ liệu cảm biến theo thời gian")
            
            if not df_sensor.empty:
                col1, col2 = st.columns(2)
                with col1:
                    if 'Temperature' in df_sensor.columns:
                        fig_temp = px.line(df_sensor, x='timestamp', y='Temperature', title='Nhiệt độ (°C)', markers=True)
                        fig_temp.add_hline(y=35, line_dash="dot", line_color="red", annotation_text="Ngưỡng nóng")
                        st.plotly_chart(fig_temp, use_container_width=True)
                    else:
                        st.info("Thiếu dữ liệu 'Temperature'")
                
                with col2:
                    if 'Moisture' in df_sensor.columns:
                        fig_moist = px.line(df_sensor, x='timestamp', y='Moisture', title='Độ ẩm (%)', markers=True)
                        fig_moist.add_hline(y=20, line_dash="dot", line_color="red", annotation_text="Ngưỡng khô")
                        st.plotly_chart(fig_moist, use_container_width=True)
                    else:
                        st.info("Thiếu dữ liệu 'Moisture'")
            else:
                st.info("Chưa có dữ liệu biểu đồ.")

        with tab2:
            st.subheader("Lịch sử Cảnh báo Gần đây")
            if alert_data:
                df_alerts = pd.DataFrame(alert_data)
                st.dataframe(df_alerts, use_container_width=True)
            else:
                st.info("Không có cảnh báo nào.")

        with tab3:
            st.subheader("Dữ liệu Dự báo (NDI, PDI, RGB Index)")
            required_cols = ['NDI_Label', 'PDI_Label', 'RGB_Index']
            if not df_sensor.empty and all(col in df_sensor.columns for col in required_cols):
                fig_ndi = px.line(df_sensor, x='timestamp', y='NDI_Label', title='Dự báo NDI (Mức độ)', markers=True)
                st.plotly_chart(fig_ndi, use_container_width=True)
                
                fig_pdi = px.line(df_sensor, x='timestamp', y='PDI_Label', title='Dự báo PDI (Mức độ)', markers=True)
                st.plotly_chart(fig_pdi, use_container_width=True)

                fig_rgb = px.line(df_sensor, x='timestamp', y='RGB_Index', title='Chỉ số RGB', markers=True)
                fig_rgb.add_hline(y=0.7, line_dash="dot", line_color="orange", annotation_text="Ngưỡng cảnh báo")
                st.plotly_chart(fig_rgb, use_container_width=True)
            else:
                st.info("Chưa có dữ liệu dự báo hoặc thiếu cột.")

        # Hiển thị dữ liệu thô
        with st.expander("Xem dữ liệu cảm biến thô (mới nhất)"):
            if not df_sensor.empty:
                st.dataframe(df_sensor, use_container_width=True)
            else:
                st.info("Không có dữ liệu.")
    
    # Tần suất làm mới dashboard (giây)
    time.sleep(10)