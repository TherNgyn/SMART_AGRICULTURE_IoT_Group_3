import streamlit as st
import requests
import pandas as pd
import time
import os

API_URL = os.getenv('API_URL', 'http://127.0.0.1:8000')
REFRESH_RATE_SECONDS = 3

st.set_page_config(layout="wide", page_title="Smart Agriculture Dashboard")
st.title("🚜 Dashboard Giám sát Nông nghiệp (100% Real-time)")

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

def decode_prediction_level(prediction):
    """Giải mã mức độ dự đoán thành tên dễ hiểu"""
    level_map = {
        "Low": "Thấp",
        "Medium": "Trung bình", 
        "High": "Cao",
        "0": "Thấp",
        "1": "Trung bình",
        "2": "Cao"
    }
    return level_map.get(str(prediction), str(prediction))

def decode_action_suggestion(action):
   
    action_map = {
        "Apply Fertilizer": "Bón phân",
        "Apply Pesticide": "Phun thuốc",
        "Irrigate": "Tưới nước",
        "Monitor": "Theo dõi"
    }
    return action_map.get(str(action), str(action))

def fetch_realtime_data(endpoint):
    try:
        response = requests.get(f"{API_URL}/{endpoint}", timeout=2)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi lấy dữ liệu {endpoint}: {e}")
        return None

def draw_chart(data, y_col, title, color=None):
    if data and len(data) > 1:
        try:
            df_chart = pd.DataFrame(data)
         
            time_col = None
            if 'event_timestamp' in df_chart.columns:
                time_col = 'event_timestamp'
            elif 'timestamp' in df_chart.columns:
                time_col = 'timestamp'
            
            if time_col:
               
                if df_chart[time_col].max() > 1e10: 
                    df_chart['datetime'] = pd.to_datetime(df_chart[time_col], unit='ms')
                else:
                    df_chart['datetime'] = pd.to_datetime(df_chart[time_col], unit='s')
                
                df_chart = df_chart.set_index('datetime')
                df_chart = df_chart.sort_index()  
                
                st.markdown(f"##### {title}")
                st.line_chart(df_chart, y=y_col, color=color)
            else:
                st.warning(f"Không tìm thấy cột thời gian trong dữ liệu {title}")
                
        except Exception as e:
            st.error(f"Lỗi khi vẽ biểu đồ {title}: {e}")
    else:
        st.info(f"Đang chờ thêm dữ liệu cho biểu đồ {title}...")

def draw_prediction_chart(data, title, color=None):
    """Vẽ biểu đồ cho predictions với data structure khác"""
    if data and len(data) > 1:
        try:
            df_chart = pd.DataFrame(data)
        
            time_col = None
            if 'event_timestamp' in df_chart.columns:
                time_col = 'event_timestamp'
            elif 'timestamp' in df_chart.columns:
                time_col = 'timestamp'
            
            if time_col:
        
                if df_chart[time_col].max() > 1e10:
                    df_chart['datetime'] = pd.to_datetime(df_chart[time_col], unit='ms')
                else:
                    df_chart['datetime'] = pd.to_datetime(df_chart[time_col], unit='s')
                df_chart = df_chart.set_index('datetime')
                df_chart = df_chart.sort_index()
                
                # Tìm cột value để vẽ
                value_col = None
                for col in ['prediction_value', 'value', 'prediction']:
                    if col in df_chart.columns:
                        value_col = col
                        break
                
                if value_col:
                    st.markdown(f"##### {title}")
                    st.line_chart(df_chart, y=value_col, color=color)
                else:
                    st.warning(f"Không tìm thấy cột dữ liệu trong {title}")
            else:
                st.warning(f"Không tìm thấy cột thời gian trong {title}")
                
        except Exception as e:
            st.error(f"Lỗi khi vẽ biểu đồ {title}: {e}")
    else:
        st.info(f"Đang chờ thêm dữ liệu cho {title}...")


if 'first_run' not in st.session_state:
    st.session_state.first_run = True

def main():
    metrics_data = fetch_realtime_data("metrics/realtime")
    alerts_data = fetch_realtime_data("alerts/realtime?limit=10")
    temp_chart_data = fetch_realtime_data("charts/realtime_temp")
    moisture_chart_data = fetch_realtime_data("charts/realtime_moisture")

    # Lấy predictions charts
    predictions_data = fetch_realtime_data("predictions/model")
    nutrition_chart_data = fetch_realtime_data("charts/nutrition_level")
    pest_disease_chart_data = fetch_realtime_data("charts/pest_disease_level")

    
    st.subheader("Trạng thái Tổng quan (Real-time)")
    if metrics_data and "overall_avg_temp" in metrics_data:
        col1, col2, col3 = st.columns(3)
        col1.metric("🌡️ Nhiệt độ", f"{metrics_data.get('overall_avg_temp', 0):.1f}°C")
        col2.metric("💧 Độ ẩm", f"{metrics_data.get('overall_avg_moisture', 0):.1f}%")
        col3.metric("📱 Thiết bị", metrics_data.get('active_devices', 0))
        
        col4, col5, col6, col7 = st.columns(4) 
        
        if predictions_data and "nutrition_level" in predictions_data:
            nutrition_level = decode_prediction_level(predictions_data.get("nutrition_level"))
            pest_level = decode_prediction_level(predictions_data.get("pest_disease_level"))
            action_suggestion = decode_action_suggestion(predictions_data.get("action_suggestion"))  # NEW
            
            col4.metric("🌿 Dinh dưỡng", 
                    nutrition_level,
                    f"{predictions_data.get('nutrition_confidence', 0):.0f}%")
            col5.metric("🐛 Sâu bệnh", 
                    pest_level,
                    f"{predictions_data.get('pest_disease_confidence', 0):.0f}%")
            col6.metric("🎯 Gợi ý",  # NEW
                    action_suggestion,
                    f"{predictions_data.get('action_confidence', 0):.0f}%")
        else:
            col4.metric("🌿 Dinh dưỡng", "Đang KT")
            col5.metric("🐛 Sâu bệnh", "Đang KT")
            col6.metric("🎯 Gợi ý", "Đang KT")  

        col7.metric("Cảnh báo", f"{metrics_data.get('high_temp_alerts', 0)}/{metrics_data.get('low_moisture_alerts', 0)}")

    elif metrics_data:
        st.info(metrics_data.get("message", "Đang tải chỉ số..."))
    else:
        st.error("Lỗi: Không thể kết nối API để lấy chỉ số.")

    st.divider()

    st.subheader("Biểu đồ Cảm biến (Real-time, 50 điểm dữ liệu)")
    col1, col2 = st.columns(2)
    with col1:
        draw_chart(temp_chart_data, "value", "Nhiệt độ Trung bình (°C)", "#FF8C00")
    with col2:
        draw_chart(moisture_chart_data, "value", "Độ ẩm Trung bình (%)", "#00BFFF")

    st.divider()

    st.subheader("🤖 Dự đoán Chỉ số Cây trồng")
    pred_col1, pred_col2 = st.columns(2)
    
    with pred_col1:
        if nutrition_chart_data and len(nutrition_chart_data) > 1:
            try:
                df_nutrition = pd.DataFrame(nutrition_chart_data)
                
                
                time_col = None
                if 'event_timestamp' in df_nutrition.columns:
                    time_col = 'event_timestamp'
                elif 'timestamp' in df_nutrition.columns:
                    time_col = 'timestamp'
                
                if time_col:
                    # Xử lý timestamp
                    if df_nutrition[time_col].max() > 1e10:
                        df_nutrition['datetime'] = pd.to_datetime(df_nutrition[time_col], unit='ms')
                    else:
                        df_nutrition['datetime'] = pd.to_datetime(df_nutrition[time_col], unit='s')
                    df_nutrition = df_nutrition.set_index('datetime')
                    df_nutrition = df_nutrition.sort_index()
                    
                    # Map prediction values
                    prediction_map = {"Low": 0, "Medium": 1, "High": 2, "0": 0, "1": 1, "2": 2}
                    if 'prediction' in df_nutrition.columns:
                        df_nutrition['prediction_value'] = df_nutrition['prediction'].map(prediction_map)
                        st.markdown("##### 📈 Mức độ Dinh dưỡng")
                        st.line_chart(df_nutrition, y='prediction_value', color="#32CD32")
                        st.caption("0: Thấp, 1: Trung bình, 2: Cao")
                    elif 'value' in df_nutrition.columns:
                        st.markdown("##### 📈 Mức độ Dinh dưỡng")
                        st.line_chart(df_nutrition, y='value', color="#32CD32")
                    else:
                        st.warning("Không tìm thấy cột dữ liệu trong biểu đồ dinh dưỡng")
                else:
                    st.warning("Không tìm thấy cột thời gian trong dữ liệu dinh dưỡng")
                    
            except Exception as e:
                st.error(f"Lỗi biểu đồ dinh dưỡng: {e}")
        else:
            st.info("⏳ Đang chờ dữ liệu dinh dưỡng...")
    
    with pred_col2:
        if pest_disease_chart_data and len(pest_disease_chart_data) > 1:
            try:
                df_pest = pd.DataFrame(pest_disease_chart_data)
                
                # SỬA: Sử dụng event_timestamp
                time_col = None
                if 'event_timestamp' in df_pest.columns:
                    time_col = 'event_timestamp'
                elif 'timestamp' in df_pest.columns:
                    time_col = 'timestamp'
                
                if time_col:
                    # Xử lý timestamp
                    if df_pest[time_col].max() > 1e10:
                        df_pest['datetime'] = pd.to_datetime(df_pest[time_col], unit='ms')
                    else:
                        df_pest['datetime'] = pd.to_datetime(df_pest[time_col], unit='s')
                    df_pest = df_pest.set_index('datetime')
                    df_pest = df_pest.sort_index()
                    
                    # Map prediction values
                    prediction_map = {"Low": 0, "Medium": 1, "High": 2, "0": 0, "1": 1, "2": 2}
                    if 'prediction' in df_pest.columns:
                        df_pest['prediction_value'] = df_pest['prediction'].map(prediction_map)
                        st.markdown("##### 📉 Mức độ Sâu bệnh")  
                        st.line_chart(df_pest, y='prediction_value', color="#FF4500")
                        st.caption("0: Thấp, 1: Trung bình, 2: Cao")
                    elif 'value' in df_pest.columns:
                        st.markdown("##### 📉 Mức độ Sâu bệnh")
                        st.line_chart(df_pest, y='value', color="#FF4500")
                    else:
                        st.warning("Không tìm thấy cột dữ liệu trong biểu đồ sâu bệnh")
                else:
                    st.warning("Không tìm thấy cột thời gian trong dữ liệu sâu bệnh")
                    
            except Exception as e:
                st.error(f"Lỗi biểu đồ sâu bệnh: {e}")
        else:
            st.info("⏳ Đang chờ dữ liệu sâu bệnh...")

   

    st.divider()

    st.subheader("🚨 Cảnh báo (10 cảnh báo gần nhất)")
    if alerts_data:
        try:
            df_alerts = pd.DataFrame(alerts_data)
            display_cols = ['event_timestamp', 'timestamp', 'device_id', 'alert_type', 'severity', 'message']
            available_cols = [col for col in display_cols if col in df_alerts.columns]
            
            if available_cols:
                st.dataframe(df_alerts[available_cols], use_container_width=True)
            else:
                st.dataframe(df_alerts, use_container_width=True)
        except Exception as e:
            st.error(f"Lỗi hiển thị cảnh báo: {e}")
    else:
        st.info("Không có cảnh báo nào gần đây.")

if st.session_state.first_run:
    st.session_state.first_run = False
    main()
else:
    main()
    
time.sleep(REFRESH_RATE_SECONDS)
st.rerun()