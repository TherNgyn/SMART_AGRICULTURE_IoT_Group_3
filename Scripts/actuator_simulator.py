import os
import json
import ssl
import time
import logging
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

MQTT_BROKER = os.getenv('MQTT_BROKER')
MQTT_PORT = int(os.getenv('MQTT_PORT', 8883))
MQTT_USERNAME = os.getenv('MQTT_USERNAME')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')

CONTROL_TOPICS = [
    'agriculture/control/fan',
    'agriculture/control/irrigation'
]

actuator_states = {
    'fan': {},
    'irrigation': {}
}

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Actuator Simulator connected thanh cong!")
        for topic in CONTROL_TOPICS:
            client.subscribe(topic, qos=1)
            logger.info(f"Subscribed to {topic}")
    else:
        logger.error(f"Connection failed voi code {rc}")

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode('utf-8')
        data = json.loads(payload_str)
        
        device_id = data.get('device_id', 'unknown')
        command = data.get('command', 'OFF')
        timestamp = data.get('timestamp', int(time.time()))
        
        actuator_type = 'fan' if 'fan' in msg.topic else 'irrigation'
        
        if command == 'ON':
            actuator_states[actuator_type][device_id] = {
                'status': 'ACTIVE',
                'activated_at': timestamp,
                'duration': 0
            }
            logger.warning(f"{'='*60}")
            logger.warning(f"KICH HOAT {actuator_type.upper()}: {device_id}")
            logger.warning(f"Thoi gian: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))}")
            logger.warning(f"{'='*60}")
            
            if actuator_type == 'fan':
                logger.info(f"[{device_id}] Quat dang hoat dong - Lam mat khu vuc")
            else:
                logger.info(f"[{device_id}] He thong tuoi nuoc dang phun - Tang do am")
                
        elif command == 'OFF':
            if device_id in actuator_states[actuator_type]:
                state = actuator_states[actuator_type][device_id]
                duration = timestamp - state['activated_at']
                logger.info(f"TAT {actuator_type.upper()}: {device_id} (Hoat dong: {duration}s)")
                actuator_states[actuator_type][device_id]['status'] = 'INACTIVE'
            
    except json.JSONDecodeError as e:
        logger.warning(f"Loi decode JSON: {e}")
    except Exception as e:
        logger.error(f"Loi xu ly message: {e}")

def on_disconnect(client, userdata, rc):
    logger.warning(f"Disconnected voi code {rc}")

def report_status():
    while True:
        time.sleep(30)
        logger.info("\n" + "="*60)
        logger.info("TRANG THAI ACTUATORS:")
        
        for actuator_type in ['fan', 'irrigation']:
            active_devices = [
                device_id for device_id, state in actuator_states[actuator_type].items()
                if state.get('status') == 'ACTIVE'
            ]
            if active_devices:
                logger.info(f"{actuator_type.upper()}: {len(active_devices)} thiet bi dang hoat dong")
                for device_id in active_devices:
                    state = actuator_states[actuator_type][device_id]
                    duration = int(time.time()) - state['activated_at']
                    logger.info(f"  - {device_id}: {duration}s")
            else:
                logger.info(f"{actuator_type.upper()}: Khong co thiet bi hoat dong")
        logger.info("="*60 + "\n")

if __name__ == "__main__":
    try:
        logger.info("=== ACTUATOR SIMULATOR (Fan + Irrigation) ===")
        logger.info(f"MQTT Broker: {MQTT_BROKER}:{MQTT_PORT}")
        
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="actuator_simulator")
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        ssl_context = ssl.create_default_context()
        client.tls_set_context(ssl_context)
        
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_disconnect = on_disconnect
        
        logger.info(f"Dang ket noi MQTT broker {MQTT_BROKER}:{MQTT_PORT}...")
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        
        import threading
        status_thread = threading.Thread(target=report_status, daemon=True)
        status_thread.start()
        
        logger.info("ACTUATOR SIMULATOR dang chay...")
        logger.info("Cho lenh dieu khien tu Alert System...")
        logger.info("(Nhan Ctrl+C de dung)")
        
        client.loop_forever()
        
    except KeyboardInterrupt:
        logger.info("\nDung simulator boi user")
    except Exception as e:
        logger.error(f"Loi nghiem trong: {e}")
        import traceback
        traceback.print_exc()
    finally:
        logger.info("Actuator simulator da dong")