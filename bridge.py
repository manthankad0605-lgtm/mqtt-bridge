"""
Railway MQTT → Supabase Bridge  (Multi-Device Edition)
=======================================================
Listens on  rs485/sensors/1  rs485/sensors/2  rs485/sensors/3
Inserts each frame into Supabase with the correct device_id.

Requirements (pip install):
  paho-mqtt==2.1.0
  supabase==2.3.4
  python-dotenv

Environment variables (set in Railway → Variables):
  HIVEMQ_HOST     22b764b37c7c440f9a258c375e19b6bf.s1.eu.hivemq.cloud
  HIVEMQ_PORT     8883
  MQTT_USER       Modbus_sniffer
  MQTT_PASS       Admin123
  SB_URL          https://bfzufsnqvjtmzvjdmoyu.supabase.co
  SB_KEY          <your supabase service_role or anon key>
"""

import os, json, logging, time
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
HIVEMQ_HOST = os.environ.get("HIVEMQ_HOST", "22b764b37c7c440f9a258c375e19b6bf.s1.eu.hivemq.cloud")
HIVEMQ_PORT = int(os.environ.get("HIVEMQ_PORT", 8883))
MQTT_USER   = os.environ.get("MQTT_USER", "Modbus_sniffer")
MQTT_PASS   = os.environ.get("MQTT_PASS", "Admin123")
SB_URL      = os.environ.get("SB_URL", "https://bfzufsnqvjtmzvjdmoyu.supabase.co")
SB_KEY      = os.environ.get("SB_KEY", "")   # set in Railway env vars

# Subscribe to all three device topics
MQTT_TOPICS = [
    ("rs485/sensors/1", 1),
    ("rs485/sensors/2", 1),
    ("rs485/sensors/3", 1),
]

# ── Column names (120 registers, matches DB columns) ──────────
DB_COLS = [
    'pump_1','pump_2','pump_3','pump_4','pump_5','pump_6','pump_7','pump_8','pump_9','pump_10',
    'comp_1','comp_2','comp_3','comp_4','comp_5','comp_6','comp_7','comp_8','comp_9','comp_10',
    'comp_11','comp_12','comp_13','comp_14','comp_15','comp_16','comp_17','comp_18','comp_19','comp_20',
    'flow_temp','return_temp','power','temp_1','temp_2','temp_3','flow_rate_lpm',
    'comp_tev_sh','comp_x1','comp_start_open_ratio',
    'comp_p_gain','comp_i_time','comp_d_time','comp_alarm','comp_superheat',
    'comp_sat_temp','comp_pressure','comp_temp','comp_x2','comp_eev_ratio',
    'tev1_sh','tev1_x','tev1_start_open_ratio','tev1_p_gain','tev1_i_time','tev1_d_time',
    'tev1_alarm','tev1_superheat','tev1_sat_temp','tev1_pressure','tev1_temp','tev1_x2',
    'tev1_eev_ratio_1','tev1_eev_ratio_2','tev1_eev_ratio_3','tev1_eev_ratio_4',
    'tev1_eev_ratio_5','tev1_eev_ratio_6','tev1_eev_ratio_7','tev1_eev_ratio_8',
    'tev2_sh','tev2_x','tev2_start_open_ratio','tev2_p_gain','tev2_i_time','tev2_d_time',
    'tev2_alarm','tev2_superheat','tev2_sat_temp','tev2_pressure','tev2_temp','tev2_x2',
    'tev2_eev_ratio_1','tev2_eev_ratio_2','tev2_eev_ratio_3','tev2_eev_ratio_4',
    'tev2_eev_ratio_5','tev2_eev_ratio_6','tev2_eev_ratio_7','tev2_eev_ratio_8',
    'tev3_sh','tev3_x','tev3_start_open_ratio','tev3_p_gain','tev3_i_time','tev3_d_time',
    'tev3_alarm','tev3_superheat','tev3_sat_temp','tev3_pressure','tev3_temp','tev3_x2',
    'tev3_eev_ratio_1','tev3_eev_ratio_2','tev3_eev_ratio_3','tev3_eev_ratio_4',
    'tev3_eev_ratio_5','tev3_eev_ratio_6','tev3_eev_ratio_7','tev3_eev_ratio_8',
    'tev4_subcool','tev4_sat_temp','tev4_sh','tev4_x','tev4_start_open_ratio',
    'tev4_p_gain','tev4_i_time','tev4_d_time','tev4_alarm','tev4_eev_ratio',
]

# ── Supabase client ───────────────────────────────────────────
sb: Client = create_client(SB_URL, SB_KEY)

def topic_to_device_id(topic: str) -> int:
    """Extract device number from topic like rs485/sensors/2  →  2"""
    try:
        return int(topic.split("/")[-1])
    except (ValueError, IndexError):
        return 1  # fallback

def insert_frame(device_id: int, registers: list, ts: str | None):
    if len(registers) != 120:
        log.warning("Expected 120 registers, got %d — skipping", len(registers))
        return

    row = {
        "device_id":   device_id,
        "recorded_at": ts or datetime.now(timezone.utc).isoformat(),
    }
    for i, col in enumerate(DB_COLS):
        row[col] = int(registers[i])

    try:
        sb.table("sensor_data").insert(row).execute()
        log.info("Device %d → inserted @ %s", device_id, row["recorded_at"])
    except Exception as exc:
        log.error("Supabase insert error (device %d): %s", device_id, exc)

# ── MQTT callbacks ────────────────────────────────────────────
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        log.info("MQTT connected")
        client.subscribe(MQTT_TOPICS)
        log.info("Subscribed: %s", [t for t, _ in MQTT_TOPICS])
    else:
        log.error("MQTT connect failed, reason=%s", reason_code)

def on_disconnect(client, userdata, flags, reason_code, properties):
    log.warning("MQTT disconnected (%s) — will reconnect", reason_code)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        registers = payload.get("r", [])
        ts        = payload.get("ts", None)
        device_id = topic_to_device_id(msg.topic)
        insert_frame(device_id, registers, ts)
    except json.JSONDecodeError as exc:
        log.warning("JSON parse error on topic %s: %s", msg.topic, exc)

# ── Main ──────────────────────────────────────────────────────
def main():
    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=f"railway-bridge-{int(time.time())}",
        protocol=mqtt.MQTTv5,
    )
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.tls_set()  # uses system CA bundle for HiveMQ cloud TLS

    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message

    log.info("Connecting to %s:%d …", HIVEMQ_HOST, HIVEMQ_PORT)
    client.connect(HIVEMQ_HOST, HIVEMQ_PORT, keepalive=60)
    client.loop_forever()

if __name__ == "__main__":
    main()
