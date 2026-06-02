"""
MQTT → Supabase Bridge
======================
Runs on Railway (free tier).
- Subscribes to HiveMQ every 30 seconds
- Buffers 2 messages, inserts the LAST one per minute into Supabase
- Auto-reconnects on disconnect
- All secrets from environment variables (set in Railway dashboard)
"""

import os, ssl, json, time, logging, uuid
from datetime import datetime, timezone
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
from supabase import create_client

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config from environment variables ────────────────────────────────────────
BROKER   = os.getenv("MQTT_BROKER",   "22b764b37c7c440f9a258c375e19b6bf.s1.eu.hivemq.cloud")
PORT     = int(os.getenv("MQTT_PORT", "8883"))
USERNAME = os.getenv("MQTT_USERNAME", "Modbus_sniffer")
PASSWORD = os.getenv("MQTT_PASSWORD", "Admin123")
TOPIC    = os.getenv("MQTT_TOPIC",    "rs485/sensors")

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://bfzufsnqvjtmzvjdmoyu.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # service_role key — set in Railway

# ── Scale factors (index → divisor). Update after filling register_scaling_map.xlsx)
# Example: 100 means raw 7889 → stored as 78.89
# Leave as 1 = store raw value unchanged
SCALE = {
    "pump_1": 10.0,
    "pump_2": 10.0,
    "pump_3": 10.0,
    "pump_4": 10.0,
    "pump_5": 10.0,
    "pump_6": 10.0,
    "pump_7": 10.0,
    "pump_8": 10.0,
    "pump_9": 10.0,
    "pump_10": 10.0,
    "comp_1": 10.0,
    "comp_2": 10.0,
    "comp_3": 10.0,
    "comp_4": 10.0,
    "comp_5": 10.0,
    "comp_6": 10.0,
    "comp_7": 10.0,
    "comp_8": 10.0,
    "comp_9": 10.0,
    "comp_10": 10.0,
    "comp_11": 10.0,
    "comp_12": 10.0,
    "comp_13": 10.0,
    "comp_14": 10.0,
    "comp_15": 10.0,
    "comp_16": 10.0,
    "comp_17": 10.0,
    "comp_18": 10.0,
    "comp_19": 10.0,
    "comp_20": 10.0,
    "flow_temp": 10.0,
    "return_temp": 10.0,
    "power": 10.0,
    "temp_1": 10.0,
    "temp_2": 10.0,
    "temp_3": 10.0,
    "flow_rate_lpm": 10.0,
    "comp_tev_sh": 10.0,
    "comp_x1": 10.0,
    "comp_start_open_ratio": 10.0,
    "comp_p_gain": 10.0,
    "comp_i_time": 10.0,
    "comp_d_time": 10.0,
    "comp_alarm": 10.0,
    "comp_superheat": 10.0,
    "comp_sat_temp": 10.0,
    "comp_pressure": 10.0,
    "comp_temp": 10.0,
    "comp_x2": 10.0,
    "comp_eev_ratio": 10.0,
    "tev1_sh": 10.0,
    "tev1_x": 10.0,
    "tev1_start_open_ratio": 10.0,
    "tev1_p_gain": 10.0,
    "tev1_i_time": 10.0,
    "tev1_d_time": 10.0,
    "tev1_alarm": 10.0,
    "tev1_superheat": 10.0,
    "tev1_sat_temp": 10.0,
    "tev1_pressure": 10.0,
    "tev1_temp": 10.0,
    "tev1_x2": 10.0,
    "tev1_eev_ratio_1": 10.0,
    "tev1_eev_ratio_2": 10.0,
    "tev1_eev_ratio_3": 10.0,
    "tev1_eev_ratio_4": 10.0,
    "tev1_eev_ratio_5": 10.0,
    "tev1_eev_ratio_6": 10.0,
    "tev1_eev_ratio_7": 10.0,
    "tev1_eev_ratio_8": 10.0,
    "tev2_sh": 10.0,
    "tev2_x": 10.0,
    "tev2_start_open_ratio": 10.0,
    "tev2_p_gain": 10.0,
    "tev2_i_time": 10.0,
    "tev2_d_time": 10.0,
    "tev2_alarm": 10.0,
    "tev2_superheat": 10.0,
    "tev2_sat_temp": 10.0,
    "tev2_pressure": 10.0,
    "tev2_temp": 10.0,
    "tev2_x2": 10.0,
    "tev2_eev_ratio_1": 10.0,
    "tev2_eev_ratio_2": 10.0,
    "tev2_eev_ratio_3": 10.0,
    "tev2_eev_ratio_4": 10.0,
    "tev2_eev_ratio_5": 10.0,
    "tev2_eev_ratio_6": 10.0,
    "tev2_eev_ratio_7": 10.0,
    "tev2_eev_ratio_8": 10.0,
    "tev3_sh": 10.0,
    "tev3_x": 10.0,
    "tev3_start_open_ratio": 10.0,
    "tev3_p_gain": 10.0,
    "tev3_i_time": 10.0,
    "tev3_d_time": 10.0,
    "tev3_alarm": 10.0,
    "tev3_superheat": 10.0,
    "tev3_sat_temp": 10.0,
    "tev3_pressure": 10.0,
    "tev3_temp": 10.0,
    "tev3_x2": 10.0,
    "tev3_eev_ratio_1": 10.0,
    "tev3_eev_ratio_2": 10.0,
    "tev3_eev_ratio_3": 10.0,
    "tev3_eev_ratio_4": 10.0,
    "tev3_eev_ratio_5": 10.0,
    "tev3_eev_ratio_6": 10.0,
    "tev3_eev_ratio_7": 10.0,
    "tev3_eev_ratio_8": 10.0,
    "tev4_subcool": 10.0,
    "tev4_sat_temp": 10.0,
    "tev4_sh": 10.0,
    "tev4_x": 10.0,
    "tev4_start_open_ratio": 10.0,
    "tev4_p_gain": 10.0,
    "tev4_i_time": 10.0,
    "tev4_d_time": 10.0,
    "tev4_alarm": 10.0,
    "tev4_eev_ratio": 10.0
}

# ── Column name map (index → DB column name) ─────────────────────────────────
COLUMNS = [
    "pump_1","pump_2","pump_3","pump_4","pump_5",
    "pump_6","pump_7","pump_8","pump_9","pump_10",
    "comp_1","comp_2","comp_3","comp_4","comp_5",
    "comp_6","comp_7","comp_8","comp_9","comp_10",
    "comp_11","comp_12","comp_13","comp_14","comp_15",
    "comp_16","comp_17","comp_18","comp_19","comp_20",
    "flow_temp","return_temp","power","temp_1","temp_2","temp_3",
    "flow_rate_lpm","comp_tev_sh","comp_x1","comp_start_open_ratio",
    "comp_p_gain","comp_i_time","comp_d_time","comp_alarm","comp_superheat",
    "comp_sat_temp","comp_pressure","comp_temp","comp_x2","comp_eev_ratio",
    "tev1_sh","tev1_x","tev1_start_open_ratio","tev1_p_gain","tev1_i_time",
    "tev1_d_time","tev1_alarm","tev1_superheat","tev1_sat_temp","tev1_pressure",
    "tev1_temp","tev1_x2","tev1_eev_ratio_1","tev1_eev_ratio_2","tev1_eev_ratio_3",
    "tev1_eev_ratio_4","tev1_eev_ratio_5","tev1_eev_ratio_6","tev1_eev_ratio_7","tev1_eev_ratio_8",
    "tev2_sh","tev2_x","tev2_start_open_ratio","tev2_p_gain","tev2_i_time",
    "tev2_d_time","tev2_alarm","tev2_superheat","tev2_sat_temp","tev2_pressure",
    "tev2_temp","tev2_x2","tev2_eev_ratio_1","tev2_eev_ratio_2","tev2_eev_ratio_3",
    "tev2_eev_ratio_4","tev2_eev_ratio_5","tev2_eev_ratio_6","tev2_eev_ratio_7","tev2_eev_ratio_8",
    "tev3_sh","tev3_x","tev3_start_open_ratio","tev3_p_gain","tev3_i_time",
    "tev3_d_time","tev3_alarm","tev3_superheat","tev3_sat_temp","tev3_pressure",
    "tev3_temp","tev3_x2","tev3_eev_ratio_1","tev3_eev_ratio_2","tev3_eev_ratio_3",
    "tev3_eev_ratio_4","tev3_eev_ratio_5","tev3_eev_ratio_6","tev3_eev_ratio_7","tev3_eev_ratio_8",
    "tev4_subcool","tev4_sat_temp","tev4_sh","tev4_x","tev4_start_open_ratio",
    "tev4_p_gain","tev4_i_time","tev4_d_time","tev4_alarm","tev4_eev_ratio",
]

# ── State ─────────────────────────────────────────────────────────────────────
supabase      = None
last_payload  = None   # holds the most recent raw registers list
last_insert   = 0.0   # epoch time of last DB insert

# ── Supabase ──────────────────────────────────────────────────────────────────
def init_supabase():
    global supabase
    if not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_KEY environment variable not set!")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    log.info("Supabase client ready → %s", SUPABASE_URL)

def insert_row(regs):
    """Apply scaling and insert one row into sensor_data."""
    row = {"recorded_at": datetime.now(timezone.utc).isoformat()}
    for i, col in enumerate(COLUMNS):
        scale = SCALE.get(i, 1)
        row[col] = round(regs[i] / scale, 4) if scale != 1 else int(regs[i])
    try:
        supabase.table("sensor_data").insert(row).execute()
        log.info("✓ Row inserted | flow_temp=%.2f  return_temp=%.2f  power=%.2f",
                 row["flow_temp"], row["return_temp"], row["power"])
    except Exception as e:
        log.error("✗ Insert failed: %s", e)

# ── MQTT callbacks ────────────────────────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    msgs = {0:"OK",1:"Bad protocol",2:"Client ID rejected",
            3:"Server unavailable",4:"Bad credentials",5:"Not authorised"}
    log.info("MQTT connect: %s (rc=%d)", msgs.get(rc,"?"), rc)
    if rc == 0:
        client.subscribe(TOPIC)
        log.info("Subscribed → %s", TOPIC)

def on_message(client, userdata, msg):
    global last_payload, last_insert
    try:
        data = json.loads(msg.payload.decode())
        regs = data.get("r", [])
        if len(regs) != 120:
            log.warning("Expected 120 registers, got %d — skipped", len(regs))
            return

        last_payload = regs   # always keep latest

        # Insert once per minute
        now = time.time()
        if now - last_insert >= 60:
            insert_row(last_payload)
            last_insert = now

    except json.JSONDecodeError as e:
        log.error("JSON error: %s", e)
    except Exception as e:
        log.exception("on_message error: %s", e)

def on_disconnect(client, userdata, rc):
    if rc != 0:
        log.warning("Unexpected disconnect rc=%d — will auto-reconnect", rc)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    init_supabase()

    unique_id = os.getenv("RAILWAY_DEPLOYMENT_ID", str(uuid.uuid4()))[:12]
    client_id = f"railway-bridge-{unique_id}"
    log.info("Using MQTT client ID: %s", client_id)
    client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311, clean_session=True)
    client.username_pw_set(USERNAME, PASSWORD)
    client.tls_set_context(ssl.create_default_context())
    client.on_connect    = on_connect
    client.on_message    = on_message
    client.on_disconnect = on_disconnect
    client.reconnect_delay_set(min_delay=5, max_delay=30)

    log.info("Connecting to %s:%d …", BROKER, PORT)
    client.connect(BROKER, PORT, keepalive=60)
    client.loop_forever()

if __name__ == "__main__":
    main()
