"""
MQTT → Supabase Bridge
======================
Runs on Railway (free tier).
- Subscribes to HiveMQ every 30 seconds
- Buffers 2 messages, inserts the LAST one per minute into Supabase
- Auto-reconnects on disconnect
- All secrets from environment variables (set in Railway dashboard)
"""

import os, ssl, json, time, logging
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
    # Pump (0-9) — update with correct scale
    0:1, 1:1, 2:1, 3:1, 4:1, 5:1, 6:1, 7:1, 8:1, 9:1,
    # Compressor (10-29)
    10:1, 11:1, 12:1, 13:1, 14:1, 15:1, 16:1, 17:1, 18:1, 19:1,
    20:1, 21:1, 22:1, 23:1, 24:1, 25:1, 26:1, 27:1, 28:1, 29:1,
    # System (30-49)
    30:1,   # flow_temp     — e.g. change to 100 if raw 7889 = 78.89°C
    31:1,   # return_temp
    32:1,   # power
    33:1,   # temp_1
    34:1,   # temp_2
    35:1,   # temp_3
    36:1,   # flow_rate_lpm
    37:1, 38:1, 39:1, 40:1, 41:1, 42:1, 43:1, 44:1, 45:1,
    46:1, 47:1, 48:1, 49:1,
    # TEV 1 (50-69)
    50:1, 51:1, 52:1, 53:1, 54:1, 55:1, 56:1, 57:1, 58:1, 59:1,
    60:1, 61:1, 62:1, 63:1, 64:1, 65:1, 66:1, 67:1, 68:1, 69:1,
    # TEV 2 (70-89)
    70:1, 71:1, 72:1, 73:1, 74:1, 75:1, 76:1, 77:1, 78:1, 79:1,
    80:1, 81:1, 82:1, 83:1, 84:1, 85:1, 86:1, 87:1, 88:1, 89:1,
    # TEV 3 (90-109)
    90:1, 91:1, 92:1, 93:1, 94:1, 95:1, 96:1, 97:1, 98:1, 99:1,
    100:1, 101:1, 102:1, 103:1, 104:1, 105:1, 106:1, 107:1, 108:1, 109:1,
    # TEV 4 (110-119)
    110:1, 111:1, 112:1, 113:1, 114:1, 115:1, 116:1, 117:1, 118:1, 119:1,
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

    client = mqtt.Client(client_id=f"railway-bridge-{int(time.time())}", protocol=mqtt.MQTTv311)
    client.username_pw_set(USERNAME, PASSWORD)
    client.tls_set_context(ssl.create_default_context())
    client.on_connect    = on_connect
    client.on_message    = on_message
    client.on_disconnect = on_disconnect

    log.info("Connecting to %s:%d …", BROKER, PORT)
    client.connect(BROKER, PORT, keepalive=60)
    client.loop_forever()

if __name__ == "__main__":
    main()
