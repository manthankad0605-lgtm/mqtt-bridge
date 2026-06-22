"""
Railway MQTT → Supabase Bridge
Broker : broker.emqx.io  port 1883  (plain TCP, no TLS)
Topic  : rs485/sensor
Payload: {"device_id": 1, "r": [v0, v1, ..., v119]}
Table  : sensor_data  (exact column names mapped below)
"""

import os
import json
import time
import logging
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from supabase import create_client

# ─── Logging ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Environment Variables ─────────────────────────────────
MQTT_HOST  = os.environ.get("MQTT_HOST",     "broker.emqx.io")
MQTT_PORT  = int(os.environ.get("MQTT_PORT", 1883))
MQTT_USER  = os.environ.get("MQTT_USER",     "Modbus_sniffer")
MQTT_PASS  = os.environ.get("MQTT_PASSWORD", "Admin123")
MQTT_TOPIC = os.environ.get("MQTT_TOPIC",    "rs485/sensor")

SB_URL = os.environ.get("SB_URL") or os.environ.get("SUPABASE_URL")
SB_KEY = os.environ.get("SB_KEY") or os.environ.get("SUPABASE_KEY")

if not SB_URL: raise RuntimeError("Set SB_URL or SUPABASE_URL in Railway Variables")
if not SB_KEY: raise RuntimeError("Set SB_KEY or SUPABASE_KEY in Railway Variables")
# ───────────────────────────────────────────────────────────

_deploy_id = os.environ.get("RAILWAY_DEPLOYMENT_ID", str(int(time.time())))
CLIENT_ID  = f"railway-bridge-{_deploy_id}"

supabase = create_client(SB_URL, SB_KEY)

# ─── r[0]…r[119] → exact Supabase column names ────────────
REGISTER_COLUMNS = [
    "pump_1",            # r[0]
    "pump_2",            # r[1]
    "pump_3",            # r[2]
    "pump_4",            # r[3]
    "pump_5",            # r[4]
    "pump_6",            # r[5]
    "pump_7",            # r[6]
    "pump_8",            # r[7]
    "pump_9",            # r[8]
    "pump_10",           # r[9]
    "comp_1",            # r[10]
    "comp_2",            # r[11]
    "comp_3",            # r[12]
    "comp_4",            # r[13]
    "comp_5",            # r[14]
    "comp_6",            # r[15]
    "comp_7",            # r[16]
    "comp_8",            # r[17]
    "comp_9",            # r[18]
    "comp_10",           # r[19]
    "comp_11",           # r[20]
    "comp_12",           # r[21]
    "comp_13",           # r[22]
    "comp_14",           # r[23]
    "comp_15",           # r[24]
    "comp_16",           # r[25]
    "comp_17",           # r[26]
    "comp_18",           # r[27]
    "comp_19",           # r[28]
    "comp_20",           # r[29]
    "flow_temp",         # r[30]
    "return_temp",       # r[31]
    "power",             # r[32]
    "temp_1",            # r[33]
    "temp_2",            # r[34]
    "temp_3",            # r[35]
    "flow_rate_lpm",     # r[36]
    "comp_tev_sh",       # r[37]
    "comp_x1",           # r[38]
    "comp_start_open_ratio", # r[39]
    "comp_p_gain",       # r[40]
    "comp_i_time",       # r[41]
    "comp_d_time",       # r[42]
    "comp_alarm",        # r[43]
    "comp_superheat",    # r[44]
    "comp_sat_temp",     # r[45]
    "comp_pressure",     # r[46]
    "comp_temp",         # r[47]
    "comp_x2",           # r[48]
    "comp_eev_ratio",    # r[49]
    "tev1_sh",           # r[50]
    "tev1_x",            # r[51]
    "tev1_start_open_ratio", # r[52]
    "tev1_p_gain",       # r[53]
    "tev1_i_time",       # r[54]
    "tev1_d_time",       # r[55]
    "tev1_alarm",        # r[56]
    "tev1_superheat",    # r[57]
    "tev1_sat_temp",     # r[58]
    "tev1_pressure",     # r[59]
    "tev1_temp",         # r[60]
    "tev1_x2",           # r[61]
    "tev1_eev_ratio_1",  # r[62]
    "tev1_eev_ratio_2",  # r[63]
    "tev1_eev_ratio_3",  # r[64]
    "tev1_eev_ratio_4",  # r[65]
    "tev1_eev_ratio_5",  # r[66]
    "tev1_eev_ratio_6",  # r[67]
    "tev1_eev_ratio_7",  # r[68]
    "tev1_eev_ratio_8",  # r[69]
    "tev2_sh",           # r[70]
    "tev2_x",            # r[71]
    "tev2_start_open_ratio", # r[72]
    "tev2_p_gain",       # r[73]
    "tev2_i_time",       # r[74]
    "tev2_d_time",       # r[75]
    "tev2_alarm",        # r[76]
    "tev2_superheat",    # r[77]
    "tev2_sat_temp",     # r[78]
    "tev2_pressure",     # r[79]
    "tev2_temp",         # r[80]
    "tev2_x2",           # r[81]
    "tev2_eev_ratio_1",  # r[82]
    "tev2_eev_ratio_2",  # r[83]
    "tev2_eev_ratio_3",  # r[84]
    "tev2_eev_ratio_4",  # r[85]
    "tev2_eev_ratio_5",  # r[86]
    "tev2_eev_ratio_6",  # r[87]
    "tev2_eev_ratio_7",  # r[88]
    "tev2_eev_ratio_8",  # r[89]
    "tev3_sh",           # r[90]
    "tev3_x",            # r[91]
    "tev3_start_open_ratio", # r[92]
    "tev3_p_gain",       # r[93]
    "tev3_i_time",       # r[94]
    "tev3_d_time",       # r[95]
    "tev3_alarm",        # r[96]
    "tev3_superheat",    # r[97]
    "tev3_sat_temp",     # r[98]  ← inferred (was cut off before)
    "tev3_pressure",     # r[99]  ← inferred
    "tev3_temp",         # r[100] ← inferred
    "tev3_x2",           # r[101] ← inferred
    "tev3_eev_ratio_1",  # r[102] ← inferred
    "tev3_eev_ratio_2",  # r[103] ← inferred
    "tev3_eev_ratio_3",  # r[104] ← inferred
    "tev3_eev_ratio_4",  # r[105]
    "tev3_eev_ratio_5",  # r[106]
    "tev3_eev_ratio_6",  # r[107]
    "tev3_eev_ratio_7",  # r[108]
    "tev3_eev_ratio_8",  # r[109]
    "tev4_subcool",      # r[110]
    "tev4_sat_temp",     # r[111]
    "tev4_sh",           # r[112]
    "tev4_x",            # r[113]
    "tev4_start_open_ratio", # r[114]
    "tev4_p_gain",       # r[115]
    "tev4_i_time",       # r[116]
    "tev4_d_time",       # r[117]
    "tev4_alarm",        # r[118]
    "tev4_eev_ratio",    # r[119]
]

assert len(REGISTER_COLUMNS) == 120, f"Column map has {len(REGISTER_COLUMNS)} entries, expected 120"

# ─── Build Supabase row ────────────────────────────────────
def build_row(payload: dict):
    r = payload.get("r")
    if not isinstance(r, list) or len(r) != 120:
        log.warning("Bad 'r' array: got %s elements", len(r) if isinstance(r, list) else "N/A")
        return None

    device_id = int(payload.get("device_id", 1))
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    row = {
        "device_id":   device_id,
        "recorded_at": now.isoformat(),
    }
    for i, col in enumerate(REGISTER_COLUMNS):
        row[col] = r[i]

    return row

# ─── MQTT Callbacks ────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("Connected to %s:%s", MQTT_HOST, MQTT_PORT)
        client.subscribe(MQTT_TOPIC, qos=0)
        log.info("Subscribed → %s", MQTT_TOPIC)
    else:
        log.error("Connection refused  rc=%s", rc)

def on_disconnect(client, userdata, rc):
    log.warning("Disconnected rc=%s — will auto-reconnect", rc)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception as e:
        log.error("JSON decode error: %s", e)
        return

    log.info("Received  device_id=%s", payload.get("device_id", "?"))

    row = build_row(payload)
    if row is None:
        return

    try:
        supabase.table("sensor_data").upsert(
            row, on_conflict="device_id,recorded_at"
        ).execute()
        log.info("Upserted  device_id=%s  recorded_at=%s  pump_1=%s  tev4_eev_ratio=%s",
                 row["device_id"], row["recorded_at"], row["pump_1"], row["tev4_eev_ratio"])
    except Exception as e:
        log.error("Supabase upsert failed: %s", e)

# ─── Main ──────────────────────────────────────────────────
def main():
    log.info("Bridge starting  client_id=%s", CLIENT_ID)
    log.info("Broker : %s:%s  (plain TCP)", MQTT_HOST, MQTT_PORT)
    log.info("Topic  : %s", MQTT_TOPIC)
    log.info("Supabase: %s", SB_URL)

    client = mqtt.Client(client_id=CLIENT_ID, clean_session=True)
    client.username_pw_set(MQTT_USER, MQTT_PASS)

    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message

    client.reconnect_delay_set(min_delay=5, max_delay=60)
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_forever()

if __name__ == "__main__":
    main()
