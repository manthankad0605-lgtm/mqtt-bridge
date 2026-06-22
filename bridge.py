"""
Railway MQTT → Supabase Bridge
Broker : broker.emqx.io  port 1883  (plain TCP, no TLS)
Topic  : rs485/sensor
Payload: {"device_id": 1, "r": [v0, v1, ..., v119]}
Table  : sensor_data  (columns: device_id, recorded_at, r1…r120)

Railway Variables required:
  MQTT_HOST       broker.emqx.io          ← ADD THIS (new)
  MQTT_PORT       1883                    ← ADD THIS (new)
  MQTT_USER       Modbus_sniffer          ← ADD THIS (new)
  MQTT_PASSWORD   Admin123                ← already exists
  SB_URL          https://xxx.supabase.co ← ADD THIS (new, same value as SUPABASE_URL)
  SB_KEY          <service_role key>      ← already exists
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
# Reads existing Railway variable names where possible
MQTT_HOST  = os.environ.get("MQTT_HOST",     "broker.emqx.io")
MQTT_PORT  = int(os.environ.get("MQTT_PORT", 1883))
MQTT_USER  = os.environ.get("MQTT_USER",     "Modbus_sniffer")
MQTT_PASS  = os.environ.get("MQTT_PASSWORD", "Admin123")   # uses your existing MQTT_PASSWORD var
MQTT_TOPIC = os.environ.get("MQTT_TOPIC",    "rs485/sensor")

# Supabase — tries SB_URL first (new), falls back to SUPABASE_URL (existing)
SB_URL = os.environ.get("SB_URL") or os.environ.get("SUPABASE_URL")
# Tries SB_KEY first (existing), falls back to SUPABASE_KEY (existing duplicate)
SB_KEY = os.environ.get("SB_KEY") or os.environ.get("SUPABASE_KEY")

if not SB_URL:
    raise RuntimeError("Set SB_URL or SUPABASE_URL in Railway Variables")
if not SB_KEY:
    raise RuntimeError("Set SB_KEY or SUPABASE_KEY in Railway Variables")
# ───────────────────────────────────────────────────────────

_deploy_id = os.environ.get("RAILWAY_DEPLOYMENT_ID", str(int(time.time())))
CLIENT_ID  = f"railway-bridge-{_deploy_id}"

supabase = create_client(SB_URL, SB_KEY)

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
    for i, val in enumerate(r):
        row[f"r{i+1}"] = val   # r[0]→r1 … r[119]→r120

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

    log.info("Received  device_id=%s  topic=%s", payload.get("device_id", "?"), msg.topic)

    row = build_row(payload)
    if row is None:
        return

    try:
        supabase.table("sensor_data").upsert(
            row, on_conflict="device_id,recorded_at"
        ).execute()
        log.info("Upserted  device_id=%s  recorded_at=%s", row["device_id"], row["recorded_at"])
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
    # No tls_set() — broker.emqx.io:1883 is plain TCP

    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message

    client.reconnect_delay_set(min_delay=5, max_delay=60)
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_forever()

if __name__ == "__main__":
    main()
