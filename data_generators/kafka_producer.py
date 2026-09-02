import os
import json
import ssl
import time
import random
import uuid
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

# ==========================================
# 1. CONFIGURATION & SECRETS
# ==========================================
AIVEN_URI = os.getenv("AIVEN_URI")
TOPIC_NAME = "fire-and-safety"

CONFIG = {
    "SEED_DIR": "seed_data",
    "OUTPUT_DIR": "raw_json_payloads",
    "INTERVAL_MINUTES": 5,
    "ZONE_FIRE_PROB": 0.003,       
    "ZONE_MAINTENANCE_PROB": 0.005, 
    "DEVICE_FAULT_PROB": 0.002
}

FAULT_CODES = {
    "optical_smoke": ["SENSOR_FAULT", "COMMUNICATION_FAILURE", "DEVICE_ERROR"],
    "ror_heat": ["SENSOR_FAULT", "COMMUNICATION_FAILURE"],
    "multi_sensor": ["SENSOR_FAULT", "COMMUNICATION_FAILURE", "BATTERY_LOW"],
    "horn_strobe": ["DEVICE_ERROR", "COMMUNICATION_FAILURE", "POWER_FAILURE"],
    "manual_call_point": ["DEVICE_ERROR", "COMMUNICATION_FAILURE"]
}

# ==========================================
# 2. DYNAMIC TIME & TICK CALCULATION
# ==========================================
def get_time_window():
    env_start = os.getenv("START_DATE")
    env_end = os.getenv("END_DATE")
    
    start_time = None
    end_time = None
    
    if env_start and env_start.strip():
        try:
            start_time = datetime.strptime(env_start.strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            print(f"⚠️ Invalid START_DATE '{env_start}'. Use YYYY-MM-DD. Falling back to default.")

    if env_end and env_end.strip():
        try:
            end_time = datetime.strptime(env_end.strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            print(f"⚠️ Invalid END_DATE '{env_end}'. Use YYYY-MM-DD.")

    if not start_time:
        today_midnight = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        # Default to tomorrow for future simulation architecture
        start_time = today_midnight + timedelta(days=1)
        end_time = start_time + timedelta(days=1)

    if not end_time:
        end_time = start_time + timedelta(days=1)

    total_seconds = (end_time - start_time).total_seconds()
    if total_seconds <= 0:
        raise ValueError(f"END_DATE ({end_time}) must be after START_DATE ({start_time}).")

    ticks = int(total_seconds // (CONFIG["INTERVAL_MINUTES"] * 60))
    return start_time, ticks

# ==========================================
# 3. TOPOLOGY & PHYSICS ENGINE
# ==========================================
def load_topology():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    seed_dir = os.path.join(base_dir, CONFIG["SEED_DIR"])
    
    print(f"📂 Loading dimension tables from {seed_dir}...")
    try:
        with open(os.path.join(seed_dir, "dim_device.json")) as f: devices = json.load(f)
        with open(os.path.join(seed_dir, "dim_zone.json")) as f: zones = {z["zone_id"]: z for z in json.load(f)}
        with open(os.path.join(seed_dir, "dim_site.json")) as f: sites = {s["site_id"]: s for s in json.load(f)}
    except Exception as e:
        print(f"❌ Error loading topology: {e}")
        exit(1)

    for d in devices:
        zone = zones[d["zone_id"]]
        site = sites[zone["site_id"]]
        d["site_id"] = site["site_id"]
        d["organization_id"] = site["organization_id"]
        d["panel_id"] = zone["panel_id"]
        d["device_type"] = d["device_type"].lower()
    return devices

def generate_telemetry(device_type, current_state, fault_code, fire_intensity):
    is_trouble = (current_state == "TROUBLE" or fault_code is not None)
    is_alarm = (current_state == "ALARM")
    
    telemetry = {
        "loop_voltage_dc": round(random.uniform(23.8, 24.2), 2),
        "self_verify_passed": not is_trouble
    }
    
    if is_trouble:
        telemetry["loop_voltage_dc"] = random.choice([round(random.uniform(18.0, 21.0), 2), round(random.uniform(28.0, 31.0), 2)])

    if device_type == "optical_smoke":
        if is_alarm:
            telemetry["smoke_obscuration_pct"] = round(2.5 + (fire_intensity * 9.5), 2)
        else:
            telemetry["smoke_obscuration_pct"] = round(random.uniform(0.01, 0.08), 2)
            
        if fault_code == "SENSOR_FAULT":
            telemetry["chamber_dirt_pct"] = round(random.uniform(60.0, 85.0), 1)
        else:
            telemetry["chamber_dirt_pct"] = round(random.uniform(5.0, 25.0), 1)
        
    elif device_type == "ror_heat":
        if is_alarm:
            telemetry["temperature_celsius"] = round(57.0 + (fire_intensity * 38.0), 1)
            telemetry["rate_of_rise_c_per_min"] = round(8.3 + (fire_intensity * 10.0), 2)
        else:
            telemetry["temperature_celsius"] = round(random.uniform(20.0, 25.0), 1)
            telemetry["rate_of_rise_c_per_min"] = round(random.uniform(0.0, 0.5), 2)

    elif device_type == "multi_sensor":
        if is_alarm:
            telemetry["smoke_obscuration_pct"] = round(2.5 + (fire_intensity * 7.5), 2)
            telemetry["temperature_celsius"] = round(57.0 + (fire_intensity * 30.0), 1)
            telemetry["co_ppm"] = int(30 + (fire_intensity * 150))
        else:
            telemetry["smoke_obscuration_pct"] = round(random.uniform(0.01, 0.05), 2)
            telemetry["temperature_celsius"] = round(random.uniform(20.0, 25.0), 1)
            telemetry["co_ppm"] = random.randint(0, 3)
            
        if fault_code == "BATTERY_LOW":
            telemetry["co_cell_health_pct"] = round(random.uniform(40.0, 69.0), 1)
        else:
            telemetry["co_cell_health_pct"] = round(random.uniform(90.0, 100.0), 1)

    elif device_type == "horn_strobe":
        telemetry["is_sounding"] = is_alarm
        telemetry["strobe_active"] = is_alarm
        if fault_code == "DEVICE_ERROR":
            telemetry["self_test_decibel_level"] = round(random.uniform(60.0, 84.0), 1)
        else:
            telemetry["self_test_decibel_level"] = round(random.uniform(85.0, 95.0), 1)
            
        telemetry["sync_offset_ms"] = round(random.uniform(1.0, 5.0), 1) if is_alarm else None

    elif device_type == "manual_call_point":
        telemetry["is_activated"] = is_alarm
        telemetry["tamper_switch"] = (fault_code == "DEVICE_ERROR")

    return telemetry

def generate_full_day_backlog(devices):
    start_time, ticks = get_time_window()
    print(f"🔥 Generating {ticks} ticks starting from {start_time.strftime('%Y-%m-%d')} midnight UTC...")
    
    active_fires_by_zone = {}     
    active_maintenance_by_zone = {}
    active_faults_by_device = {}  
    stream_payloads = []
    
    for tick in range(ticks):
        current_time = start_time + timedelta(minutes=CONFIG["INTERVAL_MINUTES"] * tick)
        timestamp_str = current_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        for zone in list(active_fires_by_zone.keys()):
            rem, tot = active_fires_by_zone[zone]
            if rem <= 1: del active_fires_by_zone[zone]
            else: active_fires_by_zone[zone] = (rem - 1, tot)
                
        for zone in list(active_maintenance_by_zone.keys()):
            active_maintenance_by_zone[zone] -= 1
            if active_maintenance_by_zone[zone] <= 0: del active_maintenance_by_zone[zone]

        for dev_id in list(active_faults_by_device.keys()):
            active_faults_by_device[dev_id]["ticks_remaining"] -= 1
            if active_faults_by_device[dev_id]["ticks_remaining"] <= 0: del active_faults_by_device[dev_id]

        all_zones = set(d["zone_id"] for d in devices)
        for zone in all_zones:
            if zone not in active_fires_by_zone and zone not in active_maintenance_by_zone:
                if random.random() < CONFIG["ZONE_FIRE_PROB"]:
                    duration = random.randint(24, 72)
                    active_fires_by_zone[zone] = (duration, duration)
                elif random.random() < CONFIG["ZONE_MAINTENANCE_PROB"]:
                    active_maintenance_by_zone[zone] = random.randint(12, 48)

        for d in devices:
            dev_id = d["device_id"]
            zone_id = d["zone_id"]
            dtype = d["device_type"]
            
            current_state = "NORMAL"
            fault_code = None
            fire_intensity = 0.0
            
            if dev_id in active_faults_by_device:
                fault_code = active_faults_by_device[dev_id]["code"]
                current_state = "TROUBLE" 
                
            elif random.random() < CONFIG["DEVICE_FAULT_PROB"]:
                fault_code = random.choice(FAULT_CODES.get(dtype, ["DEVICE_ERROR"]))
                current_state = "TROUBLE"
                duration = random.randint(12, 48)
                active_faults_by_device[dev_id] = {"code": fault_code, "total_ticks": duration, "ticks_remaining": duration}

            if zone_id in active_fires_by_zone:
                rem, tot = active_fires_by_zone[zone_id]
                fire_intensity = 1.0 - (rem / tot) 
                
                alarm_threshold = 0.10 
                if dtype == "optical_smoke" and fault_code == "SENSOR_FAULT":
                    alarm_threshold = 0.03
                elif dtype == "multi_sensor" and fault_code == "BATTERY_LOW":
                    alarm_threshold = 0.20
                
                if fire_intensity >= alarm_threshold:
                    current_state = "ALARM"  
            
            elif zone_id in active_maintenance_by_zone:
                current_state = "ISOLATED"
            
            battery_runtime = round(random.uniform(23.8, 24.0), 1)
            if fault_code == "POWER_FAILURE":
                ticks_active = active_faults_by_device[dev_id]["total_ticks"] - active_faults_by_device[dev_id]["ticks_remaining"]
                battery_drain = ticks_active * 0.5 
                battery_runtime = max(24.0 - battery_drain, 0.0)
            
            event = {
                "event_id": f"evt-{timestamp_str.replace('-','').replace(':','').replace('T','-')[:13]}-{random.randint(1000,9999)}",
                "timestamp": timestamp_str,
                "organization_id": d["organization_id"],
                "site_id": d["site_id"],
                "panel_id": d["panel_id"],
                "zone_id": zone_id,
                "device_id": dev_id,
                "device_type": dtype,
                "current_state": current_state,
                "is_test_mode": (current_state == "ISOLATED"),
                "fault_code": fault_code,
                "battery_runtime_hours": battery_runtime,
                "telemetry": generate_telemetry(dtype, current_state, fault_code, fire_intensity)
            }
            stream_payloads.append(event)
            
    return stream_payloads

# ==========================================
# 4. OUTPUT & KAFKA PRODUCER
# ==========================================
def main():
    if not AIVEN_URI:
        raise ValueError("Missing AIVEN_URI in environment variables.")

    devices = load_topology()
    payloads = generate_full_day_backlog(devices)
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_dir = os.path.join(base_dir, CONFIG["OUTPUT_DIR"])
    os.makedirs(output_dir, exist_ok=True)
    out_file = os.path.join(output_dir, "simulated_fire_safety_stream.json")
    with open(out_file, 'w') as f:
        for p in payloads:
            f.write(json.dumps(p) + "\n")
    print(f"✅ Saved {len(payloads)} records to {out_file} for local inspection.")

    print(f"\n🔌 Connecting to Aiven Kafka at {AIVEN_URI}...")
    context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile="ca.pem")
    context.load_cert_chain(certfile="service.cert", keyfile="service.key")

    try:
        # Reverted to proven configuration from test-pipeline
        producer = KafkaProducer(
            bootstrap_servers=AIVEN_URI,
            security_protocol="SSL",
            ssl_context=context,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        exit(1)

    print(f"🚀 Publishing {len(payloads)} records to topic '{TOPIC_NAME}'...")
    start_publish = time.time()
    
    for i, payload in enumerate(payloads):
        producer.send(TOPIC_NAME, payload)
        if i % 5000 == 0 and i > 0:
            print(f"  ...sent {i}/{len(payloads)} records")
            
    producer.flush()
    elapsed = round(time.time() - start_publish, 2)
    print(f"✅ Success! Backlog populated in {elapsed} seconds.")
    print("👋 Shutting down producer.")

if __name__ == "__main__":
    main()