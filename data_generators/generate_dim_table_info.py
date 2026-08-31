import json
import os
import random
from datetime import datetime, timedelta

CONFIG = {
    "NUM_ORGANIZATIONS": 1,
    "SITES_PER_ORG_RANGE": (2, 4),
    "PANELS_PER_SITE_RANGE": (1, 3),
    "ZONES_PER_PANEL_RANGE": (4, 12),
    "DEVICES_PER_ZONE_RANGE": (8, 24),
    "OUTPUT_DIR": "../seed_data"
}

CITY_METADATA = {
    "New York": {"lat": 40.7128, "lon": -74.0060, "tz": "America/New_York", "streets": ["Broadway", "5th Ave", "Wall St"]},
    "London": {"lat": 51.5074, "lon": -0.1278, "tz": "Europe/London", "streets": ["Baker St", "Oxford St", "Canary Wharf"]},
    "Singapore": {"lat": 1.3521, "lon": 103.8198, "tz": "Asia/Singapore", "streets": ["Orchard Rd", "Marina Blvd"]},
    "Dubai": {"lat": 25.2048, "lon": 55.2708, "tz": "Asia/Dubai", "streets": ["Sheikh Zayed Rd", "Financial Center Rd"]},
    "Chicago": {"lat": 41.8781, "lon": -87.6298, "tz": "America/Chicago", "streets": ["Wacker Dr", "Michigan Ave"]}
}

PANEL_MODELS = ["Autronica AutroSafe 4", "Kidde VM Series", "Edwards EST4", "Notifier INSPIRE"]
FIRMWARES = ["v4.1.2", "v4.2.0", "v5.0.1", "v3.9.8"]

# Expanded for realistic placements
ZONE_TYPES = ["Lobby", "Server Room", "Corridor", "Cafeteria Kitchen", "Executive Suites", "Loading Dock", "Underground Parking", "Warehouse", "Electrical Room", "Stairwell Exit"]

# Context-Aware Device Logic
def get_logical_device_type(zone_type):
    if "Kitchen" in zone_type or "Parking" in zone_type or "Loading Dock" in zone_type:
        # Fumes/steam expected. Use Heat Detectors to avoid false alarms.
        types = ["ROR_HEAT", "HORN_STROBE", "MANUAL_CALL_POINT"]
        weights = [0.70, 0.20, 0.10]
    elif "Server Room" in zone_type or "Electrical Room" in zone_type:
        # High value, clean environment. Need high sensitivity Multi-Sensors.
        types = ["MULTI_SENSOR", "OPTICAL_SMOKE", "HORN_STROBE"]
        weights = [0.80, 0.10, 0.10]
    elif "Stairwell Exit" in zone_type:
        # Egress routes require pull stations and alarms.
        types = ["MANUAL_CALL_POINT", "HORN_STROBE", "OPTICAL_SMOKE"]
        weights = [0.50, 0.30, 0.20]
    else:
        # Standard clean environments (Lobby, Corridors, Offices)
        types = ["OPTICAL_SMOKE", "MULTI_SENSOR", "HORN_STROBE", "MANUAL_CALL_POINT"]
        weights = [0.70, 0.10, 0.15, 0.05]
        
    return random.choices(types, weights=weights, k=1)[0]

def random_date(start_year=2021, end_year=2025):
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    return (start_date + timedelta(days=random.randrange((end_date - start_date).days))).strftime("%Y-%m-%d")

def generate_topology():
    os.makedirs(CONFIG["OUTPUT_DIR"], exist_ok=True)

    orgs, sites, panels, zones, devices = [], [], [], [], []
    global_site_count, global_panel_count, global_zone_count, global_device_count = 1, 1, 1, 1

    for o in range(1, CONFIG["NUM_ORGANIZATIONS"] + 1):
        org_id = f"ORG-{o:02d}"
        orgs.append({
            "organization_id": org_id, "organization_name": f"Enterprise Corp {o}", "contact_email": f"sec-ops@enterprisecorp{o}.com"
        })

        for s in range(1, random.randint(*CONFIG["SITES_PER_ORG_RANGE"]) + 1):
            site_id = f"SITE-{global_site_count:03d}"
            global_site_count += 1
            city = random.choice(list(CITY_METADATA.keys()))
            geo = CITY_METADATA[city]
            
            sites.append({
                "site_id": site_id, "organization_id": org_id, "site_name": f"{city} Regional Hub",
                "address": f"{random.randint(100, 9999)} {random.choice(geo['streets'])}", "city": city,
                "latitude": round(geo["lat"] + random.uniform(-0.05, 0.05), 6), "longitude": round(geo["lon"] + random.uniform(-0.05, 0.05), 6),
                "timezone": geo["tz"]
            })

            for p in range(1, random.randint(*CONFIG["PANELS_PER_SITE_RANGE"]) + 1):
                panel_id = f"PANEL-{global_panel_count:03d}"
                global_panel_count += 1
                panels.append({
                    "panel_id": panel_id, "site_id": site_id, "panel_model": random.choice(PANEL_MODELS),
                    "firmware_version": random.choice(FIRMWARES), "ip_address": f"10.0.{s}.{p + 50}"
                })

                for z in range(1, random.randint(*CONFIG["ZONES_PER_PANEL_RANGE"]) + 1):
                    zone_id = f"ZONE-{global_zone_count:04d}"
                    global_zone_count += 1
                    floor_lvl = f"FL-{random.randint(1, 15):02d}"
                    zone_type = random.choice(ZONE_TYPES)
                    zone_name = f"{floor_lvl} {zone_type}"
                    
                    zones.append({
                        "zone_id": zone_id, "panel_id": panel_id, "site_id": site_id, "floor_level": floor_lvl, "zone_name": zone_name
                    })

                    current_loop, current_address = 1, 1
                    
                    for d in range(1, random.randint(*CONFIG["DEVICES_PER_ZONE_RANGE"]) + 1):
                        device_id = f"DEV-{global_device_count:05d}"
                        global_device_count += 1
                        
                        # Apply intelligent placement logic based on the zone
                        device_type = get_logical_device_type(zone_type)
                        
                        if device_type == "MANUAL_CALL_POINT": modifier = "Near Exit"
                        elif "Server" in zone_type: modifier = f"Above Rack {random.randint(1, 20)}"
                        elif "Parking" in zone_type: modifier = f"Bay {random.randint(1, 100)}"
                        else: modifier = random.choice(["Ceiling Center", "North Wall", "South Wall", "Near HVAC Return"])
                            
                        devices.append({
                            "device_id": device_id, "zone_id": zone_id, "device_type": device_type,
                            "device_label": f"{zone_name} - {modifier}",
                            "loop_number": current_loop, "address_number": current_address,
                            "install_date": random_date()
                        })

                        current_address += 1
                        if current_address > 250:
                            current_address, current_loop = 1, current_loop + 1

    def save_json(data, filename):
        filepath = os.path.join(CONFIG["OUTPUT_DIR"], filename)
        with open(filepath, 'w') as f: json.dump(data, f, indent=4)
        print(f"Generated {len(data)} records in {filename}")

    save_json(orgs, "dim_organization.json")
    save_json(sites, "dim_site.json")
    save_json(panels, "dim_panel.json")
    save_json(zones, "dim_zone.json")
    save_json(devices, "dim_device.json")

if __name__ == "__main__":
    generate_topology()