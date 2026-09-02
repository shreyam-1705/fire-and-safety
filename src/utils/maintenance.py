TABLES_TO_MAINTAIN = [
    "bronze_table", 
    "silver_device_optical_smoke", "silver_device_ror_heat", 
    "silver_device_multi_sensor", "silver_device_horn_strobe", "silver_device_manual_call_point",
    "silver_quarantine", 
    "gold_device_optical_smoke_5m", "gold_device_ror_heat_5m",
    "gold_device_multi_sensor_5m", "gold_device_horn_strobe_5m", "gold_device_manual_call_point_5m",
    "gold_zone_snapshot_5m", "gold_site_snapshot_5m", "gold_organization_snapshot_5m"
]

def optimize_and_vacuum(spark):
    """Optimizes Delta tables for faster querying and vacuums unreferenced files."""
    print("[Maintenance] Running Delta optimization and vacuuming...", flush=True)
    for table in TABLES_TO_MAINTAIN:
        try:
            spark.sql(f"OPTIMIZE delta.`/tmp/{table}`")
            spark.sql(f"VACUUM delta.`/tmp/{table}` RETAIN 168 HOURS")
        except Exception:
            pass
    print("[Maintenance] Delta files optimized and vacuumed.", flush=True)