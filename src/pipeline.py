import os
import sys
import pyspark
from pyspark.sql import SparkSession

from utils.hf_sync import start_sync_thread, sync_all_to_hf
from utils.maintenance import optimize_and_vacuum

from layers.bronze import run_bronze
from layers.silver import run_silver
from layers.gold import run_gold

if __name__ == "__main__":
    print("1. Restoring state & starting Hugging Face background sync...", flush=True)
    start_sync_thread()

    print("2. Initializing PySpark...", flush=True)
    spark_version = pyspark.__version__
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("FireSafety_Daily_Pipeline") \
        .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version},io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    if os.path.exists("truststore.jks") and os.path.exists("keystore.p12"):
        spark.sparkContext.addFile("truststore.jks")
        spark.sparkContext.addFile("keystore.p12")

    print("3. Pre-initializing Delta Schemas...", flush=True)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS delta.`/tmp/bronze_table` (
            event_id STRING, timestamp STRING, organization_id STRING, site_id STRING, 
            zone_id STRING, panel_id STRING, battery_runtime_hours STRING, 
            device_id STRING, device_type STRING, current_state STRING, is_test_mode STRING, 
            fault_code STRING, telemetry_payload STRING, ingestion_timestamp TIMESTAMP, 
            year INT, month INT, day INT
        ) USING DELTA
    """)

    silver_base = "event_id STRING, timestamp TIMESTAMP, organization_id STRING, site_id STRING, panel_id STRING, zone_id STRING, device_id STRING, device_type STRING, current_state STRING, fault_code STRING, is_test_mode BOOLEAN, battery_runtime_hours FLOAT, ingestion_timestamp TIMESTAMP, year INT, month INT, day INT"
    
    spark.sql(f"CREATE TABLE IF NOT EXISTS delta.`/tmp/silver_device_optical_smoke` ({silver_base}, smoke_obscuration_pct FLOAT, chamber_dirt_pct FLOAT, loop_voltage_dc FLOAT, self_verify_passed BOOLEAN) USING DELTA")
    spark.sql(f"CREATE TABLE IF NOT EXISTS delta.`/tmp/silver_device_ror_heat` ({silver_base}, temperature_celsius FLOAT, rate_of_rise_c_per_min FLOAT, loop_voltage_dc FLOAT, self_verify_passed BOOLEAN) USING DELTA")
    spark.sql(f"CREATE TABLE IF NOT EXISTS delta.`/tmp/silver_device_multi_sensor` ({silver_base}, co_ppm FLOAT, co_cell_health_pct FLOAT, smoke_obscuration_pct FLOAT, temperature_celsius FLOAT, loop_voltage_dc FLOAT, self_verify_passed BOOLEAN) USING DELTA")
    spark.sql(f"CREATE TABLE IF NOT EXISTS delta.`/tmp/silver_device_horn_strobe` ({silver_base}, strobe_active BOOLEAN, is_sounding BOOLEAN, self_test_decibel_level FLOAT, sync_offset_ms FLOAT, loop_voltage_dc FLOAT, self_verify_passed BOOLEAN) USING DELTA")
    spark.sql(f"CREATE TABLE IF NOT EXISTS delta.`/tmp/silver_device_manual_call_point` ({silver_base}, is_activated BOOLEAN, tamper_switch BOOLEAN, loop_voltage_dc FLOAT) USING DELTA")
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS delta.`/tmp/silver_quarantine` (
            event_id STRING, timestamp STRING, organization_id STRING, site_id STRING, 
            zone_id STRING, panel_id STRING, battery_runtime_hours STRING, 
            device_id STRING, device_type STRING, current_state STRING, is_test_mode STRING, 
            fault_code STRING, telemetry_payload STRING, ingestion_timestamp TIMESTAMP, 
            year INT, month INT, day INT, quarantine_reason STRING
        ) USING DELTA
    """)

    gold_base = "organization_id STRING, site_id STRING, panel_id STRING, system_id STRING, device_id STRING, window_start TIMESTAMP"
    
    spark.sql(f"CREATE TABLE IF NOT EXISTS delta.`/tmp/gold_device_optical_smoke_5m` (date STRING, {gold_base}, avg_smoke_obscuration_pct DOUBLE, avg_chamber_dirt_pct DOUBLE, avg_loop_voltage_dc DOUBLE, self_verify_passed BOOLEAN, is_smoke_alarm BOOLEAN, is_dirt_warning BOOLEAN) USING DELTA")
    spark.sql(f"CREATE TABLE IF NOT EXISTS delta.`/tmp/gold_device_ror_heat_5m` (date STRING, {gold_base}, avg_temperature_celsius DOUBLE, avg_rate_of_rise_c_per_min DOUBLE, avg_loop_voltage_dc DOUBLE, self_verify_passed BOOLEAN, is_fixed_temp_alarm BOOLEAN, is_ror_alarm BOOLEAN) USING DELTA")
    spark.sql(f"CREATE TABLE IF NOT EXISTS delta.`/tmp/gold_device_multi_sensor_5m` (date STRING, {gold_base}, avg_co_ppm DOUBLE, avg_co_cell_health_pct DOUBLE, avg_smoke_obscuration_pct DOUBLE, avg_temperature_celsius DOUBLE, avg_loop_voltage_dc DOUBLE, self_verify_passed BOOLEAN, is_toxic_co_alarm BOOLEAN, is_cell_fault BOOLEAN) USING DELTA")
    spark.sql(f"CREATE TABLE IF NOT EXISTS delta.`/tmp/gold_device_horn_strobe_5m` (date STRING, {gold_base}, strobe_active BOOLEAN, is_sounding BOOLEAN, avg_decibel_level DOUBLE, avg_sync_offset_ms DOUBLE, avg_loop_voltage_dc DOUBLE, self_verify_passed BOOLEAN) USING DELTA")
    spark.sql(f"CREATE TABLE IF NOT EXISTS delta.`/tmp/gold_device_manual_call_point_5m` (date STRING, {gold_base}, is_activated BOOLEAN, tamper_switch BOOLEAN, avg_loop_voltage_dc DOUBLE) USING DELTA")

    spark.sql("CREATE TABLE IF NOT EXISTS delta.`/tmp/gold_zone_snapshot_5m` (date STRING, organization_id STRING, site_id STRING, panel_id STRING, system_id STRING, device_density BIGINT, active_alarms BIGINT, active_faults BIGINT, active_disablements BIGINT, zone_status STRING, smoke_detector_count BIGINT, horn_strobe_count BIGINT, mcp_count BIGINT, multi_sensor_count BIGINT, ror_heat_count BIGINT) USING DELTA")
    spark.sql("CREATE TABLE IF NOT EXISTS delta.`/tmp/gold_site_snapshot_5m` (date STRING, organization_id STRING, site_id STRING, active_alarms BIGINT, active_faults BIGINT, monitored_devices BIGINT, fleet_health_score DOUBLE, active_disablements BIGINT, smoke_detector_count BIGINT, horn_strobe_count BIGINT, mcp_count BIGINT, multi_sensor_count BIGINT, ror_heat_count BIGINT, daily_total_alarms BIGINT, daily_total_faults BIGINT) USING DELTA")
    spark.sql("CREATE TABLE IF NOT EXISTS delta.`/tmp/gold_organization_snapshot_5m` (date STRING, organization_id STRING, total_sites_monitored BIGINT, total_monitored_devices BIGINT, global_active_alarms BIGINT, global_active_faults BIGINT, global_fleet_health_score DOUBLE) USING DELTA")

    print("4. Executing Pipeline Stages Sequentially...", flush=True)
    
    run_bronze(spark)
    run_silver(spark)
    run_gold(spark)
    
    print("\n5. Running Database Optimizations...", flush=True)
    optimize_and_vacuum(spark)

    print("\n6. Pipeline Processing Complete. Initiating Final HF Sync...", flush=True)
    spark.stop()
    sync_all_to_hf(tag="final")
    print("✅ Run Complete. Exiting.", flush=True)
    sys.exit(0)