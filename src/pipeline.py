# FILE: src/pipeline.py
import os
import sys
import time
import signal
import threading
import pyspark
from pyspark.sql import SparkSession

from utils.hf_sync import start_sync_thread, sync_all_to_hf
from utils.maintenance import start_maintenance_thread
from layers.bronze import run_bronze
from layers.silver import run_silver
from layers.gold import run_gold

RUN_DURATION_SECONDS = int(os.getenv("RUN_DURATION_SECONDS", "1800"))
spark = None

def handle_shutdown(signum=None, frame=None):
    print(f"\n[shutdown] Signal {signum} received. Stopping Spark & syncing...", flush=True)
    try:
        if spark is not None:
            for query in list(spark.streams.active):
                query.stop()
            spark.stop()
    except Exception as exc:
        print(f"[shutdown] Spark cleanup error: {exc}", flush=True)
    sync_all_to_hf(tag="shutdown")
    sys.exit(0)

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

def launch_stream(layer_function, pool_name, spark_session):
    spark_session.sparkContext.setLocalProperty("spark.scheduler.pool", pool_name)
    try:
        print(f"[PIPELINE] Launching stream in pool '{pool_name}'...", flush=True)
        layer_function(spark_session)
    except Exception as e:
        print(f"[{pool_name}] Stream ended or error: {e}", flush=True)

def preinitialize_tables(spark_session):
    print("[PIPELINE] Pre-initializing Delta table schemas...", flush=True)

    # 1. Bronze Table
    spark_session.sql("""
        CREATE TABLE IF NOT EXISTS delta.`/tmp/bronze_table` (
            event_id STRING,
            timestamp TIMESTAMP,
            organization_id STRING,
            site_id STRING,
            panel_id STRING,
            zone_id STRING,
            device_id STRING,
            device_type STRING,
            current_state STRING,
            is_test_mode BOOLEAN,
            fault_code STRING,
            battery_runtime_hours DOUBLE,
            telemetry_payload STRING,
            ingestion_timestamp TIMESTAMP,
            year INT,
            month INT,
            day INT
        ) USING DELTA
    """)

    # 2. Silver Device & Unified Tables
    silver_base = """
        event_id STRING,
        timestamp TIMESTAMP,
        organization_id STRING,
        site_id STRING,
        panel_id STRING,
        zone_id STRING,
        device_id STRING,
        device_type STRING,
        current_state STRING,
        is_test_mode BOOLEAN,
        fault_code STRING,
        battery_runtime_hours DOUBLE,
        loop_voltage_dc DOUBLE,
        self_verify_passed BOOLEAN,
        ingestion_timestamp TIMESTAMP,
        year INT,
        month INT,
        day INT
    """

    for table_name in ["silver_optical_smoke", "silver_ror_heat", "silver_multi_sensor", "silver_horn_strobe", "silver_manual_call_point"]:
        spark_session.sql(f"CREATE TABLE IF NOT EXISTS delta.`/tmp/{table_name}` ({silver_base}) USING DELTA")

    spark_session.sql(f"CREATE TABLE IF NOT EXISTS delta.`/tmp/silver_unified` ({silver_base}) USING DELTA")

    spark_session.sql("""
        CREATE TABLE IF NOT EXISTS delta.`/tmp/silver_quarantine` (
            event_id STRING,
            timestamp TIMESTAMP,
            organization_id STRING,
            site_id STRING,
            panel_id STRING,
            zone_id STRING,
            device_id STRING,
            device_type STRING,
            failed_rules STRING,
            telemetry_payload STRING,
            quarantine_timestamp TIMESTAMP
        ) USING DELTA
    """)

    # 3. Gold 5-Minute Device Telemetry Fact Tables
    spark_session.sql("""
        CREATE TABLE IF NOT EXISTS delta.`/tmp/gold_zone_kpi_5m` (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            organization_id STRING,
            site_id STRING,
            zone_id STRING,
            active_alarms LONG,
            active_troubles LONG,
            isolated_devices LONG
        ) USING DELTA
    """)

    spark_session.sql("""
        CREATE TABLE IF NOT EXISTS delta.`/tmp/gold_optical_smoke_5m` (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            device_id STRING,
            zone_id STRING,
            avg_smoke_obscuration DOUBLE,
            max_chamber_dirt DOUBLE
        ) USING DELTA
    """)

    spark_session.sql("""
        CREATE TABLE IF NOT EXISTS delta.`/tmp/gold_ror_heat_5m` (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            device_id STRING,
            zone_id STRING,
            avg_temperature DOUBLE,
            max_rate_of_rise DOUBLE
        ) USING DELTA
    """)

    spark_session.sql("""
        CREATE TABLE IF NOT EXISTS delta.`/tmp/gold_multi_sensor_5m` (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            device_id STRING,
            zone_id STRING,
            avg_smoke_obscuration DOUBLE,
            avg_temperature DOUBLE,
            max_co_ppm INT
        ) USING DELTA
    """)

    # 4. Gold 5-Minute Expanding Daily Rolling Tables (Midnight -> window_end)
    spark_session.sql("""
        CREATE TABLE IF NOT EXISTS delta.`/tmp/gold_zone_daily_5m` (
            date STRING,
            window_end TIMESTAMP,
            organization_id STRING,
            site_id STRING,
            zone_id STRING,
            floor_level STRING,
            device_density LONG,
            active_alarms LONG,
            active_faults LONG,
            active_disablements LONG,
            zone_status STRING,
            smoke_detector_count LONG,
            horn_strobe_count LONG,
            mcp_count LONG,
            multi_sensor_count LONG,
            ror_heat_count LONG
        ) USING DELTA
    """)

    spark_session.sql("""
        CREATE TABLE IF NOT EXISTS delta.`/tmp/gold_site_daily_5m` (
            date STRING,
            window_end TIMESTAMP,
            organization_id STRING,
            site_id STRING,
            active_alarms LONG,
            active_faults LONG,
            monitored_devices LONG,
            fleet_health_score DOUBLE,
            active_disablements LONG,
            smoke_detector_count LONG,
            horn_strobe_count LONG,
            mcp_count LONG,
            multi_sensor_count LONG,
            ror_heat_count LONG,
            daily_total_alarms LONG,
            daily_total_faults LONG,
            daily_self_tests LONG,
            system_online_pct DOUBLE,
            total_online_controllers LONG,
            total_degraded_controllers LONG,
            total_offline_controllers LONG,
            nfpa72_drift_pass_rate DOUBLE,
            nac_audibility_pass_rate DOUBLE,
            strobe_sync_pass_rate DOUBLE,
            battery_runtime_hours DOUBLE
        ) USING DELTA
    """)

    spark_session.sql("""
        CREATE TABLE IF NOT EXISTS delta.`/tmp/gold_organization_daily_5m` (
            date STRING,
            window_end TIMESTAMP,
            organization_id STRING,
            total_sites_monitored LONG,
            total_monitored_devices LONG,
            global_active_alarms LONG,
            global_active_faults LONG,
            global_fleet_health_score DOUBLE,
            global_online_controllers LONG,
            global_offline_controllers LONG
        ) USING DELTA
    """)

if __name__ == "__main__":
    print("1. Restoring state & starting Hugging Face background sync worker...", flush=True)
    start_sync_thread()

    print("2. Initializing PySpark with FAIR Scheduler...", flush=True)
    spark_version = pyspark.__version__
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("FireSafety_Streaming_Pipeline") \
        .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version},io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.scheduler.mode", "FAIR") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8")) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    if os.path.exists("truststore.jks") and os.path.exists("keystore.p12"):
        spark.sparkContext.addFile("truststore.jks")
        spark.sparkContext.addFile("keystore.p12")

    print("3. Pre-initializing Delta Schemas...", flush=True)
    preinitialize_tables(spark)

    print("4. Launching Background Layer Streams and Workers...", flush=True)
    threading.Thread(target=launch_stream, args=(run_bronze, "bronze_pool", spark), daemon=True).start()
    threading.Thread(target=launch_stream, args=(run_silver, "silver_pool", spark), daemon=True).start()
    threading.Thread(target=launch_stream, args=(run_gold, "gold_pool", spark), daemon=True).start()

    start_maintenance_thread(spark)

    print(f"Pipeline active. Auto-shutdown scheduled in {RUN_DURATION_SECONDS} seconds...", flush=True)
    time.sleep(RUN_DURATION_SECONDS)

    print(f"\n[{RUN_DURATION_SECONDS}s reached] Initiating graceful shutdown...", flush=True)
    try:
        for q in list(spark.streams.active):
            q.stop()
        spark.stop()
    except Exception as exc:
        print(f"[shutdown] Error stopping Spark: {exc}", flush=True)

    sync_all_to_hf(tag="final")
    print("👋 Pipeline completed cleanly.", flush=True)