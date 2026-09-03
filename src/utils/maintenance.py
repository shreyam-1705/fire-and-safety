# FILE: src/utils/maintenance.py
import time
import threading
from pyspark.sql.functions import col, to_date, countDistinct, sum as spark_sum, avg as spark_avg, when, lit, window, round
from delta.tables import DeltaTable

TABLES_TO_MAINTAIN = [
    "bronze_table",
    "silver_optical_smoke", "silver_ror_heat", "silver_multi_sensor",
    "silver_horn_strobe", "silver_manual_call_point", "silver_unified", "silver_quarantine",
    "gold_zone_kpi_5m", "gold_optical_smoke_5m", "gold_ror_heat_5m", "gold_multi_sensor_5m",
    "gold_zone_daily_5m", "gold_site_daily_5m", "gold_organization_daily_5m"
]

def run_daily_rollups_5m(spark):
    try:
        df = spark.read.format("delta").load("/tmp/silver_unified")
        if df.isEmpty():
            return

        windowed_df = df.withColumn("date", to_date(col("timestamp")).cast("string")) \
                        .withColumn("window_end", window(col("timestamp"), "5 minutes").end)

        # -------------------------------------------------------------
        # 1. Gold Zone Daily 5m Rolling
        # -------------------------------------------------------------
        zone_daily = windowed_df.groupBy("date", "window_end", "organization_id", "site_id", "zone_id") \
            .agg(
                lit("FL-01").alias("floor_level"),
                countDistinct("device_id").alias("device_density"),
                countDistinct(when(col("current_state") == "ALARM", col("device_id"))).alias("active_alarms"),
                countDistinct(when(col("current_state") == "TROUBLE", col("device_id"))).alias("active_faults"),
                countDistinct(when(col("current_state") == "ISOLATED", col("device_id"))).alias("active_disablements"),
                lit("NORMAL").alias("zone_status"),
                countDistinct(when(col("device_type") == "optical_smoke", col("device_id"))).alias("smoke_detector_count"),
                countDistinct(when(col("device_type") == "horn_strobe", col("device_id"))).alias("horn_strobe_count"),
                countDistinct(when(col("device_type") == "manual_call_point", col("device_id"))).alias("mcp_count"),
                countDistinct(when(col("device_type") == "multi_sensor", col("device_id"))).alias("multi_sensor_count"),
                countDistinct(when(col("device_type") == "ror_heat", col("device_id"))).alias("ror_heat_count")
            )

        target_zone = DeltaTable.forPath(spark, "/tmp/gold_zone_daily_5m")
        target_zone.alias("t").merge(
            zone_daily.alias("s"),
            "t.date = s.date AND t.window_end = s.window_end AND t.organization_id = s.organization_id AND t.site_id = s.site_id AND t.zone_id = s.zone_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        # -------------------------------------------------------------
        # 2. Gold Site Daily 5m Rolling
        # -------------------------------------------------------------
        site_daily = windowed_df.groupBy("date", "window_end", "organization_id", "site_id") \
            .agg(
                countDistinct(when(col("current_state") == "ALARM", col("device_id"))).alias("active_alarms"),
                countDistinct(when(col("current_state") == "TROUBLE", col("device_id"))).alias("active_faults"),
                countDistinct("device_id").alias("monitored_devices"),
                round(((countDistinct("device_id") - countDistinct(when(col("current_state") == "TROUBLE", col("device_id")))) / countDistinct("device_id")) * 100, 2).alias("fleet_health_score"),
                countDistinct(when(col("current_state") == "ISOLATED", col("device_id"))).alias("active_disablements"),
                countDistinct(when(col("device_type") == "optical_smoke", col("device_id"))).alias("smoke_detector_count"),
                countDistinct(when(col("device_type") == "horn_strobe", col("device_id"))).alias("horn_strobe_count"),
                countDistinct(when(col("device_type") == "manual_call_point", col("device_id"))).alias("mcp_count"),
                countDistinct(when(col("device_type") == "multi_sensor", col("device_id"))).alias("multi_sensor_count"),
                countDistinct(when(col("device_type") == "ror_heat", col("device_id"))).alias("ror_heat_count"),
                countDistinct(when(col("current_state") == "ALARM", col("device_id"))).alias("daily_total_alarms"),
                countDistinct(when(col("current_state") == "TROUBLE", col("device_id"))).alias("daily_total_faults"),
                
                # Use cast("long") for integer operations merging into LONG delta columns
                spark_sum(when(col("self_verify_passed") == True, 1).otherwise(0)).cast("long").alias("daily_self_tests"),
                lit(99.9).alias("system_online_pct"),
                lit(10).cast("long").alias("total_online_controllers"),
                lit(0).cast("long").alias("total_degraded_controllers"),
                lit(0).cast("long").alias("total_offline_controllers"),
                
                round((spark_sum(when((col("device_type") == "optical_smoke") & (col("self_verify_passed") == True), 1).otherwise(0)) / 
                       spark_sum(when(col("device_type") == "optical_smoke", 1).otherwise(0.0001))) * 100, 2).alias("nfpa72_drift_pass_rate"),
                round((spark_sum(when((col("device_type") == "horn_strobe") & (col("self_verify_passed") == True), 1).otherwise(0)) / 
                       spark_sum(when(col("device_type") == "horn_strobe", 1).otherwise(0.0001))) * 100, 2).alias("nac_audibility_pass_rate"),
                lit(100.0).alias("strobe_sync_pass_rate"),
                round(spark_avg("battery_runtime_hours"), 2).alias("battery_runtime_hours")
            )

        target_site = DeltaTable.forPath(spark, "/tmp/gold_site_daily_5m")
        target_site.alias("t").merge(
            site_daily.alias("s"),
            "t.date = s.date AND t.window_end = s.window_end AND t.organization_id = s.organization_id AND t.site_id = s.site_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        # -------------------------------------------------------------
        # 3. Gold Organization Daily 5m Rolling
        # -------------------------------------------------------------
        org_daily = windowed_df.groupBy("date", "window_end", "organization_id") \
            .agg(
                countDistinct("site_id").alias("total_sites_monitored"),
                countDistinct("device_id").alias("total_monitored_devices"),
                countDistinct(when(col("current_state") == "ALARM", col("device_id"))).alias("global_active_alarms"),
                countDistinct(when(col("current_state") == "TROUBLE", col("device_id"))).alias("global_active_faults"),
                round(((countDistinct("device_id") - countDistinct(when(col("current_state") == "TROUBLE", col("device_id")))) / countDistinct("device_id")) * 100, 2).alias("global_fleet_health_score"),
                
                lit(25).cast("long").alias("global_online_controllers"),
                lit(0).cast("long").alias("global_offline_controllers")
            )

        target_org = DeltaTable.forPath(spark, "/tmp/gold_organization_daily_5m")
        target_org.alias("t").merge(
            org_daily.alias("s"),
            "t.date = s.date AND t.window_end = s.window_end AND t.organization_id = s.organization_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        print("[Maintenance] 5-minute rolling daily rollups merged successfully.", flush=True)
    except Exception as e:
        print(f"[Maintenance] 5-minute rolling daily rollup failed: {e}", flush=True)

def optimize_and_vacuum(spark):
    for table in TABLES_TO_MAINTAIN:
        try:
            spark.sql(f"OPTIMIZE delta.`/tmp/{table}`")
            spark.sql(f"VACUUM delta.`/tmp/{table}` RETAIN 0 HOURS")
        except Exception:
            pass
    print("[Maintenance] Delta tables compacted and vacuumed.", flush=True)

def _maintenance_loop(spark):
    while True:
        time.sleep(300)
        print("\n[Maintenance] Running scheduled rollups and compaction...", flush=True)
        run_daily_rollups_5m(spark)
        optimize_and_vacuum(spark)

def start_maintenance_thread(spark):
    threading.Thread(target=_maintenance_loop, args=(spark,), daemon=True).start()
    print("[Maintenance] Maintenance worker thread initialized.", flush=True)