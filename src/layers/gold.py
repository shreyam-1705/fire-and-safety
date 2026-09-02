import os
from pyspark.sql.functions import col, window, avg, expr, to_date, sum as _sum, when, lit

# ==========================================
# 1. TIER 1: DEVICE FACTS
# ==========================================
def run_gold_optical_smoke(spark):
    df = spark.readStream.format("delta").load("/tmp/silver_device_optical_smoke")
    agg_df = df.withWatermark("timestamp", "5 minutes") \
        .groupBy(col("organization_id"), col("site_id"), col("panel_id"), col("zone_id"), col("device_id"), window(col("timestamp"), "5 minutes")) \
        .agg(
            expr("max_by(current_state, timestamp)").alias("window_state"),
            expr("max_by(self_verify_passed, timestamp)").alias("self_verify_passed"),
            avg("smoke_obscuration_pct").alias("avg_smoke_obscuration_pct"),
            avg("chamber_dirt_pct").alias("avg_chamber_dirt_pct"),
            avg("loop_voltage_dc").alias("avg_loop_voltage_dc"),
        ).withColumn("is_smoke_alarm", col("avg_smoke_obscuration_pct") >= 2.50) \
         .withColumn("is_dirt_warning", col("avg_chamber_dirt_pct") >= 60.0) \
         .withColumn("date", to_date(col("window.start").cast("string"))) \
         .select("date", "organization_id", "site_id", "panel_id", col("zone_id").alias("system_id"), "device_id", col("window.start").alias("window_start"), "avg_smoke_obscuration_pct", "avg_chamber_dirt_pct", "avg_loop_voltage_dc", "self_verify_passed", "is_smoke_alarm", "is_dirt_warning")

    return agg_df.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/tmp/checkpoints/gold_smoke").option("mergeSchema", "true").trigger(availableNow=True).start("/tmp/gold_device_optical_smoke_5m")

def run_gold_ror_heat(spark):
    df = spark.readStream.format("delta").load("/tmp/silver_device_ror_heat")
    agg_df = df.withWatermark("timestamp", "5 minutes") \
        .groupBy(col("organization_id"), col("site_id"), col("panel_id"), col("zone_id"), col("device_id"), window(col("timestamp"), "5 minutes")) \
        .agg(
            expr("max_by(current_state, timestamp)").alias("window_state"),
            expr("max_by(self_verify_passed, timestamp)").alias("self_verify_passed"),
            avg("temperature_celsius").alias("avg_temperature_celsius"),
            avg("rate_of_rise_c_per_min").alias("avg_rate_of_rise_c_per_min"),
            avg("loop_voltage_dc").alias("avg_loop_voltage_dc"),
        ).withColumn("is_fixed_temp_alarm", col("avg_temperature_celsius") >= 57.0) \
         .withColumn("is_ror_alarm", col("avg_rate_of_rise_c_per_min") >= 8.3) \
         .withColumn("date", to_date(col("window.start").cast("string"))) \
         .select("date", "organization_id", "site_id", "panel_id", col("zone_id").alias("system_id"), "device_id", col("window.start").alias("window_start"), "avg_temperature_celsius", "avg_rate_of_rise_c_per_min", "avg_loop_voltage_dc", "self_verify_passed", "is_fixed_temp_alarm", "is_ror_alarm")

    return agg_df.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/tmp/checkpoints/gold_heat").option("mergeSchema", "true").trigger(availableNow=True).start("/tmp/gold_device_ror_heat_5m")

def run_gold_multi_sensor(spark):
    df = spark.readStream.format("delta").load("/tmp/silver_device_multi_sensor")
    agg_df = df.withWatermark("timestamp", "5 minutes") \
        .groupBy(col("organization_id"), col("site_id"), col("panel_id"), col("zone_id"), col("device_id"), window(col("timestamp"), "5 minutes")) \
        .agg(
            expr("max_by(current_state, timestamp)").alias("window_state"),
            expr("max_by(self_verify_passed, timestamp)").alias("self_verify_passed"),
            avg("co_ppm").alias("avg_co_ppm"),
            avg("co_cell_health_pct").alias("avg_co_cell_health_pct"),
            avg("smoke_obscuration_pct").alias("avg_smoke_obscuration_pct"),
            avg("temperature_celsius").alias("avg_temperature_celsius"),
            avg("loop_voltage_dc").alias("avg_loop_voltage_dc"),
        ).withColumn("is_toxic_co_alarm", col("avg_co_ppm") >= 30.0) \
         .withColumn("is_cell_fault", col("avg_co_cell_health_pct") < 70.0) \
         .withColumn("date", to_date(col("window.start").cast("string"))) \
         .select("date", "organization_id", "site_id", "panel_id", col("zone_id").alias("system_id"), "device_id", col("window.start").alias("window_start"), "avg_co_ppm", "avg_co_cell_health_pct", "avg_smoke_obscuration_pct", "avg_temperature_celsius", "avg_loop_voltage_dc", "self_verify_passed", "is_toxic_co_alarm", "is_cell_fault")

    return agg_df.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/tmp/checkpoints/gold_multi_sensor").option("mergeSchema", "true").trigger(availableNow=True).start("/tmp/gold_device_multi_sensor_5m")

def run_gold_horn_strobe(spark):
    df = spark.readStream.format("delta").load("/tmp/silver_device_horn_strobe")
    agg_df = df.withWatermark("timestamp", "5 minutes") \
        .groupBy(col("organization_id"), col("site_id"), col("panel_id"), col("zone_id"), col("device_id"), window(col("timestamp"), "5 minutes")) \
        .agg(
            expr("max_by(strobe_active, timestamp)").alias("strobe_active"),
            expr("max_by(is_sounding, timestamp)").alias("is_sounding"),
            expr("max_by(self_verify_passed, timestamp)").alias("self_verify_passed"),
            avg("self_test_decibel_level").alias("avg_decibel_level"),
            avg("sync_offset_ms").alias("avg_sync_offset_ms"),
            avg("loop_voltage_dc").alias("avg_loop_voltage_dc"),
        ).withColumn("date", to_date(col("window.start").cast("string"))) \
         .select("date", "organization_id", "site_id", "panel_id", col("zone_id").alias("system_id"), "device_id", col("window.start").alias("window_start"), "strobe_active", "is_sounding", "avg_decibel_level", "avg_sync_offset_ms", "avg_loop_voltage_dc", "self_verify_passed")

    return agg_df.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/tmp/checkpoints/gold_horn_strobe").option("mergeSchema", "true").trigger(availableNow=True).start("/tmp/gold_device_horn_strobe_5m")

def run_gold_manual_call_point(spark):
    df = spark.readStream.format("delta").load("/tmp/silver_device_manual_call_point")
    agg_df = df.withWatermark("timestamp", "5 minutes") \
        .groupBy(col("organization_id"), col("site_id"), col("panel_id"), col("zone_id"), col("device_id"), window(col("timestamp"), "5 minutes")) \
        .agg(
            expr("max_by(is_activated, timestamp)").alias("is_activated"),
            expr("max_by(tamper_switch, timestamp)").alias("tamper_switch"),
            avg("loop_voltage_dc").alias("avg_loop_voltage_dc"),
        ).withColumn("date", to_date(col("window.start").cast("string"))) \
         .select("date", "organization_id", "site_id", "panel_id", col("zone_id").alias("system_id"), "device_id", col("window.start").alias("window_start"), "is_activated", "tamper_switch", "avg_loop_voltage_dc")

    return agg_df.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/tmp/checkpoints/gold_mcp").option("mergeSchema", "true").trigger(availableNow=True).start("/tmp/gold_device_manual_call_point_5m")

# ==========================================
# 2. TIER 2: SPATIAL KPI SNAPSHOTS
# ==========================================
def run_gold_snapshots(spark):
    """Unifies Silver records and aggregates 5-minute KPIs for Zone, Site, and Organization."""
    base_cols = ["timestamp", "organization_id", "site_id", "panel_id", "zone_id", "device_id", "device_type", "current_state"]
    
    # Read and unify all 5 Silver tables
    df_smoke = spark.readStream.format("delta").load("/tmp/silver_device_optical_smoke").select(*base_cols)
    df_heat = spark.readStream.format("delta").load("/tmp/silver_device_ror_heat").select(*base_cols)
    df_ms = spark.readStream.format("delta").load("/tmp/silver_device_multi_sensor").select(*base_cols)
    df_horn = spark.readStream.format("delta").load("/tmp/silver_device_horn_strobe").select(*base_cols)
    df_mcp = spark.readStream.format("delta").load("/tmp/silver_device_manual_call_point").select(*base_cols)
    
    unified_df = df_smoke.union(df_heat).union(df_ms).union(df_horn).union(df_mcp).withWatermark("timestamp", "5 minutes")

    # Zone Snapshot
    zone_agg = unified_df.groupBy("organization_id", "site_id", "panel_id", "zone_id", window("timestamp", "5 minutes")).agg(
        expr("count(distinct device_id)").alias("device_density"),
        _sum(when(col("current_state") == "ALARM", 1).otherwise(0)).alias("active_alarms"),
        _sum(when(col("current_state") == "TROUBLE", 1).otherwise(0)).alias("active_faults"),
        _sum(when(col("current_state") == "ISOLATED", 1).otherwise(0)).alias("active_disablements"),
        _sum(when(col("device_type") == "optical_smoke", 1).otherwise(0)).alias("smoke_detector_count"),
        _sum(when(col("device_type") == "horn_strobe", 1).otherwise(0)).alias("horn_strobe_count"),
        _sum(when(col("device_type") == "manual_call_point", 1).otherwise(0)).alias("mcp_count"),
        _sum(when(col("device_type") == "multi_sensor", 1).otherwise(0)).alias("multi_sensor_count"),
        _sum(when(col("device_type") == "ror_heat", 1).otherwise(0)).alias("ror_heat_count")
    ).withColumn("zone_status", when(col("active_alarms") > 0, lit("ALARM")).when(col("active_faults") > 0, lit("TROUBLE")).otherwise(lit("NORMAL"))) \
     .withColumn("date", to_date(col("window.start").cast("string"))) \
     .select("date", "organization_id", "site_id", "panel_id", col("zone_id").alias("system_id"), "device_density", "active_alarms", "active_faults", "active_disablements", "zone_status", "smoke_detector_count", "horn_strobe_count", "mcp_count", "multi_sensor_count", "ror_heat_count")

    q_zone = zone_agg.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/tmp/checkpoints/gold_zone_snapshot").trigger(availableNow=True).start("/tmp/gold_zone_snapshot_5m")

    # Site Snapshot
    site_agg = unified_df.groupBy("organization_id", "site_id", window("timestamp", "5 minutes")).agg(
        expr("count(distinct device_id)").alias("monitored_devices"),
        _sum(when(col("current_state") == "ALARM", 1).otherwise(0)).alias("active_alarms"),
        _sum(when(col("current_state") == "TROUBLE", 1).otherwise(0)).alias("active_faults"),
        _sum(when(col("current_state") == "ISOLATED", 1).otherwise(0)).alias("active_disablements"),
        _sum(when(col("device_type") == "optical_smoke", 1).otherwise(0)).alias("smoke_detector_count"),
        _sum(when(col("device_type") == "horn_strobe", 1).otherwise(0)).alias("horn_strobe_count"),
        _sum(when(col("device_type") == "manual_call_point", 1).otherwise(0)).alias("mcp_count"),
        _sum(when(col("device_type") == "multi_sensor", 1).otherwise(0)).alias("multi_sensor_count"),
        _sum(when(col("device_type") == "ror_heat", 1).otherwise(0)).alias("ror_heat_count")
    ).withColumn("fleet_health_score", lit(100.0) - (col("active_faults") * 2)) \
     .withColumn("daily_total_alarms", col("active_alarms")) \
     .withColumn("daily_total_faults", col("active_faults")) \
     .withColumn("date", to_date(col("window.start").cast("string"))) \
     .select("date", "organization_id", "site_id", "active_alarms", "active_faults", "monitored_devices", "fleet_health_score", "active_disablements", "smoke_detector_count", "horn_strobe_count", "mcp_count", "multi_sensor_count", "ror_heat_count", "daily_total_alarms", "daily_total_faults")
    
    q_site = site_agg.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/tmp/checkpoints/gold_site_snapshot").trigger(availableNow=True).start("/tmp/gold_site_snapshot_5m")

    # Organization Snapshot
    org_agg = unified_df.groupBy("organization_id", window("timestamp", "5 minutes")).agg(
        expr("count(distinct site_id)").alias("total_sites_monitored"),
        expr("count(distinct device_id)").alias("total_monitored_devices"),
        _sum(when(col("current_state") == "ALARM", 1).otherwise(0)).alias("global_active_alarms"),
        _sum(when(col("current_state") == "TROUBLE", 1).otherwise(0)).alias("global_active_faults")
    ).withColumn("global_fleet_health_score", lit(100.0) - (col("global_active_faults") * 2)) \
     .withColumn("date", to_date(col("window.start").cast("string"))) \
     .select("date", "organization_id", "total_sites_monitored", "total_monitored_devices", "global_active_alarms", "global_active_faults", "global_fleet_health_score")

    q_org = org_agg.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/tmp/checkpoints/gold_org_snapshot").trigger(availableNow=True).start("/tmp/gold_organization_snapshot_5m")

    return [q_zone, q_site, q_org]

def run_gold(spark):
    print("[Gold] Starting Silver -> Gold (availableNow) Fact & Snapshot Aggregations...", flush=True)
    queries = [
        run_gold_optical_smoke(spark),
        run_gold_ror_heat(spark),
        run_gold_multi_sensor(spark),
        run_gold_horn_strobe(spark),
        run_gold_manual_call_point(spark)
    ]
    
    # Add snapshot queries
    queries.extend(run_gold_snapshots(spark))
    
    for q in queries:
        q.awaitTermination()
        
    print("[Gold] Complete. Aggregated all valid Silver records into Facts and Snapshots.", flush=True)