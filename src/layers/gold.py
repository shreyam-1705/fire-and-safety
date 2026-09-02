import os
from pyspark.sql.functions import col, window, avg, expr, to_date

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

def run_gold(spark):
    print("[Gold] Starting Silver -> Gold (availableNow) 5m Fact Aggregations...", flush=True)
    queries = [
        run_gold_optical_smoke(spark),
        run_gold_ror_heat(spark),
        run_gold_multi_sensor(spark),
        run_gold_horn_strobe(spark),
        run_gold_manual_call_point(spark)
    ]
    for q in queries:
        q.awaitTermination()
    print("[Gold] Complete. Aggregated all valid Silver records.", flush=True)