import os
from pyspark.sql.functions import col, get_json_object, lit, current_timestamp

def process_silver_batch(batch_df, batch_id):
    parsed_df = batch_df \
        .withColumn("loop_voltage_dc", get_json_object(col("telemetry_payload"), "$.loop_voltage_dc").cast("double")) \
        .withColumn("self_verify_passed", get_json_object(col("telemetry_payload"), "$.self_verify_passed").cast("boolean"))
        
    # --- 1. OPTICAL SMOKE ---
    smoke_df = parsed_df.filter(col("device_type") == "optical_smoke") \
        .withColumn("smoke_obscuration_pct", get_json_object(col("telemetry_payload"), "$.smoke_obscuration_pct").cast("double")) \
        .withColumn("chamber_dirt_pct", get_json_object(col("telemetry_payload"), "$.chamber_dirt_pct").cast("double"))
    smoke_valid = smoke_df.filter(((col("smoke_obscuration_pct") >= 0.0) | col("smoke_obscuration_pct").isNull()) & ((col("chamber_dirt_pct") >= 0.0) | col("chamber_dirt_pct").isNull()))
    smoke_invalid = smoke_df.subtract(smoke_valid).withColumn("failed_rules", lit("Invalid smoke bounds"))
    
    # --- 2. ROR HEAT ---
    heat_df = parsed_df.filter(col("device_type") == "ror_heat") \
        .withColumn("temperature_celsius", get_json_object(col("telemetry_payload"), "$.temperature_celsius").cast("double")) \
        .withColumn("rate_of_rise_c_per_min", get_json_object(col("telemetry_payload"), "$.rate_of_rise_c_per_min").cast("double"))
    heat_valid = heat_df.filter((col("temperature_celsius").between(-40.0, 150.0) | col("temperature_celsius").isNull()) & ((col("rate_of_rise_c_per_min") >= 0.0) | col("rate_of_rise_c_per_min").isNull()))
    heat_invalid = heat_df.subtract(heat_valid).withColumn("failed_rules", lit("Invalid heat bounds"))
    
    # --- 3. MULTI SENSOR ---
    multi_df = parsed_df.filter(col("device_type") == "multi_sensor") \
        .withColumn("smoke_obscuration_pct", get_json_object(col("telemetry_payload"), "$.smoke_obscuration_pct").cast("double")) \
        .withColumn("temperature_celsius", get_json_object(col("telemetry_payload"), "$.temperature_celsius").cast("double")) \
        .withColumn("co_ppm", get_json_object(col("telemetry_payload"), "$.co_ppm").cast("integer")) \
        .withColumn("co_cell_health_pct", get_json_object(col("telemetry_payload"), "$.co_cell_health_pct").cast("double"))
    multi_valid = multi_df.filter(((col("smoke_obscuration_pct") >= 0.0) | col("smoke_obscuration_pct").isNull()) & (col("temperature_celsius").between(-40.0, 150.0) | col("temperature_celsius").isNull()) & ((col("co_ppm") >= 0) | col("co_ppm").isNull()) & ((col("co_cell_health_pct") >= 0.0) | col("co_cell_health_pct").isNull()))
    multi_invalid = multi_df.subtract(multi_valid).withColumn("failed_rules", lit("Invalid multi-sensor bounds"))
    
    # --- 4. HORN STROBE ---
    horn_df = parsed_df.filter(col("device_type") == "horn_strobe") \
        .withColumn("is_sounding", get_json_object(col("telemetry_payload"), "$.is_sounding").cast("boolean")) \
        .withColumn("strobe_active", get_json_object(col("telemetry_payload"), "$.strobe_active").cast("boolean")) \
        .withColumn("self_test_decibel_level", get_json_object(col("telemetry_payload"), "$.self_test_decibel_level").cast("double")) \
        .withColumn("sync_offset_ms", get_json_object(col("telemetry_payload"), "$.sync_offset_ms").cast("double"))
    horn_valid = horn_df.filter((col("self_test_decibel_level").between(0.0, 150.0) | col("self_test_decibel_level").isNull()) & ((col("sync_offset_ms") >= 0.0) | col("sync_offset_ms").isNull()))
    horn_invalid = horn_df.subtract(horn_valid).withColumn("failed_rules", lit("Invalid horn strobe bounds"))
    
    # --- 5. MANUAL CALL POINT ---
    mcp_df = parsed_df.filter(col("device_type") == "manual_call_point") \
        .withColumn("is_activated", get_json_object(col("telemetry_payload"), "$.is_activated").cast("boolean")) \
        .withColumn("tamper_switch", get_json_object(col("telemetry_payload"), "$.tamper_switch").cast("boolean"))
    mcp_valid = mcp_df 
    mcp_invalid = mcp_df.filter(lit(False)).withColumn("failed_rules", lit("")) 

    # --- WRITE DEVICE TABLES ---
    if not smoke_valid.isEmpty(): smoke_valid.drop("telemetry_payload").write.format("delta").mode("append").option("mergeSchema", "true").save("/tmp/silver_optical_smoke")
    if not heat_valid.isEmpty(): heat_valid.drop("telemetry_payload").write.format("delta").mode("append").option("mergeSchema", "true").save("/tmp/silver_ror_heat")
    if not multi_valid.isEmpty(): multi_valid.drop("telemetry_payload").write.format("delta").mode("append").option("mergeSchema", "true").save("/tmp/silver_multi_sensor")
    if not horn_valid.isEmpty(): horn_valid.drop("telemetry_payload").write.format("delta").mode("append").option("mergeSchema", "true").save("/tmp/silver_horn_strobe")
    if not mcp_valid.isEmpty(): mcp_valid.drop("telemetry_payload").write.format("delta").mode("append").option("mergeSchema", "true").save("/tmp/silver_manual_call_point")
        
    # --- CREATE UNIFIED VALID BASE FOR GOLD SPATIAL KPIs ---
    base_cols = ["event_id", "timestamp", "organization_id", "site_id", "panel_id", "zone_id", "device_id", "device_type", "current_state", "is_test_mode", "fault_code", "battery_runtime_hours"]
    all_valid_base = smoke_valid.select(*base_cols).unionByName(heat_valid.select(*base_cols), allowMissingColumns=True) \
        .unionByName(multi_valid.select(*base_cols), allowMissingColumns=True) \
        .unionByName(horn_valid.select(*base_cols), allowMissingColumns=True) \
        .unionByName(mcp_valid.select(*base_cols), allowMissingColumns=True)
        
    if not all_valid_base.isEmpty():
        all_valid_base.write.format("delta").mode("append").option("mergeSchema", "true").save("/tmp/silver_unified")

    # --- HANDLE QUARANTINE ---
    all_invalid = smoke_invalid.unionByName(heat_invalid, allowMissingColumns=True).unionByName(multi_invalid, allowMissingColumns=True).unionByName(horn_invalid, allowMissingColumns=True).unionByName(mcp_invalid, allowMissingColumns=True)
    if not all_invalid.isEmpty():
        all_invalid.select("event_id", "timestamp", "organization_id", "site_id", "panel_id", "zone_id", "device_id", "device_type", "failed_rules", "telemetry_payload") \
            .withColumn("quarantine_timestamp", current_timestamp()) \
            .write.format("delta").mode("append").option("mergeSchema", "true").save("/tmp/silver_quarantine")


def run_silver(spark):
    print("[Silver] Starting Bronze -> Silver Validation Stream...", flush=True)
    
    bronze_df = spark.readStream.format("delta").load("/tmp/bronze_table")
        
    # CRITICAL: Watermark required before dropDuplicates in streaming mode
    dedup_df = bronze_df \
        .withWatermark("timestamp", "15 minutes") \
        .dropDuplicates(["event_id"])
        
    query = dedup_df.writeStream \
        .foreachBatch(process_silver_batch) \
        .option("checkpointLocation", "/tmp/checkpoints/silver") \
        .trigger(processingTime="30 seconds") \
        .start()
        
    query.awaitTermination()