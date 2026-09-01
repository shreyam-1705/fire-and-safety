import os
from pyspark.sql.window import Window
from pyspark.sql.functions import col, get_json_object, lit, row_number

def process_silver_batch(batch_df, batch_id):
    if batch_df.isEmpty(): return

    # 1. Deduplication & Completeness Checks
    window_spec = Window.partitionBy("event_id").orderBy("ingestion_timestamp")
    
    tagged_df = batch_df.withColumn("row_num", row_number().over(window_spec)) \
        .withColumn("timestamp_cast", col("timestamp").cast("timestamp")) \
        .withColumn("is_test_mode_cast", col("is_test_mode").cast("boolean")) \
        .withColumn("battery_runtime_hours_cast", col("battery_runtime_hours").cast("float"))

    # Include organization_id in the strict routing null checks
    null_keys_df = tagged_df.filter(
        col("organization_id").isNull() | col("site_id").isNull() | col("zone_id").isNull() | col("device_id").isNull()
    ).withColumn("quarantine_reason", lit("NULL_ROUTING_KEYS"))

    duplicates_df = tagged_df.filter(col("event_id").isNotNull() & (col("row_num") > 1)) \
        .withColumn("quarantine_reason", lit("DUPLICATE_EVENT_ID"))

    candidates_df = tagged_df.filter(
        col("organization_id").isNotNull() & col("site_id").isNotNull() & 
        col("zone_id").isNotNull() & col("device_id").isNotNull() & (col("row_num") == 1)
    )
    candidates_df.cache()

    # Apply casts to valid base stream
    base_df = candidates_df \
        .drop("timestamp", "is_test_mode", "battery_runtime_hours") \
        .withColumnRenamed("timestamp_cast", "timestamp") \
        .withColumnRenamed("is_test_mode_cast", "is_test_mode") \
        .withColumnRenamed("battery_runtime_hours_cast", "battery_runtime_hours") \
        .withColumn("loop_voltage_dc", get_json_object(col("telemetry_payload"), "$.loop_voltage_dc").cast("float")) \
        .withColumn("self_verify_passed", get_json_object(col("telemetry_payload"), "$.self_verify_passed").cast("boolean"))
        
    voltage_invalid_df = base_df.filter((col("loop_voltage_dc") < 0.0) | (col("loop_voltage_dc") > 30.0)) \
        .withColumn("quarantine_reason", lit("INVALID_LOOP_VOLTAGE"))
    valid_base_df = base_df.filter(((col("loop_voltage_dc") >= 0.0) & (col("loop_voltage_dc") <= 30.0)) | col("loop_voltage_dc").isNull())

    # 2. Branch: Optical Smoke
    smoke_df = valid_base_df.filter(col("device_type") == "optical_smoke") \
        .withColumn("smoke_obscuration_pct", get_json_object(col("telemetry_payload"), "$.smoke_obscuration_pct").cast("float")) \
        .withColumn("chamber_dirt_pct", get_json_object(col("telemetry_payload"), "$.chamber_dirt_pct").cast("float"))
        
    smoke_valid = smoke_df.filter((col("smoke_obscuration_pct") >= 0.0) & (col("chamber_dirt_pct").between(0.0, 100.0)))
    smoke_invalid = smoke_df.subtract(smoke_valid).withColumn("quarantine_reason", lit("INVALID_SMOKE_METRICS"))
    
    if not smoke_valid.isEmpty():
        smoke_valid.drop("telemetry_payload", "row_num").write.format("delta").mode("append").option("mergeSchema", "true").save("/tmp/silver_device_optical_smoke")

    # 3. Branch: RoR Heat
    heat_df = valid_base_df.filter(col("device_type") == "ror_heat") \
        .withColumn("temperature_celsius", get_json_object(col("telemetry_payload"), "$.temperature_celsius").cast("float")) \
        .withColumn("rate_of_rise_c_per_min", get_json_object(col("telemetry_payload"), "$.rate_of_rise_c_per_min").cast("float"))

    heat_valid = heat_df.filter(
        (col("temperature_celsius").between(-40.0, 150.0)) & 
        (col("rate_of_rise_c_per_min") >= 0.0)
    )
    heat_invalid = heat_df.subtract(heat_valid).withColumn("quarantine_reason", lit("INVALID_HEAT_METRICS"))
    
    if not heat_valid.isEmpty():
        heat_valid.drop("telemetry_payload", "row_num").write.format("delta").mode("append").option("mergeSchema", "true").save("/tmp/silver_device_ror_heat")

    # 4. Branch: Multi-Sensor
    ms_df = valid_base_df.filter(col("device_type") == "multi_sensor") \
        .withColumn("co_ppm", get_json_object(col("telemetry_payload"), "$.co_ppm").cast("float")) \
        .withColumn("co_cell_health_pct", get_json_object(col("telemetry_payload"), "$.co_cell_health_pct").cast("float")) \
        .withColumn("smoke_obscuration_pct", get_json_object(col("telemetry_payload"), "$.smoke_obscuration_pct").cast("float")) \
        .withColumn("temperature_celsius", get_json_object(col("telemetry_payload"), "$.temperature_celsius").cast("float"))

    ms_valid = ms_df.filter(
        (col("temperature_celsius").between(-40.0, 150.0)) & 
        (col("smoke_obscuration_pct") >= 0.0) &
        (col("co_ppm") >= 0.0) &
        (col("co_cell_health_pct").between(0.0, 100.0))
    )
    ms_invalid = ms_df.subtract(ms_valid).withColumn("quarantine_reason", lit("INVALID_MULTISENSOR_METRICS"))
    
    if not ms_valid.isEmpty():
        ms_valid.drop("telemetry_payload", "row_num").write.format("delta").mode("append").option("mergeSchema", "true").save("/tmp/silver_device_multi_sensor")

    # 5. Branch: Horn Strobe
    horn_df = valid_base_df.filter(col("device_type") == "horn_strobe") \
        .withColumn("strobe_active", get_json_object(col("telemetry_payload"), "$.strobe_active").cast("boolean")) \
        .withColumn("is_sounding", get_json_object(col("telemetry_payload"), "$.is_sounding").cast("boolean")) \
        .withColumn("self_test_decibel_level", get_json_object(col("telemetry_payload"), "$.self_test_decibel_level").cast("float")) \
        .withColumn("sync_offset_ms", get_json_object(col("telemetry_payload"), "$.sync_offset_ms").cast("float"))

    horn_valid = horn_df.filter(
        (col("self_test_decibel_level").between(0.0, 150.0) | col("self_test_decibel_level").isNull()) &
        (col("sync_offset_ms") >= 0.0 | col("sync_offset_ms").isNull())
    )
    horn_invalid = horn_df.subtract(horn_valid).withColumn("quarantine_reason", lit("INVALID_HORN_METRICS"))
    
    if not horn_valid.isEmpty():
        horn_valid.drop("telemetry_payload", "row_num").write.format("delta").mode("append").option("mergeSchema", "true").save("/tmp/silver_device_horn_strobe")

    # 6. Branch: Manual Call Point (No analog physics to validate, just booleans)
    mcp_df = valid_base_df.filter(col("device_type") == "manual_call_point") \
        .withColumn("is_activated", get_json_object(col("telemetry_payload"), "$.is_activated").cast("boolean")) \
        .withColumn("tamper_switch", get_json_object(col("telemetry_payload"), "$.tamper_switch").cast("boolean"))

    if not mcp_df.isEmpty():
        mcp_df.drop("telemetry_payload", "row_num").write.format("delta").mode("append").option("mergeSchema", "true").save("/tmp/silver_device_manual_call_point")

    # 7. Quarantine Rejection Routing
    def prepare_for_quarantine(invalid_df):
        return invalid_df.drop("timestamp", "is_test_mode", "battery_runtime_hours") \
                         .withColumnRenamed("timestamp_cast", "timestamp") \
                         .withColumnRenamed("is_test_mode_cast", "is_test_mode") \
                         .withColumnRenamed("battery_runtime_hours_cast", "battery_runtime_hours")

    voltage_invalid_q = prepare_for_quarantine(voltage_invalid_df)
    smoke_invalid_q = prepare_for_quarantine(smoke_invalid)
    heat_invalid_q = prepare_for_quarantine(heat_invalid)
    ms_invalid_q = prepare_for_quarantine(ms_invalid)
    horn_invalid_q = prepare_for_quarantine(horn_invalid)

    all_quarantine = null_keys_df.drop("timestamp_cast", "is_test_mode_cast", "battery_runtime_hours_cast") \
        .unionByName(duplicates_df.drop("timestamp_cast", "is_test_mode_cast", "battery_runtime_hours_cast"), allowMissingColumns=True) \
        .unionByName(voltage_invalid_q, allowMissingColumns=True) \
        .unionByName(smoke_invalid_q, allowMissingColumns=True) \
        .unionByName(heat_invalid_q, allowMissingColumns=True) \
        .unionByName(ms_invalid_q, allowMissingColumns=True) \
        .unionByName(horn_invalid_q, allowMissingColumns=True)

    if not all_quarantine.isEmpty():
        all_quarantine.select(
            "event_id", "timestamp", "organization_id", "site_id", "zone_id", "panel_id", 
            "battery_runtime_hours", "device_id", "device_type", "current_state", "is_test_mode", "fault_code", 
            "telemetry_payload", "ingestion_timestamp", "year", "month", "day", "quarantine_reason"
        ).write.format("delta").mode("append").option("mergeSchema", "true").save("/tmp/silver_quarantine")

    candidates_df.unpersist()

def run_silver(spark):
    print("[Silver] Starting Bronze -> Silver Validation Stream...", flush=True)
    bronze_df = spark.readStream.format("delta").load("/tmp/bronze_table")
    query = bronze_df.writeStream.foreachBatch(process_silver_batch) \
        .option("checkpointLocation", "/tmp/checkpoints/silver") \
        .trigger(processingTime="15 seconds").start()
    query.awaitTermination()