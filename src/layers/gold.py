import os
from pyspark.sql.functions import col, window, avg, max as spark_max, sum as spark_sum, when

def run_gold(spark):
    print("[Gold] Starting Silver -> Gold Aggregation Streams...", flush=True)

    # ==========================================
    # TIER 1: SPATIAL ROLLUPS (ZONE KPIs)
    # ==========================================
    unified_df = spark.readStream.format("delta").load("/tmp/silver_unified")

    # 5-Minute Zone KPIs
    zone_kpi_5m = unified_df \
        .withWatermark("timestamp", "15 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            col("organization_id"),
            col("site_id"),
            col("zone_id")
        ).agg(
            spark_sum(when(col("current_state") == "ALARM", 1).otherwise(0)).alias("active_alarms"),
            spark_sum(when(col("current_state") == "TROUBLE", 1).otherwise(0)).alias("active_troubles"),
            spark_sum(when(col("current_state") == "ISOLATED", 1).otherwise(0)).alias("isolated_devices")
        )

    query1 = zone_kpi_5m.writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/gold_zone_5m") \
        .option("mergeSchema", "true") \
        .trigger(processingTime="30 seconds") \
        .start("/tmp/gold_zone_kpi_5m")

    # ==========================================
    # TIER 2: DEVICE TELEMETRY ROLLUPS
    # ==========================================
    
    # Smoke Detectors (5m)
    smoke_df = spark.readStream.format("delta").load("/tmp/silver_optical_smoke")
    smoke_5m = smoke_df \
        .withWatermark("timestamp", "15 minutes") \
        .groupBy(window(col("timestamp"), "5 minutes"), col("device_id"), col("zone_id")) \
        .agg(
            avg("smoke_obscuration_pct").alias("avg_smoke_obscuration"),
            spark_max("chamber_dirt_pct").alias("max_chamber_dirt")
        )
    query2 = smoke_5m.writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/gold_smoke_5m") \
        .option("mergeSchema", "true") \
        .trigger(processingTime="30 seconds") \
        .start("/tmp/gold_optical_smoke_5m")

    # Heat Detectors (5m)
    heat_df = spark.readStream.format("delta").load("/tmp/silver_ror_heat")
    heat_5m = heat_df \
        .withWatermark("timestamp", "15 minutes") \
        .groupBy(window(col("timestamp"), "5 minutes"), col("device_id"), col("zone_id")) \
        .agg(
            avg("temperature_celsius").alias("avg_temperature"),
            spark_max("rate_of_rise_c_per_min").alias("max_rate_of_rise")
        )
    query3 = heat_5m.writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/gold_heat_5m") \
        .option("mergeSchema", "true") \
        .trigger(processingTime="30 seconds") \
        .start("/tmp/gold_ror_heat_5m")

    # Multi-Sensors (5m)
    multi_df = spark.readStream.format("delta").load("/tmp/silver_multi_sensor")
    multi_5m = multi_df \
        .withWatermark("timestamp", "15 minutes") \
        .groupBy(window(col("timestamp"), "5 minutes"), col("device_id"), col("zone_id")) \
        .agg(
            avg("smoke_obscuration_pct").alias("avg_smoke_obscuration"),
            avg("temperature_celsius").alias("avg_temperature"),
            spark_max("co_ppm").alias("max_co_ppm")
        )
    query4 = multi_5m.writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/gold_multi_5m") \
        .option("mergeSchema", "true") \
        .trigger(processingTime="30 seconds") \
        .start("/tmp/gold_multi_sensor_5m")

    spark.streams.awaitAnyTermination()