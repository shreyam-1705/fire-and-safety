# FILE: src/layers/gold.py
from pyspark.sql.functions import col, window, avg, max as spark_max, sum as spark_sum, when

def run_gold_zone_kpi(spark):
    df = spark.readStream.format("delta").load("/tmp/silver_unified")
    
    zone_kpi_5m = df.withWatermark("timestamp", "2 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            col("organization_id"),
            col("site_id"),
            col("zone_id")
        ).agg(
            spark_sum(when(col("current_state") == "ALARM", 1).otherwise(0)).alias("active_alarms"),
            spark_sum(when(col("current_state") == "TROUBLE", 1).otherwise(0)).alias("active_troubles"),
            spark_sum(when(col("current_state") == "ISOLATED", 1).otherwise(0)).alias("isolated_devices")
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "organization_id", "site_id", "zone_id",
            "active_alarms", "active_troubles", "isolated_devices"
        )

    zone_kpi_5m.writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/gold_zone_5m") \
        .option("mergeSchema", "true") \
        .trigger(processingTime="15 seconds") \
        .start("/tmp/gold_zone_kpi_5m")

def run_gold_smoke(spark):
    df = spark.readStream.format("delta").load("/tmp/silver_optical_smoke")
    
    smoke_5m = df.withWatermark("timestamp", "2 minutes") \
        .groupBy(window(col("timestamp"), "5 minutes"), col("device_id"), col("zone_id")) \
        .agg(
            avg("smoke_obscuration_pct").alias("avg_smoke_obscuration"),
            spark_max("chamber_dirt_pct").alias("max_chamber_dirt")
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "device_id", "zone_id",
            "avg_smoke_obscuration", "max_chamber_dirt"
        )

    smoke_5m.writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/gold_smoke_5m") \
        .option("mergeSchema", "true") \
        .trigger(processingTime="15 seconds") \
        .start("/tmp/gold_optical_smoke_5m")

def run_gold_heat(spark):
    df = spark.readStream.format("delta").load("/tmp/silver_ror_heat")
    
    heat_5m = df.withWatermark("timestamp", "2 minutes") \
        .groupBy(window(col("timestamp"), "5 minutes"), col("device_id"), col("zone_id")) \
        .agg(
            avg("temperature_celsius").alias("avg_temperature"),
            spark_max("rate_of_rise_c_per_min").alias("max_rate_of_rise")
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "device_id", "zone_id",
            "avg_temperature", "max_rate_of_rise"
        )

    heat_5m.writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/gold_heat_5m") \
        .option("mergeSchema", "true") \
        .trigger(processingTime="15 seconds") \
        .start("/tmp/gold_ror_heat_5m")

def run_gold_multi(spark):
    df = spark.readStream.format("delta").load("/tmp/silver_multi_sensor")
    
    multi_5m = df.withWatermark("timestamp", "2 minutes") \
        .groupBy(window(col("timestamp"), "5 minutes"), col("device_id"), col("zone_id")) \
        .agg(
            avg("smoke_obscuration_pct").alias("avg_smoke_obscuration"),
            avg("temperature_celsius").alias("avg_temperature"),
            spark_max("co_ppm").alias("max_co_ppm")
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "device_id", "zone_id",
            "avg_smoke_obscuration", "avg_temperature", "max_co_ppm"
        )

    multi_5m.writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/gold_multi_5m") \
        .option("mergeSchema", "true") \
        .trigger(processingTime="15 seconds") \
        .start("/tmp/gold_multi_sensor_5m")

def run_gold(spark):
    print("[Gold] Starting Silver -> Gold 5m Aggregations...", flush=True)
    run_gold_zone_kpi(spark)
    run_gold_smoke(spark)
    run_gold_heat(spark)
    run_gold_multi(spark)
    spark.streams.awaitAnyTermination()