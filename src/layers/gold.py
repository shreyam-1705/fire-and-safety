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
            spark_max(when(col("current_state") == "ALARM", 1).otherwise(0)).cast("long").alias("active_alarms"),
            spark_max(when(col("current_state") == "TROUBLE", 1).otherwise(0)).cast("long").alias("active_troubles"),
            spark_max(when(col("current_state") == "ISOLATED", 1).otherwise(0)).cast("long").alias("isolated_devices")
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "organization_id", "site_id", 
            col("zone_id").alias("system_id"),
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
        .groupBy(
            window(col("timestamp"), "5 minutes"), 
            col("organization_id"), col("site_id"), col("zone_id"), col("device_id")
        ) \
        .agg(
            avg("smoke_obscuration_pct").alias("avg_smoke_obscuration"),
            spark_max("chamber_dirt_pct").alias("max_chamber_dirt"),
            spark_max(when(col("current_state") == "ALARM", True).otherwise(False)).alias("is_smoke_alarm"),
            spark_max(when(col("fault_code") == "SENSOR_FAULT", True).otherwise(False)).alias("is_dirt_warning")
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "organization_id", "site_id",
            col("zone_id").alias("system_id"),
            "device_id",
            "avg_smoke_obscuration", "max_chamber_dirt",
            "is_smoke_alarm", "is_dirt_warning"
        )

    smoke_5m.writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/gold_smoke_5m") \
        .option("mergeSchema", "true") \
        .trigger(processingTime="15 seconds") \
        .start("/tmp/gold_optical_smoke_5m")

def run_gold_heat(spark):
    df = spark.readStream.format("delta").load("/tmp/silver_ror_heat")
    
    heat_5m = df.withWatermark("timestamp", "2 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes"), 
            col("organization_id"), col("site_id"), col("zone_id"), col("device_id")
        ) \
        .agg(
            avg("temperature_celsius").alias("avg_temperature"),
            spark_max("rate_of_rise_c_per_min").alias("max_rate_of_rise"),
            spark_max(when(col("current_state") == "ALARM", True).otherwise(False)).alias("is_ror_alarm"),
            spark_max(when(col("current_state") == "ALARM", True).otherwise(False)).alias("is_fixed_temp_alarm")
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "organization_id", "site_id",
            col("zone_id").alias("system_id"),
            "device_id",
            "avg_temperature", "max_rate_of_rise",
            "is_ror_alarm", "is_fixed_temp_alarm"
        )

    heat_5m.writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/gold_heat_5m") \
        .option("mergeSchema", "true") \
        .trigger(processingTime="15 seconds") \
        .start("/tmp/gold_ror_heat_5m")

def run_gold_multi(spark):
    df = spark.readStream.format("delta").load("/tmp/silver_multi_sensor")
    
    multi_5m = df.withWatermark("timestamp", "2 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes"), 
            col("organization_id"), col("site_id"), col("zone_id"), col("device_id")
        ) \
        .agg(
            avg("smoke_obscuration_pct").alias("avg_smoke_obscuration"),
            avg("temperature_celsius").alias("avg_temperature"),
            spark_max("co_ppm").alias("max_co_ppm"),
            spark_max(when(col("current_state") == "ALARM", True).otherwise(False)).alias("is_toxic_co_alarm"),
            spark_max(when(col("fault_code") == "BATTERY_LOW", True).otherwise(False)).alias("is_cell_fault")
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "organization_id", "site_id",
            col("zone_id").alias("system_id"),
            "device_id",
            "avg_smoke_obscuration", "avg_temperature", "max_co_ppm",
            "is_toxic_co_alarm", "is_cell_fault"
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