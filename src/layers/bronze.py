import os
from pyspark import SparkFiles
from pyspark.sql.functions import from_json, col, current_timestamp, to_timestamp, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType

def run_bronze(spark):
    print("[Bronze] Starting Kafka -> Bronze Stream...", flush=True)
    
    # 100% String schema for maximum resilience against edge payload failures
    envelope_schema = StructType([
        StructField("event_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("organization_id", StringType()),
        StructField("site_id", StringType()),
        StructField("panel_id", StringType()),
        StructField("zone_id", StringType()),
        StructField("device_id", StringType()),
        StructField("device_type", StringType()),
        StructField("event_type", StringType()),
        StructField("current_state", StringType()),
        StructField("is_test_mode", StringType()), 
        StructField("fault_code", StringType()),
        StructField("battery_runtime_hours", StringType()), 
        StructField("telemetry", StringType())
    ])

    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_URI")) \
        .option("subscribe", "fire-and-safety") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 10000) \
        .option("failOnDataLoss", "false") \
        .option("kafka.security.protocol", "SSL") \
        .option("kafka.ssl.truststore.location", SparkFiles.get("truststore.jks")) \
        .option("kafka.ssl.truststore.password", "changeit") \
        .option("kafka.ssl.keystore.location", SparkFiles.get("keystore.p12")) \
        .option("kafka.ssl.keystore.password", "changeit") \
        .load()

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), envelope_schema).alias("envelope")).select("envelope.*")

    final_bronze_df = parsed_df \
        .withColumnRenamed("telemetry", "telemetry_payload") \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("event_date", to_timestamp(col("timestamp"))) \
        .withColumn("year", year(col("event_date")).cast("integer")) \
        .withColumn("month", month(col("event_date")).cast("integer")) \
        .withColumn("day", dayofmonth(col("event_date")).cast("integer")) \
        .drop("event_date")

    query = final_bronze_df.writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoints/bronze") \
        .option("mergeSchema", "true").trigger(processingTime="15 seconds") \
        .start("/tmp/bronze_table")
        
    query.awaitTermination()