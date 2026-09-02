import os
import sys
import time
import threading
import signal
import pyspark

from pyspark.sql import SparkSession
from utils.hf_sync import start_sync_thread, sync_all_to_hf
from layers.bronze import run_bronze
from layers.silver import run_silver
from layers.gold import run_gold
# Optional: import maintenance if you want to run it at the end
# from utils.maintenance import run_maintenance

# Default to 30 minutes (1800 seconds) if not specified in env
RUN_DURATION_SECONDS = int(os.getenv("RUN_DURATION_SECONDS", "1800"))

def handle_shutdown(signum=None, frame=None):
    print(f"\n[shutdown] Signal {signum} received. Stopping Spark & syncing...", flush=True)
    try:
        spark.stop()
    except Exception:
        pass
    sync_all_to_hf(tag="shutdown")
    sys.exit(0)

# Register signals for graceful termination
signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

def launch_stream(layer_function, pool_name, spark_session):
    """Assigns the thread to a specific FAIR scheduler pool and runs the layer"""
    spark_session.sparkContext.setLocalProperty("spark.scheduler.pool", pool_name)
    try:
        layer_function(spark_session)
    except Exception as e:
        print(f"[{pool_name}] Stream stopped or error: {e}", flush=True)

if __name__ == "__main__":
    print("1. Restoring state & starting Hugging Face Sync...")
    start_sync_thread()

    print("2. Initializing PySpark with FAIR Scheduler...")
    spark_version = pyspark.__version__
    
    global spark
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("FireSafety_Streaming_Pipeline") \
        .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version},io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.scheduler.mode", "FAIR") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    # Attach the SSL certs to the Spark Context for Kafka
    if os.path.exists("truststore.jks") and os.path.exists("keystore.p12"):
        spark.sparkContext.addFile("truststore.jks")
        spark.sparkContext.addFile("keystore.p12")

    print("3. Launching Background Threads (30s Triggers)...")
    threading.Thread(target=launch_stream, args=(run_bronze, "bronze_pool", spark), daemon=True).start()
    threading.Thread(target=launch_stream, args=(run_silver, "silver_pool", spark), daemon=True).start()
    threading.Thread(target=launch_stream, args=(run_gold, "gold_pool", spark), daemon=True).start()
    
    print(f"Pipeline active. Auto-shutdown scheduled in {RUN_DURATION_SECONDS} seconds...")
    time.sleep(RUN_DURATION_SECONDS)
    
    print(f"\n[{RUN_DURATION_SECONDS}s reached] Initiating graceful shutdown...")
    try:
        spark.stop() 
    except Exception:
        pass
        
    sync_all_to_hf(tag="final")