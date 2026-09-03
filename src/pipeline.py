import os
import signal
import time
import threading

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from layers.bronze import run_bronze
from layers.gold import run_gold
from layers.silver import run_silver
from utils.hf_sync import (
    restore_from_hf,
    start_sync_thread,
    sync_all_to_hf,
)
from utils.maintenance import start_maintenance_thread


RUN_DURATION_SECONDS = int(
    os.getenv("RUN_DURATION_SECONDS", "1800")
)

spark = None
shutdown_requested = False


def handle_shutdown(signum=None, frame=None):
    """Request a graceful pipeline shutdown."""

    global shutdown_requested

    print(
        f"[PIPELINE] Shutdown signal received: {signum}",
        flush=True,
    )

    shutdown_requested = True


def launch_stream(
    layer_function,
    pool_name,
    spark_session,
):
    """
    Run one streaming layer in its own daemon thread.

    Bronze, Silver and Gold all use the same SparkSession,
    but each layer runs independently.
    """

    try:
        spark_session.sparkContext.setLocalProperty(
            "spark.scheduler.pool",
            pool_name,
        )

        print(
            f"[PIPELINE] Starting {pool_name} stream...",
            flush=True,
        )

        layer_function(spark_session)

    except Exception as exc:
        print(
            f"[{pool_name}] Stream stopped or failed: {exc}",
            flush=True,
        )


def create_delta_table(
    spark_session,
    path,
    schema,
    partition_by=None,
):
    """
    Create an empty Delta table with the required schema
    if the table does not already exist.
    """

    partition_clause = (
        f"PARTITIONED BY ({partition_by})"
        if partition_by
        else ""
    )

    spark_session.sql(
        f"""
        CREATE TABLE IF NOT EXISTS delta.`{path}` (
            {schema}
        )
        USING DELTA
        {partition_clause}
        """
    )


def initialize_tables(spark_session):
    """
    Pre-create the Bronze and Silver Delta tables.

    Gold tables are created by the Gold layer because their
    schemas are defined by their aggregation logic.
    """

    print(
        "[PIPELINE] Pre-initializing Delta tables...",
        flush=True,
    )

    # ------------------------------------------------------------------
    # BRONZE
    # ------------------------------------------------------------------

    create_delta_table(
        spark_session,
        "/tmp/bronze_table",
        """
        event_id STRING,
        timestamp TIMESTAMP,
        organization_id STRING,
        site_id STRING,
        panel_id STRING,
        zone_id STRING,
        device_id STRING,
        device_type STRING,
        current_state STRING,
        is_test_mode BOOLEAN,
        fault_code STRING,
        battery_runtime_hours DOUBLE,
        telemetry_payload STRING,
        ingestion_timestamp TIMESTAMP,
        year INT,
        month INT,
        day INT
        """,
        "year, month, day",
    )

    # ------------------------------------------------------------------
    # SILVER COMMON SCHEMA
    # ------------------------------------------------------------------

    silver_common = """
        event_id STRING,
        timestamp TIMESTAMP,
        organization_id STRING,
        site_id STRING,
        panel_id STRING,
        zone_id STRING,
        device_id STRING,
        device_type STRING,
        current_state STRING,
        is_test_mode BOOLEAN,
        fault_code STRING,
        battery_runtime_hours DOUBLE,
        loop_voltage_dc DOUBLE,
        self_verify_passed BOOLEAN,
        ingestion_timestamp TIMESTAMP,
        year INT,
        month INT,
        day INT
    """

    # ------------------------------------------------------------------
    # SILVER DEVICE TABLES
    # ------------------------------------------------------------------

    silver_tables = [
        (
            "/tmp/silver_optical_smoke",
            silver_common
            + """
            ,
            smoke_obscuration_pct DOUBLE,
            chamber_dirt_pct DOUBLE
            """,
        ),
        (
            "/tmp/silver_ror_heat",
            silver_common
            + """
            ,
            temperature_celsius DOUBLE,
            rate_of_rise_c_per_min DOUBLE
            """,
        ),
        (
            "/tmp/silver_multi_sensor",
            silver_common
            + """
            ,
            smoke_obscuration_pct DOUBLE,
            temperature_celsius DOUBLE,
            co_ppm INT,
            co_cell_health_pct DOUBLE
            """,
        ),
        (
            "/tmp/silver_horn_strobe",
            silver_common
            + """
            ,
            is_sounding BOOLEAN,
            strobe_active BOOLEAN,
            self_test_decibel_level DOUBLE,
            sync_offset_ms DOUBLE
            """,
        ),
        (
            "/tmp/silver_manual_call_point",
            silver_common
            + """
            ,
            is_activated BOOLEAN,
            tamper_switch BOOLEAN
            """,
        ),
        (
            "/tmp/silver_unified",
            silver_common,
        ),
        (
            "/tmp/silver_quarantine",
            """
            event_id STRING,
            timestamp TIMESTAMP,
            organization_id STRING,
            site_id STRING,
            panel_id STRING,
            zone_id STRING,
            device_id STRING,
            device_type STRING,
            current_state STRING,
            fault_code STRING,
            failed_rules STRING,
            telemetry_payload STRING,
            quarantine_timestamp TIMESTAMP,
            year INT,
            month INT,
            day INT
            """,
        ),
    ]

    for path, schema in silver_tables:
        create_delta_table(
            spark_session,
            path,
            schema,
            "year, month, day",
        )

    print(
        "[PIPELINE] Delta table initialization completed.",
        flush=True,
    )


def create_spark_session():
    """
    Create the shared SparkSession used by Bronze, Silver
    and Gold streaming layers.
    """

    kafka_package = (
        "org.apache.spark:spark-sql-kafka-0-10_2.12:"
        f"{pyspark.__version__}"
    )

    builder = (
        SparkSession.builder
        .appName(
            "FireSafetyDataEngineeringPipeline"
        )
        .master("local[*]")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    kafka_package,
                    "io.delta:delta-spark_2.12:3.1.0",
                ]
            ),
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.scheduler.mode",
            "FAIR",
        )
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .config(
            "spark.databricks.delta.retentionDurationCheck.enabled",
            "false",
        )
        .config(
            "spark.sql.shuffle.partitions",
            os.getenv(
                "SPARK_SHUFFLE_PARTITIONS",
                "8",
            ),
        )
    )

    spark_session = (
        configure_spark_with_delta_pip(
            builder
        )
        .getOrCreate()
    )

    spark_session.sparkContext.setLogLevel(
        os.getenv(
            "SPARK_LOG_LEVEL",
            "WARN",
        )
    )

    print(
        "[PIPELINE] SparkSession created.",
        flush=True,
    )

    return spark_session


def main():
    """
    Main orchestration function.

    Overall execution:

        Restore HF state
              ↓
        Create Spark
              ↓
        Initialize Delta tables
              ↓
        Start HF sync worker
              ↓
        Start Bronze
              ↓
        Start Silver
              ↓
        Start Gold
              ↓
        Start Maintenance
              ↓
        Keep pipeline alive
              ↓
        Stop streaming
              ↓
        Final HF sync
              ↓
        Stop Spark
    """

    global spark

    print("=" * 80, flush=True)
    print(
        "FIRE & SAFETY DATA ENGINEERING PIPELINE",
        flush=True,
    )
    print("=" * 80, flush=True)

    # ------------------------------------------------------------------
    # 1. RESTORE PREVIOUS STATE
    # ------------------------------------------------------------------

    try:
        restore_from_hf()

    except Exception as exc:
        print(
            f"[PIPELINE] HF restore failed: {exc}",
            flush=True,
        )

    # ------------------------------------------------------------------
    # 2. CREATE SPARK
    # ------------------------------------------------------------------

    spark = create_spark_session()

    # ------------------------------------------------------------------
    # 3. INITIALIZE DELTA TABLES
    # ------------------------------------------------------------------

    initialize_tables(spark)

    # ------------------------------------------------------------------
    # 4. START PERIODIC HF SYNCHRONIZATION
    #
    # The worker periodically synchronizes:
    # - Delta tables
    # - streaming checkpoints
    #
    # The exact interval is controlled by HF_SYNC_INTERVAL_SECONDS.
    # Default = 5 minutes.
    # ------------------------------------------------------------------

    try:
        start_sync_thread()

        print(
            "[PIPELINE] HF synchronization worker started.",
            flush=True,
        )

    except Exception as exc:
        print(
            "[PIPELINE] HF synchronization worker "
            f"failed to start: {exc}",
            flush=True,
        )

    # ------------------------------------------------------------------
    # 5. START BRONZE, SILVER AND GOLD CONCURRENTLY
    # ------------------------------------------------------------------

    streams = [
        (
            run_bronze,
            "bronze",
        ),
        (
            run_silver,
            "silver",
        ),
        (
            run_gold,
            "gold",
        ),
    ]

    stream_threads = []

    for layer_function, pool_name in streams:

        thread = threading.Thread(
            target=launch_stream,
            args=(
                layer_function,
                pool_name,
                spark,
            ),
            name=f"{pool_name}-stream",
            daemon=True,
        )

        thread.start()

        stream_threads.append(thread)

    # ------------------------------------------------------------------
    # 6. START MAINTENANCE WORKER
    #
    # Maintenance runs every 5 minutes.
    #
    # It performs:
    # - one-month rolling daily KPI maintenance
    # - Delta OPTIMIZE
    # - Delta VACUUM
    #
    # No UI cache is generated.
    # ------------------------------------------------------------------

    try:
        start_maintenance_thread(spark)

        print(
            "[PIPELINE] Maintenance worker started.",
            flush=True,
        )

    except Exception as exc:
        print(
            "[PIPELINE] Maintenance worker "
            f"failed to start: {exc}",
            flush=True,
        )

    # ------------------------------------------------------------------
    # 7. KEEP THE DAILY RUN ALIVE
    # ------------------------------------------------------------------

    print(
        f"[PIPELINE] Pipeline running for "
        f"{RUN_DURATION_SECONDS} seconds.",
        flush=True,
    )

    start_time = time.time()

    try:

        while not shutdown_requested:

            elapsed = time.time() - start_time

            if elapsed >= RUN_DURATION_SECONDS:

                print(
                    "[PIPELINE] Configured run duration reached.",
                    flush=True,
                )

                break

            time.sleep(5)

    except KeyboardInterrupt:

        print(
            "[PIPELINE] Keyboard interrupt received.",
            flush=True,
        )

    finally:

        # --------------------------------------------------------------
        # 8. STOP STREAMING
        # --------------------------------------------------------------

        print(
            "[PIPELINE] Stopping active Spark streams...",
            flush=True,
        )

        try:

            active_queries = list(
                spark.streams.active
            )

            for query in active_queries:

                print(
                    f"[PIPELINE] Stopping query: "
                    f"{query.name}",
                    flush=True,
                )

                query.stop()

            for query in active_queries:

                try:
                    query.awaitTermination(10)

                except Exception:
                    pass

        except Exception as exc:

            print(
                "[PIPELINE] Error while stopping streams: "
                f"{exc}",
                flush=True,
            )

        # --------------------------------------------------------------
        # 9. GIVE STREAMING WRITERS TIME TO FLUSH
        # --------------------------------------------------------------

        time.sleep(5)

        # --------------------------------------------------------------
        # 10. FINAL HF SYNCHRONIZATION
        #
        # This is important because data/checkpoints may have changed
        # after the last 5-minute periodic synchronization.
        # --------------------------------------------------------------

        try:

            print(
                "[PIPELINE] Performing final HF synchronization...",
                flush=True,
            )

            sync_all_to_hf(
                tag="shutdown"
            )

            print(
                "[PIPELINE] Final HF synchronization completed.",
                flush=True,
            )

        except Exception as exc:

            print(
                "[PIPELINE] Final HF synchronization failed: "
                f"{exc}",
                flush=True,
            )

        # --------------------------------------------------------------
        # 11. STOP SPARK
        # --------------------------------------------------------------

        try:

            spark.stop()

            print(
                "[PIPELINE] Spark stopped.",
                flush=True,
            )

        except Exception as exc:

            print(
                "[PIPELINE] Spark shutdown failed: "
                f"{exc}",
                flush=True,
            )

    print("=" * 80, flush=True)
    print(
        "FIRE & SAFETY PIPELINE FINISHED",
        flush=True,
    )
    print("=" * 80, flush=True)


signal.signal(
    signal.SIGTERM,
    handle_shutdown,
)

signal.signal(
    signal.SIGINT,
    handle_shutdown,
)


if __name__ == "__main__":
    main()