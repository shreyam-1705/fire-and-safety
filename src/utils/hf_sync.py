import os
import time
import threading
import inspect
import traceback
import tarfile
from huggingface_hub import create_bucket, sync_bucket, batch_bucket_files, download_bucket_files

NAMESPACE = os.getenv("HF_NAMESPACE")
BUCKET_NAME = os.getenv("HF_BUCKET_NAME")
TOKEN = os.getenv("HF_TOKEN")
BUCKET_ID = f"{NAMESPACE}/{BUCKET_NAME}" if NAMESPACE and BUCKET_NAME else None
ARCHIVE_PATH = "/tmp/medallion_state.tar.gz"

SYNC_TARGETS = {
    # Bronze
    "bronze_table": "/tmp/bronze_table",

    # Silver
    "silver_optical_smoke": "/tmp/silver_optical_smoke",
    "silver_ror_heat": "/tmp/silver_ror_heat",
    "silver_multi_sensor": "/tmp/silver_multi_sensor",
    "silver_horn_strobe": "/tmp/silver_horn_strobe",
    "silver_manual_call_point": "/tmp/silver_manual_call_point",
    "silver_unified": "/tmp/silver_unified",
    "silver_quarantine": "/tmp/silver_quarantine",

    # Gold
    "gold_zone_kpi_5m": "/tmp/gold_zone_kpi_5m",
    "gold_optical_smoke_5m": "/tmp/gold_optical_smoke_5m",
    "gold_ror_heat_5m": "/tmp/gold_ror_heat_5m",
    "gold_multi_sensor_5m": "/tmp/gold_multi_sensor_5m",
    "gold_zone_daily": "/tmp/gold_zone_daily",
    "gold_site_daily": "/tmp/gold_site_daily",
    "gold_organization_daily": "/tmp/gold_organization_daily",

    # Checkpoints
    "checkpoints": "/tmp/checkpoints"
}

_SYNC_PARAMS = set(inspect.signature(sync_bucket).parameters.keys())
_FILTER_PATTERNS = ["*.tmp", "*.crc", ".*"]

def _sync_kwargs():
    kwargs = {}
    if "exclude" in _SYNC_PARAMS:
        kwargs["exclude"] = _FILTER_PATTERNS
    elif "ignore_patterns" in _SYNC_PARAMS:
        kwargs["ignore_patterns"] = _FILTER_PATTERNS
    return kwargs

_sync_lock = threading.Lock()

def pack_state():
    """Packs local Delta folders and checkpoints into a single tarball."""
    try:
        with tarfile.open(ARCHIVE_PATH, "w:gz") as tar:
            for name, local_path in SYNC_TARGETS.items():
                if os.path.exists(local_path):
                    tar.add(local_path, arcname=name)
        return True
    except Exception as e:
        print(f"[pack] Error creating state archive: {e}", flush=True)
        return False

def unpack_state():
    """Unpacks the downloaded tarball into /tmp."""
    if os.path.exists(ARCHIVE_PATH):
        try:
            with tarfile.open(ARCHIVE_PATH, "r:gz") as tar:
                tar.extractall(path="/tmp")
            print("[unpack] Successfully restored full pipeline state from tarball.", flush=True)
        except Exception as e:
            print(f"[unpack] Error unpacking state: {e}", flush=True)

def sync_all_to_hf(tag="periodic"):
    if not BUCKET_ID or not TOKEN:
        print("[sync] HF credentials or bucket info missing. Skipping sync.", flush=True)
        return

    with _sync_lock:
        # 1. Upload compressed tarball to Bucket
        if pack_state():
            try:
                batch_bucket_files(
                    BUCKET_ID,
                    add=[(ARCHIVE_PATH, "medallion_state.tar.gz")],
                    token=TOKEN
                )
                print(f"[sync:{tag}] Tarball uploaded to Bucket.", flush=True)
            except Exception as e:
                print(f"[sync:{tag}] Failed to upload tarball: {e}", flush=True)

        # 2. Sync individual table directories to Bucket
        for name, local_path in SYNC_TARGETS.items():
            remote_path = f"hf://buckets/{BUCKET_ID}/{name}"
            if os.path.exists(local_path):
                try:
                    sync_bucket(local_path, remote_path, token=TOKEN, **_sync_kwargs())
                except Exception:
                    print(f"[sync:{tag}] Failed syncing {name}:\n{traceback.format_exc()}", flush=True)
            else:
                print(f"[sync:{tag}] {local_path} does not exist yet, skipping", flush=True)

        print(f"[sync:{tag}] Sync cycle complete.", flush=True)

def _sync_loop():
    while True:
        time.sleep(120)
        sync_all_to_hf(tag="periodic")

def restore_from_hf():
    """Restores pipeline state on cold start."""
    if not BUCKET_ID or not TOKEN:
        print("[restore] HF_NAMESPACE, HF_BUCKET_NAME, or HF_TOKEN missing. Skipping restore.", flush=True)
        return

    try:
        download_bucket_files(
            BUCKET_ID,
            files=[("medallion_state.tar.gz", ARCHIVE_PATH)],
            token=TOKEN
        )
        unpack_state()
    except Exception:
        print("[restore] No previous tarball found (likely first run or empty bucket).", flush=True)

def ensure_bucket():
    if not BUCKET_ID or not TOKEN:
        return
    while True:
        try:
            create_bucket(BUCKET_ID, token=TOKEN, exist_ok=True)
            print(f"[sync] Bucket ready: hf://buckets/{BUCKET_ID}", flush=True)
            return
        except Exception:
            print("[sync] create_bucket failed, retrying in 30s...", flush=True)
            time.sleep(30)

def start_sync_thread():
    ensure_bucket()
    restore_from_hf()
    threading.Thread(target=_sync_loop, daemon=True).start()
    print("[sync] Background sync thread initialized.", flush=True)