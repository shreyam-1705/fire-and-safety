import os
import time
import threading
import subprocess
from huggingface_hub import HfApi

# Syncs the local directories up to Hugging Face
SYNC_TARGETS = {
    "bronze_table": "/tmp/bronze_table",
    "silver_device_optical_smoke": "/tmp/silver_device_optical_smoke",
    "silver_device_ror_heat": "/tmp/silver_device_ror_heat",
    "silver_device_multi_sensor": "/tmp/silver_device_multi_sensor",
    "silver_device_horn_strobe": "/tmp/silver_device_horn_strobe",
    "silver_device_manual_call_point": "/tmp/silver_device_manual_call_point",
    "silver_quarantine": "/tmp/silver_quarantine",
    "gold_device_optical_smoke_5m": "/tmp/gold_device_optical_smoke_5m",
    "gold_device_ror_heat_5m": "/tmp/gold_device_ror_heat_5m",
    "gold_device_multi_sensor_5m": "/tmp/gold_device_multi_sensor_5m",
    "gold_device_horn_strobe_5m": "/tmp/gold_device_horn_strobe_5m",
    "gold_device_manual_call_point_5m": "/tmp/gold_device_manual_call_point_5m",
    "gold_zone_snapshot_5m": "/tmp/gold_zone_snapshot_5m",
    "gold_site_snapshot_5m": "/tmp/gold_site_snapshot_5m",
    "gold_organization_snapshot_5m": "/tmp/gold_organization_snapshot_5m",
    "checkpoints": "/tmp/checkpoints",
    "cache": "/tmp/cache"
}

HF_NAMESPACE = os.getenv("HF_NAMESPACE")
HF_BUCKET_NAME = os.getenv("HF_BUCKET_NAME")
HF_TOKEN = os.getenv("HF_TOKEN")
REPO_ID = f"{HF_NAMESPACE}/{HF_BUCKET_NAME}"

sync_lock = threading.Lock()
api = HfApi()

def ensure_bucket():
    if not (HF_NAMESPACE and HF_BUCKET_NAME and HF_TOKEN):
        return
    try:
        api.create_repo(repo_id=REPO_ID, repo_type="dataset", token=HF_TOKEN, exist_ok=True, private=True)
    except Exception as e:
        print(f"[HF_SYNC] Error ensuring bucket: {e}")

def restore_from_hf():
    """Downloads the previous run's compressed state and extracts it to /tmp/ to restore checkpoints."""
    if not (HF_NAMESPACE and HF_BUCKET_NAME and HF_TOKEN):
        return
    archive_path = "/tmp/medallion_state.tar.gz"
    try:
        if api.file_exists(repo_id=REPO_ID, filename="medallion_state.tar.gz", repo_type="dataset", token=HF_TOKEN):
            api.hf_hub_download(repo_id=REPO_ID, filename="medallion_state.tar.gz", repo_type="dataset", token=HF_TOKEN, local_dir="/tmp")
            subprocess.run(["tar", "-xzf", archive_path, "-C", "/"], check=True)
            print("[HF_SYNC] Successfully restored previous state and checkpoints from Hugging Face.")
    except Exception as e:
        print(f"[HF_SYNC] No previous state found or restore failed. Starting fresh. ({e})")

def sync_all_to_hf(tag="periodic"):
    """Compresses the /tmp/ targets and uploads them to HF."""
    if not (HF_NAMESPACE and HF_BUCKET_NAME and HF_TOKEN):
        return
        
    with sync_lock:
        archive_path = "/tmp/medallion_state.tar.gz"
        try:
            # Only tar the paths that actually exist to avoid errors on the first run
            existing_paths = [path for path in SYNC_TARGETS.values() if os.path.exists(path)]
            if not existing_paths:
                return

            subprocess.run(["tar", "-czf", archive_path] + existing_paths, check=True)
            api.upload_file(
                path_or_fileobj=archive_path,
                path_in_repo="medallion_state.tar.gz",
                repo_id=REPO_ID,
                repo_type="dataset",
                token=HF_TOKEN,
                commit_message=f"Pipeline {tag} state sync"
            )
            print(f"[HF_SYNC] [{tag}] State successfully persisted to Hugging Face.", flush=True)
        except Exception as e:
            print(f"[HF_SYNC] [{tag}] Sync failed: {e}", flush=True)

def _sync_loop():
    while True:
        time.sleep(60) # Syncs every 60 seconds during the run to protect against runner crash
        sync_all_to_hf(tag="periodic")

def start_sync_thread():
    ensure_bucket()
    restore_from_hf()
    threading.Thread(target=_sync_loop, daemon=True).start()