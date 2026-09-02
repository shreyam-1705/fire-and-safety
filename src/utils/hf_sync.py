import os
import time
import threading
import subprocess
from huggingface_hub import HfApi, hf_hub_download

HF_TOKEN = os.getenv("HF_TOKEN")
REPO_ID = os.getenv("HF_REPO_ID") # e.g., "shreyam1705/fire-safety-state"
STATE_FILE = "medallion_state.tar.gz"
LOCAL_TAR = f"/tmp/{STATE_FILE}"

api = HfApi()

def restore_from_hf():
    print(f"[HF_SYNC] Attempting to restore {STATE_FILE} from HF...", flush=True)
    if not HF_TOKEN or not REPO_ID:
        print("[HF_SYNC] HF_TOKEN or HF_REPO_ID missing. Skipping restore.", flush=True)
        return
        
    try:
        download_path = hf_hub_download(repo_id=REPO_ID, filename=STATE_FILE, token=HF_TOKEN, repo_type="dataset")
        # Extract the tar file into the root (which restores /tmp/ folders)
        subprocess.run(["tar", "-xzf", download_path, "-C", "/"], check=True)
        print("[HF_SYNC] State restored successfully.", flush=True)
    except Exception as e:
        print(f"[HF_SYNC] No existing state found or restore failed: {e}. Starting fresh.", flush=True)

def sync_all_to_hf(tag="periodic"):
    if not HF_TOKEN or not REPO_ID:
        return
        
    try:
        # Compress all Delta tables and checkpoints. We use 2>/dev/null to ignore errors 
        # if some folders (like silver or gold) haven't been created yet by Spark.
        subprocess.run(
            "tar -czf /tmp/medallion_state.tar.gz -C / tmp/bronze_table tmp/silver_* tmp/gold_* tmp/checkpoints 2>/dev/null", 
            shell=True
        )
        
        if os.path.exists(LOCAL_TAR):
            api.upload_file(
                path_or_fileobj=LOCAL_TAR,
                path_in_repo=STATE_FILE,
                repo_id=REPO_ID,
                repo_type="dataset",
                token=HF_TOKEN,
                commit_message=f"Auto-sync: {tag}"
            )
            if tag != "periodic":
                print(f"[HF_SYNC] [{tag}] State successfully persisted to Hugging Face.", flush=True)
    except Exception as e:
        print(f"[HF_SYNC] [{tag}] Sync failed: {e}", flush=True)

def _sync_loop():
    # Wait 60 seconds before the first sync to allow Spark time to generate initial tables
    time.sleep(60)
    while True:
        sync_all_to_hf(tag="periodic")
        time.sleep(120)  # Sync every 2 minutes

def start_sync_thread():
    restore_from_hf()
    t = threading.Thread(target=_sync_loop, daemon=True)
    t.start()