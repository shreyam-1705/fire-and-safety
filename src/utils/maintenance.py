import glob
from delta.tables import DeltaTable

def run_maintenance(spark):
    print("[Maintenance] Starting Delta table optimization and vacuum...", flush=True)
    
    # Locate all Delta table directories in /tmp
    tables = glob.glob("/tmp/bronze_*") + glob.glob("/tmp/silver_*") + glob.glob("/tmp/gold_*")
    
    for path in tables:
        try:
            dt = DeltaTable.forPath(spark, path)
            
            # 1. OPTIMIZE: Compacts small streaming micro-batch files into larger files
            dt.optimize().executeCompaction()
            print(f"[Maintenance] Optimized (compacted) {path}", flush=True)
            
            # 2. VACUUM: Removes historical files no longer needed (0 hours retention for lean POC storage)
            dt.vacuum(0) 
            print(f"[Maintenance] Vacuumed {path}", flush=True)
            
        except Exception as e:
            # Ignore if table is empty, doesn't exist yet, or hasn't been fully initialized
            pass
            
    print("[Maintenance] Maintenance complete.", flush=True)