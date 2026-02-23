import subprocess
import sys
import time
import os

# BUCKETS (not individual values)
BUCKETS = [0.0, 0.5, 1.5, 2.0, -0.5, -1.5, -2.0]

MIN_ACCURACY = 85
MIN_SAMPLES = 8
GENERATIONS = 50

def run_training():
    script_path = os.path.join("scripts", "universal_specialist.py")
    
    print("=" * 60)
    print("🚀 ENTRENAMIENTO POR BUCKETS")
    print(f"📊 Buckets: {BUCKETS}")
    print("=" * 60)
    
    for bucket in BUCKETS:
        print(f"\n🔄 Entrenando Bucket {bucket} ...")
        
        cmd = [
            sys.executable,
            script_path,
            "--handicap", str(bucket),
            "--min_acc", str(MIN_ACCURACY),
            "--generations", str(GENERATIONS),
            "--min_samples", str(MIN_SAMPLES)
        ]
        
        try:
            subprocess.run(cmd, check=True)
        except Exception as e:
            print(f"❌ Error Bucket {bucket}: {e}")

    print("\n✅ ENTRENAMIENTO POR BUCKETS COMPLETADO")

if __name__ == "__main__":
    run_training()
