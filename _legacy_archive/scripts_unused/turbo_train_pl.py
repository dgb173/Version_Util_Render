import subprocess
import sys
import time
import os

# Handicaps relevant for PL (Found in validation data: 0.5, 0.0, -0.5, etc)
# We test all standard
HANDICAPS = [
    0.0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0,
    -0.25, -0.5, -0.75, -1.0, -1.25, -1.5, -1.75, -2.0
]

LEAGUE_ID = "39"

def run_sniper():
    script_path = os.path.join("scripts", "universal_specialist.py")
    if not os.path.exists(script_path):
        print(f"❌ Error: {script_path} not found")
        return

    print(f"🔫 INICIANDO ENTRENAMIENTO FRANCOTIRADOR (LIGA {LEAGUE_ID})")
    
    for ah in HANDICAPS:
        print(f"\n🔄 Especialista AH {ah} (Sniper Mode)...")
        # We lower generations and samples because dataset is small (Only PL)
        cmd = [
            sys.executable,
            script_path,
            "--handicap", str(ah),
            "--league_id", LEAGUE_ID,
            "--min_acc", "80",    # Slightly lower because small sample
            "--min_samples", "5", # Allow rare patterns in 500 matches
            "--generations", "30" # Faster
        ]
        
        try:
            subprocess.run(cmd, check=True)
        except Exception as e:
            print(f"⚠️ Error AH {ah}: {e}")

    print("\n✅ ENTRENAMIENTO COMPLETADO.")

if __name__ == "__main__":
    run_sniper()
