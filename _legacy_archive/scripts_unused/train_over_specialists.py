import subprocess
import sys
import time
import os

# List of OU Lines to train
LINES = [
    2.0, 2.25, 2.5, 2.75, 3.0, 3.25, 3.5, 3.75
]

# Training configuration
MIN_ACCURACY = 85  # STRICTER Accuracy
MIN_SAMPLES = 8    # LOWER samples for "Sniper" mode
GENERATIONS = 50   # Standard generations for niche search

def run_training():
    print("="*60)
    print(f"🚀 INICIANDO ENTRENAMIENTO ESPECIALISTAS O/U")
    print(f"🎯 Target Accuracy: {MIN_ACCURACY}%")
    print(f"🎯 Min Samples: {MIN_SAMPLES}")
    print(f"📊 Lines: {LINES}")
    print("="*60)

    total_start = time.time()
    
    script_path = os.path.join("scripts", "universal_specialist.py")
    if not os.path.exists(script_path):
        print(f"❌ Error: No se encuentra {script_path}")
        return

    for line in LINES:
        print(f"\n🔄 Entrenando Especialista O/U {line} ...")
        start_time = time.time()
        
        # Construct command
        # python scripts/universal_specialist.py --handicap 2.5 --type OU --min_acc 100 ...
        cmd = [
            sys.executable,
            script_path,
            "--handicap", str(line),
            "--type", "OU",
            "--min_acc", str(MIN_ACCURACY),
            "--generations", str(GENERATIONS),
            "--min_samples", str(MIN_SAMPLES)
        ]
        
        try:
            result = subprocess.run(cmd, check=True)
            elapsed = time.time() - start_time
            print(f"✅ Completado O/U {line} en {elapsed:.1f}s")
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Falló entrenamiento para O/U {line}: {e}")
        except KeyboardInterrupt:
            print("\n⚠️ Entrenamiento interrumpido por usuario.")
            break
            
    total_elapsed = time.time() - total_start
    print("="*60)
    print(f"✅ ENTRENAMIENTO O/U FINALIZADO")
    print(f"⏱️ Tiempo total: {total_elapsed/60:.1f} minutos")
    print("="*60)

if __name__ == "__main__":
    os.makedirs("backtest_results", exist_ok=True)
    run_training()
