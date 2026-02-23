import subprocess
import sys
import time
import os

# List of handicaps to train
# Covers the full spectrum of main Asian Handicaps
HANDICAPS = [
    0.0,
    0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0,
    -0.25, -0.5, -0.75, -1.0, -1.25, -1.5, -1.75, -2.0
]

# Training configuration
MIN_ACCURACY = 85  # STRICTER Accuracy to compensate for low samples
MIN_SAMPLES = 8    # LOWER samples to find "Sniper" patterns (Rare but lethal)
GENERATIONS = 50   # 50 Gens is enough for small samples
POPULATION = 300    # Population size (passed to script if supported, or default)

def run_training():
    print("="*60)
    print(f"🚀 INICIANDO ENTRENAMIENTO MASIVO DE ESPECIALISTAS")
    print(f"🎯 Target Accuracy: {MIN_ACCURACY}%")
    print(f"🎯 Min Samples: {MIN_SAMPLES}")
    print(f"📊 Handicaps: {HANDICAPS}")
    print("="*60)

    total_start = time.time()
    
    script_path = os.path.join("scripts", "universal_specialist.py")
    if not os.path.exists(script_path):
        print(f"❌ Error: No se encuentra {script_path}")
        return

    for ah in HANDICAPS:
        print(f"\n🔄 Entrenando Especialista AH {ah} ...")
        start_time = time.time()
        
        # Construct command
        # python scripts/universal_specialist.py --target_ah 0.5 --min_acc 100 --generations 50 --min_samples 30
        cmd = [
            sys.executable,
            script_path,
            "--handicap", str(ah),
            "--min_acc", str(MIN_ACCURACY),
            "--generations", str(GENERATIONS),
            "--min_samples", str(MIN_SAMPLES)
        ]
        
        try:
            # Run subprocess and capture output real-time (optional, here we let it print to stdout)
            result = subprocess.run(cmd, check=True)
            elapsed = time.time() - start_time
            print(f"✅ Completado AH {ah} en {elapsed:.1f}s")
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Falló entrenamiento para AH {ah}: {e}")
        except KeyboardInterrupt:
            print("\n⚠️ Entrenamiento interrumpido por usuario.")
            break
            
    total_elapsed = time.time() - total_start
    print("="*60)
    print(f"✅ ENTRENAMIENTO MASIVO FINALIZADO")
    print(f"⏱️ Tiempo total: {total_elapsed/60:.1f} minutos")
    print("="*60)

if __name__ == "__main__":
    # Ensure backtest_results dir exists
    os.makedirs("backtest_results", exist_ok=True)
    run_training()
