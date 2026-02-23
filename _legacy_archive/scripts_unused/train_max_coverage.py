import subprocess
import sys
import time
import os

# ALL handicaps
HANDICAPS = [
    0.0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0,
    -0.25, -0.5, -0.75, -1.0, -1.25, -1.5, -1.75, -2.0
]

# MORE PERMISSIVE PARAMS for maximum coverage
MIN_ACCURACY = 75   # Lower threshold = more rules
MIN_SAMPLES = 5     # Fewer samples = more rules  
GENERATIONS = 80    # More generations = better search

def run_training():
    script_path = os.path.join("scripts", "universal_specialist.py")
    if not os.path.exists(script_path):
        print(f"❌ Error: {script_path} not found")
        return

    print("=" * 60)
    print("🚀 ENTRENAMIENTO MÁXIMA COBERTURA")
    print(f"🎯 Target Accuracy: {MIN_ACCURACY}%")
    print(f"🎯 Min Samples: {MIN_SAMPLES}")
    print(f"📊 Handicaps: {HANDICAPS}")
    print("=" * 60)
    
    total_rules = 0
    start = time.time()
    
    for ah in HANDICAPS:
        print(f"\n🔄 Entrenando AH {ah} ...")
        t0 = time.time()
        
        cmd = [
            sys.executable,
            script_path,
            "--handicap", str(ah),
            "--min_acc", str(MIN_ACCURACY),
            "--generations", str(GENERATIONS),
            "--min_samples", str(MIN_SAMPLES)
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            # Count rules from output
            for line in result.stdout.split('\n'):
                if 'Saved' in line and 'rules' in line:
                    try:
                        n = int(line.split('Saved')[1].split('rules')[0].strip())
                        total_rules += n
                        print(f"   ✅ {n} reglas guardadas")
                    except:
                        pass
                print(line)
            elapsed = time.time() - t0
            print(f"   ⏱️ {elapsed:.1f}s")
            
        except Exception as e:
            print(f"❌ Error AH {ah}: {e}")

    print("\n" + "=" * 60)
    print(f"✅ ENTRENAMIENTO COMPLETADO")
    print(f"📊 TOTAL REGLAS GENERADAS: {total_rules}")
    print(f"⏱️ Tiempo total: {(time.time()-start)/60:.1f} minutos")
    print("=" * 60)

if __name__ == "__main__":
    run_training()
