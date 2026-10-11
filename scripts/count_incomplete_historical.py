import sqlite3
import json

conn = sqlite3.connect('data/app_data.db')
cur = conn.cursor()

# Contar cuántos partidos históricos no tienen datos de H2H (prev_home, h2h_col3, etc.)
cur.execute('''
    SELECT 
        bucket,
        COUNT(*) as total,
        SUM(CASE WHEN payload_json NOT LIKE '%last_home_match%' THEN 1 ELSE 0 END) as sin_h2h_estudio
    FROM matches
    WHERE state = 'historical'
    GROUP BY bucket
''')
print("PARTIDOS HISTÓRICOS SIN ESTUDIO H2H:")
for r in cur.fetchall():
    print(f"  Bucket: {r[0]} | Total: {r[1]} | Sin H2H completo: {r[2]}")

conn.close()
