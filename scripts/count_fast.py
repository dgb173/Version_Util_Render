import sqlite3

conn = sqlite3.connect('data/app_data.db')
cur = conn.cursor()

cur.execute('''
    SELECT 
        bucket,
        COUNT(*) as total,
        SUM(CASE WHEN explorer_json LIKE '%"prev_home": null%' OR explorer_json LIKE '%"prev_home":null%' THEN 1 ELSE 0 END) as sin_prev_home
    FROM matches
    WHERE state = 'historical'
    GROUP BY bucket
''')
print("ESTADÍSTICAS FAST:")
for r in cur.fetchall():
    print(f"  Bucket: {r[0]} | Total: {r[1]} | Sin Prev Home: {r[2]}")

conn.close()
