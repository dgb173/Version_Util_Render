import sqlite3

conn = sqlite3.connect('database.sqlite')
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [r[0] for r in cursor.fetchall()]

for table in tables:
    try:
        cursor.execute(f"PRAGMA table_info({table})")
        cols = [c[1] for c in cursor.fetchall()]
        home_col = next((c for c in cols if 'home' in c.lower()), None)
        away_col = next((c for c in cols if 'away' in c.lower()), None)
        if home_col and away_col:
            cursor.execute(f"SELECT * FROM {table} WHERE {home_col} LIKE '%Breidablik%' OR {away_col} LIKE '%Breidablik%' LIMIT 5")
            rows = cursor.fetchall()
            if rows:
                print(f"Table {table}: {len(rows)} rows found")
                for r in rows:
                    print(" ", r[:6])
    except Exception as e:
        pass
