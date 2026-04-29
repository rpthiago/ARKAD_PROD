import sqlite3
import os

db_path = 'Arquivados_Apostas_Diarias/Relatorios/Producao_Ciclo/monitoring_kpis_prod_v1.db'
if not os.path.exists(db_path):
    print(f"DB not found at {db_path}")
else:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cur.fetchall()
    print(f"Tables: {tables}")
    for table in tables:
        tname = table[0]
        cur.execute(f"SELECT COUNT(*) FROM {tname}")
        count = cur.fetchone()[0]
        print(f"Table {tname}: {count} rows")
        if tname == 'ops':
            cur.execute(f"SELECT * FROM {tname} ORDER BY ts DESC LIMIT 5")
            print(f"Latest ops: {cur.fetchall()}")
