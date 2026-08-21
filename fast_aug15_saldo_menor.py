import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
from futpythontrader_client import get_daily_dataframe
from metodo_saldo_menor_strategy import check_entry_conditions, normalize_live_data

df = get_daily_dataframe("betfair", "2026-08-15")
print(f"=== TESTE ULTRARRÁPIDO SALDO MENOR EM 15/08/2026 ({len(df)} jogos) ===", flush=True)

aprovados = []
motivos = {}

for idx, r in df.iterrows():
    norm = normalize_live_data(r.to_dict())
    ok, rsn = check_entry_conditions(norm, check_betmines=False)
    home = str(r.get("Home", r.get("Home_Team", "")))
    away = str(r.get("Away", r.get("Away_Team", "")))
    if ok:
        aprovados.append(f"{home} x {away}")
    else:
        motivos[rsn] = motivos.get(rsn, 0) + 1

print(f"\n[+] SINAIS APROVADOS NO SALDO MENOR EM 15/08: {len(aprovados)} JOGOS", flush=True)
for a in aprovados:
    print(f"   * {a}", flush=True)

print(f"\n[-] RESUMO DOS MOTIVOS DE REJEIÇÃO:", flush=True)
for rsn, count in sorted(motivos.items(), key=lambda x: x[1], reverse=True):
    print(f"   - {rsn}: {count} jogos", flush=True)
