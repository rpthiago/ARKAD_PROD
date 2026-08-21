import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
from futpythontrader_client import get_daily_dataframe
from metodo_saldo_menor_strategy import evaluate_game

date_str = "2026-08-20"
df_day = get_daily_dataframe("betfair", date_str)

print(f"=== DIAGNÓSTICO DO MÉTODO SALDO MENOR HOJE ({date_str}) ===", flush=True)
print(f"[+] Total de partidas na grade Betfair de hoje: {len(df_day)}", flush=True)

if not df_day.empty:
    reasons = {}
    aprovados = []
    
    for idx, r in df_day.iterrows():
        g_dict = r.to_dict()
        res = evaluate_game(g_dict, check_betmines=False)
        decision = res.get("Decision")
        reason = res.get("Reason", "DESCONHECIDO")
        
        home = str(r.get("Home", r.get("Home_Team", "")))
        away = str(r.get("Away", r.get("Away_Team", "")))
        
        if decision == "APOSTA":
            aprovados.append(f"{home} x {away}")
        else:
            reasons[reason] = reasons.get(reason, 0) + 1
            
    print(f"\n[+] SINAIS APROVADOS HOJE NO SALDO MENOR: {len(aprovados)}", flush=True)
    for a in aprovados:
        print(f"   * {a}", flush=True)
        
    print(f"\n[-] BREAKDOWN DE MOTIVOS DE REJEIÇÃO ({len(df_day)} JOGOS):", flush=True)
    for rsn, count in sorted(reasons.items(), key=lambda x: x[1], reverse=True):
        print(f"   - {rsn}: {count} jogos", flush=True)
