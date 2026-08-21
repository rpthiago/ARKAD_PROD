import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np
from futpythontrader_client import get_daily_dataframe
from metodo_saldo_menor_strategy import evaluate_game, normalize_live_data, identify_zebra_and_handicap, check_entry_conditions

df = get_daily_dataframe("betfair", "2026-08-15")

print(f"=== TESTANDO SALDO MENOR COM FALLBACK EH+3 PARA 15/08/2026 ({len(df)} jogos) ===", flush=True)

sinais_antes = 0
sinais_depois = 0

reasons_after = {}

for idx, r in df.iterrows():
    g = r.to_dict()
    res_before = evaluate_game(g, check_betmines=False)
    if res_before.get("Decision") == "APOSTA":
        sinais_antes += 1
        
    # Aplicar fallback para EH+3 se missing
    fav_odd = min(float(r.get("Odd_H_FT_Back") or r.get("Odd_H_FT") or r.get("Odd_H") or 99),
                  float(r.get("Odd_A_FT_Back") or r.get("Odd_A_FT") or r.get("Odd_A") or 99))
    if pd.isna(g.get("EH_H_pos_3")) and pd.isna(g.get("EH_A_pos_3")):
        # Estima EH+3 odd quantitativa se o jogo estiver na faixa competitiva
        if 2.00 <= fav_odd <= 5.00:
            est_eh = round(min(1.45, max(1.08, 1.05 + (fav_odd - 2.00) * 0.10)), 2)
            g["EH_H_pos_3"] = est_eh
            g["EH_A_pos_3"] = est_eh
            
    res_after = evaluate_game(g, check_betmines=False)
    if res_after.get("Decision") == "APOSTA":
        sinais_depois += 1
    else:
        rsn = res_after.get("Reason")
        reasons_after[rsn] = reasons_after.get(rsn, 0) + 1

print(f"[+] Sinais ANTES da correção do EH+3: {sinais_antes}", flush=True)
print(f"[+] Sinais DEPOIS da correção do EH+3: {sinais_depois}", flush=True)
print("\nBreakdown de motivos depois:", reasons_after, flush=True)
