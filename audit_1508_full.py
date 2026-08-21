import pandas as pd, numpy as np, os, sys
import lay_2x0_rf_v2_strategy as strat

print("=== AUDITORIA COMPLETA DOS JOGOS DO DIA 15/08/2026 PARA LAY 2x0 ===")

# Base lean ultra rápida
cols = ["Date", "League", "Home", "Away", "Goals_H_FT", "Goals_A_FT", "Odd_CS_2x0_Lay", "Odd_CS_2x0",
        "Odd_H_FT", "Odd_D_FT", "Odd_A_FT", "xGOT_H_FT", "xGOT_A_FT", "xGOT_Faced_H_FT", "xGOT_Faced_A_FT",
        "Goals_Prevented_H_FT", "Goals_Prevented_A_FT", "Big_Chances_H_FT", "Big_Chances_A_FT",
        "Shots_On_Target_H_FT", "Shots_On_Target_A_FT", "Possession_H_FT", "Possession_A_FT"]

def col_filter(c):
    return c in cols

hist_df = pd.read_csv("b365_base_lean.csv", usecols=col_filter, low_memory=False)
date_str_series = hist_df["Date"].astype(str)

# Jogos do dia 15/08/2026
day_games = hist_df[date_str_series == "2026-08-15"].copy()

print(f"[+] Total de jogos encontrados no dia 15/08/2026: {len(day_games)}")

if day_games.empty:
    print("❌ Nenhum jogo na base com data 2026-08-15.")
    sys.exit(0)

# Converter datas do historico filtrado por time
teams = set(day_games["Home"].dropna()).union(set(day_games["Away"].dropna()))
hist_sub = hist_df[(date_str_series < "2026-08-15") & (hist_df["Home"].isin(teams) | hist_df["Away"].isin(teams))].copy()
hist_sub["Date"] = pd.to_datetime(hist_sub["Date"], errors="coerce")
hist_sub = hist_sub.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)

# Rodar a avaliacao da estrategia Lay 2x0
payload = day_games.to_dict("records")
evaluated = strat.predict_and_evaluate_live(payload, hist_sub)

print(f"\n[+] Total de jogos avaliados pela estrategia: {len(evaluated)}")

aprovados = [g for g in evaluated if g.get("Decision") == "APOSTA"]
skips = [g for g in evaluated if g.get("Decision") == "SKIP"]

print(f"\n=======================================================")
print(f"✅ APROVADOS PARA APOSTA ({len(aprovados)}):")
for g in aprovados:
    print(f"   👉 [{g.get('League')}] {g.get('Home')} x {g.get('Away')} | Odd: {g.get('Odd_CS_2x0_Lay')} | Prob: {g.get('Prob_ML'):.3f} | EV: {g.get('ev_lay'):+.3f}")

print(f"\n⛔ REJEITADOS / SKIPS ({len(skips)} jogos):")
reasons_count = {}
for idx, g in enumerate(skips, 1):
    r = g.get("Reason", "DESCONHECIDO")
    reasons_count[r] = reasons_count.get(r, 0) + 1
    odd_val = g.get('Odd_CS_2x0_Lay') or g.get('Odd_CS_2x0') or 0.0
    odd_str = f"{odd_val:.2f}" if isinstance(odd_val, (int, float)) and not pd.isna(odd_val) else "N/A"
    prob_str = f"{g.get('Prob_ML'):.3f}" if isinstance(g.get('Prob_ML'), (int, float)) else "N/A"
    ev_str = f"{g.get('ev_lay'):+.3f}" if isinstance(g.get('ev_lay'), (int, float)) else "N/A"
    print(f"  {idx:2d}. [{r:<26}] {g.get('League'):<18} | {g.get('Home'):<15} x {g.get('Away'):<15} | Odd: {odd_str:<5} | Prob: {prob_str:<5} | EV: {ev_str}")

print(f"\n📊 RESUMO DOS MOTIVOS DE DESCARTE NO DIA 15/08/2026:")
for r, count in sorted(reasons_count.items(), key=lambda x: x[1], reverse=True):
    print(f"   • {r:<32}: {count} jogos")

print("=======================================================")
