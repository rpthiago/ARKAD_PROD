import os, sys, pandas as pd, numpy as np
import lay_2x0_rf_v2_strategy as strat

date_str = "2026-08-15"
print(f"=== AUDITORIA COMPLETA LAY 2x0 NA DATA {date_str} ===")

# Base lean ultra rápida
hist_df = pd.read_csv("b365_base_lean.csv", low_memory=False)

# String slice rápido
date_str_series = hist_df["Date"].astype(str)
day_games = hist_df[date_str_series.str.startswith(date_str)].copy()
print(f"[+] Total de jogos na base para {date_str}: {len(day_games)}")

teams = set(day_games["Home"].dropna()).union(set(day_games["Away"].dropna()))

# Subconjunto filtrado por time antes da conversao de datas
hist_sub = hist_df[(date_str_series < date_str) & (hist_df["Home"].isin(teams) | hist_df["Away"].isin(teams))].copy()
hist_sub["Date"] = pd.to_datetime(hist_sub["Date"], errors="coerce")
hist_sub = hist_sub.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)

payload = day_games.to_dict("records")
res = strat.predict_and_evaluate_live(payload, hist_sub)

print(f"[+] Total de jogos avaliados pelo modelo Lay 2x0: {len(res)}")

aprovados = [g for g in res if g.get("Decision") == "APOSTA"]
skips = [g for g in res if g.get("Decision") == "SKIP"]

print(f"\n✅ APROVADOS ({len(aprovados)}):")
for g in aprovados:
    print(f"   [APOSTA] {g.get('League')} | {g.get('Home')} x {g.get('Away')} | Odd Lay 2x0: {g.get('Odd_CS_2x0_Lay')} | Prob: {g.get('Prob_ML'):.3f} | EV: {g.get('ev_lay'):+.3f}")

print(f"\n⛔ MOTIVOS DE REJEIÇÃO / SKIP ({len(skips)} jogos):")
reasons_summary = {}
for g in skips:
    r = g.get("Reason", "UNKNOWN")
    reasons_summary[r] = reasons_summary.get(r, 0) + 1
    print(f"   [SKIP - {r:<30}] {g.get('League'):<18} | {g.get('Home'):<15} x {g.get('Away'):<15} | Odd Lay: {g.get('Odd_CS_2x0_Lay')}")

print("\n📊 RESUMO DOS MOTIVOS DE REJEIÇÃO:")
for r, count in sorted(reasons_summary.items(), key=lambda x: x[1], reverse=True):
    print(f"   - {r:<35}: {count} jogos")

print("\n=======================================================")
