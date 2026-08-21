import pandas as pd, numpy as np, os, sys
import lay_2x0_rf_v2_strategy as strat

print("=== VERIFICAÇÃO DIRETA DIA 15/08 NA STRATEGY LAY 2x0 ===")

# Carrega base 2026
df_full = pd.read_csv("Resultados_2026_Full.csv", low_memory=False)
df_full["Date"] = pd.to_datetime(df_full["Date"], errors="coerce")
df_full = df_full.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)

max_dt = df_full["Date"].max()
min_dt = df_full["Date"].min()
print(f"[+] Intervalo de datas na base 2026: {min_dt.strftime('%Y-%m-%d')} ate {max_dt.strftime('%Y-%m-%d')}")

# Filtra dia 15/08/2026 ou data mais recente
target_date = "2026-08-15"
day_df = df_full[df_full["Date"].dt.strftime("%Y-%m-%d") == target_date].copy()
print(f"[+] Total de jogos no dia {target_date}: {len(day_df)}")

if day_df.empty:
    # Se nao houver 15/08/2026, pega a data com maior número de jogos em Agosto/2026
    aug_df = df_full[df_full["Date"].dt.strftime("%Y-%m") == "2026-08"].copy()
    print(f"[+] Total de jogos no mes de Agosto/2026: {len(aug_df)}")
    if not aug_df.empty:
        top_dates = aug_df["Date"].dt.strftime("%Y-%m-%d").value_counts()
        print(f"[+] Datas de Agosto/2026 disponíveis no histórico: {top_dates.head(5).to_dict()}")
        target_date = top_dates.index[0]
        day_df = df_full[df_full["Date"].dt.strftime("%Y-%m-%d") == target_date].copy()
        print(f"[+] Usando a data {target_date} com {len(day_df)} jogos para auditoria...")

# Avaliar jogos no modelo Lay 2x0
payload = day_df.to_dict("records")
res = strat.predict_and_evaluate_live(payload, df_full)

print(f"\n[+] Total de jogos processados pelo Lay 2x0: {len(res)}")
aprovados = [g for g in res if g.get("Decision") == "APOSTA"]
skips = [g for g in res if g.get("Decision") == "SKIP"]

print(f"✅ APROVADOS PARA APOSTA: {len(aprovados)}")
for g in aprovados:
    print(f"   👉 {g.get('League')} | {g.get('Home')} x {g.get('Away')} | Odd CS 2x0: {g.get('Odd_CS_2x0_Lay')} | Prob: {g.get('Prob_ML'):.3f} | EV: {g.get('ev_lay'):+.3f}")

print(f"\n⛔ MOTIVOS DE SKIP DA DATA {target_date} ({len(skips)} jogos):")
reasons = {}
for g in skips:
    r = g.get("Reason", "UNKNOWN")
    reasons[r] = reasons.get(r, 0) + 1
    print(f"   - {r:<30}: {g.get('League'):<18} | {g.get('Home')} x {g.get('Away')} | Odd: {g.get('Odd_CS_2x0_Lay')}")

print("\n📊 RESUMO DOS MOTIVOS DE REJEIÇÃO:")
for r, cnt in sorted(reasons.items(), key=lambda x: x[1], reverse=True):
    print(f"   • {r:<35}: {cnt} jogos")
