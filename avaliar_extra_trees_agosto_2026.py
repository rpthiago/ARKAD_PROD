import sys
sys.stdout.reconfigure(encoding='utf-8')
import joblib, numpy as np, pandas as pd

print("==================================================================", flush=True)
print("     AVALIAÇÃO DO EXTRA TREES NO MÊS DE AGOSTO DE 2026 (REAL)    ", flush=True)
print("==================================================================", flush=True)

import treinar_arena_modelos_lay_draw as arena

clf_et = arena.trained_models["2. Extra Trees"]
scaler = arena.scaler
features = arena.features
df_clean = arena.df_clean

# Filtrar estritamente o mês de Agosto de 2026
august_mask = (df_clean["Date"] >= "2026-08-01") & (df_clean["Date"] <= "2026-08-24")
df_aug = df_clean[august_mask].copy()

print(f"[+] Total de jogos no mês de Agosto/2026 avaliados com Odd Lay real: {len(df_aug)}", flush=True)

X_aug = scaler.transform(df_aug[features])
probs_aug = clf_et.predict_proba(X_aug)[:, 1]

COMMISSION = 0.045 # 4.5% Betfair
STAKE = 100.0

df_aug["prob_lay"] = probs_aug
df_aug["ev_lay"] = df_aug["prob_lay"] * (1.0 - COMMISSION) - (1.0 - df_aug["prob_lay"]) * (df_aug["Odd_D_Lay"] - 1.0)
df_aug["pnl_lay"] = np.where(
    df_aug["target_lay_win"] == 1,
    STAKE * (1.0 - COMMISSION),
    -STAKE * (df_aug["Odd_D_Lay"] - 1.0)
)

# Testar diferentes níveis de probabilidade no mês de Agosto
print("\n--- GRADE DE CORTES NO MÊS DE AGOSTO/2026 ---")
for p_cut in [0.73, 0.74, 0.75, 0.76, 0.77, 0.78]:
    cond = (
        (df_aug["Odd_D_Lay"] >= 3.00) &
        (df_aug["Odd_D_Lay"] <= 4.50) &
        (df_aug["prob_lay"] >= p_cut) &
        (df_aug["ev_lay"] >= 0.02)
    )
    sub = df_aug[cond]
    n = len(sub)
    if n > 0:
        gr = (sub["target_lay_win"] == 1).sum()
        rd = n - gr
        wr = gr / n * 100.0
        odd_m = sub["Odd_D_Lay"].mean()
        be = ((odd_m - 1.0) / (odd_m - COMMISSION)) * 100.0
        pnl = sub["pnl_lay"].sum()
        roi = pnl / (n * STAKE) * 100.0
        print(f"Prob >= {p_cut*100:.0f}%: N={n:3d} | Greens={gr:3d} | Reds={rd:2d} | WR={wr:.1f}% vs BE={be:.1f}% (Margem: {wr-be:+.1f}%) | Lucro=R$ {pnl:8.2f} | ROI={roi:+.1f}%")

# Detalhamento na Prob >= 75%
cond_75 = (
    (df_aug["Odd_D_Lay"] >= 3.00) &
    (df_aug["Odd_D_Lay"] <= 4.50) &
    (df_aug["prob_lay"] >= 0.75) &
    (df_aug["ev_lay"] >= 0.02)
)
sub_75 = df_aug[cond_75].copy().sort_values("Date", kind="mergesort").reset_index(drop=True)

print(f"\n==================================================")
print(f"     DETALHAMENTO DE JOGOS EM AGOSTO (PROB >= 75%)")
print(f"==================================================")
sub_75["Placar"] = sub_75["Goals_H_FT"].astype(int).astype(str) + " x " + sub_75["Goals_A_FT"].astype(int).astype(str)
sub_75["Resultado"] = np.where(sub_75["target_lay_win"] == 1, "GREEN", "RED")
sub_75["Data_Str"] = sub_75["Date"].dt.strftime("%Y-%m-%d")
sub_75["Prob_ET"] = (sub_75["prob_lay"] * 100).round(1).astype(str) + "%"

cols_show = ["Data_Str", "League", "Home", "Away", "Placar", "Odd_D_Lay", "Prob_ET", "Resultado", "pnl_lay"]
print(sub_75[cols_show].rename(columns={"Data_Str": "Data", "Odd_D_Lay": "Odd Lay", "pnl_lay": "Lucro (R$)"}).to_string(index=False))
