import sys
sys.stdout.reconfigure(encoding='utf-8')
import joblib, numpy as np, pandas as pd

print("==================================================================", flush=True)
print("  AVALIAÇÃO DOS NOVOS MODELOS DE IA EM AGOSTO/2026 (LAY 2X2 & 0X0) ", flush=True)
print("==================================================================", flush=True)

# 1. Carregar base de dados com features avaliadas
df_eval = pd.read_feather("df_eval_lay_draw.feather")
df_eval["Date"] = pd.to_datetime(df_eval["Date"])

# Filtrar Agosto de 2026
aug_mask = (df_eval["Date"] >= "2026-08-01") & (df_eval["Date"] <= "2026-08-24")
df_aug = df_eval[aug_mask].copy().sort_values("Date", kind="mergesort").reset_index(drop=True)

COMMISSION = 0.045 # 4.5% Betfair
STAKE = 100.0

# -----------------------------------------------------------------
# 1. NOVO MODELO LAY 2X2 (Random Forest Treinado na Odd Real 2x2)
# -----------------------------------------------------------------
print("\n--- 1. NOVO MODELO DE MACHINE LEARNING: LAY 2X2 ---")
clf_2x2 = joblib.load("modelo_lay_2x2_arena.pkl")
scaler_2x2 = joblib.load("scaler_lay_2x2_arena.pkl")
features_2x2 = joblib.load("features_lay_2x2_arena.pkl")

# Target e Odd
df_aug["is_2x2"] = ((df_aug["Goals_H_FT"] == 2) & (df_aug["Goals_A_FT"] == 2)).astype(int)
df_aug["lay_2x2_win"] = 1 - df_aug["is_2x2"]
df_aug["Odd_2x2"] = pd.to_numeric(df_aug.get("Odd_CS_2x2_Lay", df_aug.get("Odd_CS_2x2", np.nan)), errors="coerce")
df_aug["Odd_Under25_FT"] = pd.to_numeric(df_aug.get("Odd_Under25_FT", np.nan), errors="coerce").fillna(1.90)
df_aug["mkt_prob_2x2"] = 1.0 / df_aug["Odd_2x2"].replace(0, np.nan)
df_aug["H_h_2x2_rate"] = df_aug.get("H_h_draw_rate", 0.05) # proxy
df_aug["A_a_2x2_rate"] = df_aug.get("A_a_draw_rate", 0.05)
df_aug["H_h_over25_rate"] = 0.50
df_aug["A_a_over25_rate"] = 0.50
df_aug["total_over25_rate"] = 0.50
df_aug["under_tendency"] = 0.50
df_aug["liga_2x2_rate"] = 0.05
df_aug["liga_over25_rate"] = 0.50

# Avaliar apenas onde há features_2x2 e Odd_2x2
valid_2x2 = df_aug.dropna(subset=features_2x2 + ["Odd_2x2"]).copy()
if not valid_2x2.empty:
    X_2x2 = scaler_2x2.transform(valid_2x2[features_2x2])
    probs_2x2 = clf_2x2.predict_proba(X_2x2)[:, 1]
    valid_2x2["prob_2x2"] = probs_2x2
    valid_2x2["ev_2x2"] = valid_2x2["prob_2x2"] * (1.0 - COMMISSION) - (1.0 - valid_2x2["prob_2x2"]) * (valid_2x2["Odd_2x2"] - 1.0)
    valid_2x2["pnl_2x2"] = np.where(
        valid_2x2["lay_2x2_win"] == 1,
        STAKE * (1.0 - COMMISSION),
        -STAKE * (valid_2x2["Odd_2x2"] - 1.0)
    )
    
    # Filtro da IA Lay 2x2
    for p_cut in [0.93, 0.94, 0.95]:
        cond = (valid_2x2["Odd_2x2"] >= 8.0) & (valid_2x2["Odd_2x2"] <= 20.0) & (valid_2x2["prob_2x2"] >= p_cut) & (valid_2x2["ev_2x2"] >= 0.01)
        sub = valid_2x2[cond]
        n = len(sub)
        if n > 0:
            gr = (sub["lay_2x2_win"] == 1).sum()
            rd = n - gr
            wr = gr / n * 100.0
            odd_m = sub["Odd_2x2"].mean()
            be = ((odd_m - 1.0) / (odd_m - COMMISSION)) * 100.0
            pnl = sub["pnl_2x2"].sum()
            roi = pnl / (n * STAKE) * 100.0
            print(f"  IA Prob >= {p_cut*100:.0f}%: N={n:4d} | Greens={gr:4d} | Reds={rd:2d} | WR={wr:.1f}% vs BE={be:.1f}% (Margem: {wr-be:+.1f}%) | Lucro=R$ {pnl:8.2f} | ROI={roi:+.1f}%")

# -----------------------------------------------------------------
# 2. NOVO MODELO LAY 0X0 (LightGBM Treinado na Odd Real 0x0)
# -----------------------------------------------------------------
print("\n--- 2. NOVO MODELO DE MACHINE LEARNING: LAY 0X0 ---")
clf_0x0 = joblib.load("modelo_lay_0x0_arena.pkl")
scaler_0x0 = joblib.load("scaler_lay_0x0_arena.pkl")
features_0x0 = joblib.load("features_lay_0x0_arena.pkl")

df_aug["is_0x0"] = ((df_aug["Goals_H_FT"] == 0) & (df_aug["Goals_A_FT"] == 0)).astype(int)
df_aug["lay_0x0_win"] = 1 - df_aug["is_0x0"]
df_aug["Odd_0x0"] = pd.to_numeric(df_aug.get("Odd_CS_0x0_Lay", df_aug.get("Odd_CS_0x0", np.nan)), errors="coerce")
df_aug["mkt_prob_0x0"] = 1.0 / df_aug["Odd_0x0"].replace(0, np.nan)
df_aug["H_h_0x0_rate"] = df_aug.get("H_h_draw_rate", 0.08)
df_aug["A_a_0x0_rate"] = df_aug.get("A_a_draw_rate", 0.08)
df_aug["total_0x0_rate"] = 0.08
df_aug["liga_0x0_rate"] = 0.08

valid_0x0 = df_aug.dropna(subset=features_0x0 + ["Odd_0x0"]).copy()
if not valid_0x0.empty:
    X_0x0 = scaler_0x0.transform(valid_0x0[features_0x0])
    probs_0x0 = clf_0x0.predict_proba(X_0x0)[:, 1]
    valid_0x0["prob_0x0"] = probs_0x0
    valid_0x0["ev_0x0"] = valid_0x0["prob_0x0"] * (1.0 - COMMISSION) - (1.0 - valid_0x0["prob_0x0"]) * (valid_0x0["Odd_0x0"] - 1.0)
    valid_0x0["pnl_0x0"] = np.where(
        valid_0x0["lay_0x0_win"] == 1,
        STAKE * (1.0 - COMMISSION),
        -STAKE * (valid_0x0["Odd_0x0"] - 1.0)
    )
    
    # Filtro da IA Lay 0x0
    for p_cut in [0.91, 0.92, 0.93]:
        cond = (valid_0x0["Odd_0x0"] >= 6.0) & (valid_0x0["Odd_0x0"] <= 16.0) & (valid_0x0["prob_0x0"] >= p_cut) & (valid_0x0["ev_0x0"] >= 0.01)
        sub = valid_0x0[cond]
        n = len(sub)
        if n > 0:
            gr = (sub["lay_0x0_win"] == 1).sum()
            rd = n - gr
            wr = gr / n * 100.0
            odd_m = sub["Odd_0x0"].mean()
            be = ((odd_m - 1.0) / (odd_m - COMMISSION)) * 100.0
            pnl = sub["pnl_0x0"].sum()
            roi = pnl / (n * STAKE) * 100.0
            print(f"  IA Prob >= {p_cut*100:.0f}%: N={n:4d} | Greens={gr:4d} | Reds={rd:2d} | WR={wr:.1f}% vs BE={be:.1f}% (Margem: {wr-be:+.1f}%) | Lucro=R$ {pnl:8.2f} | ROI={roi:+.1f}%")
