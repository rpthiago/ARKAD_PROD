import sys
sys.stdout.reconfigure(encoding='utf-8')
import joblib, numpy as np, pandas as pd

print("==================================================================", flush=True)
print("   AVALIAÇÃO COMPLETA NO MÊS DE AGOSTO DE 2026: LAY 2X2 & LAY 0X0  ", flush=True)
print("==================================================================", flush=True)

# 1. Carregar base histórica para construir features de Agosto
import hist_rf_loader
df_hist = hist_rf_loader.load_hist_rf()
df_hist["Date"] = pd.to_datetime(df_hist["Date"])

aug_mask = (df_hist["Date"] >= "2026-08-01") & (df_hist["Date"] <= "2026-08-24")
df_aug = df_hist[aug_mask].dropna(subset=["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT"]).copy()
df_aug = df_aug.sort_values("Date", kind="mergesort").reset_index(drop=True)

print(f"[+] Total de jogos no mês de Agosto/2026 com placar real: {len(df_aug)}")

# Odd columns
odd_2x2_col = "Odd_CS_2x2_Lay" if "Odd_CS_2x2_Lay" in df_aug.columns else "Odd_CS_2x2"
odd_0x0_col = "Odd_CS_0x0_Lay" if "Odd_CS_0x0_Lay" in df_aug.columns else "Odd_CS_0x0"

df_aug["Odd_2x2"] = pd.to_numeric(df_aug.get(odd_2x2_col, np.nan), errors="coerce")
df_aug["Odd_0x0"] = pd.to_numeric(df_aug.get(odd_0x0_col, np.nan), errors="coerce")
df_aug["Odd_Under25"] = pd.to_numeric(df_aug.get("Odd_Under25_FT", np.nan), errors="coerce")

df_aug["is_2x2"] = ((df_aug["Goals_H_FT"] == 2) & (df_aug["Goals_A_FT"] == 2)).astype(int)
df_aug["is_0x0"] = ((df_aug["Goals_H_FT"] == 0) & (df_aug["Goals_A_FT"] == 0)).astype(int)

df_aug["lay_2x2_win"] = 1 - df_aug["is_2x2"]
df_aug["lay_0x0_win"] = 1 - df_aug["is_0x0"]

COMMISSION = 0.045 # 4.5% Betfair
STAKE = 100.0

# ---------------------------------------------------------
# A. AVALIAÇÃO DO LAY 2X2 EM AGOSTO DE 2026
# ---------------------------------------------------------
print("\n==================================================================")
print("                     1. RESULTADOS DO LAY 2X2 EM AGOSTO           ")
print("==================================================================")

# 1. Regra Heurística Atual (Under 2.5 <= 2.00, Odd 2x2 entre 8.00 e 20.00)
c_heur_2x2 = (
    (df_aug["Odd_2x2"] >= 8.00) &
    (df_aug["Odd_2x2"] <= 20.00) &
    (df_aug["Odd_Under25"] <= 2.00)
)
sub_heur_2x2 = df_aug[c_heur_2x2].copy()

n_h2 = len(sub_heur_2x2)
if n_h2 > 0:
    gr_h2 = (sub_heur_2x2["lay_2x2_win"] == 1).sum()
    rd_h2 = n_h2 - gr_h2
    wr_h2 = gr_h2 / n_h2 * 100.0
    odd_m_h2 = sub_heur_2x2["Odd_2x2"].mean()
    be_h2 = ((odd_m_h2 - 1.0) / (odd_m_h2 - COMMISSION)) * 100.0
    pnl_h2 = np.where(sub_heur_2x2["lay_2x2_win"] == 1, STAKE * (1.0 - COMMISSION), -STAKE * (sub_heur_2x2["Odd_2x2"] - 1.0)).sum()
    roi_h2 = pnl_h2 / (n_h2 * STAKE) * 100.0
    print(f"📌 Regra Atual (Under 2.5 <= 2.00 | Odd 8-20):")
    print(f"   Jogos: {n_h2} | Greens: {gr_h2} ({wr_h2:.1f}%) | Reds: {rd_h2} ({100-wr_h2:.1f}%)")
    print(f"   Odd Média: {odd_m_h2:.2f} | Break-even: {be_h2:.1f}% (Margem: {wr_h2 - be_h2:+.1f}%)")
    print(f"   Lucro Líquido: R$ {pnl_h2:,.2f} | ROI: {roi_h2:+.1f}%\n")

# 2. Avaliação dos Reds do 2x2 em Agosto
reds_2x2 = sub_heur_2x2[sub_heur_2x2["lay_2x2_win"] == 0]
if not reds_2x2.empty:
    print(f"--- DETALHE DOS {len(reds_2x2)} REDS DO LAY 2X2 EM AGOSTO ---")
    print(reds_2x2[["Date", "League", "Home", "Away", "Goals_H_FT", "Goals_A_FT", "Odd_2x2"]].to_string(index=False))

# ---------------------------------------------------------
# B. AVALIAÇÃO DO LAY 0X0 EM AGOSTO DE 2026
# ---------------------------------------------------------
print("\n==================================================================")
print("                     2. RESULTADOS DO LAY 0X0 EM AGOSTO           ")
print("==================================================================")

# 1. Estratégia Atual Lay 0x0 (Odd 0x0 entre 6.00 e 16.00)
c_0x0 = (
    (df_aug["Odd_0x0"] >= 6.00) &
    (df_aug["Odd_0x0"] <= 16.00)
)
sub_0x0 = df_aug[c_0x0].copy()

n_0 = len(sub_0x0)
if n_0 > 0:
    gr_0 = (sub_0x0["lay_0x0_win"] == 1).sum()
    rd_0 = n_0 - gr_0
    wr_0 = gr_0 / n_0 * 100.0
    odd_m_0 = sub_0x0["Odd_0x0"].mean()
    be_0 = ((odd_m_0 - 1.0) / (odd_m_0 - COMMISSION)) * 100.0
    pnl_0 = np.where(sub_0x0["lay_0x0_win"] == 1, STAKE * (1.0 - COMMISSION), -STAKE * (sub_0x0["Odd_0x0"] - 1.0)).sum()
    roi_0 = pnl_0 / (n_0 * STAKE) * 100.0
    print(f"📌 Estratégia Lay 0x0 (Odd 6-16 em todos os jogos elegíveis):")
    print(f"   Jogos: {n_0} | Greens: {gr_0} ({wr_0:.1f}%) | Reds: {rd_0} ({100-wr_0:.1f}%)")
    print(f"   Odd Média: {odd_m_0:.2f} | Break-even: {be_0:.1f}% (Margem: {wr_0 - be_0:+.1f}%)")
    print(f"   Lucro Líquido: R$ {pnl_0:,.2f} | ROI: {roi_0:+.1f}%\n")

# 2. Filtrando com xGOT Alto (xGOT >= 2.20) no Lay 0x0
# Carregar xGOT do df_eval
df_eval = pd.read_feather("df_eval_lay_draw.feather")
df_eval["Date"] = pd.to_datetime(df_eval["Date"])
df_aug_xgot = pd.merge(
    df_aug,
    df_eval[["Date", "Home", "Away", "total_xGOT", "liga_draw_rate"]],
    on=["Date", "Home", "Away"],
    how="inner"
)

c_0x0_xgot = (
    (df_aug_xgot["Odd_0x0"] >= 6.00) &
    (df_aug_xgot["Odd_0x0"] <= 16.00) &
    (df_aug_xgot["total_xGOT"] >= 2.20)
)
sub_0x0_xg = df_aug_xgot[c_0x0_xgot].copy()

n_0xg = len(sub_0x0_xg)
if n_0xg > 0:
    gr_0xg = (sub_0x0_xg["lay_0x0_win"] == 1).sum()
    rd_0xg = n_0xg - gr_0xg
    wr_0xg = gr_0xg / n_0xg * 100.0
    odd_m_0xg = sub_0x0_xg["Odd_0x0"].mean()
    be_0xg = ((odd_m_0xg - 1.0) / (odd_m_0xg - COMMISSION)) * 100.0
    pnl_0xg = np.where(sub_0x0_xg["lay_0x0_win"] == 1, STAKE * (1.0 - COMMISSION), -STAKE * (sub_0x0_xg["Odd_0x0"] - 1.0)).sum()
    roi_0xg = pnl_0xg / (n_0xg * STAKE) * 100.0
    print(f"📌 Lay 0x0 com Filtro de Poder Ofensivo (xGOT >= 2.20):")
    print(f"   Jogos: {n_0xg} | Greens: {gr_0xg} ({wr_0xg:.1f}%) | Reds: {rd_0xg} ({100-wr_0xg:.1f}%)")
    print(f"   Odd Média: {odd_m_0xg:.2f} | Break-even: {be_0xg:.1f}% (Margem: {wr_0xg - be_0xg:+.1f}%)")
    print(f"   Lucro Líquido: R$ {pnl_0xg:,.2f} | ROI: {roi_0xg:+.1f}%\n")
