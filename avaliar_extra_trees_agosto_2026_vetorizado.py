import sys
sys.stdout.reconfigure(encoding='utf-8')
import joblib, numpy as np, pandas as pd

print("==================================================================", flush=True)
print("  AVALIAÇÃO OFICIAL RÁPIDA: EXTRA TREES NO MÊS DE AGOSTO DE 2026  ", flush=True)
print("==================================================================", flush=True)

# 1. Carregar modelo campeão Extra Trees e artefatos
clf_et = joblib.load("modelo_lay_draw_campeao_et.pkl")
scaler = joblib.load("scaler_lay_draw_arena.pkl")
features = joblib.load("features_lay_draw_arena.pkl")

# 2. Carregar dataset avaliado com todas as 34 features
df_eval = pd.read_feather("df_eval_lay_draw.feather")
df_eval["Date"] = pd.to_datetime(df_eval["Date"])

# Filtrar apenas o mês de Agosto de 2026
aug_mask = (df_eval["Date"] >= "2026-08-01") & (df_eval["Date"] <= "2026-08-24")
df_aug = df_eval[aug_mask].copy().sort_values("Date", kind="mergesort").reset_index(drop=True)

print(f"[+] Total de jogos no mês de Agosto de 2026 avaliados com features ricas: {len(df_aug)}")

# 3. Predição de probabilidade com o Extra Trees
X_aug = scaler.transform(df_aug[features])
df_aug["prob_lay_et"] = clf_et.predict_proba(X_aug)[:, 1]

COMMISSION = 0.045 # 4.5% Betfair
STAKE = 100.0

df_aug["ev_lay_et"] = df_aug["prob_lay_et"] * (1.0 - COMMISSION) - (1.0 - df_aug["prob_lay_et"]) * (df_aug["Odd_D_FT"] - 1.0)
df_aug["pnl_lay_et"] = np.where(
    df_aug["lay_win"] == 1,
    STAKE * (1.0 - COMMISSION),
    -STAKE * (df_aug["Odd_D_FT"] - 1.0)
)

print("\n==================================================================")
print("   PERFORMANCE EM AGOSTO/2026 POR DIFERENTES CORTES DE PROBABILIDADE ")
print("==================================================================")

for p_cut in [0.73, 0.74, 0.75, 0.76, 0.77, 0.78, 0.80]:
    cond = (
        (df_aug["Odd_D_FT"] >= 3.00) &
        (df_aug["Odd_D_FT"] <= 4.50) &
        (df_aug["prob_lay_et"] >= p_cut) &
        (df_aug["total_xGOT"] >= 2.20) &
        (df_aug["ev_lay_et"] >= 0.02)
    )
    sub = df_aug[cond]
    n = len(sub)
    if n > 0:
        gr = (sub["lay_win"] == 1).sum()
        rd = n - gr
        wr = gr / n * 100.0
        odd_m = sub["Odd_D_FT"].mean()
        be = ((odd_m - 1.0) / (odd_m - COMMISSION)) * 100.0
        pnl = sub["pnl_lay_et"].sum()
        roi = pnl / (n * STAKE) * 100.0
        print(f"Prob >= {p_cut*100:.0f}%: N={n:3d} | Greens={gr:3d} | Reds={rd:2d} | WR={wr:.1f}% vs BE={be:.1f}% (Margem: {wr-be:+.1f}%) | Lucro=R$ {pnl:8.2f} | ROI={roi:+.1f}%")
    else:
        print(f"Prob >= {p_cut*100:.0f}%: N=  0 sinais")

# Detalhamento na Prob >= 75%
cond_75 = (
    (df_aug["Odd_D_FT"] >= 3.00) &
    (df_aug["Odd_D_FT"] <= 4.50) &
    (df_aug["prob_lay_et"] >= 0.75) &
    (df_aug["total_xGOT"] >= 2.20) &
    (df_aug["ev_lay_et"] >= 0.02)
)
sub_75 = df_aug[cond_75].copy().reset_index(drop=True)

if not sub_75.empty:
    sub_75["Placar"] = sub_75["Goals_H_FT"].astype(int).astype(str) + " x " + sub_75["Goals_A_FT"].astype(int).astype(str)
    sub_75["Resultado"] = np.where(sub_75["lay_win"] == 1, "GREEN", "RED")
    sub_75["Data_Str"] = sub_75["Date"].dt.strftime("%Y-%m-%d")
    sub_75["Prob_ET"] = (sub_75["prob_lay_et"] * 100).round(1).astype(str) + "%"
    sub_75["xGOT"] = sub_75["total_xGOT"].round(2)
    
    cols = ["Data_Str", "League", "Home", "Away", "Placar", "Odd_D_FT", "Prob_ET", "xGOT", "Resultado", "pnl_lay_et"]
    print("\n--- TODOS OS JOGOS APROVADOS PELO EXTRA TREES EM AGOSTO/2026 ---")
    print(sub_75[cols].rename(columns={"Data_Str": "Data", "Odd_D_FT": "Odd Lay", "pnl_lay_et": "Lucro (R$)"}).to_string(index=False))

    # Exportar para Excel
    sub_75[cols].rename(columns={"Data_Str": "Data", "Odd_D_FT": "Odd Lay", "pnl_lay_et": "Lucro (R$)"}).to_excel("Extra_Trees_Lay_Draw_Agosto_2026.xlsx", index=False)
    print("\n[+] Planilha gerada: Extra_Trees_Lay_Draw_Agosto_2026.xlsx", flush=True)
