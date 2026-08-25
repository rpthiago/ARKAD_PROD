import sys
sys.stdout.reconfigure(encoding='utf-8')
import joblib, numpy as np, pandas as pd
from datetime import datetime

print("==================================================================", flush=True)
print("   AVALIAÇÃO OFICIAL DO EXTRA TREES NO MÊS DE AGOSTO DE 2026     ", flush=True)
print("==================================================================", flush=True)

# 1. Carregar artefatos do campeão Extra Trees
clf_et = joblib.load("modelo_lay_draw_campeao_et.pkl")
scaler = joblib.load("scaler_lay_draw_arena.pkl")
features = joblib.load("features_lay_draw_arena.pkl")

# 2. Carregar base histórica para features
import hist_rf_loader, b365_data_utils
df_hist = hist_rf_loader.load_hist_rf()
df_aug = df_hist[(df_hist["Date"] >= "2026-08-01") & (df_hist["Date"] <= "2026-08-24")].copy()
df_aug = df_aug.dropna(subset=["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT", "Odd_D_FT"]).copy()
df_aug = df_aug.sort_values("Date", kind="mergesort").reset_index(drop=True)

print(f"[+] Total de jogos no mês de Agosto/2026 na base com resultados: {len(df_aug)}", flush=True)

# Importar o calculador de features da estratégia
import lay_draw_rf_v2_strategy as sdraw

# Avaliar dia a dia no mês de Agosto (idêntico ao robô ao vivo)
datas = sorted(df_aug["Date"].dt.strftime("%Y-%m-%d").unique())
print(f"[+] Avaliando dia a dia de {datas[0]} até {datas[-1]}...", flush=True)

resultados_jogos = []
for d_str in datas:
    sub_dia = df_aug[df_aug["Date"] == d_str].to_dict("records")
    # Executar com a função predict_and_evaluate_live
    res = sdraw.predict_and_evaluate_live(sub_dia, df_hist)
    for g in res:
        # Casar com o placar real de df_aug
        m_cand = df_aug[(df_aug["Date"] == d_str) & (df_aug["Home"] == g["Home"]) & (df_aug["Away"] == g["Away"])]
        if not m_cand.empty:
            row_m = m_cand.iloc[0]
            g["Goals_H_FT"] = row_m["Goals_H_FT"]
            g["Goals_A_FT"] = row_m["Goals_A_FT"]
            g["is_draw"] = int(row_m["Goals_H_FT"] == row_m["Goals_A_FT"])
            g["target_lay_win"] = 1 - g["is_draw"]
            resultados_jogos.append(g)

df_res_aug = pd.DataFrame(resultados_jogos)
print(f"[+] Total de jogos processados em Agosto: {len(df_res_aug)}")

# Filtrar apenas as apostas aprovadas pelo modelo Extra Trees
aprovados = df_res_aug[df_res_aug["Decision"] == "APOSTA"].copy()
print(f"\n==================================================")
print(f"      APOSTAS APROVADAS PELO EXTRA TREES EM AGOSTO")
print(f"==================================================")
print(f"Total de Sinais Aprovados: {len(aprovados)}")

COMMISSION = 0.045
STAKE = 100.0

if not aprovados.empty:
    aprovados["pnl_lay"] = np.where(
        aprovados["target_lay_win"] == 1,
        STAKE * (1.0 - COMMISSION),
        -STAKE * (aprovados["Odd_D_FT"] - 1.0)
    )
    
    greens = (aprovados["target_lay_win"] == 1).sum()
    reds = len(aprovados) - greens
    wr = greens / len(aprovados) * 100.0
    odd_med = aprovados["Odd_D_FT"].mean()
    be_wr = ((odd_med - 1.0) / (odd_med - COMMISSION)) * 100.0
    lucro = aprovados["pnl_lay"].sum()
    roi = lucro / (len(aprovados) * STAKE) * 100.0
    
    print(f"Greens: {greens} ({wr:.1f}%) | Reds: {reds} ({100-wr:.1f}%)")
    print(f"Odd Média: {odd_med:.2f}")
    print(f"Break-even Win Rate: {be_wr:.1f}% (Margem: {wr - be_wr:+.1f}%)")
    print(f"Lucro Líquido Real (Stake R$ 100): R$ {lucro:,.2f}")
    print(f"ROI Líquido Real: {roi:+.1f}%\n")
    
    aprovados["Placar"] = aprovados["Goals_H_FT"].astype(int).astype(str) + " x " + aprovados["Goals_A_FT"].astype(int).astype(str)
    aprovados["Resultado"] = np.where(aprovados["target_lay_win"] == 1, "GREEN", "RED")
    aprovados["Prob_IA"] = (aprovados["Prob_ML"] * 100).round(1).astype(str) + "%"
    aprovados["xGOT"] = aprovados["total_xGOT"].round(2)
    aprovados["Data"] = pd.to_datetime(aprovados["Date"]).dt.strftime("%Y-%m-%d")
    
    cols = ["Data", "League", "Home", "Away", "Placar", "Odd_D_FT", "Prob_IA", "xGOT", "Resultado", "pnl_lay"]
    print(aprovados[cols].rename(columns={"Odd_D_FT": "Odd Lay", "pnl_lay": "Lucro (R$)"}).to_string(index=False))
else:
    print("Nenhuma aposta aprovada nos filtros estritos em Agosto.")
