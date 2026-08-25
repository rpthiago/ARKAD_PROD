import sys
sys.stdout.reconfigure(encoding='utf-8')
import os, joblib, numpy as np, pandas as pd
from datetime import datetime

print("==================================================================", flush=True)
print("   ESTUDO CIENTÍFICO E QUANTITATIVO DE FILTROS - LAY DRAW (ARKAD)  ", flush=True)
print("==================================================================", flush=True)

# 1. Carregar a base histórica
import hist_rf_loader
df_raw = hist_rf_loader.load_hist_rf()
print(f"[+] Base carregada: {len(df_raw)} jogos | Datas: {df_raw['Date'].min().strftime('%Y-%m-%d')} a {df_raw['Date'].max().strftime('%Y-%m-%d')}", flush=True)

# 2. Filtrar apenas jogos com odds e resultados válidos
df = df_raw.dropna(subset=["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT", "Odd_D_FT"]).copy()
df["Date"] = pd.to_datetime(df["Date"])
df = df.sort_values("Date", kind="mergesort").reset_index(drop=True)

df["Odd_D_FT"] = pd.to_numeric(df["Odd_D_FT"], errors="coerce")
df["Odd_H_FT"] = pd.to_numeric(df.get("Odd_H_FT", np.nan), errors="coerce")
df["Odd_A_FT"] = pd.to_numeric(df.get("Odd_A_FT", np.nan), errors="coerce")
df = df[(df["Odd_D_FT"] >= 2.0) & (df["Odd_D_FT"] <= 10.0)].copy()

# Target: 1 se o Lay GANHOU (jogo NÃO empatou), 0 se empatou
df["lay_win"] = (df["Goals_H_FT"] != df["Goals_A_FT"]).astype(int)

print(f"[+] Total de jogos com Odd Empate válida: {len(df)} | Taxa Geral Não-Empate: {df['lay_win'].mean()*100:.2f}%", flush=True)

# 3. Carregar modelo, scaler e features
MODEL_PATH    = "modelo_lay_draw_rf_v2.pkl"
SCALER_PATH   = "scaler_lay_draw_rf_v2.pkl"
FEATURES_PATH = "features_lay_draw_rf_v2.pkl"

if not (os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH) and os.path.exists(FEATURES_PATH)):
    print("[-] Erro: Artefatos do modelo não encontrados!")
    sys.exit(1)

model = joblib.load(MODEL_PATH)
scaler = joblib.load(SCALER_PATH)
features = joblib.load(FEATURES_PATH)
print(f"[+] Modelo Random Forest carregado com {len(features)} features.", flush=True)

# 4. Construção Vetorizada Leak-Free das 34 Features para toda a base
print("[*] Construindo séries temporais unshifted com janelas de mando...", flush=True)

def _decay_roll_grouped_unshifted(df_in, group_col, val_col, window=6, alpha=0.25):
    g = df_in.groupby(group_col)[val_col]
    numer = np.zeros(len(df_in)); count = np.zeros(len(df_in)); wsum = 0.0
    for j in range(window):
        sj = g.shift(j + 1) # shift(1) estrito para evitar lookahead
        ej = np.exp(-alpha * j)
        m = sj.notna().to_numpy()
        numer += np.where(m, np.nan_to_num(sj.to_numpy()) * ej, 0.0)
        count += m
        wsum += ej
    res = numer / wsum
    res[count < 3] = np.nan
    return pd.Series(res, index=df_in.index)

# Views de mando
df["_draw_flag"] = (df["Goals_H_FT"] == df["Goals_A_FT"]).astype(float)
df["won_h"] = (df["Goals_H_FT"] > df["Goals_A_FT"]).astype(float)
df["won_a"] = (df["Goals_A_FT"] > df["Goals_H_FT"]).astype(float)

# Mandante em Casa
dh_map = [
    ("Goals_H_FT", "H_h_Gf"), ("Goals_A_FT", "H_h_Gc"), ("xGOT_H_FT", "H_h_xGOT"),
    ("xGOT_Faced_H_FT", "H_h_xGOT_faced"), ("Goals_Prevented_H_FT", "H_h_GP"),
    ("Big_Chances_H_FT", "H_h_BC"), ("Shots_On_Target_H_FT", "H_h_SoT"),
    ("Possession_H_FT", "H_h_Poss"), ("won_h", "H_h_WR"), ("_draw_flag", "H_h_draw_rate")
]
for raw_c, feat_c in dh_map:
    df[feat_c] = _decay_roll_grouped_unshifted(df, "Home", raw_c)

# Visitante Fora
da_map = [
    ("Goals_A_FT", "A_a_Gf"), ("Goals_H_FT", "A_a_Gc"), ("xGOT_A_FT", "A_a_xGOT"),
    ("xGOT_Faced_A_FT", "A_a_xGOT_faced"), ("Goals_Prevented_A_FT", "A_a_GP"),
    ("Big_Chances_A_FT", "A_a_BC"), ("Shots_On_Target_A_FT", "A_a_SoT"),
    ("Possession_A_FT", "A_a_Poss"), ("won_a", "A_a_WR"), ("_draw_flag", "A_a_draw_rate")
]
for raw_c, feat_c in da_map:
    df[feat_c] = _decay_roll_grouped_unshifted(df, "Away", raw_c)

# Liga e H2H
df["liga_draw_rate"] = df.groupby("League")["_draw_flag"].transform(lambda x: x.shift(1).rolling(100, min_periods=20).mean())

import unicodedata, re
def _canon(s):
    if pd.isna(s) or not s: return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

c_h = df["Home"].map(_canon)
c_a = df["Away"].map(_canon)
df["h2h_pair"] = [tuple(sorted(x)) for x in zip(c_h, c_a)]
df["h2h_draw_rate"] = df.groupby("h2h_pair")["_draw_flag"].transform(lambda x: x.shift(1).rolling(8, min_periods=2).mean())

# Interações
df["total_WR"] = df["H_h_WR"] + df["A_a_WR"]
df["wr_diff"] = abs(df["H_h_WR"] - df["A_a_WR"])
df["draw_rate_prod"] = df["H_h_draw_rate"] * df["A_a_draw_rate"]
df["draw_rate_mean"] = (df["H_h_draw_rate"] + df["A_a_draw_rate"]) / 2.0
df["total_xGOT"] = df["H_h_xGOT"] + df["A_a_xGOT"]
df["xGOT_diff"] = abs(df["H_h_xGOT"] - df["A_a_xGOT"])
df["total_Gf"] = df["H_h_Gf"] + df["A_a_Gf"]
df["gf_diff"] = abs(df["H_h_Gf"] - df["A_a_Gf"])
df["decisive_score"] = (df["total_Gf"] * 0.4) + (df["total_xGOT"] * 0.4) + (df["wr_diff"] * 2.0) - (df["draw_rate_mean"] * 3.0)

df["mkt_prob_draw"] = 1.0 / df["Odd_D_FT"]
_ov = (1.0 / df["Odd_H_FT"].replace(0, np.nan)) + (1.0 / df["Odd_A_FT"].replace(0, np.nan)) + df["mkt_prob_draw"]
df["mkt_prob_draw_norm"] = df["mkt_prob_draw"] / _ov
df["mkt_overvalue_draw"] = df["mkt_prob_draw_norm"] - df["liga_draw_rate"]

# Drop NaNs estrito
valid_mask = df[features].notna().all(axis=1)
df_eval = df[valid_mask].copy()
print(f"[+] Total de jogos com 34 features completas (sem NaNs): {len(df_eval)}", flush=True)

# 5. Predição de Probabilidade com o Scaler e Random Forest
print("[*] Gerando probabilidades com o modelo Random Forest...", flush=True)
X_scaled = scaler.transform(df_eval[features])
df_eval["prob_lay_win"] = model.predict_proba(X_scaled)[:, 1] # P(Não-Empate)

# EV com comissão de 5%
COMMISSION = 0.05
df_eval["ev_lay"] = df_eval["prob_lay_win"] * (1.0 - COMMISSION) - (1.0 - df_eval["prob_lay_win"]) * (df_eval["Odd_D_FT"] - 1.0)
df_eval["be_wr"] = (df_eval["Odd_D_FT"] - 1.0) / (df_eval["Odd_D_FT"] - COMMISSION)

# Lucro por stake unitária de R$ 100
STAKE = 100.0
df_eval["pnl_lay"] = np.where(df_eval["lay_win"] == 1, STAKE * (1.0 - COMMISSION), -STAKE * (df_eval["Odd_D_FT"] - 1.0))

print("\n--- BASE PRONTA PARA AUDITORIA POR GRADES DE FILTRO ---")
print(f"Total de jogos válidos: {len(df_eval)}")
print(f"Período: {df_eval['Date'].dt.year.value_counts().sort_index().to_dict()}")

# Salvar DataFrame avaliado para testes rápidos
df_eval.to_feather("df_eval_lay_draw.feather")
print("[+] Salvo df_eval_lay_draw.feather com sucesso!", flush=True)
