import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np, joblib
import hist_rf_loader, unicodedata, re

print("=== EXECUTANDO BACKTEST CONSOLIDADO DE 2026 PARA OS 5 MÉTODOS LAY CS ===", flush=True)

# 1. Carregar base
df = hist_rf_loader.load_hist_rf()
print(f"[+] Base histórica: {len(df):,} partidas", flush=True)

def _canon(s):
    if pd.isna(s) or not s: return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df = df.dropna(subset=["Goals_H_FT", "Goals_A_FT", "Date", "Home", "Away"]).copy()
df = df.sort_values("Date", kind="mergesort").reset_index(drop=True)

df["won_H"] = (df["Goals_H_FT"] > df["Goals_A_FT"]).astype(float)
df["won_A"] = (df["Goals_A_FT"] > df["Goals_H_FT"]).astype(float)
df["shut_H"] = (df["Goals_A_FT"] == 0).astype(float)
df["score1_A"] = (df["Goals_A_FT"] == 1).astype(float)
df["score10_H"] = ((df["Goals_H_FT"] == 1) & (df["Goals_A_FT"] == 0)).astype(float)
df["multi_H"] = (df["Goals_H_FT"] >= 2).astype(float)
df["concede0_A"] = (df["Goals_H_FT"] == 0).astype(float)

# Flags de CS
df["_0x0_flag"] = ((df["Goals_H_FT"] == 0) & (df["Goals_A_FT"] == 0)).astype(float)
df["_0x1_flag"] = ((df["Goals_H_FT"] == 0) & (df["Goals_A_FT"] == 1)).astype(float)
df["_0x2_flag"] = ((df["Goals_H_FT"] == 0) & (df["Goals_A_FT"] == 2)).astype(float)
df["_1x0_flag"] = ((df["Goals_H_FT"] == 1) & (df["Goals_A_FT"] == 0)).astype(float)
df["_2x0_flag"] = ((df["Goals_H_FT"] == 2) & (df["Goals_A_FT"] == 0)).astype(float)

df["c_Home"] = df["Home"].map(_canon)
df["c_Away"] = df["Away"].map(_canon)
df["h2h_pair"] = [tuple(sorted(x)) for x in zip(df["c_Home"], df["c_Away"])]

# Mapear Odds CS da base
for cs in ["0x0", "0x1", "0x2", "1x0", "2x0"]:
    df[f"odd_{cs}"] = pd.to_numeric(df.get(f"Odd_CS_{cs}", df.get(f"Odd_{cs}_FT", np.nan)), errors="coerce")
df["odd_h"] = pd.to_numeric(df.get("Odd_H_FT", np.nan), errors="coerce")
df["odd_a"] = pd.to_numeric(df.get("Odd_A_FT", np.nan), errors="coerce")

stat_cols = [
    "Goals_H_FT", "Goals_A_FT", "xGOT_H_FT", "xGOT_A_FT",
    "xGOT_Faced_H_FT", "xGOT_Faced_A_FT", "Goals_Prevented_H_FT", "Goals_Prevented_A_FT",
    "Big_Chances_H_FT", "Big_Chances_A_FT", "Shots_On_Target_H_FT", "Shots_On_Target_A_FT",
    "Possession_H_FT", "Possession_A_FT"
]
for c in stat_cols:
    df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0.0) if c in df.columns else 0.0

print("[+] Computando rollings por mando...", flush=True)

g_h = df.groupby("c_Home")
g_a = df.groupby("c_Away")
wsum = sum(np.exp(-0.25 * (j - 1)) for j in range(1, 7))

def _roll_h(src):
    numer = np.zeros(len(df)); count = np.zeros(len(df))
    for j in range(1, 7):
        sj = g_h[src].shift(j).to_numpy()
        ej = np.exp(-0.25 * (j - 1))
        m = ~np.isnan(sj)
        numer += np.where(m, sj * ej, 0.0)
        count += m
    res = numer / wsum
    res[count < 3] = np.nan
    return res

def _roll_a(src):
    numer = np.zeros(len(df)); count = np.zeros(len(df))
    for j in range(1, 7):
        sj = g_a[src].shift(j).to_numpy()
        ej = np.exp(-0.25 * (j - 1))
        m = ~np.isnan(sj)
        numer += np.where(m, sj * ej, 0.0)
        count += m
    res = numer / wsum
    res[count < 3] = np.nan
    return res

# Base Home
df["H_h_Gf"] = _roll_h("Goals_H_FT")
df["H_h_Gc"] = _roll_h("Goals_A_FT")
df["H_h_xGOT"] = _roll_h("xGOT_H_FT")
df["H_h_xGOT_faced"] = _roll_h("xGOT_Faced_H_FT")
df["H_h_GP"] = _roll_h("Goals_Prevented_H_FT")
df["H_h_BC"] = _roll_h("Big_Chances_H_FT")
df["H_h_SoT"] = _roll_h("Shots_On_Target_H_FT")
df["H_h_Poss"] = _roll_h("Possession_H_FT")
df["H_h_WR"] = _roll_h("won_H")
df["H_h_goals_rate"] = df["H_h_Gf"]
df["H_h_shut_rate"] = _roll_h("shut_H")
df["H_h_score10_rate"] = _roll_h("score10_H")
df["H_h_multi_goal"] = _roll_h("multi_H")

# Base Away
df["A_a_Gf"] = _roll_a("Goals_A_FT")
df["A_a_Gc"] = _roll_a("Goals_H_FT")
df["A_a_xGOT"] = _roll_a("xGOT_A_FT")
df["A_a_xGOT_faced"] = _roll_a("xGOT_Faced_A_FT")
df["A_a_GP"] = _roll_a("Goals_Prevented_A_FT")
df["A_a_BC"] = _roll_a("Big_Chances_A_FT")
df["A_a_SoT"] = _roll_a("Shots_On_Target_A_FT")
df["A_a_Poss"] = _roll_a("Possession_A_FT")
df["A_a_WR"] = _roll_a("won_A")
df["A_a_goals_rate"] = df["A_a_Gf"]
df["A_a_score1_rate"] = _roll_a("score1_A")
df["A_a_concede0"] = _roll_a("concede0_A")

# Combinadas gerais
df["total_xGOT"] = df["H_h_xGOT"] + df["A_a_xGOT"]
df["total_Gf"] = df["H_h_Gf"] + df["A_a_Gf"]
df["total_BC"] = df["H_h_BC"] + df["A_a_BC"]
df["total_SoT"] = df["H_h_SoT"] + df["A_a_SoT"]
df["total_def_weak"] = df["H_h_Gc"] + df["A_a_Gc"]
df["weaker_gk"] = np.minimum(df["H_h_GP"], df["A_a_GP"])
df["attack_imbalance"] = abs(df["H_h_Gf"] - df["A_a_Gf"])
df["spread_forca"] = df["H_h_WR"] - df["A_a_WR"]
df["home_strength_x_away_weakness"] = df["H_h_goals_rate"] * df["A_a_score1_rate"]
df["total_goals_proxy"] = df["H_h_goals_rate"] + df["A_a_goals_rate"]

# CS Específicos
for cs in ["0x0", "0x2", "2x0"]:
    df[f"H_h_{cs}_rate"] = _roll_h(f"_{cs}_flag")
    df[f"A_a_{cs}_rate"] = _roll_a(f"_{cs}_flag")
    df[f"liga_{cs}_rate"] = df.groupby("League")[f"_{cs}_flag"].transform(lambda s: s.shift(1).rolling(100, min_periods=20).mean())
    df[f"h2h_{cs}_rate"] = df.groupby("h2h_pair")[f"_{cs}_flag"].transform(lambda s: s.shift(1).rolling(8, min_periods=2).mean())
    df[f"h2h_{cs}_rate_raw"] = df[f"h2h_{cs}_rate"]
    df[f"mkt_prob_{cs}"] = 1.0 / df[f"odd_{cs}"]
    _ov = (1/df["odd_h"]) + (1/df["odd_a"]) + (1/df[f"odd_{cs}"])
    df[f"mkt_prob_{cs}_norm"] = df[f"mkt_prob_{cs}"] / _ov

df["mkt_prob_0x1"] = 1.0 / df["odd_0x1"]
df["mkt_edge_signal"] = (1.0 / df["odd_h"]) - df["mkt_prob_0x1"]

df["mkt_prob_1x0"] = 1.0 / df["odd_1x0"]
df["mkt_edge_signal_1x0"] = (1.0 / df["odd_h"]) - df["mkt_prob_1x0"]

COMMISSION = 0.05
STAKE = 100.0

methods = [
    ("Lay 0x0", "0x0", "modelo_lay_0x0_rf_v2.pkl", "scaler_lay_0x0_rf_v2.pkl", "features_lay_0x0_rf_v2.pkl", 6.0, 16.0),
    ("Lay 0x1", "0x1", "modelo_lay_0x1_rf_v2.pkl", "scaler_lay_0x1_rf_v2.pkl", "features_lay_0x1_rf_v2.pkl", 6.0, 16.0),
    ("Lay 0x2", "0x2", "modelo_lay_0x2_rf_v2.pkl", "scaler_lay_0x2_rf_v2.pkl", "features_lay_0x2_rf_v2.pkl", 6.0, 16.0),
    ("Lay 1x0", "1x0", "modelo_lay_1x0_rf_v2.pkl", "scaler_lay_1x0_rf_v2.pkl", "features_lay_1x0_rf_v2.pkl", 6.0, 16.0),
    ("Lay 2x0", "2x0", "modelo_lay_2x0_rf_v2.pkl", "scaler_lay_2x0_rf_v2.pkl", "features_lay_2x0_rf_v2.pkl", 6.0, 16.0),
]

# Filtrar 2026
df_2026 = df[(df["Date"] >= "2026-01-01") & (df["Date"] <= "2026-08-20")].copy()

results = []

for name, cs, mod_f, sca_f, feat_f, odd_min, odd_max in methods:
    model = joblib.load(mod_f)
    scaler = joblib.load(sca_f)
    features = joblib.load(feat_f)
    
    df_clean = df_2026.copy()
    if cs == "1x0" and "mkt_edge_signal" in features:
        df_clean["mkt_edge_signal"] = df_clean["mkt_edge_signal_1x0"]
        
    df_clean = df_clean.replace([np.inf, -np.inf], np.nan).dropna(subset=features + [f"odd_{cs}"]).copy()
    if df_clean.empty:
        continue
        
    X = scaler.transform(df_clean[features])
    df_clean["Prob_ML"] = model.predict_proba(X)[:, 1]
    
    df_clean["ev_lay"] = df_clean["Prob_ML"] * (1 - COMMISSION) - (1 - df_clean["Prob_ML"]) * (df_clean[f"odd_{cs}"] - 1.0)
    
    aprov = df_clean[
        (df_clean[f"odd_{cs}"] >= odd_min) &
        (df_clean[f"odd_{cs}"] <= odd_max) &
        (df_clean["Prob_ML"] >= 0.85) &
        (df_clean["ev_lay"] >= 0.03)
    ].copy()
    
    tot = len(aprov)
    if tot == 0:
        results.append({
            "Método": name, "Entradas": 0, "Greens": 0, "Reds": 0, "Win Rate": "0.0%", "Break-even WR": "0.0%", "Odd Média": "0.0", "Lucro Líquido (R$)": "R$ 0,00", "Profit Factor": "0.00", "Status": "Sem Sinais Qualificados"
        })
        continue
        
    grn = (aprov[f"_{cs}_flag"] == 0).sum()
    red = (aprov[f"_{cs}_flag"] == 1).sum()
    wr = (grn / tot) * 100.0
    
    # Odd de Lay real da Betfair (com spread de bolsa)
    pnl_arr = np.where(aprov[f"_{cs}_flag"] == 0, STAKE * (1 - COMMISSION), -(aprov[f"odd_{cs}"] - 1.0) * STAKE)
    tot_pnl = pnl_arr.sum()
    lucro_b = pnl_arr[pnl_arr > 0].sum()
    perda_b = abs(pnl_arr[pnl_arr < 0].sum())
    pf = lucro_b / perda_b if perda_b > 0 else 0.0
    
    avg_odd = aprov[f"odd_{cs}"].mean()
    be_wr = ((avg_odd - 1.0) / (avg_odd - 0.05)) * 100.0
    
    results.append({
        "Método": name,
        "Entradas 2026": tot,
        "Greens": grn,
        "Reds": red,
        "Win Rate Real": f"{wr:.2f}%",
        "Break-even WR": f"{be_wr:.2f}%",
        "Odd Média": f"{avg_odd:.2f}",
        "Lucro Líquido (Stake R$ 100)": f"R$ {tot_pnl:,.2f}",
        "Profit Factor": f"{pf:.2f}",
        "Diagnóstico": "🟢 Positivo" if tot_pnl > 0 and pf > 1.0 else "🔴 Negativo"
    })

print("\n" + "="*120, flush=True)
print("📊 RESULTADOS OFICIAIS DE 2026 — OS 5 MÉTODOS LAY CS (REFATORADOS SEM VAZAMENTO):", flush=True)
print("="*120, flush=True)
df_res = pd.DataFrame(results)
print(df_res.to_string(index=False), flush=True)
