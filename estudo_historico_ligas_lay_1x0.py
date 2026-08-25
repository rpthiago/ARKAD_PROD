import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np
import hist_rf_loader, joblib

print("==================================================================", flush=True)
print("     HISTÓRICO DO LAY 1X0 EM MESES ANTERIORES & POR LIGA        ", flush=True)
print("==================================================================", flush=True)

df_raw = hist_rf_loader.load_hist_rf()
df = df_raw.dropna(subset=["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT"]).copy()
df["Date"] = pd.to_datetime(df["Date"])
df = df.sort_values("Date", kind="mergesort").reset_index(drop=True)

odd_col = "Odd_CS_1x0_Lay" if "Odd_CS_1x0_Lay" in df.columns else "Odd_CS_1x0"
df["Odd_1x0"] = pd.to_numeric(df[odd_col], errors="coerce")
df["Odd_H_FT"] = pd.to_numeric(df.get("Odd_H_FT", np.nan), errors="coerce")
df = df[(df["Odd_1x0"] >= 5.0) & (df["Odd_1x0"] <= 25.0)].copy()

df["is_1x0"] = ((df["Goals_H_FT"] == 1) & (df["Goals_A_FT"] == 0)).astype(float)
df["lay_win"] = (df["is_1x0"] == 0).astype(int)

MODEL_PATH = "modelo_lay_1x0_rf_v2.pkl"
SCALER_PATH = "scaler_lay_1x0_rf_v2.pkl"
FEATURES_PATH = "features_lay_1x0_rf_v2.pkl"

model = joblib.load(MODEL_PATH)
scaler = joblib.load(SCALER_PATH)
features = joblib.load(FEATURES_PATH)

def _decay_roll_grouped_unshifted(df_in, group_col, val_col, window=6, alpha=0.25):
    g = df_in.groupby(group_col)[val_col]
    numer = np.zeros(len(df_in)); count = np.zeros(len(df_in)); wsum = 0.0
    for j in range(window):
        sj = g.shift(j + 1)
        ej = np.exp(-alpha * j)
        m = sj.notna().to_numpy()
        numer += np.where(m, np.nan_to_num(sj.to_numpy()) * ej, 0.0)
        count += m
        wsum += ej
    res = numer / wsum
    res[count < 3] = np.nan
    return pd.Series(res, index=df_in.index)

df["won_H"] = (df["Goals_H_FT"] > df["Goals_A_FT"]).astype(float)
df["won_A"] = (df["Goals_A_FT"] > df["Goals_H_FT"]).astype(float)
df["score10_H"] = ((df["Goals_H_FT"] == 1) & (df["Goals_A_FT"] == 0)).astype(float)
df["multi_H"] = (df["Goals_H_FT"] >= 2).astype(float)
df["concede0_A"] = (df["Goals_H_FT"] == 0).astype(float)

df["H_h_WR"] = _decay_roll_grouped_unshifted(df, "Home", "won_H")
df["H_h_goals_rate"] = _decay_roll_grouped_unshifted(df, "Home", "Goals_H_FT")
df["H_h_score10_rate"] = _decay_roll_grouped_unshifted(df, "Home", "score10_H")
df["H_h_multi_goal"] = _decay_roll_grouped_unshifted(df, "Home", "multi_H")

df["A_a_WR"] = _decay_roll_grouped_unshifted(df, "Away", "won_A")
df["A_a_goals_rate"] = _decay_roll_grouped_unshifted(df, "Away", "Goals_A_FT")
df["A_a_concede0"] = _decay_roll_grouped_unshifted(df, "Away", "concede0_A")

df["spread_forca"] = df["H_h_WR"] - df["A_a_WR"]
df["total_goals_proxy"] = df["H_h_goals_rate"] + df["A_a_goals_rate"]
df["mkt_prob_1x0"] = 1.0 / df["Odd_1x0"]
df["mkt_edge_signal"] = (1.0 / df["Odd_H_FT"].replace(0, np.nan).fillna(2.0)) - df["mkt_prob_1x0"]

valid = df[features].notna().all(axis=1)
df_eval = df[valid].copy()

X_scaled = scaler.transform(df_eval[features])
df_eval["prob_lay_win"] = model.predict_proba(X_scaled)[:, 1]

COMMISSION = 0.05
STAKE = 100.0
df_eval["ev_lay"] = df_eval["prob_lay_win"] * (1.0 - COMMISSION) - (1.0 - df_eval["prob_lay_win"]) * (df_eval["Odd_1x0"] - 1.0)
df_eval["pnl_lay"] = np.where(df_eval["lay_win"] == 1, STAKE * (1.0 - COMMISSION), -STAKE * (df_eval["Odd_1x0"] - 1.0))

# Filtros oficiais
cond = (
    (df_eval["Odd_1x0"] >= 6.0) &
    (df_eval["Odd_1x0"] <= 16.0) &
    (df_eval["prob_lay_win"] >= 0.85) &
    (df_eval["ev_lay"] >= 0.03)
)

sub = df_eval[cond].copy()
sub["Year"] = sub["Date"].dt.year
sub["Month"] = sub["Date"].dt.to_period("M")

print(f"\n[+] Total de jogos aprovados no filtro do Lay 1x0 (2025-2026): {len(sub[sub['Year']>=2025])}")

print("\n--- PERFORMANCE MÊS A MÊS DO LAY 1X0 (2025 E 2026) ---")
monthly = sub[sub["Year"] >= 2025].groupby("Month").agg(
    jogos=("lay_win", "count"),
    greens=("lay_win", "sum"),
    reds=("lay_win", lambda x: (x == 0).sum()),
    odd_med=("Odd_1x0", "mean"),
    lucro=("pnl_lay", "sum")
).reset_index()
monthly["wr"] = (monthly["greens"] / monthly["jogos"]) * 100.0
monthly["be_wr"] = ((monthly["odd_med"] - 1.0) / (monthly["odd_med"] - 0.05)) * 100.0
monthly["roi"] = (monthly["lucro"] / (monthly["jogos"] * STAKE)) * 100.0

print(monthly[["Month", "jogos", "greens", "reds", "wr", "be_wr", "lucro", "roi"]].to_string(index=False))

print("\n--- DESEMPENHO DAS LIGAS COM MAIS REDS EM MESES ANTERIORES (JAN/2025 a JUL/2026) ---")
problem_leagues = [
    "ARGENTINA 2", "BRAZIL 2", "BRAZIL 3", "PORTUGAL 1", "PORTUGAL 2",
    "FRANCE 2", "SPAIN 2", "URUGUAY 1", "COLOMBIA 2", "SCOTLAND 2"
]
sub_prev = sub[(sub["Date"] >= "2025-01-01") & (sub["Date"] < "2026-08-01") & (sub["League"].isin(problem_leagues))].copy()

ligas_prev = sub_prev.groupby("League").agg(
    jogos=("lay_win", "count"),
    greens=("lay_win", "sum"),
    reds=("lay_win", lambda x: (x == 0).sum()),
    odd_med=("Odd_1x0", "mean"),
    lucro=("pnl_lay", "sum")
).reset_index()
ligas_prev["wr"] = (ligas_prev["greens"] / ligas_prev["jogos"]) * 100.0
ligas_prev["be_wr"] = ((ligas_prev["odd_med"] - 1.0) / (ligas_prev["odd_med"] - 0.05)) * 100.0
ligas_prev["roi"] = (ligas_prev["lucro"] / (ligas_prev["jogos"] * STAKE)) * 100.0

print(ligas_prev[["League", "jogos", "greens", "reds", "wr", "be_wr", "lucro", "roi"]].sort_values("lucro").to_string(index=False))
