import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np, joblib
import hist_rf_loader, unicodedata, re

print("=== EXECUTANDO BACKTEST OFICIAL 2026 DO LAY DRAW (ALTA VELOCIDADE & PRECISÃO) ===", flush=True)

# 1. Carregar base oficial
df = hist_rf_loader.load_hist_rf()
print(f"[+] Base histórica carregada: {len(df):,} partidas", flush=True)

# 2. Carregar modelo e features
MODEL_PATH    = "modelo_lay_draw_rf_v2.pkl"
SCALER_PATH   = "scaler_lay_draw_rf_v2.pkl"
FEATURES_PATH = "features_lay_draw_rf_v2.pkl"

model    = joblib.load(MODEL_PATH)
scaler   = joblib.load(SCALER_PATH)
features = joblib.load(FEATURES_PATH)

def _canon(s):
    if pd.isna(s) or not s: return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df = df.dropna(subset=["Goals_H_FT", "Goals_A_FT", "Date", "Home", "Away"]).copy()
df = df.sort_values("Date", kind="mergesort").reset_index(drop=True)

df["_draw_flag"] = (df["Goals_H_FT"] == df["Goals_A_FT"]).astype(float)
df["won_H"] = (df["Goals_H_FT"] > df["Goals_A_FT"]).astype(float)
df["won_A"] = (df["Goals_A_FT"] > df["Goals_H_FT"]).astype(float)

df["c_Home"] = df["Home"].map(_canon)
df["c_Away"] = df["Away"].map(_canon)
df["h2h_pair"] = [tuple(sorted(x)) for x in zip(df["c_Home"], df["c_Away"])]

# Odds
df["odd_d"] = pd.to_numeric(df.get("Odd_D_FT", np.nan), errors="coerce")
df["odd_h"] = pd.to_numeric(df.get("Odd_H_FT", np.nan), errors="coerce")
df["odd_a"] = pd.to_numeric(df.get("Odd_A_FT", np.nan), errors="coerce")

stat_cols = [
    "Goals_H_FT", "Goals_A_FT", "xGOT_H_FT", "xGOT_A_FT",
    "xGOT_Faced_H_FT", "xGOT_Faced_A_FT",
    "Goals_Prevented_H_FT", "Goals_Prevented_A_FT",
    "Big_Chances_H_FT", "Big_Chances_A_FT",
    "Shots_On_Target_H_FT", "Shots_On_Target_A_FT",
    "Possession_H_FT", "Possession_A_FT"
]
for c in stat_cols:
    df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0.0) if c in df.columns else 0.0

print("[+] Computando features por mando (Home view e Away view)...", flush=True)

# 1. Features HOME (jogando em casa)
h_cols_in = ["Goals_H_FT", "Goals_A_FT", "xGOT_H_FT", "xGOT_Faced_H_FT", "Goals_Prevented_H_FT", "Big_Chances_H_FT", "Shots_On_Target_H_FT", "Possession_H_FT", "won_H", "_draw_flag"]
h_cols_out = ["H_h_Gf", "H_h_Gc", "H_h_xGOT", "H_h_xGOT_faced", "H_h_GP", "H_h_BC", "H_h_SoT", "H_h_Poss", "H_h_WR", "H_h_draw_rate"]

g_h = df.groupby("c_Home")
wsum = sum(np.exp(-0.25 * (j - 1)) for j in range(1, 7))

for src, dst in zip(h_cols_in, h_cols_out):
    numer = np.zeros(len(df))
    count = np.zeros(len(df))
    for j in range(1, 7):
        sj = g_h[src].shift(j).to_numpy()
        ej = np.exp(-0.25 * (j - 1))
        m = ~np.isnan(sj)
        numer += np.where(m, sj * ej, 0.0)
        count += m
    res = numer / wsum
    res[count < 3] = np.nan
    df[dst] = res

# 2. Features AWAY (jogando fora)
a_cols_in = ["Goals_A_FT", "Goals_H_FT", "xGOT_A_FT", "xGOT_Faced_A_FT", "Goals_Prevented_A_FT", "Big_Chances_A_FT", "Shots_On_Target_A_FT", "Possession_A_FT", "won_A", "_draw_flag"]
a_cols_out = ["A_a_Gf", "A_a_Gc", "A_a_xGOT", "A_a_xGOT_faced", "A_a_GP", "A_a_BC", "A_a_SoT", "A_a_Poss", "A_a_WR", "A_a_draw_rate"]

g_a = df.groupby("c_Away")
for src, dst in zip(a_cols_in, a_cols_out):
    numer = np.zeros(len(df))
    count = np.zeros(len(df))
    for j in range(1, 7):
        sj = g_a[src].shift(j).to_numpy()
        ej = np.exp(-0.25 * (j - 1))
        m = ~np.isnan(sj)
        numer += np.where(m, sj * ej, 0.0)
        count += m
    res = numer / wsum
    res[count < 3] = np.nan
    df[dst] = res

# 3. Liga Draw Rate (rolling 100, min 20)
print("[+] Computando Liga Draw Rate e H2H Draw Rate...", flush=True)
df["liga_draw_rate"] = df.groupby("League")["_draw_flag"].transform(
    lambda s: s.shift(1).rolling(100, min_periods=20).mean())

# 4. H2H Draw Rate (rolling 8, min 2)
df["h2h_draw_rate"] = df.groupby("h2h_pair")["_draw_flag"].transform(
    lambda s: s.shift(1).rolling(8, min_periods=2).mean())

# Features Combinadas
df["total_WR"]       = df["H_h_WR"] + df["A_a_WR"]
df["wr_diff"]        = abs(df["H_h_WR"] - df["A_a_WR"])
df["draw_rate_prod"] = df["H_h_draw_rate"] * df["A_a_draw_rate"]
df["draw_rate_mean"] = (df["H_h_draw_rate"] + df["A_a_draw_rate"]) / 2
df["total_xGOT"]     = df["H_h_xGOT"] + df["A_a_xGOT"]
df["xGOT_diff"]      = abs(df["H_h_xGOT"] - df["A_a_xGOT"])
df["total_Gf"]       = df["H_h_Gf"] + df["A_a_Gf"]
df["gf_diff"]        = abs(df["H_h_Gf"] - df["A_a_Gf"])
df["decisive_score"] = df["total_WR"] * df["wr_diff"]

df["mkt_prob_draw"]     = 1.0 / df["odd_d"]
_ov = 1.0 / df["odd_h"] + 1.0 / df["odd_d"] + 1.0 / df["odd_a"]
df["mkt_prob_draw_norm"] = df["mkt_prob_draw"] / _ov
df["mkt_overvalue_draw"] = df["mkt_prob_draw"] - df["draw_rate_mean"]

# Filtrar ano 2026 com dropna estrito nas 34 features (SEM fallbacks)
df_2026 = df[(df["Date"] >= "2026-01-01") & (df["Date"] <= "2026-08-20")].copy()
print(f"[+] Total de jogos em 2026 antes do dropna: {len(df_2026):,}", flush=True)

df_2026_clean = df_2026.dropna(subset=features).copy()
print(f"[+] Jogos em 2026 com todas as 34 features completas (dropna): {len(df_2026_clean):,}", flush=True)

# Rodar predição da IA
X = scaler.transform(df_2026_clean[features])
df_2026_clean["Prob_ML"] = model.predict_proba(X)[:, 1]

# EV do Lay
COMMISSION = 0.05
STAKE = 100.0
df_2026_clean["ev_lay"] = df_2026_clean["Prob_ML"] * (1 - COMMISSION) - (1 - df_2026_clean["Prob_ML"]) * (df_2026_clean["odd_d"] - 1.0)

# Filtros Oficiais do Lay Draw:
# 1. Odd Sweet Spot 3.20 a 4.20
# 2. Favorito Claro (Odd_H <= 2.10 ou Odd_A <= 2.10)
# 3. Prob IA >= 75.0%
# 4. EV >= +0.03
# 5. Liga Draw Rate <= 0.36
aprovados = df_2026_clean[
    (df_2026_clean["odd_d"] >= 3.20) & 
    (df_2026_clean["odd_d"] <= 4.20) & 
    ((df_2026_clean["odd_h"] <= 2.10) | (df_2026_clean["odd_a"] <= 2.10)) &
    (df_2026_clean["Prob_ML"] >= 0.75) &
    (df_2026_clean["ev_lay"] >= 0.03) &
    (df_2026_clean["liga_draw_rate"] <= 0.36)
].copy()

tot_aprov = len(aprovados)
grn = (aprovados["_draw_flag"] == 0).sum()
red = (aprovados["_draw_flag"] == 1).sum()
wr = (grn / tot_aprov) * 100.0 if tot_aprov > 0 else 0

pnl_arr = np.where(aprovados["_draw_flag"] == 0, STAKE * (1 - COMMISSION), -(aprovados["odd_d"] - 1.0) * STAKE)
aprovados["pnl"] = pnl_arr
tot_pnl = pnl_arr.sum()
lucro_bruto = pnl_arr[pnl_arr > 0].sum()
perda_bruta = abs(pnl_arr[pnl_arr < 0].sum())
pf = lucro_bruto / perda_bruta if perda_bruta > 0 else 0

cum_pnl = np.cumsum(pnl_arr)
peak = np.maximum.accumulate(cum_pnl)
dd = peak - cum_pnl
max_dd = np.max(dd) if len(dd) > 0 else 0

print("\n" + "="*85, flush=True)
print(f"📊 BACKTEST 2026 OFICIAL DO LAY DRAW (MOTOR 100% CORRIGIDO / DROPNA ESTRITO):", flush=True)
print("="*85, flush=True)
print(f"Total de Entradas Selecionadas: {tot_aprov:,} jogos")
print(f"Greens (Não-Empate): {grn:,} jogos")
print(f"Reds (Empate): {red:,} jogos")
print(f"Taxa de Acerto Real (Win Rate): {wr:.2f}%")
print(f"Lucro Bruto dos Greens: R$ {lucro_bruto:,.2f}")
print(f"Perda Bruta dos Reds: R$ {perda_bruta:,.2f}")
print(f"LUCRO LÍQUIDO FINAL (Stake R$ 100): R$ {tot_pnl:,.2f}")
print(f"Profit Factor: {pf:.2f}")
print(f"Drawdown Máximo: R$ {max_dd:,.2f}")
print("="*85, flush=True)

# Quebra Mensal
aprovados["Month"] = aprovados["Date"].dt.strftime("%Y-%m")
resumo_mes = []
for m, g in aprovados.groupby("Month"):
    t_m = len(g)
    g_m = (g["_draw_flag"] == 0).sum()
    r_m = (g["_draw_flag"] == 1).sum()
    wr_m = (g_m / t_m) * 100 if t_m > 0 else 0
    pnl_m = g["pnl"].sum()
    resumo_mes.append({
        "Mês": m,
        "Jogos": t_m,
        "Greens": g_m,
        "Reds": r_m,
        "Win Rate": f"{wr_m:.1f}%",
        "Lucro Líquido R$": f"R$ {pnl_m:,.2f}"
    })

df_mes = pd.DataFrame(resumo_mes)
print("\n📅 DESEMPENHO MÊS A MÊS EM 2026:")
print(df_mes.to_string(index=False), flush=True)
