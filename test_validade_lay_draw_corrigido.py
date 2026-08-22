import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np, joblib
import hist_rf_loader, unicodedata, re

print("=== INICIANDO TESTE DE VALIDAÇÃO DO LAY DRAW (MOTOR CORRIGIDO 100%) ===", flush=True)

# 1. Carregar base oficial com todas as métricas ricas
df = hist_rf_loader.load_hist_rf()
print(f"[+] Base carregada: {len(df)} partidas", flush=True)

# 2. Carregar artefatos do modelo
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
df["_draw_flag"] = (df["Goals_H_FT"] == df["Goals_A_FT"]).astype(float)
df["c_Home"] = df["Home"].map(_canon)
df["c_Away"] = df["Away"].map(_canon)

# Odds
df["odd_d"] = pd.to_numeric(df.get("Odd_D_FT", np.nan), errors="coerce")
df["odd_h"] = pd.to_numeric(df.get("Odd_H_FT", np.nan), errors="coerce")
df["odd_a"] = pd.to_numeric(df.get("Odd_A_FT", np.nan), errors="coerce")

# Filtro de ano 2026
df_2026 = df[(df["Date"] >= "2026-01-01") & (df["Date"] <= "2026-08-20") & df["odd_d"].notna()].copy()
print(f"[+] Total de jogos em 2026: {len(df_2026)} partidas", flush=True)

# Simular a estratégia com parâmetros realistas
COMMISSION = 0.05
STAKE = 100.0

# Sweet spot testado: Odd 3.20 a 4.20, Favorito <= 2.10
cands = df_2026[(df_2026["odd_d"] >= 3.20) & (df_2026["odd_d"] <= 4.20) & ((df_2026["odd_h"] <= 2.10) | (df_2026["odd_a"] <= 2.10))].copy()
print(f"[+] Candidatos filtrados por Odds Sweet Spot (3.20 a 4.20 + Fav <= 2.10): {len(cands)} jogos", flush=True)

tot_jogos = len(cands)
grn = (cands["_draw_flag"] == 0).sum()
red = (cands["_draw_flag"] == 1).sum()
wr = (grn / tot_jogos) * 100 if tot_jogos > 0 else 0

pnl_arr = np.where(cands["_draw_flag"] == 0, STAKE * (1 - COMMISSION), -(cands["odd_d"] - 1.0) * STAKE)
tot_pnl = pnl_arr.sum()
lucro_bruto = pnl_arr[pnl_arr > 0].sum()
perda_bruta = abs(pnl_arr[pnl_arr < 0].sum())
pf = lucro_bruto / perda_bruta if perda_bruta > 0 else 0

# Drawdown
cum_pnl = np.cumsum(pnl_arr)
peak = np.maximum.accumulate(cum_pnl)
dd = peak - cum_pnl
max_dd = np.max(dd) if len(dd) > 0 else 0

print("\n" + "="*85, flush=True)
print(f"📊 RESULTADO DO BACKTEST DO LAY DRAW EM 2026 (JANEIRO A AGOSTO):", flush=True)
print("="*85, flush=True)
print(f"Total de Entradas: {tot_jogos:,} jogos")
print(f"Greens (Não-Empate): {grn:,} jogos")
print(f"Reds (Empate): {red:,} jogos")
print(f"Taxa de Acerto Real (Win Rate): {wr:.2f}%")
print(f"Lucro Bruto dos Greens: R$ {lucro_bruto:,.2f}")
print(f"Perda Bruta dos Reds: R$ {perda_bruta:,.2f}")
print(f"LUCRO LÍQUIDO FINAL (Stake R$ 100): R$ {tot_pnl:,.2f}")
print(f"Profit Factor: {pf:.2f}")
print(f"Drawdown Máximo: R$ {max_dd:,.2f}")
print("="*85, flush=True)

# Quebra Mensal de 2026
cands["Month"] = cands["Date"].dt.strftime("%Y-%m")
cands["pnl"] = pnl_arr

resumo_mes = []
for m, g in cands.groupby("Month"):
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
