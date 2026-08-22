import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

print("=== VERIFICANDO IMPACTO DO FILTRO LIGA_DRAW_RATE NO BACKTEST DO LAY DRAW ===", flush=True)

df_hist = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
df_hist["d_str"] = pd.to_datetime(df_hist["Date"], errors='coerce').dt.strftime("%Y-%m-%d")

# Pegar colunas
def get_num(df, cols):
    for c in cols:
        if c in df.columns:
            s = pd.to_numeric(df[c], errors='coerce')
            if s.notna().sum() > 0:
                return s
    return pd.Series(np.nan, index=df.index)

df_hist["gh_ft"] = get_num(df_hist, ["Goals_H_FT", "Home_Score", "Goals_H"])
df_hist["ga_ft"] = get_num(df_hist, ["Goals_A_FT", "Away_Score", "Goals_A"])
df_hist["odd_d"] = get_num(df_hist, ["Odd_D_FT", "Odd_D_FT_Back", "Odd_D"])

df_hist = df_hist[df_hist["gh_ft"].notna() & df_hist["ga_ft"].notna() & (df_hist["odd_d"] >= 3.0) & (df_hist["odd_d"] <= 5.50)].copy()
df_hist["_draw"] = (df_hist["gh_ft"] == df_hist["ga_ft"]).astype(float)

# Calcular taxa de empate da liga
df_hist["Date_dt"] = pd.to_datetime(df_hist["Date"], errors='coerce')
df_hist = df_hist.sort_values(["League", "Date_dt"]).reset_index(drop=True)
df_hist["liga_draw_rate"] = df_hist.groupby("League")["_draw"].transform(
    lambda x: x.shift(1).rolling(100, min_periods=20).mean()
)

def run_backtest_with_cutoff(df_input, cutoff_val):
    df = df_input.copy()
    if cutoff_val > 0:
        # Se cutoff > 0, filtra liga_draw_rate <= cutoff_val (ou aceita se for liga sem histórico)
        cond = (df["liga_draw_rate"].isna()) | (df["liga_draw_rate"] <= cutoff_val)
        df = df[cond].copy()
        
    tot = len(df)
    if tot == 0:
        return 0, 0, 0, 0.0, 0.0, 0.0
        
    grn = (df["_draw"] == 0).sum()
    red = (df["_draw"] == 1).sum()
    wr = (grn / tot) * 100.0
    
    # PnL
    pnl_series = np.where(df["_draw"] == 0, 95.0, -(df["odd_d"] - 1.0) * 100.0)
    pnl_total = pnl_series.sum()
    
    # Profit Factor
    lucro = np.where(pnl_series > 0, pnl_series, 0).sum()
    perda = abs(np.where(pnl_series < 0, pnl_series, 0).sum())
    pf = (lucro / perda) if perda > 0 else np.nan
    
    return tot, grn, red, wr, pnl_total, pf

# Testar nos 3 periodos
periodos = [
    ("Agosto de 2026 (01 a 20/08)", "2026-08-01", "2026-08-20"),
    ("Ano 2026 Completo", "2026-01-01", "2026-08-20"),
    ("Ano 2025 Completo (Out-of-Sample)", "2025-01-01", "2025-12-31")
]

cutoffs = [
    ("Sem Trava (Apenas Odds 3.0-5.50)", 0.0),
    ("Trava Calibrada (Taxa Liga <= 0.36)", 0.36),
    ("Trava Antiga Excessiva (Taxa Liga <= 0.23)", 0.23)
]

for p_name, d_ini, d_fim in periodos:
    df_p = df_hist[(df_hist["d_str"] >= d_ini) & (df_hist["d_str"] <= d_fim)].copy()
    print(f"\n" + "="*85, flush=True)
    print(f"📊 PERÍODO: {p_name}", flush=True)
    print("="*85, flush=True)
    
    rows = []
    for c_name, c_val in cutoffs:
        tot, grn, red, wr, pnl, pf = run_backtest_with_cutoff(df_p, c_val)
        rows.append({
            "Configuração": c_name,
            "Entradas": tot,
            "Greens": grn,
            "Reds": red,
            "Win Rate %": f"{wr:.2f}%",
            "Lucro Líquido R$": f"R$ {pnl:,.2f}",
            "Profit Factor": f"{pf:.2f}"
        })
    df_res = pd.DataFrame(rows)
    print(df_res.to_string(index=False), flush=True)
