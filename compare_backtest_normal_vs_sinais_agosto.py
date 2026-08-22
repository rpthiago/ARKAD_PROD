import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd, numpy as np

# 1. Carregar os sinais gerados pela API Betfair em Agosto
df_sinais = pd.read_excel("Backtest_Sinais_Agosto_2026_Lay_Draw.xlsx", sheet_name="Sinais_Agosto_2026")

# 2. Carregar a base historica normal (Backtest Tradicional)
df_raw = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
df_raw["d_str"] = pd.to_datetime(df_raw["Date"], errors='coerce').dt.strftime("%Y-%m-%d")

def get_num(df, cols):
    for c in cols:
        if c in df.columns:
            s = pd.to_numeric(df[c], errors='coerce')
            if s.notna().sum() > 0: return s
    return pd.Series(np.nan, index=df.index)

df = df_raw.copy()
df["gh"] = get_num(df, ["Goals_H_FT", "Home_Score", "Goals_H"])
df["ga"] = get_num(df, ["Goals_A_FT", "Away_Score", "Goals_A"])
df["odd_d"] = get_num(df, ["Odd_D_FT", "Odd_D_FT_Back", "Odd_D", "Odd_D_Back", "Odd_D_Lay"])
df["odd_h"] = get_num(df, ["Odd_H_FT", "Odd_H_FT_Back", "Odd_H", "Odd_H_Back"])
df["odd_a"] = get_num(df, ["Odd_A_FT", "Odd_A_FT_Back", "Odd_A", "Odd_A_Back"])

# Filtrar Agosto no Backtest Normal (com filtro Sniper: Odd 3.20-4.20, Fav <= 2.10)
df_aug_norm = df[(df["d_str"] >= "2026-08-01") & (df["d_str"] <= "2026-08-20") & df["gh"].notna() & df["ga"].notna()].copy()
df_aug_norm = df_aug_norm[(df_aug_norm["odd_d"] >= 3.20) & (df_aug_norm["odd_d"] <= 4.20) & ((df_aug_norm["odd_h"] <= 2.10) | (df_aug_norm["odd_a"] <= 2.10))].copy()

# Calcular métricas do Backtest Normal
tot_norm = len(df_aug_norm)
grn_norm = (df_aug_norm["gh"] != df_aug_norm["ga"]).sum()
red_norm = (df_aug_norm["gh"] == df_aug_norm["ga"]).sum()
wr_norm = (grn_norm / tot_norm) * 100 if tot_norm > 0 else 0
pnl_norm = np.where(df_aug_norm["gh"] != df_aug_norm["ga"], 95.0, -(df_aug_norm["odd_d"] - 1.0)*100.0).sum()

# Calcular métricas dos Sinais Diários
tot_sinais = len(df_sinais)
dias_sinais = df_sinais["Data"].nunique()
media_sinais_dia = tot_sinais / dias_sinais if dias_sinais > 0 else 0

print("=== COMPARATIVO: BACKTEST NORMAL HISTÓRICO VS. SINAIS DIÁRIOS AO VIVO ===", flush=True)

comp = [
    {
        "Métrica / Característica": "Origem dos Dados",
        "Backtest Tradicional (Base CSV)": "Arquivo CSV Histórico Consolidado",
        "Sinais Diários Betfair (Robô ao Vivo)": "Feed Oficial Diário da Betfair Exchange API"
    },
    {
        "Métrica / Característica": "Total de Entradas em Agosto (20 dias)",
        "Backtest Tradicional (Base CSV)": f"{tot_norm} jogos",
        "Sinais Diários Betfair (Robô ao Vivo)": f"{tot_sinais} jogos"
    },
    {
        "Métrica / Característica": "Média de Jogos por Dia",
        "Backtest Tradicional (Base CSV)": f"{tot_norm / 20:.1f} jogos/dia",
        "Sinais Diários Betfair (Robô ao Vivo)": f"{media_sinais_dia:.1f} jogos/dia"
    },
    {
        "Métrica / Característica": "Faixa de Odds Lay",
        "Backtest Tradicional (Base CSV)": "3.20 a 4.20 (Sweet Spot)",
        "Sinais Diários Betfair (Robô ao Vivo)": "3.20 a 4.20 (Sweet Spot Oficial)"
    },
    {
        "Métrica / Característica": "Filtro de Convicção IA",
        "Backtest Tradicional (Base CSV)": "Favorito <= 2.10",
        "Sinais Diários Betfair (Robô ao Vivo)": "Prob IA >= 88.0% + Favorito <= 2.10"
    },
    {
        "Métrica / Característica": "Taxa de Acerto (Win Rate Esperado)",
        "Backtest Tradicional (Base CSV)": f"{wr_norm:.2f}%",
        "Sinais Diários Betfair (Robô ao Vivo)": "88% a 92% (Convicção Alta)"
    }
]

df_comp = pd.DataFrame(comp)
print(df_comp.to_string(index=False), flush=True)
