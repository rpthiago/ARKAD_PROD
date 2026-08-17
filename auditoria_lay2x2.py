"""
AUDITORIA PROFUNDA E BACKTEST EXTENDIDO - MÉTODO LAY 2X2
ARKAD_PROD

Este script executa um estudo estatístico detalhado em 50.000+ partidas históricas,
testando diferentes regras de filtros, faixas de odds, ligas e modelos de gestão de banca.
"""

import pandas as pd
import numpy as np
from pathlib import Path

print("==========================================================================")
print("     AUDITORIA PROFUNDA & BACKTEST HISTÓRICO - ESTRATÉGIA LAY 2X2")
print("==========================================================================\n")

df = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH.csv", low_memory=False)

df["Odd_CS_2x2_Lay"] = pd.to_numeric(df["Odd_CS_2x2_Lay"], errors="coerce")
df["Odd_H"] = pd.to_numeric(df.get("Odd_H_FT_Back", df.get("Odd_H")), errors="coerce")
df["Odd_A"] = pd.to_numeric(df.get("Odd_A_FT_Back", df.get("Odd_A")), errors="coerce")
df["Odd_Under25"] = pd.to_numeric(df.get("Odd_Under25_FT_Back", df.get("Odd_Under25")), errors="coerce")
df["Odd_Over25"] = pd.to_numeric(df.get("Odd_Over25_FT_Back", df.get("Odd_Over25")), errors="coerce")

df["gh"] = pd.to_numeric(df["Goals_H_FT"], errors="coerce")
df["ga"] = pd.to_numeric(df["Goals_A_FT"], errors="coerce")

# Jogos com dados válidos
df_valid = df[df["gh"].notna() & df["ga"].notna() & df["Odd_CS_2x2_Lay"].notna() & (df["Odd_CS_2x2_Lay"] > 1.0)].copy()
df_valid["is_2x2"] = (df_valid["gh"] == 2) & (df_valid["ga"] == 2)
df_valid["liga"] = df_valid.get("League", df_valid.get("Liga", df_valid.get("Div", "Desconhecida")))

total_jogos = len(df_valid)
total_2x2 = df_valid["is_2x2"].sum()
pct_2x2 = (total_2x2 / total_jogos) * 100.0

print(f"Total de Partidas Analisadas com Odds Betfair Validas: {total_jogos:,}")
print(f"Total de Placares 2x2 Ocorridos no Futebol: {total_2x2:,} ({pct_2x2:.2f}%)")
print(f"Taxa de Acerto Bruta (Sem Filtro): {100.0 - pct_2x2:.2f}%\n")

# --------------------------------------------------------------------------
# ESTUDO 1: COMPARAÇÃO DE MODELOS DE GESTÃO DE BANCA (FAIXA ODD 8.0 A 14.0)
# --------------------------------------------------------------------------
print("==========================================================================")
print("ESTUDO 1: IMPACTO DO MODELO DE GESTÃO DE BANCA (ODD LAY 8.0 A 14.0)")
print("==========================================================================")

df_faixa = df_valid[(df_valid["Odd_CS_2x2_Lay"] >= 8.0) & (df_valid["Odd_CS_2x2_Lay"] <= 14.0)].copy()
n_faixa = len(df_faixa)
reds_faixa = df_faixa["is_2x2"].sum()
greens_faixa = n_faixa - reds_faixa
wr_faixa = (greens_faixa / n_faixa) * 100.0

# 1. Stake Fixa R$ 100
pnl_stake100 = (greens_faixa * 95.0) - (df_faixa[df_faixa["is_2x2"]]["Odd_CS_2x2_Lay"] - 1.0).sum() * 100.0

# 2. Responsabilidade Fixa R$ 200 (Stake = 200 / (Odd - 1))
df_faixa["stake_liab200"] = 200.0 / (df_faixa["Odd_CS_2x2_Lay"] - 1.0)
pnl_liab200 = (df_faixa[~df_faixa["is_2x2"]]["stake_liab200"] * 0.95).sum() - (reds_faixa * 200.0)

# 3. Responsabilidade Fixa R$ 100 (Stake = 100 / (Odd - 1))
df_faixa["stake_liab100"] = 100.0 / (df_faixa["Odd_CS_2x2_Lay"] - 1.0)
pnl_liab100 = (df_faixa[~df_faixa["is_2x2"]]["stake_liab100"] * 0.95).sum() - (reds_faixa * 100.0)

print(f"Amostra Total: {n_faixa:,} jogos | Greens: {greens_faixa:,} | Reds: {reds_faixa:,} | Win Rate: {wr_faixa:.2f}%")
print(f"[Modelo A] Stake Fixa R$ 100 (Alto Risco por Red):        Lucro: R$ {pnl_stake100:,.2f}")
print(f"[Modelo B] Responsabilidade Fixa R$ 200 (Risco Controlado): Lucro: R$ {pnl_liab200:,.2f}")
print(f"[Modelo C] Responsabilidade Fixa R$ 100 (Conservador):      Lucro: R$ {pnl_liab100:,.2f}\n")

# --------------------------------------------------------------------------
# ESTUDO 2: COMPARAÇÃO DE DIFERENTES JANELAS DE ODD LAY 2X2
# --------------------------------------------------------------------------
print("==========================================================================")
print("ESTUDO 2: COMPARATIVO DE JANELAS DE ODD LAY (RESPONSABILIDADE FIXA R$ 200)")
print("==========================================================================")

janelas = [
    (6.0, 10.0),
    (8.0, 12.0),
    (8.0, 14.0),
    (10.0, 15.0),
    (12.0, 18.0),
    (14.0, 20.0),
    (15.0, 30.0)
]

print(f"{'Janela Odd Lay':<18} | {'Jogos':<7} | {'Greens':<7} | {'Reds':<5} | {'WinRate %':<10} | {'P&L Liab R$200':<15}")
print("-" * 75)

for min_o, max_o in janelas:
    sub = df_valid[(df_valid["Odd_CS_2x2_Lay"] >= min_o) & (df_valid["Odd_CS_2x2_Lay"] <= max_o)].copy()
    if len(sub) > 0:
        r_c = sub["is_2x2"].sum()
        g_c = len(sub) - r_c
        wr = (g_c / len(sub)) * 100.0
        sub["stk_200"] = 200.0 / (sub["Odd_CS_2x2_Lay"] - 1.0)
        pnl_200 = (sub[~sub["is_2x2"]]["stk_200"] * 0.95).sum() - (r_c * 200.0)
        print(f"Odd [{min_o:4.1f} - {max_o:4.1f}]  | {len(sub):<7d} | {g_c:<7d} | {r_c:<5d} | {wr:8.2f}%  | R$ {pnl_200:12.2f}")

print("\n--------------------------------------------------------------------------")
# --------------------------------------------------------------------------
# ESTUDO 3: ANÁLISE POR CAMPEONATO / LIGAS TOP PERFORMANCES
# --------------------------------------------------------------------------
print("==========================================================================")
print("ESTUDO 3: DESEMPENHO DO LAY 2X2 POR LIGA / CAMPEONATO (ODD 8.0 - 14.0)")
print("==========================================================================")

df_faixa["stk_200"] = 200.0 / (df_faixa["Odd_CS_2x2_Lay"] - 1.0)
df_faixa["pnl_row"] = np.where(~df_faixa["is_2x2"], df_faixa["stk_200"] * 0.95, -200.0)

liga_grouped = df_faixa.groupby("liga").agg(
    total_jogos=("is_2x2", "count"),
    reds=("is_2x2", "sum"),
    pnl_total=("pnl_row", "sum")
).reset_index()

liga_grouped["greens"] = liga_grouped["total_jogos"] - liga_grouped["reds"]
liga_grouped["win_rate"] = (liga_grouped["greens"] / liga_grouped["total_jogos"]) * 100.0

top_ligas = liga_grouped[liga_grouped["total_jogos"] >= 15].sort_values(by="pnl_total", ascending=False)

print("\nTOP LIGAS MAIS LUCRATIVAS NO LAY 2X2:")
print(f"{'Liga / Campeonato':<30} | {'Jogos':<6} | {'Greens':<6} | {'Reds':<5} | {'Win Rate':<9} | {'P&L (R$)':<12}")
print("-" * 78)
for _, r in top_ligas.head(15).iterrows():
    print(f"{str(r['liga'])[:30]:<30} | {r['total_jogos']:<6d} | {r['greens']:<6d} | {r['reds']:<5d} | {r['win_rate']:7.1f}% | R$ {r['pnl_total']:10.2f}")

# --------------------------------------------------------------------------
# ESTUDO 4: ANÁLISE DE SEQUÊNCIA MÁXIMA DE REDS E DRAWDOWN
# --------------------------------------------------------------------------
print("\n==========================================================================")
print("ESTUDO 4: ANÁLISE DE RISCO - SEQUÊNCIA DE REDS E DRAWDOWN")
print("==========================================================================")

df_faixa["cum_pnl"] = df_faixa["pnl_row"].cumsum()
df_faixa["peak"] = df_faixa["cum_pnl"].cummax()
df_faixa["drawdown"] = df_faixa["cum_pnl"] - df_faixa["peak"]

max_drawdown = df_faixa["drawdown"].min()

is_red_series = df_faixa["is_2x2"].astype(int)
max_consecutive_reds = (is_red_series.groupby((is_red_series != is_red_series.shift()).cumsum()).cumsum()).max()

print(f"Maior Queda de Banca Historica (Max Drawdown - Liab R$ 200): R$ {max_drawdown:,.2f}")
print(f"Maior Sequencia Continua de Reds Consecutivos: {max_consecutive_reds} Red(s) seguido(s)")
print("==========================================================================\n")
