# -*- coding: utf-8 -*-
"""
backtest_2026_lay2x2_comparativo_completo.py — Backtest Oficial & Comparativo Canônico do Lay 2x2 Quant
ARKAD_PROD

Objetivo:
- Alinhar 100% o Backtest com as Regras Operacionais do Live (Lei 2 do GEMINI.md).
- Utiliza exatamente as mesmas funções:
    - validar_entrada_lay2x2() de metodo_lay2x2_strategy.py
    - eh_jogo_ignorado_governanca() de metodo_lay2x2_strategy.py
    - Trava Top 3 Menor Odd de Betfair
- Compara a evolução de performance entre:
    1. Bruto Original (Sem filtro de ligas e sem Top 3)
    2. Com Blacklist 4 Ligas (Sérvia, Irlanda, Turquia, Escócia)
    3. Com Blacklist + Governança Oficial (Sem Periféricas, Sem Seleções, Sem Feminino, Sem Div. Baixas)
    4. LIVE CANÔNICO IDÊNTICO (Blacklist + Governança + Trava Top 3 Menor Odd)
"""

import os
import sys
import numpy as np
import pandas as pd

sys.stdout.reconfigure(encoding='utf-8')

from metodo_lay2x2_strategy import (
    validar_entrada_lay2x2,
    eh_jogo_ignorado_governanca,
    BLACKLIST_LIGAS_2X2,
    calcular_resultado_lay2x2,
    ODD_LAY_2X2_MIN,
    ODD_LAY_2X2_MAX,
    COMISSAO_BETFAIR
)

print("=" * 80)
print("=== BACKTEST CANÔNICO DO MÉTODO LAY 2X2 QUANT (100% ALINHADO AO LIVE) ===")
print("=" * 80)

# 1. Carrega base de dados canônica
fpath = "Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH.csv"
if not os.path.exists(fpath):
    fpath = "Bases_de_Dados_API_FutPythonTrader_Bet365.csv"

print(f"\n[+] Carregando base de dados: {fpath}...")
df_hist = pd.read_csv(fpath, low_memory=False)
df_hist["d_str"] = pd.to_datetime(df_hist["Date"], errors='coerce').dt.strftime("%Y-%m-%d")

# Mapeia colunas dinamicamente
c_2x2 = [c for c in df_hist.columns if '2x2' in c.lower() and 'lay' in c.lower()]
if not c_2x2:
    c_2x2 = [c for c in df_hist.columns if '2x2' in c.lower()]
c_2x2 = c_2x2[0]

c_u25 = [c for c in df_hist.columns if 'under25' in c.lower() and 'ht' not in c.lower()]
c_u25 = c_u25[0] if c_u25 else None

c_h = [c for c in df_hist.columns if c.lower() in ['odd_h_back', 'odd_h_ft_back', 'odd_h', 'odd_h_ft']][0]
c_a = [c for c in df_hist.columns if c.lower() in ['odd_a_back', 'odd_a_ft_back', 'odd_a', 'odd_a_ft']][0]
c_gh = [c for c in df_hist.columns if 'goals_h_ft' in c.lower() or 'gols_h' in c.lower() or 'home_score' in c.lower()][0]
c_ga = [c for c in df_hist.columns if 'goals_a_ft' in c.lower() or 'gols_a' in c.lower() or 'away_score' in c.lower()][0]
c_liga = 'League' if 'League' in df_hist.columns else ('Liga' if 'Liga' in df_hist.columns else 'Div')

df_hist["o_2x2"] = pd.to_numeric(df_hist[c_2x2], errors='coerce')
df_hist["o_u25"] = pd.to_numeric(df_hist[c_u25], errors='coerce') if c_u25 else np.nan
df_hist["o_h"] = pd.to_numeric(df_hist[c_h], errors='coerce')
df_hist["o_a"] = pd.to_numeric(df_hist[c_a], errors='coerce')
df_hist["gh"] = pd.to_numeric(df_hist[c_gh], errors='coerce')
df_hist["ga"] = pd.to_numeric(df_hist[c_ga], errors='coerce')

# Filtra partidas válidas finalizadas
df_val = df_hist[df_hist["gh"].notna() & df_hist["ga"].notna() & (df_hist["o_2x2"] > 1.0)].copy()
df_val["is_2x2"] = (df_val["gh"].astype(int) == 2) & (df_val["ga"].astype(int) == 2)

print(f"[+] Total de jogos com odd Lay 2x2 e placar finalizado: {len(df_val):,}")

# Filtragem de candidatos
rows_candidatos = []
for idx, r in df_val.iterrows():
    o22 = r["o_2x2"]
    ou25 = r["o_u25"]
    oh = r["o_h"]
    oa = r["o_a"]
    liga = str(r.get(c_liga, ""))
    home = str(r.get("Home", r.get("Home_Team", "")))
    away = str(r.get("Away", r.get("Away_Team", "")))
    d_str = r["d_str"]
    is_red = r["is_2x2"]
    
    # 1. Validação básica de odds e tendência
    ok_base, _ = validar_entrada_lay2x2(o22, ou25, None, oh, oa, liga=None)
    if not ok_base:
        continue
        
    # Checagem de ligas
    is_blacklist_2x2 = any(b in liga.upper() for b in BLACKLIST_LIGAS_2X2)
    is_gov_ignorado, _ = eh_jogo_ignorado_governanca(liga, home, away)
    
    rows_candidatos.append({
        "Date": d_str,
        "Year": d_str[:4] if pd.notna(d_str) else "",
        "Month": d_str[:7] if pd.notna(d_str) else "",
        "Home": home,
        "Away": away,
        "League": liga,
        "Odd_2x2": o22,
        "is_2x2": is_red,
        "is_blacklist_2x2": is_blacklist_2x2,
        "is_gov_ignorado": is_gov_ignorado
    })

df_cand = pd.DataFrame(rows_candidatos)
print(f"[+] Total de oportunidades pré-qualificadas por mercado: {len(df_cand):,}")

def processar_metricas(df_sub, nome, apply_top3=False):
    if df_sub.empty:
        return {"Cenário": nome, "N": 0}
        
    df_e = df_sub.copy()
    if apply_top3:
        # Ordena por Odd Lay menor dentro de cada dia e pega os top 3
        df_e = df_e.sort_values(["Date", "Odd_2x2"], ascending=[True, True])
        df_e = df_e.groupby("Date").head(3).reset_index(drop=True)
        
    tot = len(df_e)
    reds = df_e["is_2x2"].sum()
    greens = tot - reds
    wr = (greens / tot) * 100.0
    
    # Break-even WR médio
    be_wr = ((df_e["Odd_2x2"] - 1.0) / (df_e["Odd_2x2"] - COMISSAO_BETFAIR)).mean() * 100.0
    
    # P&L com Stake R$ 100
    pnl_stake100 = np.where(~df_e["is_2x2"], 100.0 * (1.0 - COMISSAO_BETFAIR), -(df_e["Odd_2x2"] - 1.0) * 100.0).sum()
    
    # P&L com Responsabilidade Fixa R$ 200
    stk_liab200 = 200.0 / (df_e["Odd_2x2"] - 1.0)
    pnl_liab200 = np.where(~df_e["is_2x2"], stk_liab200 * (1.0 - COMISSAO_BETFAIR), -200.0).sum()
    
    # Yield e ROI sobre risco
    yield_stake = (pnl_stake100 / (tot * 100.0)) * 100.0
    total_liab = ((df_e["Odd_2x2"] - 1.0) * 100.0).sum()
    roi_liab = (pnl_stake100 / total_liab) * 100.0
    
    # Max Drawdown (em R$ 200 de liability)
    pnl_row = np.where(~df_e["is_2x2"], stk_liab200 * (1.0 - COMISSAO_BETFAIR), -200.0)
    cum = np.cumsum(pnl_row)
    peak = np.maximum.accumulate(cum)
    dd = (cum - peak).min()
    
    return {
        "Cenário": nome,
        "N (Jogos)": tot,
        "Greens": greens,
        "Reds": reds,
        "Win Rate %": f"{wr:.2f}%",
        "BE Win Rate %": f"{be_wr:.2f}%",
        "Edge (WR - BE)": f"{wr - be_wr:+.2f}pp",
        "P&L (Stake 100)": f"R$ {pnl_stake100:+,.2f}",
        "P&L (Liab 200)": f"R$ {pnl_liab200:+,.2f}",
        "Yield %": f"{yield_stake:+.2f}%",
        "ROI Liab %": f"{roi_liab:+.2f}%",
        "Max DD (R$)": f"R$ {dd:,.2f}"
    }

# Executa para 2026
df_cand_2026 = df_cand[df_cand["Year"] == "2026"].copy()

c1 = processar_metricas(df_cand_2026, "1. Bruto Original (Sem Filtro Ligas, Odd 8-20)", apply_top3=False)
c2 = processar_metricas(df_cand_2026[~df_cand_2026["is_blacklist_2x2"]], "2. Com Blacklist 4 Ligas (Sérvia, Irlanda, Turquia, Escócia)", apply_top3=False)
c3 = processar_metricas(df_cand_2026[~df_cand_2026["is_gov_ignorado"]], "3. Com Governança Global (Odd 8-20, Sem Periféricas)", apply_top3=False)
c4 = processar_metricas(df_cand_2026[(~df_cand_2026["is_gov_ignorado"]) & (df_cand_2026["Odd_2x2"] <= 14.0)], "4. LIVE OFICIAL CAUSAL (Governança + Teto Odd <= 14.00, Sem Lookahead)", apply_top3=False)
c5 = processar_metricas(df_cand_2026[~df_cand_2026["is_gov_ignorado"]], "5. [Referência Comparativa] Top 3 Menor Odd (com lookahead)", apply_top3=True)

df_comp_2026 = pd.DataFrame([c1, c2, c3, c4, c5])

print("\n" + "=" * 125)
print("=== RESULTADOS COMPARATIVOS: LAY 2X2 NO ANO DE 2026 COMPLETO ===")
print("=" * 125)
print(df_comp_2026.to_string(index=False))
print("=" * 125)

# Detalhamento Mês a Mês do Cenário 4 (Live Oficial Causal com Teto 14.00) em 2026
df_live_2026 = df_cand_2026[(~df_cand_2026["is_gov_ignorado"]) & (df_cand_2026["Odd_2x2"] <= 14.0)].sort_values(["Date", "Odd_2x2"]).reset_index(drop=True)

monthly_records = []
for mes, g in df_live_2026.groupby("Month"):
    tot = len(g)
    reds = g["is_2x2"].sum()
    greens = tot - reds
    wr = (greens / tot) * 100.0
    pnl_s100 = np.where(~g["is_2x2"], 100.0 * (1.0 - COMISSAO_BETFAIR), -(g["Odd_2x2"] - 1.0) * 100.0).sum()
    stk_l200 = 200.0 / (g["Odd_2x2"] - 1.0)
    pnl_l200 = np.where(~g["is_2x2"], stk_l200 * (1.0 - COMISSAO_BETFAIR), -200.0).sum()
    monthly_records.append({
        "Mês": mes,
        "Jogos": tot,
        "Greens": greens,
        "Reds": reds,
        "Win Rate %": f"{wr:.2f}%",
        "P&L Stake 100": f"R$ {pnl_s100:+,.2f}",
        "P&L Liab 200": f"R$ {pnl_l200:+,.2f}"
    })

df_monthly = pd.DataFrame(monthly_records)
print("\n=== EVOLUÇÃO MENSAL EM 2026 — MÉTODO LIVE OFICIAL CAUSAL (TETO ODD <= 14.00) ===")
print(df_monthly.to_string(index=False))
print("=" * 125)
