# -*- coding: utf-8 -*-
"""
cruzar_ledger_bet365.py — Cruzamento Inverso:
Pega os jogos reais selecionados de Agosto a Outubro nos Lays da Betfair
e compara diretamente com as odds da Bet365 para os mesmos jogos.
"""
import sys, re, unicodedata
import pandas as pd
import numpy as np

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

def canon(s):
    s = unicodedata.normalize('NFKD', str(s)).encode('ascii', 'ignore').decode().lower()
    return re.sub(r'[^a-z0-9]', '', s)

print("1. Carregando ledger oficial de Agosto a Outubro (Betfair Lay real)...")
ledger = pd.read_csv("metodos_aprovados/forward_5metodos_ledger.csv")

# Filtrar jogos liquidados
ledger = ledger[ledger["status"] == "LIQUIDADO"].copy()
ledger["Data"] = ledger["Data"].astype(str)
ledger["h_can"] = ledger["Home"].apply(canon)
ledger["a_can"] = ledger["Away"].apply(canon)

m_draw = "Lay Draw (Fav<=1.40)"
m_home = "Lay Home/DC X2 (FavVis<=1.65)"

ld_jogos = ledger[ledger["Metodo"] == m_draw].copy()
lh_jogos = ledger[ledger["Metodo"] == m_home].copy()

print(f"Total liquidados no Ledger: {len(ledger)}")
print(f"  - Lay Draw: {len(ld_jogos)} jogos")
print(f"  - Lay Home: {len(lh_jogos)} jogos")

print("\n2. Carregando base Bet365...")
b365 = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
b365 = b365[b365["Date"] >= "2026-07-25"].copy()
b365["Date"] = b365["Date"].astype(str)
b365["h_can"] = b365["Home"].apply(canon)
b365["a_can"] = b365["Away"].apply(canon)

for col in ["Odd_H_FT", "Odd_D_FT", "Odd_A_FT", "Odd_DC_12", "Odd_DC_X2", "Odd_DC_1X", "AH_A_pos_0_5", "AH_H_pos_0_5"]:
    if col in b365.columns:
        b365[col] = pd.to_numeric(b365[col], errors="coerce")

print(f"Jogos Bet365 carregados: {len(b365)}")

# -------------------------------------------------------------------------
# ANÁLISE 1: LAY DRAW
# -------------------------------------------------------------------------
print("\n" + "="*70)
print("ANÁLISE INVERSA: LAY DRAW (Agosto a Outubro/2026)")
print("="*70)

# Merge exato
m_ld = pd.merge(ld_jogos, b365, left_on=["Data", "h_can", "a_can"], right_on=["Date", "h_can", "a_can"], suffixes=("_bf", "_b365"))

# Fuzzy match para os que não bateram exato por diferença sutil de grafia
nao_casados = ld_jogos[~ld_jogos["Data"].isin(m_ld["Data"]) | ~ld_jogos["h_can"].isin(m_ld["h_can"])]
print(f"Jogos Lay Draw casados com a Bet365: {len(m_ld)} de {len(ld_jogos)} ({len(m_ld)/len(ld_jogos)*100:.1f}%)")

if len(m_ld) > 0:
    m_ld["Odd_Lay"] = pd.to_numeric(m_ld["Odd_Lay"], errors="coerce")
    m_ld["pnl_u"] = pd.to_numeric(m_ld["pnl_u"], errors="coerce")
    m_ld["green"] = m_ld["resultado"] == "GREEN"
    
    # Spread real entre a odd de Lay da Betfair e o Draw da Bet365
    m_ld["spread_real"] = m_ld["Odd_Lay"] / m_ld["Odd_D_FT"]
    
    # P&L se tivéssemos feito BACK na Dupla Chance 12 na Bet365 nesses mesmos jogos:
    # Green se Goals_H != Goals_A (mesmo evento do Lay Draw)
    m_ld["pnl_dc12_b365"] = np.where(m_ld["green"], m_ld["Odd_DC_12"] - 1.0, -1.0)
    
    # P&L se tivéssemos feito Lay na Betfair com 5% de comissão
    wr_bf = m_ld["green"].mean() * 100
    pnl_bf_u = m_ld["pnl_u"].sum()
    roi_bf_liab = (m_ld["pnl_u"] / (m_ld["Odd_Lay"] - 1.0)).mean() * 100
    
    # P&L na Bet365 Dupla Chance 12
    validos_dc12 = m_ld[m_ld["Odd_DC_12"] > 1.0]
    n_dc12 = len(validos_dc12)
    pnl_dc12_u = validos_dc12["pnl_dc12_b365"].sum()
    roi_dc12 = pnl_dc12_u / n_dc12 * 100 if n_dc12 > 0 else 0
    odd_med_dc12 = validos_dc12["Odd_DC_12"].median() if n_dc12 > 0 else 0
    
    print(f"\n[Amostra Casada: {len(m_ld)} jogos]")
    print(f"Taxa de Acerto Real (Win Rate): {wr_bf:.2f}% ({m_ld['green'].sum()} Greens / {(~m_ld['green']).sum()} Reds)")
    print(f"Odd Betfair Lay Mediana: {m_ld['Odd_Lay'].median():.2f}")
    print(f"Odd Bet365 Draw Mediana: {m_ld['Odd_D_FT'].median():.2f}")
    print(f"Spread Real Mediano (Betfair Lay / Bet365): {m_ld['spread_real'].median():.3f}x (Média: {m_ld['spread_real'].mean():.3f}x)")
    print(f"\n--- EXECUÇÃO NA BETFAIR (LAY REAL, comissão 5%) ---")
    print(f"  Lucro Acumulado: {pnl_bf_u:+.2f}u")
    print(f"  ROI / Capital em Risco: {roi_bf_liab:+.2f}%")
    
    print(f"\n--- SE TIVÉSSEMOS FEITO BACK DUPLA CHANCE 12 NA BET365 ---")
    print(f"  Jogos com Odd DC 12 válida: {n_dc12} de {len(m_ld)}")
    print(f"  Odd DC 12 Mediana na Bet365: {odd_med_dc12:.2f}")
    print(f"  Lucro Acumulado: {pnl_dc12_u:+.2f}u")
    print(f"  ROI sobre Stake: {roi_dc12:+.2f}%")

# -------------------------------------------------------------------------
# ANÁLISE 2: LAY HOME / DC X2
# -------------------------------------------------------------------------
print("\n" + "="*70)
print("ANÁLISE INVERSA: LAY HOME / DC X2 (Agosto a Outubro/2026)")
print("="*70)

m_lh = pd.merge(lh_jogos, b365, left_on=["Data", "h_can", "a_can"], right_on=["Date", "h_can", "a_can"], suffixes=("_bf", "_b365"))
print(f"Jogos Lay Home casados com a Bet365: {len(m_lh)} de {len(lh_jogos)} ({len(m_lh)/len(lh_jogos)*100:.1f}%)")

if len(m_lh) > 0:
    m_lh["Odd_Lay"] = pd.to_numeric(m_lh["Odd_Lay"], errors="coerce")
    m_lh["pnl_u"] = pd.to_numeric(m_lh["pnl_u"], errors="coerce")
    m_lh["green"] = m_lh["resultado"] == "GREEN"
    
    # Spread real entre a odd de Lay da Betfair e o Home da Bet365
    m_lh["spread_real"] = m_lh["Odd_Lay"] / m_lh["Odd_H_FT"]
    
    # P&L se tivéssemos feito BACK na Dupla Chance X2 na Bet365 nesses mesmos jogos:
    # Green se Goals_A >= Goals_H (mesmo evento do Lay Home)
    m_lh["pnl_dcx2_b365"] = np.where(m_lh["green"], m_lh["Odd_DC_X2"] - 1.0, -1.0)
    
    # P&L se tivéssemos feito Handicap Asiático +0.5 Visitante (AH_A_pos_0_5) na Bet365:
    tem_ah = "AH_A_pos_0_5" in m_lh.columns
    if tem_ah:
        m_lh["pnl_ah_b365"] = np.where(m_lh["green"], m_lh["AH_A_pos_0_5"] - 1.0, -1.0)
    
    wr_bf = m_lh["green"].mean() * 100
    pnl_bf_u = m_lh["pnl_u"].sum()
    roi_bf_liab = (m_lh["pnl_u"] / (m_lh["Odd_Lay"] - 1.0)).mean() * 100
    
    validos_dcx2 = m_lh[m_lh["Odd_DC_X2"] > 1.0]
    n_dcx2 = len(validos_dcx2)
    pnl_dcx2_u = validos_dcx2["pnl_dcx2_b365"].sum()
    roi_dcx2 = pnl_dcx2_u / n_dcx2 * 100 if n_dcx2 > 0 else 0
    odd_med_dcx2 = validos_dcx2["Odd_DC_X2"].median() if n_dcx2 > 0 else 0
    
    print(f"\n[Amostra Casada: {len(m_lh)} jogos]")
    print(f"Taxa de Acerto Real (Win Rate): {wr_bf:.2f}% ({m_lh['green'].sum()} Greens / {(~m_lh['green']).sum()} Reds)")
    print(f"Odd Betfair Lay Mediana: {m_lh['Odd_Lay'].median():.2f}")
    print(f"Odd Bet365 Home Mediana: {m_lh['Odd_H_FT'].median():.2f}")
    print(f"Spread Real Mediano (Betfair Lay / Bet365): {m_lh['spread_real'].median():.3f}x (Média: {m_lh['spread_real'].mean():.3f}x)")
    print(f"\n--- EXECUÇÃO NA BETFAIR (LAY REAL, comissão 5%) ---")
    print(f"  Lucro Acumulado: {pnl_bf_u:+.2f}u")
    print(f"  ROI / Capital em Risco: {roi_bf_liab:+.2f}%")
    
    print(f"\n--- SE TIVÉSSEMOS FEITO BACK DUPLA CHANCE X2 NA BET365 ---")
    print(f"  Jogos com Odd DC X2 válida: {n_dcx2} de {len(m_lh)}")
    print(f"  Odd DC X2 Mediana na Bet365: {odd_med_dcx2:.2f}")
    print(f"  Lucro Acumulado: {pnl_dcx2_u:+.2f}u")
    print(f"  ROI sobre Stake: {roi_dcx2:+.2f}%")
    
    if tem_ah:
        validos_ah = m_lh[m_lh["AH_A_pos_0_5"] > 1.0]
        n_ah = len(validos_ah)
        pnl_ah_u = validos_ah["pnl_ah_b365"].sum()
        roi_ah = pnl_ah_u / n_ah * 100 if n_ah > 0 else 0
        odd_med_ah = validos_ah["AH_A_pos_0_5"].median() if n_ah > 0 else 0
        print(f"\n--- SE TIVÉSSEMOS FEITO ASIAN HANDICAP +0.5 VISITANTE NA BET365 ---")
        print(f"  Jogos com Odd AH +0.5 válida: {n_ah} de {len(m_lh)}")
        print(f"  Odd AH +0.5 Mediana na Bet365: {odd_med_ah:.2f}")
        print(f"  Lucro Acumulado: {pnl_ah_u:+.2f}u")
        print(f"  ROI sobre Stake: {roi_ah:+.2f}%")
