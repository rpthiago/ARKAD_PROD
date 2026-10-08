# -*- coding: utf-8 -*-
"""
backtest_fluxo_agosto_setembro.py — Simulação Exata do Fluxo Proposto pelo Thiago:
Período: 01/08/2026 até 24/09/2026
1. Identifica os candidatos no radar da Bet365
2. Consulta a odd de Lay REAL na Betfair (OrbitX)
3. Aplica o filtro de teto de odd
4. Calcula P&L, Win Rate, Break-Even e ROI reais
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

print("1. Carregando dados da Bet365 (01/08/2026 em diante)...")
b365 = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Bet365.csv", low_memory=False)
b365 = b365.dropna(subset=["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT"]).copy()
b365["Date"] = pd.to_datetime(b365["Date"], errors="coerce")
b365 = b365[b365["Date"] >= "2026-08-01"].copy()
b365["d_str"] = b365["Date"].dt.strftime("%Y-%m-%d")
b365["h_can"] = b365["Home"].apply(canon)
b365["a_can"] = b365["Away"].apply(canon)

for col in ["Odd_H_FT", "Odd_D_FT", "Odd_A_FT"]:
    b365[col] = pd.to_numeric(b365[col], errors="coerce")

print(f"Total jogos Bet365 no período: {len(b365)}")

print("\n2. Carregando dados reais de Lay da Betfair (.cache_base_betfair.csv)...")
bf = pd.read_csv("metodos_aprovados/.cache_base_betfair.csv", low_memory=False)
bf = bf.dropna(subset=["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT"]).copy()
bf["Date"] = pd.to_datetime(bf["Date"], errors="coerce")
bf = bf[bf["Date"] >= "2026-08-01"].copy()
bf["d_str"] = bf["Date"].dt.strftime("%Y-%m-%d")
bf["h_can"] = bf["Home"].apply(canon)
bf["a_can"] = bf["Away"].apply(canon)

for col in ["Odd_H_Lay", "Odd_D_Lay", "Odd_A_Lay", "Odd_H_Back", "Odd_A_Back", "Odd_D_Back"]:
    if col in bf.columns:
        bf[col] = pd.to_numeric(bf[col], errors="coerce")

print(f"Total jogos Betfair no período: {len(bf)}")

# Merge para simular: Radar b365 -> Confirmação na Betfair
df = pd.merge(
    b365,
    bf[["d_str", "h_can", "a_can", "Odd_H_Lay", "Odd_D_Lay", "Odd_A_Lay", "Odd_H_Back", "Odd_A_Back", "Odd_D_Back"]],
    on=["d_str", "h_can", "a_can"],
    how="inner",
    suffixes=("_b365", "_bf")
)
print(f"Jogos casados com sucesso: {len(df)}")

RE_COPA = re.compile(r"\b(CUP|COPA|CHAMPIONS LEAGUE|EUROPA LEAGUE|CONFERENCE LEAGUE|LIBERTADORES|POKAL|COUPE|COPPA|TAÇA|TACA)\b", re.IGNORECASE)
df["is_copa"] = df["League"].astype(str).apply(lambda x: bool(RE_COPA.search(x)))

def relatorio_metodo(nome, sub, col_odd, cond_vitoria, tetos):
    print("\n" + "="*80)
    print(f"METODO: {nome}")
    print("="*80)
    
    for teto in tetos:
        val = sub[(sub[col_odd] > 1.0) & (sub[col_odd] <= teto)].copy()
        n = len(val)
        if n == 0:
            print(f"Teto <= {teto:.2f}: Nenhum jogo encontrado.")
            continue
            
        g = cond_vitoria(val)
        greens = g.sum()
        reds = (~g).sum()
        wr = greens / n * 100
        
        odds = val[col_odd].values
        # Break-Even a 3% (OrbitX) e 5% (Betfair)
        be_3 = ((odds - 1.0) / (odds - 0.03)).mean() * 100
        be_5 = ((odds - 1.0) / (odds - 0.05)).mean() * 100
        
        # P&L nominal por 1u de stake no green (+0.97u no green, -(odd-1) no red)
        pnl_nom_3 = np.where(g, 0.97, -(odds - 1.0)).sum()
        pnl_nom_5 = np.where(g, 0.95, -(odds - 1.0)).sum()
        
        # ROI por liability (capital em risco)
        pnl_liab_3 = np.where(g, 0.97 / (odds - 1.0), -1.0)
        pnl_liab_5 = np.where(g, 0.95 / (odds - 1.0), -1.0)
        roi_liab_3 = pnl_liab_3.mean() * 100
        roi_liab_5 = pnl_liab_5.mean() * 100
        
        spread_med = (val[col_odd] / val[col_odd.replace('_Lay', '_FT')]).median() if col_odd.replace('_Lay', '_FT') in val.columns else np.nan
        
        print(f"--- Teto Odd Lay <= {teto:4.2f} (N={n}) ---")
        print(f"  Greens: {greens} | Reds: {reds} | Win Rate Real: {wr:.2f}%")
        print(f"  Odd Lay Mediana: {np.median(odds):.2f} (Liability Mediana: {np.median(odds)-1:.2f}u)")
        print(f"  Break-Even Necessário (c=3%): {be_3:.2f}% | Edge (WR - BE): {wr - be_3:+.2f} pp")
        print(f"  PnL Nominal (c=3% OrbitX):  {pnl_nom_3:+.2f}u | ROI/Liability: {roi_liab_3:+.2f}%")
        print(f"  PnL Nominal (c=5% Betfair): {pnl_nom_5:+.2f}u | ROI/Liability: {roi_liab_5:+.2f}%")

# ---------------------------------------------------------
# 1. LAY DRAW (Scanner Bet365: Super Fav <= 1.40)
# ---------------------------------------------------------
# Regra de Copas: Em copas só se for Mandante <= 1.40
cand_ld = df[
    (df[["Odd_H_FT", "Odd_A_FT"]].min(axis=1) <= 1.40) &
    (~df["is_copa"] | (df["is_copa"] & (df["Odd_H_FT"] <= 1.40)))
].copy()

relatorio_metodo(
    nome="1. LAY DRAW (Radar B365 Fav <= 1.40 -> Filtro Lay OrbitX)",
    sub=cand_ld,
    col_odd="Odd_D_Lay",
    cond_vitoria=lambda d: d["Goals_H_FT"] != d["Goals_A_FT"],
    tetos=[6.50, 7.00, 7.50, 8.00, 10.00]
)

# ---------------------------------------------------------
# 2. LAY AWAY (Scanner Bet365: Mandante Fav <= 1.40)
# ---------------------------------------------------------
cand_la = df[df["Odd_H_FT"] <= 1.40].copy()

relatorio_metodo(
    nome="2. LAY AWAY (Radar B365 Mandante Fav <= 1.40 -> Filtro Lay OrbitX)",
    sub=cand_la,
    col_odd="Odd_A_Lay",
    cond_vitoria=lambda d: d["Goals_H_FT"] >= d["Goals_A_FT"],
    tetos=[12.00, 15.00, 20.00, 25.00]
)

# ---------------------------------------------------------
# 3. LAY HOME (Scanner Bet365: Visitante Fav <= 1.65)
# ---------------------------------------------------------
cand_lh = df[df["Odd_A_FT"] <= 1.65].copy()

relatorio_metodo(
    nome="3. LAY HOME (Radar B365 Visitante Fav <= 1.65 -> Filtro Lay OrbitX)",
    sub=cand_lh,
    col_odd="Odd_H_Lay",
    cond_vitoria=lambda d: d["Goals_H_FT"] <= d["Goals_A_FT"],
    tetos=[6.00, 7.00, 8.00, 10.00]
)
