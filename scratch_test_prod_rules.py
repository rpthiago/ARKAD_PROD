# -*- coding: utf-8 -*-
import sys, re
import pandas as pd
import numpy as np

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

print("Carregando Bases_de_Dados_API_FutPythonTrader_Betfair.csv...")
bf = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Betfair.csv", low_memory=False)
bf = bf.dropna(subset=["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT"]).copy()
bf["Date"] = pd.to_datetime(bf["Date"], errors="coerce")
bf = bf.dropna(subset=["Date"]).sort_values("Date")

RE_FEMININO = re.compile(
    r"(\b(WOMEN|FEMININ\w*|FEMENIN\w*|FRAUEN|LADIES|WSL|W-LEAGUE|DAMALLSVENSKAN|PREMIERE LIGUE|LIGA F|DE LA REINA)\b|\(W\))",
    re.IGNORECASE
)
RE_SELECAO = re.compile(
    r"\b(NATIONS LEAGUE|WORLD CUP|EUROCUP|EURO CUP|UEFA EURO|AMERICA CUP|COPA AMERICA|FRIENDLIES|FRIENDLY|INTERNATIONALS?|QUALIF\w*|AFCON|AFRICA CUP|ASIAN CUP|CONCACAF|U23|U21|U20|U19)\b",
    re.IGNORECASE
)
RE_COPA = re.compile(
    r"\b(CUP|COPA|CHAMPIONS LEAGUE|EUROPA LEAGUE|CONFERENCE LEAGUE|LIBERTADORES|SUDAMERICANA|POKAL|COUPE|COPPA|TAÇA|TACA|TROPHY|SHIELD|SUPERCUP|SUPER CUP|PLAYOFFS?)\b",
    re.IGNORECASE
)
RE_DIV_BAIXA = re.compile(
    r"\b(3RD DIVISION|4TH DIVISION|5TH DIVISION|3\. DIVISION|4\. DIVISION|TERCERA|MSFL|LEAGUE 3|LEAGUE THREE|LIGA 3|LIGA III|LIGA BET|LIGA ALEF|SECOND DIVISION B|MIZORAM|SHILLONG|BANGALORE|CHINESE LEAGUE 2|CHINA LEAGUE TWO)\b",
    re.IGNORECASE
)
RE_PERIFERICA = re.compile(
    r"\b(BOLIVIA\w*|PANAMA\w*|JAMAICA\w*|GUATEMALA\w*|SALVADOR\w*|NICARAGUA\w*|COSTA RICA\w*|HONDURAS\w*|PARAGUA\w*|PERU\w*|VENEZUELA\w*|ECUADOR\w*|"
    r"HONG KONG\w*|THAI\w*|INDONESIA\w*|INDIA\w*|SINGAPORE|CAMBODIA|BHUTAN|BANGLADESH|MONGOLIA|PHILIPPINES|VIETNAM|"
    r"KENYA\w*|TANZANIA\w*|RWANDA\w*|UGANDA\w*|NIGERIA\w*|GHANA\w*|ZAMBIA\w*|ZIMBABWE\w*|ETHIOPIA\w*|ALGERI\w*|EGYPT\w*|"
    r"FAROE|ESTONIA\w*|LATVIA\w*|LITHUANIA\w*|ARMENIA\w*|GEORGIA\w*|AZERBAIJAN\w*|KAZAKHSTAN\w*|UZBEKISTAN\w*|JORDAN\w*|KUWAIT\w*|OMAN\w*|LEBAN\w*|QATAR\w*|UAE\w*|SAUDI\w*|BAHRAIN\w*|CYPRUS|CYPRIOT\w*|MALTA|MALTESE\w*|MOLDOVA\w*|BELARUS\w*|KOSOVO\w*|ALBANIA\w*|MONTENEGR\w*|BOSNIA\w*|OTHER COMPETITIONS)\b",
    re.IGNORECASE
)

def eh_ignorado(row):
    txt = f"{row['Home']} {row['Away']} {row['League']}"
    comp = str(row['League'] or "")
    return bool(RE_FEMININO.search(txt) or RE_SELECAO.search(comp) or RE_DIV_BAIXA.search(comp) or RE_PERIFERICA.search(comp))

bf["ignorado"] = bf.apply(eh_ignorado, axis=1)
bf_clean = bf[~bf["ignorado"]].copy()
print(f"Total inicial: {len(bf)} | Apos governanca: {len(bf_clean)}")

def avaliar_metodo(sub, col_odd, is_green, nome):
    sub = sub[sub[col_odd] > 1.0].copy()
    if len(sub) == 0:
        print(f"{nome}: 0 apostas")
        return
    n = len(sub)
    g = is_green(sub)
    wr = g.mean() * 100
    odds = sub[col_odd].values
    
    # Break even médio
    be_3 = ((odds - 1) / (odds - 0.03)).mean() * 100
    be_5 = ((odds - 1) / (odds - 0.05)).mean() * 100
    
    # ROI sobre liability (capital em risco)
    pnl_liab_3 = np.where(g, 0.97 / (odds - 1), -1.0)
    pnl_liab_5 = np.where(g, 0.95 / (odds - 1), -1.0)
    
    roi_3 = pnl_liab_3.mean() * 100
    roi_5 = pnl_liab_5.mean() * 100
    
    # PnL nominal (1u por green)
    pnl_nom_3 = np.where(g, 0.97, -(odds - 1)).sum()
    pnl_nom_5 = np.where(g, 0.95, -(odds - 1)).sum()
    
    print(f"\n=======================================================")
    print(f"=== {nome} ===")
    print(f"=======================================================")
    print(f"N = {n} | Greens = {g.sum()} | Reds = {(~g).sum()} | WR = {wr:.2f}%")
    print(f"Odd Lay Mediana = {sub[col_odd].median():.2f}")
    print(f"Comissao 3% (OrbitX):  BE = {be_3:.2f}% | ROI/liab = {roi_3:+.2f}% | PnL = {pnl_nom_3:+.1f}u")
    print(f"Comissao 5% (Betfair): BE = {be_5:.2f}% | ROI/liab = {roi_5:+.2f}% | PnL = {pnl_nom_5:+.1f}u")
    
    # Bootstrap IC95 por dia
    rng = np.random.default_rng(42)
    dias = sub["Date"].values
    uq = np.unique(dias)
    idx_map = {d: np.where(dias == d)[0] for d in uq}
    boot_liab = np.empty(2000)
    for i in range(2000):
        escolhidos = rng.choice(uq, size=len(uq), replace=True)
        idx_boot = np.concatenate([idx_map[d] for d in escolhidos])
        boot_liab[i] = pnl_liab_3[idx_boot].mean() * 100
    ic95 = np.percentile(boot_liab, [2.5, 97.5])
    print(f"IC95% ROI/liab (c=3%): [{ic95[0]:+.2f}%, {ic95[1]:+.2f}%]")
    
    # Quebra por ano
    sub["ano"] = sub["Date"].dt.year
    for ano, ga in sub.groupby("ano"):
        ga_g = is_green(ga)
        ga_odds = ga[col_odd].values
        ga_pnl = np.where(ga_g, 0.97 / (ga_odds - 1), -1.0)
        ga_roi = ga_pnl.mean() * 100
        ga_wr = ga_g.mean() * 100
        print(f"  Ano {ano}: N={len(ga):4d} | WR={ga_wr:5.2f}% | ROI(3%)={ga_roi:+6.2f}%")

# 1. LAY DRAW (PRODUÇÃO SINAIS_KO_CORE)
is_copa = bf_clean["League"].astype(str).apply(lambda x: bool(RE_COPA.search(x)))
cond_ld = (
    (bf_clean["Odd_D_Lay"] >= 4.5) & (bf_clean["Odd_D_Lay"] <= 10.0) &
    (
        (is_copa & (bf_clean["Odd_H_Back"] <= 1.40)) |
        (~is_copa & (bf_clean[["Odd_H_Back", "Odd_A_Back"]].min(axis=1) <= 1.40))
    )
)
sub_ld = bf_clean[cond_ld].copy()
avaliar_metodo(sub_ld, "Odd_D_Lay", lambda df: df["Goals_H_FT"] != df["Goals_A_FT"], "LAY DRAW (PRODUCAO REAL SINAIS_KO)")

# 2. LAY HOME (PRODUÇÃO SINAIS_KO_CORE)
cond_lh = (
    (bf_clean["Odd_A_Back"] <= 1.65) &
    (bf_clean["Odd_H_Lay"] >= 2.0) & (bf_clean["Odd_H_Lay"] <= 10.0)
)
sub_lh = bf_clean[cond_lh].copy()
avaliar_metodo(sub_lh, "Odd_H_Lay", lambda df: df["Goals_H_FT"] <= df["Goals_A_FT"], "LAY HOME (PRODUCAO REAL SINAIS_KO)")

# 3. LAY AWAY
cond_la = (
    (bf_clean["Odd_H_Back"] <= 1.40) &
    (bf_clean["Odd_A_Lay"] >= 5.0) & (bf_clean["Odd_A_Lay"] <= 25.0)
)
sub_la = bf_clean[cond_la].copy()
avaliar_metodo(sub_la, "Odd_A_Lay", lambda df: df["Goals_H_FT"] >= df["Goals_A_FT"], "LAY AWAY (FAV MANDANTE <= 1.40, LAY <= 25.0)")
