# -*- coding: utf-8 -*-
"""
reliquidar_planilhas_diarias.py — re-liquida as planilhas Sinais_Metodos_Aprovados_<data>.xlsx
pelas bases de placar (Betfair + b365, exato -> fuzzy), na convencao LIABILITY=1u / 5%.

Motivo (11/09/2026): as planilhas de 03-09/09 estavam 100% liquidadas em convencao STAKE (0.95 /
-(odd-1)) e com placar da reconstrucao do coletor (27% errada) — inclusive 06-09/09, dias que as
bases ainda nem cobriam. E a pagina 02 faz drop_duplicates(keep="last") com essas planilhas
concatenadas DEPOIS da mestre: para essas datas, a planilha diaria vence a mestre corrigida.

Regra: placar de base -> liquida e grava Fonte_Placar. Sem placar em base -> PENDENTE (a rotina
noturna, ja corrigida, retenta todo dia). Nunca herda placar sem fonte. Backup .bak_<data> de cada.
"""
import os, sys, glob, shutil, re, warnings
warnings.filterwarnings("ignore"); sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, ROOT)
import pandas as pd, numpy as np
import relatorio_forward_5metodos as RF
C = 0.05
P = RF.placares(); IDX = RF._por_dia(P)

def red(metodo, gh, ga):
    m = metodo.lower()
    if "draw" in m: return gh == ga
    if "over 4.5" in m: return gh + ga >= 5
    if "under 1.5" in m: return gh + ga < 2
    if "under 0.5" in m: return gh + ga == 0
    if "away" in m: return ga > gh
    if "home" in m: return gh > ga
    mm = re.search(r"(\d)x(\d)", m)
    if mm: return gh == int(mm.group(1)) and ga == int(mm.group(2))
    raise ValueError(metodo)

for f in sorted(glob.glob(os.path.join(ROOT, "metodos_aprovados", "Sinais_Metodos_Aprovados_20*.xlsx"))):
    if "Odds_Reais" in f: continue
    ds = re.search(r"(\d{4}-\d{2}-\d{2})", f).group(1)
    try:
        d = pd.read_excel(f)
    except PermissionError:
        print("%s: ABERTO NO EXCEL — pulado" % ds); continue
    if d.empty: continue
    cm = [c for c in d.columns if "todo" in c.lower()][0]
    antes = (d["Resultado"].isin(["GREEN", "RED"])).sum() if "Resultado" in d.columns else 0
    pnl_antes = pd.to_numeric(d.get("PnL_u"), errors="coerce").sum()
    shutil.copy(f, f + ".bak_20260911")
    for col in ("Fonte_Placar", "PnL_stake_u", "Convencao_PnL", "Placar_Original", "Resultado_Original"):
        if col not in d.columns: d[col] = ""
    d["Placar_Original"] = d.get("Placar", ""); d["Resultado_Original"] = d.get("Resultado", "")
    liq = trocou = 0
    for i, r in d.iterrows():
        h, a = [x.strip() for x in str(r["Jogo"]).split(" x ", 1)] if " x " in str(r["Jogo"]) else (str(r.get("Home")), str(r.get("Away")))
        odd = float(r["Odd_Entrada"]); liab = float(r.get("Risco_Red_R$", 100.0) or 100.0)
        sc = RF.achar_placar(P, IDX, ds, h, a)
        if sc is None:
            d.at[i, "Resultado"] = "PENDENTE"; d.at[i, "Placar"] = "?"; d.at[i, "1/0"] = np.nan
            d.at[i, "PnL_u"] = np.nan; d.at[i, "PnL_stake_u"] = np.nan; d.at[i, "PnL_R$"] = np.nan
            d.at[i, "Fonte_Placar"] = ""; d.at[i, "Convencao_PnL"] = ""
            continue
        gh, ga, fonte = sc; rd = red(str(r[cm]), gh, ga); res = "RED" if rd else "GREEN"
        if str(r.get("Resultado")) in ("GREEN", "RED") and res != r.get("Resultado"): trocou += 1
        pnl = -1.0 if rd else (1 - C) / (odd - 1)
        d.at[i, "Placar"] = "%dx%d" % (gh, ga); d.at[i, "Resultado"] = res; d.at[i, "1/0"] = 0 if rd else 1
        d.at[i, "PnL_u"] = round(pnl, 5); d.at[i, "PnL_stake_u"] = round(-(odd - 1) if rd else (1 - C), 5)
        d.at[i, "PnL_R$"] = round(liab * pnl, 2); d.at[i, "Fonte_Placar"] = fonte
        d.at[i, "Convencao_PnL"] = "LIABILITY=1u;comissao=5%"; liq += 1
    d.to_excel(f, index=False)
    print("%s: N=%2d | antes: %2d liquidados (PnL stake %+7.2f) | agora: %2d liquidados pelas bases, %2d PENDENTE, %d resultado(s) TROCADO(s) | PnL liab %+.3f u"
          % (ds, len(d), antes, pnl_antes, liq, len(d) - liq, trocou, pd.to_numeric(d["PnL_u"], errors="coerce").sum()))
