# -*- coding: utf-8 -*-
"""
conferir_placares_manuais.py — SO AVISA. Nao escreve em nenhum arquivo do usuario.
Compara o placar/resultado das planilhas (mestre + diarias dos ultimos 10 dias) com as bases de
placar (Betfair + b365) e, se houver divergencia, manda a lista no Telegram para o Thiago decidir.
"""
import os, sys, re, glob, warnings
from datetime import date, timedelta
warnings.filterwarnings("ignore"); sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, ROOT)
import pandas as pd
import relatorio_forward_5metodos as RF

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
    return None

def conferir(df, rotulo, P, idx, so_desde=None):
    out = []
    cm = [c for c in df.columns if "todo" in c.lower()]
    if not cm or "Resultado" not in df.columns: return out
    cm = cm[0]
    for _, r in df.iterrows():
        if str(r.get("Resultado")) not in ("GREEN", "RED"): continue
        ds = str(r.get("Data", ""))[:10]
        if so_desde and ds < so_desde: continue
        jogo = str(r.get("Jogo", ""))
        h, a = [x.strip() for x in jogo.split(" x ", 1)] if " x " in jogo else (str(r.get("Home")), str(r.get("Away")))
        sc = RF.achar_placar(P, idx, ds, h, a)
        if sc is None: continue
        gh, ga, fonte = sc
        rd = red(str(r[cm]), gh, ga)
        if rd is None: continue
        res_base = "RED" if rd else "GREEN"
        if res_base != r["Resultado"]:
            out.append("%s | %s | %s | planilha: %s (%s) | base: %s (%d-%d, %s)"
                       % (rotulo, ds, jogo, r["Resultado"], r.get("Placar", "?"), res_base, gh, ga, fonte.split(":")[0]))
    return out

P = RF.placares(); idx = RF._por_dia(P)
div = []
mestre = os.path.join(ROOT, "metodos_aprovados", "Sinais_Metodos_Aprovados_Odds_Reais_Betfair.csv")
if os.path.exists(mestre):
    div += conferir(pd.read_csv(mestre, encoding="utf-8"), "mestre", P, idx)
desde = (date.today() - timedelta(days=10)).isoformat()
for f in sorted(glob.glob(os.path.join(ROOT, "metodos_aprovados", "Sinais_Metodos_Aprovados_20*.xlsx"))):
    if "Odds_Reais" in f: continue
    try: div += conferir(pd.read_excel(f), os.path.basename(f)[-15:-5], P, idx, so_desde=desde)
    except PermissionError: pass
print("divergencias planilha x base: %d" % len(div))
for d in div: print("  " + d)
if div and "--sem-telegram" not in sys.argv:
    from telegram_notifier import enviar_mensagem_telegram
    txt = "<b>Conferência de placares</b> — %d divergência(s) entre planilha e base. Nada foi alterado.\n<pre>%s</pre>" % (
        len(div), "\n".join(x.replace("<", "&lt;") for x in div[:40]))
    enviar_mensagem_telegram(txt[:4000], parse_mode="HTML")
