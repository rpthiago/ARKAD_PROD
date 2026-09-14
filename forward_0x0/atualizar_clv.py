# -*- coding: utf-8 -*-
"""
atualizar_clv.py — atualiza o fechamento e calcula o CLV dos picks de 0x0.
Roda de hora em hora. Para cada pick OPEN:
  - se ainda PRE-KO: guarda a odd atual (rolling 'ultima pre-KO') dos gemeos.
  - se JA passou o KO: congela o fechamento = ultima pre-KO e calcula o CLV.
CLV lay 0x0  = (fechamento/entrada - 1)*100   (lay: fechamento MAIOR = 0-0 menos provavel = +)
CLV back O0.5 = (entrada/fechamento - 1)*100   (back: fechamento MENOR = +) — MESMO evento, mercado fundo.
"""
import os, sys, csv
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass
import unicodedata, re
def canon(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii","ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)
LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "clv_0x0_log.csv")
if not os.path.exists(LOG):
    print("sem picks de 0x0 ainda (log vazio)"); sys.exit()
rows = list(csv.DictReader(open(LOG, encoding="utf-8-sig")))
cols = rows[0].keys()
opens = [r for r in rows if r.get("status") == "OPEN"]
if not opens:
    print("0 picks OPEN"); sys.exit()

from futpythontrader_client import get_daily_dataframe
import pandas as pd
now = datetime.now()
def _n(r, c):
    v = pd.to_numeric(r.get(c), errors="coerce"); return float(v) if pd.notna(v) and v > 1 else None
# feed atual por data
feeds = {}
for d in sorted(set(r["Data"] for r in opens)):
    try:
        bf = get_daily_dataframe("betfair", d)
        m = {}
        for _, x in bf.iterrows():
            m[(canon(x.get("Home","")), canon(x.get("Away","")))] = (
                _n(x,"Odd_CS_0x0_Lay"), _n(x,"Odd_Over05_FT_Back"), _n(x,"Odd_Under05_FT_Lay"))
        feeds[d] = m
    except Exception as e:
        print("[feed erro %s]" % d, str(e)[:60])

fechados = 0
for r in opens:
    k = (canon(r["Home"]), canon(r["Away"]))
    cur = feeds.get(r["Data"], {}).get(k)
    ko_dt = None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try: ko_dt = datetime.strptime(r["Data"] + " " + str(r["ko"]).strip(), fmt); break
        except Exception: pass
    pre_ko = (ko_dt is None) or (now < ko_dt)
    if cur and pre_ko:
        if cur[0]: r["c_cs0x0_lay"] = round(cur[0], 2)
        if cur[1]: r["c_over05_back"] = round(cur[1], 3)
        if cur[2]: r["c_under05_lay"] = round(cur[2], 2)
        r["last_ts"] = now.isoformat(timespec="seconds")
    if ko_dt is not None and now >= ko_dt:
        # CONGELA: usa a ultima pre-KO (ja em c_*); se vazio, usa o atual como ultimo recurso
        if not r.get("c_cs0x0_lay") and cur and cur[0]: r["c_cs0x0_lay"] = round(cur[0], 2)
        if not r.get("c_over05_back") and cur and cur[1]: r["c_over05_back"] = round(cur[1], 3)
        def fl(x):
            try: return float(x)
            except: return None
        e0, c0 = fl(r["e_cs0x0_lay"]), fl(r["c_cs0x0_lay"])
        eo, co = fl(r["e_over05_back"]), fl(r["c_over05_back"])
        if e0 and c0: r["clv_lay0x0"] = round((c0/e0 - 1)*100, 2)
        if eo and co: r["clv_backover05"] = round((eo/co - 1)*100, 2)
        r["status"] = "CLOSED"; fechados += 1

with open(LOG, "w", newline="", encoding="utf-8-sig") as fh:
    w = csv.DictWriter(fh, fieldnames=cols); w.writeheader(); w.writerows(rows)

# resumo acumulado dos CLOSED
cl = [r for r in rows if r.get("status") == "CLOSED" and r.get("clv_backover05") not in (None,"")]
print("OPEN atualizados: %d | congelados agora: %d | total CLOSED: %d" % (len(opens), fechados, len(cl)))
if cl:
    import statistics
    v0 = [float(r["clv_lay0x0"]) for r in cl if r.get("clv_lay0x0") not in (None,"")]
    vo = [float(r["clv_backover05"]) for r in cl]
    print("=== CLV ACUMULADO do 0x0 (metrica-mae) ===")
    if v0: print("  Lay CS_0x0  : N=%d | mediana=%+.2f%% | %%CLV+=%.0f%%" % (len(v0), statistics.median(v0), sum(1 for x in v0 if x>0)/len(v0)*100))
    print("  Back Over0.5: N=%d | mediana=%+.2f%% | %%CLV+=%.0f%%" % (len(vo), statistics.median(vo), sum(1 for x in vo if x>0)/len(vo)*100))
    print("  (CLV+ mediano E %%CLV+ > 50%% = o MODELO bate a linha de fechamento = edge real)")
