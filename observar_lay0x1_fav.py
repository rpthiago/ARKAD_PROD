# -*- coding: utf-8 -*-
"""
observar_lay0x1_fav.py — forward STAKE-ZERO da IDEIA 2 (Super Favorito <= 1.80):
  - Lay 0x1: Odd_H_Back <= 1.80 E 5.0 <= Odd_CS_0x1_Lay <= 13.0 -> RED se FT == 0-1
  - Lay 1x0: Odd_A_Back <= 1.80 E 5.0 <= Odd_CS_1x0_Lay <= 13.0 -> RED se FT == 1-0
Sinais 100% via API Betfair FutPythonTrader (sem filtro Over25, funciona direto no Cloud).
Liquidacao pelo COLETOR (FT robusto pos-FT mtk <= -100).
Aviso de auditoria web obrigatoria para todo RED (bug do gol tardio).
"""
import os, re, sys, subprocess, unicodedata
from datetime import date, timedelta
import numpy as np, pandas as pd
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

ROOT = os.path.dirname(os.path.abspath(__file__))
LOG = os.path.join(ROOT, "lay0x1_fav_acumulado.csv")
VALID_START = "2026-08-28"
FAV_SUPER_MAX = 1.90
LAY_LO, LAY_HI = 5.0, 15.0
COMM = 0.045
VPS = "ubuntu@163.176.59.215"
KEY = os.path.expanduser("~/Downloads/ssh-key-2026-07-31.key")
COLL = "/home/ubuntu/betfair-collector/betfair_live_odds.csv"


def _cn(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


def sinais_do_dia():
    """Ideia 2 (Super Favorito <= 1.80, sem filtro Over25) da API Betfair."""
    try:
        from futpythontrader_client import get_daily_dataframe
    except Exception as e:
        print("[aviso] API:", str(e)[:60]); return []
    rows = []
    for dd in (0, 1):
        ds = (date.today() + timedelta(days=dd)).isoformat()
        try:
            df = get_daily_dataframe(source="betfair", date_str=ds)
        except Exception:
            continue
        if df is None or df.empty or "Odd_CS_0x1_Lay" not in df.columns:
            continue
        oh = pd.to_numeric(df["Odd_H_Back"], errors="coerce")
        oa = pd.to_numeric(df["Odd_A_Back"], errors="coerce")
        l01 = pd.to_numeric(df["Odd_CS_0x1_Lay"], errors="coerce")
        l10 = pd.to_numeric(df["Odd_CS_1x0_Lay"], errors="coerce")
        
        for _, r in df.iterrows():
            i = r.name
            h, a = str(r["Home"]), str(r["Away"])
            jogo = f"{h} x {a}"
            # 1. Lay 0x1: Super Favorito Mandante
            if oh[i] <= FAV_SUPER_MAX and LAY_LO <= l01[i] <= LAY_HI:
                rows.append(dict(data=ds, jogo=jogo, liga=str(r.get("League", "N/A")), metodo="Lay 0x1",
                                 fav_odd=round(float(oh[i]), 2), lay=round(float(l01[i]), 2), tgt=(0, 1)))
            # 2. Lay 1x0: Super Favorito Visitante
            if oa[i] <= FAV_SUPER_MAX and LAY_LO <= l10[i] <= LAY_HI:
                rows.append(dict(data=ds, jogo=jogo, liga=str(r.get("League", "N/A")), metodo="Lay 1x0",
                                 fav_odd=round(float(oa[i]), 2), lay=round(float(l10[i]), 2), tgt=(1, 0)))
    return rows


def puxar_ft():
    """FT robusto (janela pos-FT) do coletor, por jogo canon."""
    awk = (r'BEGIN{FS=","} $2=="CORRECT_SCORE" && $6>="%s" && $8 ~ /^[0-9]+ - [0-9]+$/ && $12!="" '
           r'{print $4"|"$5"|"$6"|"$7"|"$8"|"$12}') % (VALID_START[:7] + "-01")
    try:
        r = subprocess.run(["ssh", "-i", KEY, "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=25",
                            VPS, "awk '%s' %s" % (awk, COLL)], capture_output=True, text=True, timeout=300)
        ls = [l for l in r.stdout.splitlines() if l.strip()]
        if not ls:
            return {}
        d = pd.DataFrame([l.split("|") for l in ls], columns=["h", "a", "ko", "mtk", "run", "lay"])
        d["mtk"] = pd.to_numeric(d["mtk"], errors="coerce")
        d["lay"] = pd.to_numeric(d["lay"], errors="coerce")
        d = d.dropna(subset=["mtk", "lay"])
        d["g"] = pd.to_datetime(d["ko"], errors="coerce").dt.strftime("%Y-%m-%d") + "|" + d["h"].map(_cn) + "|" + d["a"].map(_cn)
        ft = {}
        for g, gg in d.groupby("g"):
            late = gg[gg["mtk"] <= -100]
            use = late if len(late) else gg
            rr = use.loc[use["lay"].idxmin(), "run"]
            a, b = rr.split(" - ")
            ft[g] = (int(a), int(b))
        return ft
    except Exception as e:
        print("[aviso] coletor:", str(e)[:60]); return {}


def main():
    log = pd.read_csv(LOG) if os.path.exists(LOG) else pd.DataFrame(
        columns=["data", "jogo", "liga", "metodo", "fav_odd", "odd_lay01", "stake", "primeiro_visto", "status", "resultado", "pnl", "conferir_web"])
    
    if "conferir_web" not in log.columns:
        log["conferir_web"] = ""

    vistos = set((log["data"].astype(str) + "|" + log["jogo"].astype(str) + "|" + log["metodo"].astype(str))) if len(log) else set()
    hoje = date.today()
    novos = 0
    
    for s in sinais_do_dia():
        if s["data"] < VALID_START:
            continue
        key = s["data"] + "|" + s["jogo"] + "|" + s["metodo"]
        if key in vistos:
            continue
        log = pd.concat([log, pd.DataFrame([{
            "data": s["data"], "jogo": s["jogo"], "liga": s["liga"], "metodo": s["metodo"],
            "fav_odd": s["fav_odd"], "odd_lay01": s["lay"], "stake": 0, "primeiro_visto": hoje.isoformat(),
            "status": "Pendente", "resultado": "Pendente", "pnl": 0.0, "conferir_web": ""}])], ignore_index=True)
        vistos.add(key)
        novos += 1

    ft = puxar_ft()
    liq = 0
    for i in log.index[log["status"] == "Pendente"]:
        d = str(log.loc[i, "data"])
        h, a = str(log.loc[i, "jogo"]).split(" x ", 1)
        g = d + "|" + _cn(h) + "|" + _cn(a)
        if g in ft:
            gh, ga = ft[g]
            ol = float(log.loc[i, "odd_lay01"])
            tgt = (0, 1) if log.loc[i, "metodo"] == "Lay 0x1" else (1, 0)
            green = (gh, ga) != tgt
            log.loc[i, "status"] = "Finalizado"
            log.loc[i, "resultado"] = "GREEN" if green else "RED"
            log.loc[i, "pnl"] = round((1 - COMM) if green else -(ol - 1), 4)
            if not green:
                log.loc[i, "conferir_web"] = f"⚠️ Auditar na Web (coletor gravou {gh}x{ga})"
            else:
                log.loc[i, "conferir_web"] = f"Placar {gh}x{ga}"
            liq += 1

    log.to_csv(LOG, index=False, encoding="utf-8-sig")
    fin = log[log["status"] == "Finalizado"]
    print("novos: %d | liquidados: %d | total %d (pend %d, liq %d)"
          % (novos, liq, len(log), (log["status"] == "Pendente").sum(), len(fin)))
    
    if len(fin):
        # 1. Total Combinado
        wr_tot = (fin["resultado"] == "GREEN").mean() * 100
        roi_tot = fin["pnl"].sum() / (fin["odd_lay01"] - 1).sum() * 100
        print("\n=== FORWARD IDEIA 2 (Super Favorito <= 1.80, Stake-Zero) ===")
        print("TOTAL COMBINADO: N=%d | WR=%.1f%% | ROI/liability=%+.2f%%" % (len(fin), wr_tot, roi_tot))
        
        # 2. Lay 0x1 Isolado
        f01 = fin[fin["metodo"] == "Lay 0x1"]
        if len(f01):
            wr01 = (f01["resultado"] == "GREEN").mean() * 100
            roi01 = f01["pnl"].sum() / (f01["odd_lay01"] - 1).sum() * 100
            print("  🟢 Lay 0x1 (Mandante Fav <= 1.80): N=%d | WR=%.1f%% | ROI/liability=%+.2f%%" % (len(f01), wr01, roi01))
            
        # 3. Lay 1x0 Isolado (Elo Fraco)
        f10 = fin[fin["metodo"] == "Lay 1x0"]
        if len(f10):
            wr10 = (f10["resultado"] == "GREEN").mean() * 100
            roi10 = f10["pnl"].sum() / (f10["odd_lay01"] - 1).sum() * 100
            print("  ⚠️ Lay 1x0 (Visitante Fav <= 1.80): N=%d | WR=%.1f%% | ROI/liability=%+.2f%%" % (len(f10), wr10, roi10))
    else:
        print("Ainda sem jogos liquidados a partir de %s." % VALID_START)


if __name__ == "__main__":
    main()
