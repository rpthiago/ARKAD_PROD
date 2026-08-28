# -*- coding: utf-8 -*-
"""
observar_lay0x1_fav.py — forward STAKE-ZERO do Lay 0x1 favoritao (Odd_H<=2.20).
Regra congelada (auditoria 2026-08-28): Lay Correct Score 0x1, so quando Odd_H_FT<=2.20 e a
odd de LAY REAL do 0-1 (coletor Betfair) esta entre 5.00 e 13.00. Hold ate o fim.
GREEN se FT != 0-1 (+0.955u); RED se FT == 0-1 (-(odd_lay-1)). stake=0 (observacao).

Fluxo diario: (1) pega os favoritoes do dia na API; (2) puxa a lay REAL do 0-1 + o placar FT do
coletor; (3) loga sinais NOVOS de jogos ainda nao jogados como Pendente; (4) liquida os pendentes
cujo placar ja saiu. So conta a partir de VALID_START (forward de verdade).

Uso: python observar_lay0x1_fav.py
"""
import os, re, sys, subprocess, unicodedata
from datetime import date, datetime, timedelta
import numpy as np, pandas as pd
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

ROOT = os.path.dirname(os.path.abspath(__file__))
LOG = os.path.join(ROOT, "lay0x1_fav_acumulado.csv")
VALID_START = "2026-08-29"          # forward comeca aqui
ODDH_MAX = 2.20
LAY_LO, LAY_HI = 5.0, 13.0
COMM = 0.045
VPS = "ubuntu@163.176.59.215"
KEY = os.path.expanduser("~/Downloads/ssh-key-2026-07-31.key")
COLL = "/home/ubuntu/betfair-collector/betfair_live_odds.csv"


def _cn(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


def favoritoes_do_dia(dias=(0, 1)):
    """API: jogos de hoje/amanha com Odd_H_Back <= 2.20 (favoritao mandante)."""
    try:
        from futpythontrader_client import get_daily_dataframe
    except Exception as e:
        print("[aviso] API indisponivel:", str(e)[:60]); return pd.DataFrame()
    out = []
    for dd in dias:
        ds = (date.today() + timedelta(days=dd)).isoformat()
        try:
            df = get_daily_dataframe(source="betfair", date_str=ds)
            if df is None or df.empty:
                continue
            df["Date"] = ds
            oh = pd.to_numeric(df.get("Odd_H_Back", df.get("Odd_H_FT")), errors="coerce")
            df = df[oh <= ODDH_MAX].copy(); df["odd_h"] = oh[oh <= ODDH_MAX]
            out.append(df)
        except Exception:
            continue
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def puxar_coletor():
    """0-1 lay pre-jogo (mais proxima do KO) + placar FT, do coletor. Chave canon."""
    awk = (r'BEGIN{FS=","} $2=="CORRECT_SCORE" && $6>="%s" && $8 ~ /^[0-9]+ - [0-9]+$/ '
           r'{print $4"|"$5"|"$6"|"$7"|"$8"|"$12"|"$14}') % (VALID_START[:8] + "01")
    try:
        r = subprocess.run(["ssh", "-i", KEY, "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=25",
                            VPS, "awk '%s' %s" % (awk, COLL)], capture_output=True, text=True, timeout=300)
        ls = [l for l in r.stdout.splitlines() if l.strip()]
        if not ls:
            return {}, {}
        d = pd.DataFrame([l.split("|") for l in ls], columns=["h", "a", "ko", "mtk", "run", "lay", "ltp"])
        for c in ["mtk", "lay", "ltp"]:
            d[c] = pd.to_numeric(d[c], errors="coerce")
        d["g"] = pd.to_datetime(d["ko"], errors="coerce").dt.strftime("%Y-%m-%d") + "|" + d["h"].map(_cn) + "|" + d["a"].map(_cn)
        # lay 0-1 pre-jogo (captura de menor |mtk|, com lay preenchida)
        o01 = {}
        z = d[(d["run"] == "0 - 1") & d["lay"].notna()].copy()
        for g, gg in z.groupby("g"):
            gg = gg.assign(dist=gg["mtk"].abs()); o01[g] = float(gg.loc[gg["dist"].idxmin(), "lay"])
        # FT ROBUSTO: runner de menor lay na janela POS-FT (mtk<=-100), nao numa unica captura
        # (evita o bug do coletor perder gol tardio e gravar placar intermediario).
        ft = {}
        for g, gg in d.dropna(subset=["lay"]).groupby("g"):
            late = gg[gg["mtk"] <= -100]
            use = late if len(late) else gg
            rr = use.loc[use["lay"].idxmin(), "run"]; a, b = rr.split(" - "); ft[g] = (int(a), int(b))
        return o01, ft
    except Exception as e:
        print("[aviso] coletor:", str(e)[:60]); return {}, {}


def main():
    log = pd.read_csv(LOG) if os.path.exists(LOG) else pd.DataFrame(
        columns=["data", "jogo", "liga", "odd_h", "odd_lay01", "stake", "primeiro_visto", "status", "resultado", "pnl"])
    vistos = set((log["data"].astype(str) + "|" + log["jogo"].astype(str))) if len(log) else set()
    o01, ft = puxar_coletor()
    hoje = date.today()

    # 1) loga favoritoes novos (jogo >= hoje) com lay 0-1 real na faixa
    fav = favoritoes_do_dia()
    novos = 0
    if not fav.empty:
        hc = "Home" if "Home" in fav.columns else ("Mandante" if "Mandante" in fav.columns else None)
        ac = "Away" if "Away" in fav.columns else ("Visitante" if "Visitante" in fav.columns else None)
        lc = "League" if "League" in fav.columns else ("Liga" if "Liga" in fav.columns else None)
        for _, r in fav.iterrows():
            d = str(r["Date"])[:10]
            if d < VALID_START:          # forward so a partir de VALID_START (jogos genuinamente novos)
                continue
            h, a = str(r.get(hc, "?")), str(r.get(ac, "?"))
            jogo = f"{h} x {a}"; key = d + "|" + jogo
            if key in vistos:
                continue
            g = d + "|" + _cn(h) + "|" + _cn(a)
            lay01 = o01.get(g)                       # lay real do 0-1 (coletor); pode faltar se o coletor ainda nao pegou
            if lay01 is None or lay01 < LAY_LO or lay01 > LAY_HI:
                continue
            log = pd.concat([log, pd.DataFrame([{
                "data": d, "jogo": jogo, "liga": str(r.get(lc, "?")), "odd_h": round(float(r.get("odd_h", np.nan)), 2),
                "odd_lay01": round(lay01, 2), "stake": 0, "primeiro_visto": hoje.isoformat(),
                "status": "Pendente", "resultado": "Pendente", "pnl": 0.0}])], ignore_index=True)
            novos += 1

    # 2) liquida pendentes cujo placar ja saiu (coletor)
    liq = 0
    for i in log.index[log["status"] == "Pendente"]:
        d = str(log.loc[i, "data"]); h, a = str(log.loc[i, "jogo"]).split(" x ", 1)
        g = d + "|" + _cn(h) + "|" + _cn(a)
        if g in ft:
            gh, ga = ft[g]; ol = float(log.loc[i, "odd_lay01"]); green = not (gh == 0 and ga == 1)
            log.loc[i, "status"] = "Finalizado"; log.loc[i, "resultado"] = "GREEN" if green else "RED"
            log.loc[i, "pnl"] = round((1 - COMM) if green else -(ol - 1), 4); liq += 1

    log.to_csv(LOG, index=False, encoding="utf-8-sig")
    fin = log[log["status"] == "Finalizado"]
    print("novos pendentes: %d | liquidados hoje: %d | total no log: %d (pendentes %d, liquidados %d)"
          % (novos, liq, len(log), (log["status"] == "Pendente").sum(), len(fin)))
    if len(fin) >= 1:
        wr = (fin["resultado"] == "GREEN").mean() * 100
        be = ((fin["odd_lay01"] - 1) / (fin["odd_lay01"] - COMM)).mean() * 100
        roi = fin["pnl"].mean() * 100
        print("=== FORWARD Lay 0x1 favoritao (stake-zero) ===")
        print("N=%d | WR=%.1f%% vs BE=%.1f%% (margem %+.1f%%) | ROI/liability=%+.1f%%"
              % (len(fin), wr, be, wr - be, fin["pnl"].sum() / (fin["odd_lay01"] - 1).sum() * 100))
        if len(fin) >= 100:
            print("  (N>=100: rodar bootstrap+FDR e decidir)")
    else:
        print("(sem liquidados ainda — comeca a valer a partir de %s)" % VALID_START)


if __name__ == "__main__":
    main()
