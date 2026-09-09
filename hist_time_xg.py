#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
hist_time_xg.py — pull historico de stats de JOGO COMPLETO por partida, para montar features
rolling de time (xG a favor/contra, aberto x bola parada, xGOT, ameaca real).

Serve o LOTE PRE-REGISTRADO H1+H2+H3 de HIPOTESES_CANDIDATAS_API.md:
  H1  xG rolling no lugar de gols como feature do Lay 0x0
  H2  desacordo modelo-preco x espessura do mercado (usa `matched` do coletor Betfair)
  H3  xG de bola parada nao vale o mesmo que xG de bola rolando

ESCOPO CONGELADO: as 25 ligas que concentram 60% dos sinais OOS do Lay 0x0, ultimos 300 dias.
CUSTO: ~282 requisicoes de agenda + ~6.974 de stats = ~7.250 de 20.000/mes.
RETOMAVEL: agenda e stats em cache no disco; rodar de novo so busca o que falta.

  python hist_time_xg.py --dias 300 --top-ligas 25 --max-req 8000
"""
import os, csv, sys, json, argparse
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import hist_xg_ht as H          # reaproveita cn/nomes_batem/get/agenda/load_key

AQUI = os.path.dirname(os.path.abspath(__file__))
OOS_0X0 = r"c:\Users\thiag\OneDrive\Documentos\GitHub\DASHBOARD_ARKAD-1\lay_0x0_real_oos_bets_xgb.csv"
DIR_FT = os.path.join(AQUI, "hist_stats_ft")
SAIDA = os.path.join(AQUI, "hist_time_stats.csv")

CAMPOS = [("expected_goals", "xg"), ("expected_goals_open_play", "xg_open"),
          ("expected_goals_set_play", "xg_set"), ("expected_goals_non_penalty", "xg_np"),
          ("expected_goals_on_target", "xgot"), ("total_shots", "shots"),
          ("ShotsOnTarget", "sot"), ("big_chance", "bigch"),
          ("touches_opp_box", "tbox"), ("keeper_saves", "saves"),
          ("corners", "corners"), ("BallPossesion", "poss")]


def stats_ft(eventid, key, teto):
    """Stats do JOGO COMPLETO. 1 requisicao, cacheada. {} = liga sem cobertura."""
    os.makedirs(DIR_FT, exist_ok=True)
    p = os.path.join(DIR_FT, "%s.json" % eventid)
    if os.path.exists(p):
        return json.load(open(p, encoding="utf-8"))
    try:
        d = H.get("football-get-match-all-stats?eventid=%s" % eventid, key, teto)
    except RuntimeError:
        raise
    except Exception as e:
        print("  [stats %s] %s" % (eventid, str(e)[:50]), flush=True)
        return None
    out = {}
    if d.get("status") == "success":
        for grupo in (d.get("response", {}) or {}).get("stats", []) or []:
            for st in grupo.get("stats", []) or []:
                k, v = st.get("key"), st.get("stats")
                if k and v and v[0] is not None and k not in out:
                    out[k] = v
    json.dump(out, open(p, "w", encoding="utf-8"))
    return out


def ligas_do_0x0(n):
    import pandas as pd
    v = pd.read_csv(OOS_0X0, usecols=["League"], low_memory=False).League.value_counts()
    return list(v.index[:n])


def alvos(dias, ligas):
    import pandas as pd
    cols = ["Date", "League", "Home", "Away", "Goals_H_FT", "Goals_A_FT",
            "Odd_H_FT", "Odd_D_FT", "Odd_A_FT", "Odd_Over25_FT", "Odd_CS_0x0"]
    df = pd.read_csv(H.BASE_CSV, usecols=cols, low_memory=False)
    df["Date"] = pd.to_datetime(df.Date, errors="coerce")
    for c in cols[4:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    lim = df.Date.max() - pd.Timedelta(days=dias)
    r = df[(df.Date >= lim) & (df.League.isin(ligas)) & df.Goals_H_FT.notna()]
    return r.sort_values("Date").reset_index(drop=True)


def num(v):
    """'22 (52%)' -> 22.0 ; '0.77' -> 0.77 ; 63 -> 63.0"""
    if v is None:
        return ""
    s = str(v).split("(")[0].strip().replace("%", "")
    try:
        return float(s)
    except Exception:
        return ""


COLS = (["data", "liga", "home", "away", "gols_h", "gols_a", "odd_h", "odd_d", "odd_a",
         "odd_over25", "odd_cs_0x0", "eventid", "league_id", "tem_stats", "tem_xg"]
        + ["%s_h" % s for _, s in CAMPOS] + ["%s_a" % s for _, s in CAMPOS])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dias", type=int, default=300)
    ap.add_argument("--top-ligas", type=int, default=25)
    ap.add_argument("--max-req", type=int, default=8000)
    args = ap.parse_args()

    key = H.load_key()
    if not key:
        print("[erro] sem RAPIDAPI_KEY"); sys.exit(1)

    ligas = ligas_do_0x0(args.top_ligas)
    alvo = alvos(args.dias, ligas)
    print("=== hist_time_xg (lote H1+H2+H3) ===")
    print("ligas (top %d do 0x0): %s" % (args.top_ligas, ", ".join(ligas[:8]) + " ..."))
    print("partidas na janela de %d dias: %d" % (args.dias, len(alvo)))
    print("datas distintas: %d | teto de requisicoes: %d\n"
          % (alvo.Date.dt.date.nunique(), args.max_req), flush=True)

    linhas = []
    casou = comstats = comxg = 0
    for i, row in alvo.iterrows():
        cands = []
        try:
            for d in (0, 1, -1):
                cands += H.agenda((row.Date + timedelta(days=d)).strftime("%Y%m%d"), key, args.max_req)
        except RuntimeError as e:
            print("\n[parou] %s" % e); break
        eid = lid = ""
        for m in cands:
            if H.nomes_batem(row.Home, m["home"]) and H.nomes_batem(row.Away, m["away"]):
                eid, lid = m["id"], m["league_id"]; break
        st = None
        if eid:
            casou += 1
            try:
                st = stats_ft(eid, key, args.max_req)
            except RuntimeError as e:
                print("\n[parou] %s" % e); break
        if st:
            comstats += 1
            if "expected_goals" in st:
                comxg += 1
        base = [row.Date.strftime("%Y-%m-%d"), row.League, row.Home, row.Away,
                row.Goals_H_FT, row.Goals_A_FT, row.Odd_H_FT, row.Odd_D_FT, row.Odd_A_FT,
                row.Odd_Over25_FT, row.Odd_CS_0x0, eid, lid,
                int(bool(st)), int("expected_goals" in (st or {}))]
        h = [num((st or {}).get(k, [None, None])[0]) if (st or {}).get(k) else "" for k, _ in CAMPOS]
        a = [num((st or {}).get(k, [None, None])[1]) if (st or {}).get(k) else "" for k, _ in CAMPOS]
        linhas.append(base + h + a)
        if (i + 1) % 200 == 0:
            print("  %5d/%d | casou %d | com stats %d | com xG %d | req %d"
                  % (i + 1, len(alvo), casou, comstats, comxg, H.req[0]), flush=True)
            with open(SAIDA, "w", encoding="utf-8-sig", newline="") as f:
                w = csv.writer(f); w.writerow(COLS); w.writerows(linhas)

    with open(SAIDA, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f); w.writerow(COLS); w.writerows(linhas)
    print("\ngravado %s (%d linhas)" % (SAIDA, len(linhas)))
    print("casou %d | com stats %d | com xG %d | requisicoes gastas: %d"
          % (casou, comstats, comxg, H.req[0]))


if __name__ == "__main__":
    main()
