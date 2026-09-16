#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
stream_medir_odds.py — Leitura por streaming (memoria constante < 30MB) para medir
as faixas de odd dos 4 metodos trader no coletor da VPS.
"""
import os, sys, csv, re
import numpy as np

COLL = "/home/ubuntu/betfair-collector/betfair_live_odds.csv"
if not os.path.exists(COLL):
    print("Coletor nao encontrado em:", COLL)
    sys.exit(1)

# Amostragem das ultimas 500.000 linhas (~7 dias de operacao)
import subprocess
print("Executando amostragem das ultimas 600.000 linhas via streaming...")
proc = subprocess.Popen(["tail", "-n", "600000", COLL], stdout=subprocess.PIPE, text=True, errors="replace")

m1_draw_lays = []
m2_fav_backs = []
m3_under_backs = []
late_goal_backs = []

# Mapeamento rapido de favoritos pre-jogo
favs = {} # (ko, h, a) -> menor back em MO

reader = csv.reader(proc.stdout)
header = next(reader, None)

for row in reader:
    if len(row) < 14:
        continue
    # capture_ts, market_type, competition, home, away, ko, min_to_ko, runner, selection_id, back, back_size, lay, lay_size, ltp, matched
    ts = row[0]
    mtype = row[1]
    h = row[3]
    a = row[4]
    ko = row[5]
    try:
        mtk = float(row[6])
    except Exception:
        continue
    runner = row[7]
    
    # Minuto = -min_to_ko - 15
    minuto = int(-mtk - 15)
    
    # Pre-jogo: min_to_ko entre -25 e -15 (perto de -5 do apito)
    if -25 <= mtk <= -10 and mtype == "MATCH_ODDS":
        try:
            b = float(row[9])
            k = (ko[:16], h, a)
            if runner == h and b > 1.01:
                cur = favs.get(k)
                if cur is None or b < cur:
                    favs[k] = b
        except Exception:
            pass

    # M1: LTD Trader (15' a 25', Lay Draw em fav mandante <= 1.45)
    if 15 <= minuto <= 25 and mtype == "MATCH_ODDS" and runner == "The Draw":
        k = (ko[:16], h, a)
        f = favs.get(k)
        if f and f <= 1.45:
            try:
                ly = float(row[11])
                if 1.05 <= ly <= 20.0:
                    m1_draw_lays.append(ly)
            except Exception:
                pass

    # M2: Fav em Desvantagem (20' a 45', Back Fav mandante)
    if 20 <= minuto <= 45 and mtype == "MATCH_ODDS":
        k = (ko[:16], h, a)
        f = favs.get(k)
        if f and f <= 1.35 and runner == h:
            try:
                b = float(row[9])
                if 1.05 <= b <= 20.0:
                    m2_fav_backs.append(b)
            except Exception:
                pass

    # M3: Scalping Under 1.5 HT (33' a 38')
    if 33 <= minuto <= 38 and mtype == "OVER_UNDER_15" and "Under" in runner:
        try:
            b = float(row[9])
            if 1.01 <= b <= 5.0:
                m3_under_backs.append(b)
        except Exception:
            pass

    # M4: Late Goal (82' a 86', Over 1.5 ou Over 3.5)
    if 82 <= minuto <= 86 and mtype in ("OVER_UNDER_15", "OVER_UNDER_35") and "Over" in runner:
        try:
            b = float(row[9])
            if 1.05 <= b <= 10.0:
                late_goal_backs.append(b)
        except Exception:
            pass

proc.stdout.close()
proc.wait()

def print_pcts(nome, arr):
    if not arr:
        print(f"{nome}: N=0")
        return
    arr = np.array(arr)
    p5, p25, med, p75, p95 = np.percentile(arr, [5, 25, 50, 75, 95])
    print(f"\n{nome} (N={len(arr)} capturas):")
    print(f"  p5={p5:.2f} | p25={p25:.2f} | mediana={med:.2f} | p75={p75:.2f} | p95={p95:.2f}")

print("\n=======================================================")
print("MEDICOES REAIS DAS ODDS NO COLETOR BETFAIR IN-PLAY")
print("=======================================================")
print_pcts("M1: Lay Draw @ 15'-25' (Fav Mandante <= 1.45)", m1_draw_lays)
print_pcts("M2: Back Fav Mandante @ 20'-45' (Fav <= 1.35)", m2_fav_backs)
print_pcts("M3: Back Under 1.5 HT @ 33'-38'", m3_under_backs)
print_pcts("M4: Back Over 1.5/3.5 @ 82'-86' (Late Goal)", late_goal_backs)
