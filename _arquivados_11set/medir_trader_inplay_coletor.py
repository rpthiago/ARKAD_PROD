# -*- coding: utf-8 -*-
"""
medir_trader_inplay_coletor.py — Mede as distribuições de odds empíricas e o Primeiro Olhar
dos 4 Métodos Trader In-Play sobre o coletor oficial da VPS (/home/ubuntu/betfair-collector/betfair_live_odds.csv).
"""

import os
import sys
import re
import csv
import pandas as pd
import numpy as np

COLL = "/home/ubuntu/betfair-collector/betfair_live_odds.csv"
if not os.path.exists(COLL):
    # Fallback local
    COLL = os.path.join(os.environ.get("TEMP", "."), "var", "betfair_live_odds.csv")

LINHAS = {"OVER_UNDER_05": 0.5, "OVER_UNDER_15": 1.5, "OVER_UNDER_25": 2.5, "OVER_UNDER_35": 3.5}

def medir():
    print(f"Lendo coletor: {COLL}...")
    # Lê colunas essenciais
    cols = ["capture_ts", "market_type", "home", "away", "ko", "min_to_ko", "runner", "back", "back_size", "lay", "lay_size"]
    
    # Processa via chunks para não estourar memória com 2GB
    # Precisamos de: MATCH_ODDS e OVER_UNDER_05..35
    chunksize = 200000
    
    mo_rows = []
    ou_rows = []
    
    count = 0
    for chunk in pd.read_csv(COLL, usecols=cols, chunksize=chunksize, dtype=str, low_memory=False):
        count += len(chunk)
        chunk["min_to_ko"] = pd.to_numeric(chunk["min_to_ko"], errors="coerce")
        chunk["minuto"] = -chunk["min_to_ko"] - 15
        
        # Filtra MATCH_ODDS e OVER_UNDER
        c_mo = chunk[chunk["market_type"] == "MATCH_ODDS"].copy()
        c_ou = chunk[chunk["market_type"].isin(LINHAS.keys())].copy()
        
        if not c_mo.empty:
            mo_rows.append(c_mo)
        if not c_ou.empty:
            ou_rows.append(c_ou)
            
        print(f"  processadas {count:,} linhas...", flush=True)
        if count >= 2000000:  # amostra de 2M linhas
            break
            
    df_mo = pd.concat(mo_rows, ignore_index=True)
    df_ou = pd.concat(ou_rows, ignore_index=True)
    
    df_mo["back"] = pd.to_numeric(df_mo["back"], errors="coerce")
    df_mo["lay"] = pd.to_numeric(df_mo["lay"], errors="coerce")
    df_ou["back"] = pd.to_numeric(df_ou["back"], errors="coerce")
    df_ou["lay"] = pd.to_numeric(df_ou["lay"], errors="coerce")
    
    print(f"Carregados: MO={len(df_mo)}, OU={len(df_ou)}")
    
    # 1. Favorito pré-jogo por partida (menor back em MATCH_ODDS perto de -5 min, ie mtk entre -25 e -15)
    pre = df_mo[(df_mo["min_to_ko"] >= -25) & (df_mo["min_to_ko"] <= -10)]
    favs = {}
    for (ko, h, a), g in pre.groupby(["ko", "home", "away"]):
        r_h = g[g["runner"] == h]
        if not r_h.empty:
            b_h = r_h["back"].dropna()
            if not b_h.empty:
                favs[(ko, h, a)] = float(b_h.min())
                
    print(f"Jogos com fav mandante pré-jogo mapeado: {len(favs)}")
    
    # 2. Gols por O/U
    o = df_ou[df_ou["runner"].str.startswith("Over", na=False)].copy()
    o["L"] = o["market_type"].map(LINHAS)
    ou_gols = {}
    
    for (ko, h, a), g in o.groupby(["ko", "home", "away"]):
        g = g.sort_values("minuto")
        ult = g.groupby("L")["minuto"].max().to_dict()
        for ts, cap in g.groupby("capture_ts"):
            mn = float(cap["minuto"].iloc[0])
            pres = {float(r.L): float(r.back) for r in cap.itertuples()}
            bat, nao = [], []
            for L in LINHAS.values():
                if L in pres:
                    (bat if pres[L] <= 1.02 else nao).append(L)
                elif L in ult and ult[L] < mn:
                    bat.append(L)
            lo_ = (max(bat) + 0.5) if bat else 0
            hi_ = (min(nao) - 0.5) if nao else None
            if hi_ is not None and lo_ == hi_:
                ou_gols[(ts, ko, h, a)] = int(lo_)
            elif hi_ is None and bat and max(bat) == 3.5:
                ou_gols[(ts, ko, h, a)] = 4

    print(f"Capturas com placar total O/U exato: {len(ou_gols)}")
    
    # --- MEDIÇÃO M1: Lay Draw aos 15'-25' em 0-0 com fav mandante <= 1.45 ---
    m1_odds = []
    mo_in = df_mo[(df_mo["minuto"] >= 15) & (df_mo["minuto"] <= 25) & (df_mo["runner"] == "The Draw")]
    for _, r in mo_in.iterrows():
        ko, h, a = r["ko"], r["home"], r["away"]
        f = favs.get((ko, h, a))
        if f and f <= 1.45:
            ts = r["capture_ts"]
            gols = ou_gols.get((ts, ko, h, a))
            if gols == 0:  # 0-0
                ly = r["lay"]
                if ly and 1.05 <= ly <= 20.0:
                    m1_odds.append(ly)
                    
    # --- MEDIÇÃO M2: Back Fav aos 20'-45' em 0-1 com fav mandante <= 1.35 ---
    m2_odds = []
    mo_fav = df_mo[(df_mo["minuto"] >= 20) & (df_mo["minuto"] <= 45)]
    for _, r in mo_fav.iterrows():
        ko, h, a = r["ko"], r["home"], r["away"]
        if r["runner"] == h:
            f = favs.get((ko, h, a))
            if f and f <= 1.35:
                ts = r["capture_ts"]
                gols = ou_gols.get((ts, ko, h, a))
                if gols == 1:
                    bk = r["back"]
                    if bk and 1.05 <= bk <= 20.0:
                        m2_odds.append(bk)
                        
    # --- MEDIÇÃO M3: Back Under 1.5 HT aos 33'-38' em 0-0 ---
    m3_odds = []
    u_ht = df_ou[(df_ou["minuto"] >= 33) & (df_ou["minuto"] <= 38) & (df_ou["market_type"] == "OVER_UNDER_15") & (df_ou["runner"].str.startswith("Under", na=False))]
    for _, r in u_ht.iterrows():
        ko, h, a = r["ko"], r["home"], r["away"]
        ts = r["capture_ts"]
        gols = ou_gols.get((ts, ko, h, a))
        if gols == 0:
            bk = r["back"]
            if bk and 1.01 <= bk <= 5.0:
                m3_odds.append(bk)

    def stats(nome, arr):
        s = pd.Series(arr).dropna()
        if s.empty:
            print(f"\n{nome}: N=0")
            return
        p5, p25, med, p75, p95 = np.percentile(s, [5, 25, 50, 75, 95])
        print(f"\n{nome} (N={len(s)}):")
        print(f"  p5={p5:.2f} | p25={p25:.2f} | mediana={med:.2f} | p75={p75:.2f} | p95={p95:.2f}")

    print("\n=======================================================")
    print("DISTRIBUIÇÃO DAS ODDS MEDIDAS NO COLETOR BETFAIR REAL")
    print("=======================================================")
    stats("M1 — Lay Draw @ 15'-25' (0-0, Fav <= 1.45)", m1_odds)
    stats("M2 — Back Fav @ 20'-45' (0-1, Fav <= 1.35)", m2_odds)
    stats("M3 — Back Under 1.5 HT @ 33'-38' (0-0)", m3_odds)

if __name__ == "__main__":
    medir()
