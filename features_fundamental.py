# -*- coding: utf-8 -*-
"""
features_fundamental.py — Engenharia de Features Físicas Leak-Free para Modelo 1X2

Calcula:
1. Métricas por mando estrito (Casa vs Fora):
   - H_GF_venue_r5, H_GA_venue_r5, H_Win_venue_r5, H_CS_venue_r5
   - A_GF_venue_r5, A_GA_venue_r5, A_Win_venue_r5, A_CS_venue_r5
2. Métricas globais recentes (últimos 10 jogos em qualquer mando):
   - H_GF_all_r10, H_GA_all_r10, H_Won_all_r10, H_CS_all_r10
   - A_GF_all_r10, A_GA_all_r10, A_Won_all_r10, A_CS_all_r10
3. Diferenciais de confronto (Ataque vs Defesa):
   - Diff_GF_venue, Diff_GA_venue, Diff_Net_venue, Diff_Win_venue
   - Diff_GF_all, Diff_GA_all, Diff_Net_all, Diff_Win_all
4. Cruzamento exato com a base Betfair FRESH (odds reais de Back e Lay executáveis).
"""

import sys
import os
import time
import unicodedata
import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SCRATCH = ROOT / "scratch"
SCRATCH.mkdir(exist_ok=True)

OUTPUT_PARQUET = SCRATCH / "dataset_fundamental_1x2.parquet"
B365_CSV = ROOT / "Bases_de_Dados_API_FutPythonTrader_Bet365.csv"
FRESH_CSV = ROOT / "Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH.csv"


def canon_name(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.strip().lower()


def build_fundamental_dataset():
    t0 = time.time()
    print("=" * 80)
    print("  INICIANDO CONSTRUÇÃO DO DATASET FUNDAMENTAL 1X2 (LEAK-FREE)")
    print("=" * 80)

    # 1. Carregar base Bet365 com 242k partidas
    print(f"[*] Carregando base Bet365: {B365_CSV.name}...")
    cols_b365 = [
        "Date", "League", "Home", "Away",
        "Goals_H_FT", "Goals_A_FT",
        "Odd_H_FT", "Odd_D_FT", "Odd_A_FT"
    ]
    df_b = pd.read_csv(B365_CSV, usecols=cols_b365, low_memory=False)
    df_b["Date"] = pd.to_datetime(df_b["Date"], errors="coerce")
    df_b = df_b.dropna(subset=["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT"]).copy()
    df_b = df_b.sort_values("Date").reset_index(drop=True)
    df_b["Match_ID"] = df_b.index
    print(f"[+] Total de jogos válidos Bet365: {len(df_b)} ({df_b['Date'].min().date()} a {df_b['Date'].max().date()})")

    # 2. Métricas por mando estrito (Casa vs Fora) com shift(1)
    print("[*] Computando métricas móveis por mando estrito (r5)...")
    df_b["H_win"] = (df_b["Goals_H_FT"] > df_b["Goals_A_FT"]).astype(float)
    df_b["H_cs"] = (df_b["Goals_A_FT"] == 0).astype(float)
    df_b["H_GF_venue_r5"] = df_b.groupby("Home")["Goals_H_FT"].transform(lambda s: s.shift(1).rolling(5, min_periods=3).mean())
    df_b["H_GA_venue_r5"] = df_b.groupby("Home")["Goals_A_FT"].transform(lambda s: s.shift(1).rolling(5, min_periods=3).mean())
    df_b["H_Win_venue_r5"] = df_b.groupby("Home")["H_win"].transform(lambda s: s.shift(1).rolling(5, min_periods=3).mean())
    df_b["H_CS_venue_r5"] = df_b.groupby("Home")["H_cs"].transform(lambda s: s.shift(1).rolling(5, min_periods=3).mean())

    df_b["A_win"] = (df_b["Goals_A_FT"] > df_b["Goals_H_FT"]).astype(float)
    df_b["A_cs"] = (df_b["Goals_H_FT"] == 0).astype(float)
    df_b["A_GF_venue_r5"] = df_b.groupby("Away")["Goals_A_FT"].transform(lambda s: s.shift(1).rolling(5, min_periods=3).mean())
    df_b["A_GA_venue_r5"] = df_b.groupby("Away")["Goals_H_FT"].transform(lambda s: s.shift(1).rolling(5, min_periods=3).mean())
    df_b["A_Win_venue_r5"] = df_b.groupby("Away")["A_win"].transform(lambda s: s.shift(1).rolling(5, min_periods=3).mean())
    df_b["A_CS_venue_r5"] = df_b.groupby("Away")["A_cs"].transform(lambda s: s.shift(1).rolling(5, min_periods=3).mean())

    # 3. Métricas globais recentes (últimos 10 jogos em qualquer mando)
    print("[*] Computando métricas móveis globais (r10)...")
    h_rec = pd.DataFrame({
        "Match_ID": df_b["Match_ID"],
        "Date": df_b["Date"],
        "Team": df_b["Home"],
        "is_home": 1,
        "GF": df_b["Goals_H_FT"],
        "GA": df_b["Goals_A_FT"],
        "Won": df_b["H_win"],
        "CS": df_b["H_cs"],
    })
    a_rec = pd.DataFrame({
        "Match_ID": df_b["Match_ID"],
        "Date": df_b["Date"],
        "Team": df_b["Away"],
        "is_home": 0,
        "GF": df_b["Goals_A_FT"],
        "GA": df_b["Goals_H_FT"],
        "Won": df_b["A_win"],
        "CS": df_b["A_cs"],
    })
    team_logs = pd.concat([h_rec, a_rec], ignore_index=True).sort_values(["Team", "Date"]).reset_index(drop=True)
    team_logs["GF_all_r10"] = team_logs.groupby("Team")["GF"].transform(lambda s: s.shift(1).rolling(10, min_periods=5).mean())
    team_logs["GA_all_r10"] = team_logs.groupby("Team")["GA"].transform(lambda s: s.shift(1).rolling(10, min_periods=5).mean())
    team_logs["Won_all_r10"] = team_logs.groupby("Team")["Won"].transform(lambda s: s.shift(1).rolling(10, min_periods=5).mean())
    team_logs["CS_all_r10"] = team_logs.groupby("Team")["CS"].transform(lambda s: s.shift(1).rolling(10, min_periods=5).mean())

    h_feats = team_logs[team_logs["is_home"] == 1].set_index("Match_ID")[["GF_all_r10", "GA_all_r10", "Won_all_r10", "CS_all_r10"]].rename(
        columns={"GF_all_r10": "H_GF_all_r10", "GA_all_r10": "H_GA_all_r10", "Won_all_r10": "H_Won_all_r10", "CS_all_r10": "H_CS_all_r10"}
    )
    a_feats = team_logs[team_logs["is_home"] == 0].set_index("Match_ID")[["GF_all_r10", "GA_all_r10", "Won_all_r10", "CS_all_r10"]].rename(
        columns={"GF_all_r10": "A_GF_all_r10", "GA_all_r10": "A_GA_all_r10", "Won_all_r10": "A_Won_all_r10", "CS_all_r10": "A_CS_all_r10"}
    )
    df_b = df_b.join(h_feats, on="Match_ID").join(a_feats, on="Match_ID")

    # 4. Diferenciais de confronto
    print("[*] Computando diferenciais táticos de confronto...")
    df_b["Diff_GF_venue"] = df_b["H_GF_venue_r5"] - df_b["A_GA_venue_r5"]
    df_b["Diff_GA_venue"] = df_b["A_GF_venue_r5"] - df_b["H_GA_venue_r5"]
    df_b["Diff_Net_venue"] = (df_b["H_GF_venue_r5"] - df_b["H_GA_venue_r5"]) - (df_b["A_GF_venue_r5"] - df_b["A_GA_venue_r5"])
    df_b["Diff_Win_venue"] = df_b["H_Win_venue_r5"] - df_b["A_Win_venue_r5"]

    df_b["Diff_GF_all"] = df_b["H_GF_all_r10"] - df_b["A_GA_all_r10"]
    df_b["Diff_GA_all"] = df_b["A_GF_all_r10"] - df_b["H_GA_all_r10"]
    df_b["Diff_Net_all"] = (df_b["H_GF_all_r10"] - df_b["H_GA_all_r10"]) - (df_b["A_GF_all_r10"] - df_b["A_GA_all_r10"])
    df_b["Diff_Win_all"] = df_b["H_Won_all_r10"] - df_b["A_Won_all_r10"]

    # Target 1X2: 0=Home Win, 1=Draw, 2=Away Win
    conditions = [
        df_b["Goals_H_FT"] > df_b["Goals_A_FT"],
        df_b["Goals_H_FT"] == df_b["Goals_A_FT"],
        df_b["Goals_H_FT"] < df_b["Goals_A_FT"]
    ]
    df_b["Target_1X2"] = np.select(conditions, [0, 1, 2], default=-1)

    # 5. Cruzar com a base FRESH (Betfair Real Back & Lay odds)
    print(f"[*] Carregando base FRESH Betfair: {FRESH_CSV.name}...")
    cols_fresh = [
        "Date", "Home", "Away",
        "Odd_H_Back", "Odd_H_Lay",
        "Odd_D_Back", "Odd_D_Lay",
        "Odd_A_Back", "Odd_A_Lay"
    ]
    df_f = pd.read_csv(FRESH_CSV, usecols=cols_fresh, low_memory=False)
    df_f["Date"] = pd.to_datetime(df_f["Date"], errors="coerce")
    df_f = df_f.dropna(subset=["Date", "Home", "Away"]).copy()

    # Chaves canônicas para merge
    df_b["Date_str"] = df_b["Date"].dt.strftime("%Y-%m-%d")
    df_b["H_canon"] = df_b["Home"].apply(canon_name)
    df_b["A_canon"] = df_b["Away"].apply(canon_name)

    df_f["Date_str"] = df_f["Date"].dt.strftime("%Y-%m-%d")
    df_f["H_canon"] = df_f["Home"].apply(canon_name)
    df_f["A_canon"] = df_f["Away"].apply(canon_name)

    print("[*] Cruzando dados das duas bases por [Date, Home, Away] canônico...")
    # Deduplicar FRESH por chave antes de merge
    df_f = df_f.drop_duplicates(subset=["Date_str", "H_canon", "A_canon"]).copy()
    
    # Left join a partir da FRESH para ter as odds reais da Betfair
    merged = pd.merge(
        df_f,
        df_b.drop(columns=["Home", "Away", "Date"]),
        on=["Date_str", "H_canon", "A_canon"],
        how="inner"
    )

    print(f"[+] Total de jogos pareados com Betfair executável: {len(merged)} de {len(df_f)} ({len(merged)/len(df_f):.1%})")

    # Salvar em Parquet
    merged.to_parquet(OUTPUT_PARQUET, index=False)
    print(f"[+] Dataset salvo com sucesso em: {OUTPUT_PARQUET} ({time.time()-t0:.2f}s total)")
    print(f"    Colunas no dataset: {list(merged.columns)}")
    return OUTPUT_PARQUET


if __name__ == "__main__":
    build_fundamental_dataset()
