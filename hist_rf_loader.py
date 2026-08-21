"""hist_rf_loader.py — Carregador de Alta Fidelidade da Base Histórica com Métricas Ricas | ARKAD PROD"""
import os
import pandas as pd
import streamlit as st

HIST_PATH = "Bases_de_Dados_API_FutPythonTrader_Bet365.csv"

REQUIRED_RICH_METRICS = [
    "Goals_H_FT", "Goals_A_FT",
    "xGOT_H_FT", "xGOT_A_FT",
    "xGOT_Faced_H_FT", "xGOT_Faced_A_FT",
    "Goals_Prevented_H_FT", "Goals_Prevented_A_FT",
    "Big_Chances_H_FT", "Big_Chances_A_FT",
    "Shots_On_Target_H_FT", "Shots_On_Target_A_FT",
    "Possession_H_FT", "Possession_A_FT"
]

@st.cache_data(show_spinner=False, ttl=3600)
def load_hist_rf(file_path=HIST_PATH):
    """
    Carrega a base oficial Bet365 com todas as features estatísticas ricas (xGOT, Posse, Finalizações, etc).
    Garante que nenhuma coluna avançada falte ou seja zerada silenciosamente.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Base de dados histórica não encontrada em {file_path}")

    # Ler a base oficial
    df = pd.read_csv(file_path, low_memory=False)

    # Validar presença de colunas ricas
    missing_cols = [c for c in REQUIRED_RICH_METRICS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Base histórica corrompida: faltam as seguintes colunas ricas: {missing_cols}")

    # Padronizar datas
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT"]).copy()

    # Converter numéricas
    for c in REQUIRED_RICH_METRICS:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    # Ordenar cronologicamente
    df = df.sort_values("Date", kind="mergesort").reset_index(drop=True)
    return df
