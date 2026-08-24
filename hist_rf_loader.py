"""hist_rf_loader.py — Carregador de Alta Fidelidade da Base Histórica com Métricas Ricas | ARKAD PROD"""
import os
from pathlib import Path
import pandas as pd
import streamlit as st

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
def load_hist_rf(file_path=None):
    """
    Carrega a base oficial Bet365 com todas as features estatísticas ricas (xGOT, Posse, Finalizações, etc).
    Funciona tanto no ambiente local quanto no Streamlit Cloud (usando b365_base_lean.csv rastreada no Git).
    """
    root = Path(__file__).resolve().parent
    
    candidates = [
        file_path,
        root / "Bases_de_Dados_API_FutPythonTrader_Bet365.csv",
        root / "b365_base_lean.csv",
        root / "Resultados_2024_2026.csv",
        root / "Resultados_2026_Full.csv"
    ]
    
    df = None
    for candidate in candidates:
        if candidate and os.path.exists(str(candidate)):
            try:
                candidate_df = pd.read_csv(str(candidate), low_memory=False)
                if not candidate_df.empty and "Date" in candidate_df.columns:
                    # Verifica se contém as colunas ricas
                    has_metrics = all(c in candidate_df.columns for c in REQUIRED_RICH_METRICS[:4])
                    if has_metrics:
                        df = candidate_df
                        break
            except Exception:
                continue

    if df is None or df.empty:
        try:
            from b365_data_utils import load_b365_historical
            df = load_b365_historical()
        except Exception:
            df = pd.DataFrame()

    if df.empty:
        raise FileNotFoundError("Nenhuma base histórica rica pôde ser carregada no ambiente.")

    # Garantir presença de todas as colunas ricas (preenchendo 0.0 caso alguma secundária falte)
    for c in REQUIRED_RICH_METRICS:
        if c not in df.columns:
            df[c] = 0.0
        else:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    # Padronizar datas
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date", "Home", "Away", "Goals_H_FT", "Goals_A_FT"]).copy()

    # Ordenar cronologicamente
    df = df.sort_values("Date", kind="mergesort").reset_index(drop=True)
    return df
