"""
lay_1x0_rf_v2_strategy.py — Lay 1x0 RF v2 (faixa 6.0–9.5)
Complementa lay_1x0_agressivo_strategy.py (faixa 9.0–11.5).
AUTO-GERADO por treinar_lay_1x0_rf_v2.py — nao editar manualmente.
"""
import os, joblib
import pandas as pd
import numpy as np
from datetime import datetime

MODEL_PATH    = "modelo_lay_1x0_rf_v2.pkl"
SCALER_PATH   = "scaler_lay_1x0_rf_v2.pkl"
FEATURES_PATH = "features_lay_1x0_rf_v2.pkl"

COMMISSION  = 0.05
ODD_MIN     = 6.0
ODD_MAX     = 9.5
EV_MIN      = 0.03
WINDOW      = 8
DECAY_ALPHA = 0.2
MIN_GAMES   = 4


def normalize_live_data(live_payload):
    n = {}
    n["Home"]           = live_payload.get("Home") or live_payload.get("HomeTeam") or ""
    n["Away"]           = live_payload.get("Away") or live_payload.get("AwayTeam") or ""
    n["League"]         = live_payload.get("League") or live_payload.get("Liga") or ""
    n["Time"]           = live_payload.get("Time") or ""
    date_val            = live_payload.get("Date") or live_payload.get("Data_Jogo") or datetime.now().date()
    n["Date"]           = pd.to_datetime(date_val)
    n["Odd_CS_1x0_Lay"] = pd.to_numeric(
        live_payload.get("Odd_CS_1x0_Lay") or live_payload.get("Odd_CS_1x0") or np.nan, errors="coerce")
    n["Odd_H_FT"] = pd.to_numeric(live_payload.get("Odd_H_FT") or live_payload.get("Odd_H_Back") or np.nan, errors="coerce")
    n["Odd_A_FT"] = pd.to_numeric(live_payload.get("Odd_A_FT") or live_payload.get("Odd_A_Back") or np.nan, errors="coerce")
    return n


def check_entry_conditions(match_state):
    odd = match_state.get("Odd_CS_1x0_Lay") or 0.0
    if pd.isna(odd) or odd < ODD_MIN or odd > ODD_MAX:
        return False, "ODD_FORA_FAIXA"
    ev = match_state.get("ev_lay", 0.0)
    if ev < EV_MIN:
        return False, "EV_INSUFICIENTE"
    return True, "APROVADO"


def log_paper_trade(match_state):
    log_file = "paper_trading_log_lay_1x0_rf_v2.csv"
    row = {
        "Date": str(match_state.get("Date", "")),
        "League": match_state.get("League", ""),
        "Home": match_state.get("Home", ""),
        "Away": match_state.get("Away", ""),
        "Odd_CS_1x0_Lay": match_state.get("Odd_CS_1x0_Lay", 0.0),
        "Prob_ML": match_state.get("Prob_ML", 0.0),
        "EV_LAY": match_state.get("ev_lay", 0.0),
        "Decision": match_state.get("Decision", ""),
        "Reason": match_state.get("Reason", ""),
        "Timestamp": datetime.now().isoformat()
    }
    df_r = pd.DataFrame([row])
    if not os.path.exists(log_file):
        df_r.to_csv(log_file, index=False)
    else:
        df_r.to_csv(log_file, mode="a", header=False, index=False)


def _decay_roll(series):
    weights = np.exp(-DECAY_ALPHA * np.arange(WINDOW)[::-1])
    def _wm(arr):
        n = len(arr)
        if n < MIN_GAMES:
            return np.nan
        w = weights[-n:]
        return np.dot(arr, w) / w.sum()
    return series.shift(1).rolling(WINDOW, min_periods=MIN_GAMES).apply(_wm, raw=True)


def predict_and_evaluate_live(live_games_payload, df_historical):
    if not (os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH) and os.path.exists(FEATURES_PATH)):
        return []

    model    = joblib.load(MODEL_PATH)
    scaler   = joblib.load(SCALER_PATH)
    features = joblib.load(FEATURES_PATH)

    df_hist = df_historical.copy()
    df_hist["Date"] = pd.to_datetime(df_hist["Date"], errors="coerce")
    df_hist = df_hist.dropna(subset=["Goals_H_FT","Goals_A_FT","Date","Home","Away"]).copy()

    if live_games_payload:
        first_date = pd.to_datetime(live_games_payload[0].get("Date") or datetime.now().date()).date()
        df_hist = df_hist[df_hist["Date"].dt.date < first_date].copy()

    df_hist = df_hist.sort_values("Date").reset_index(drop=True)
    for c in ["Goals_H_FT","Goals_A_FT"]:
        df_hist[c] = pd.to_numeric(df_hist[c], errors="coerce").fillna(0.0)

    # Contexto mandante (casa)
    df_h = df_hist[["Date","Home","Goals_H_FT","Goals_A_FT"]].copy().rename(columns={"Home":"Team"})
    df_h["won"]        = (df_h["Goals_H_FT"] > df_h["Goals_A_FT"]).astype(float)
    df_h["goals_sc"]   = df_h["Goals_H_FT"]
    df_h["score_1x0"]  = ((df_h["Goals_H_FT"] == 1) & (df_h["Goals_A_FT"] == 0)).astype(float)
    df_h["multi_goal"] = (df_h["Goals_H_FT"] >= 2).astype(float)
    df_h = df_h.sort_values(["Team","Date"]).reset_index(drop=True)
    df_h["H_h_WR"]           = df_h.groupby("Team")["won"].transform(_decay_roll)
    df_h["H_h_goals_rate"]   = df_h.groupby("Team")["goals_sc"].transform(_decay_roll)
    df_h["H_h_score10_rate"] = df_h.groupby("Team")["score_1x0"].transform(_decay_roll)
    df_h["H_h_multi_goal"]   = df_h.groupby("Team")["multi_goal"].transform(_decay_roll)
    home_last = df_h.groupby("Team")[["H_h_WR","H_h_goals_rate","H_h_score10_rate","H_h_multi_goal"]].last().reset_index()

    # Contexto visitante (fora)
    df_a = df_hist[["Date","Away","Goals_A_FT","Goals_H_FT"]].copy().rename(columns={"Away":"Team"})
    df_a["won"]       = (df_a["Goals_A_FT"] > df_a["Goals_H_FT"]).astype(float)
    df_a["goals_sc"]  = df_a["Goals_A_FT"]
    df_a["concede0"]  = (df_a["Goals_H_FT"] == 0).astype(float)
    df_a = df_a.sort_values(["Team","Date"]).reset_index(drop=True)
    df_a["A_a_WR"]         = df_a.groupby("Team")["won"].transform(_decay_roll)
    df_a["A_a_goals_rate"] = df_a.groupby("Team")["goals_sc"].transform(_decay_roll)
    df_a["A_a_concede0"]   = df_a.groupby("Team")["concede0"].transform(_decay_roll)
    away_last = df_a.groupby("Team")[["A_a_WR","A_a_goals_rate","A_a_concede0"]].last().reset_index()

    evaluated = []
    for g in live_games_payload:
        norm_g = normalize_live_data(g)
        home, away = norm_g["Home"], norm_g["Away"]

        sh = home_last[home_last["Team"] == home]
        sa = away_last[away_last["Team"] == away]
        if sh.empty or sa.empty:
            continue
        sh, sa = sh.iloc[0], sa.iloc[0]

        odd = norm_g.get("Odd_CS_1x0_Lay") or np.nan
        if pd.isna(odd):
            continue

        odd_h = norm_g.get("Odd_H_FT") or 2.0
        odd_a = norm_g.get("Odd_A_FT") or 2.0

        h_goals = sh.get("H_h_goals_rate", 1.2)
        a_goals = sa.get("A_a_goals_rate", 0.8)
        h_score10 = sh.get("H_h_score10_rate", 0.08)

        feat_vals = {
            "H_h_WR":           sh.get("H_h_WR", 0.5),
            "H_h_goals_rate":   h_goals,
            "H_h_score10_rate": h_score10,
            "H_h_multi_goal":   sh.get("H_h_multi_goal", 0.45),
            "A_a_WR":           sa.get("A_a_WR", 0.28),
            "A_a_goals_rate":   a_goals,
            "A_a_concede0":     sa.get("A_a_concede0", 0.30),
            "spread_forca":     (1.0/(odd_h+1e-9)) - (1.0/(odd_a+1e-9)),
            "mkt_prob_1x0":     1.0/(odd+1e-9),
            "mkt_edge_signal":  (1.0/(odd+1e-9)) - h_score10,
            "total_goals_proxy": h_goals * a_goals,
        }

        row_mat = pd.DataFrame([{col: feat_vals.get(col, 0.0) for col in features}]).fillna(0.0)
        prob_ml = float(model.predict_proba(scaler.transform(row_mat))[0, 1])

        ev_lay = prob_ml * (1 - COMMISSION) - (1 - prob_ml) * (odd - 1)

        norm_g["Prob_ML"] = prob_ml
        norm_g["ev_lay"]  = ev_lay

        apostar, reason = check_entry_conditions(norm_g)
        norm_g["Decision"] = "APOSTA" if apostar else "SKIP"
        norm_g["Reason"]   = reason
        if apostar:
            log_paper_trade(norm_g)
        evaluated.append(norm_g)

    return evaluated
