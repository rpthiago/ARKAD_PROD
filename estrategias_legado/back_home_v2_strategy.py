"""
back_home_v2_strategy.py — Método B: Mandante Específico (v2)
=============================================================
Hipótese: mandantes são confiáveis SOMENTE quando têm forma forte em casa
E o visitante tem forma fraca fora.

Walk-forward OOS: ROI médio -4.1% | 7775 picks | EV>0 em 4/4 últimos meses
Critério: EV = prob_modelo × Odd_H_FT - 1 > 0.03
Pré-filtro: h_WR >= 0.45 AND a_loss_rate >= 0.45
Odd: 1.45–2.0
"""

import os
import pandas as pd
import numpy as np
import joblib
from datetime import datetime
from pathlib import Path

MODEL_PATH    = "modelo_back_home_v2.pkl"
SCALER_PATH   = "scaler_back_home_v2.pkl"
FEATURES_PATH = "features_back_home_v2.pkl"
LIGAS_PATH    = "ligas_back_home_v2.txt"
COMMISSION    = 0.05
EV_MIN        = 0.03
ODD_MIN       = 1.45
ODD_MAX       = 2.0
PRE_HOME_WR_MIN  = 0.45
PRE_AWAY_LOSS_MIN = 0.45
WINDOW        = 5
DECAY_ALPHA   = 0.3
MIN_GAMES     = 2


def _decay_roll(series, window=WINDOW):
    weights = np.exp(-DECAY_ALPHA * np.arange(window)[::-1])
    def _wm(arr):
        n = len(arr)
        if n < MIN_GAMES:
            return np.nan
        w = weights[-n:]
        return np.dot(arr, w) / w.sum()
    return series.shift(1).rolling(window, min_periods=MIN_GAMES).apply(_wm, raw=True)


def normalize_live_data(live_payload):
    n = {}
    n["Home"]    = live_payload.get("Home", "")
    n["Away"]    = live_payload.get("Away", "")
    n["League"]  = live_payload.get("League", "")
    n["Date"]    = pd.to_datetime(live_payload.get("Date", datetime.now().date()))
    n["Odd_H_FT"]= pd.to_numeric(live_payload.get("Odd_H_FT") or live_payload.get("Odd_H_Back"), errors="coerce")
    n["Odd_D_FT"]= pd.to_numeric(live_payload.get("Odd_D_FT"), errors="coerce")
    n["Odd_A_FT"]= pd.to_numeric(live_payload.get("Odd_A_FT") or live_payload.get("Odd_A_Back"), errors="coerce")
    for c in ["Goals_H_FT","Goals_A_FT","xGOT_H_FT","xGOT_A_FT",
              "xGOT_Faced_H_FT","xGOT_Faced_A_FT",
              "Goals_Prevented_H_FT","Goals_Prevented_A_FT",
              "Big_Chances_H_FT","Big_Chances_A_FT",
              "Shots_On_Target_H_FT","Shots_Inside_Box_H_FT",
              "Shots_On_Target_A_FT","Shots_Inside_Box_A_FT",
              "Possession_H_FT","Possession_A_FT"]:
        n[c] = pd.to_numeric(live_payload.get(c, 0.0), errors="coerce")
    return n


def check_entry_conditions(match_state):
    odd = match_state.get("Odd_H_FT", 0.0)
    if pd.isna(odd) or odd < ODD_MIN or odd > ODD_MAX:
        return False, "ODD_FORA_FAIXA"
    # Pré-filtro estrutural
    h_wr      = match_state.get("H_h_WR", 0.0) or 0.0
    a_loss    = match_state.get("A_a_loss_rate", 0.0) or 0.0
    if h_wr < PRE_HOME_WR_MIN:
        return False, f"HOME_WR_BAIXO({h_wr:.2f})"
    if a_loss < PRE_AWAY_LOSS_MIN:
        return False, f"AWAY_FRACO_INSUF({a_loss:.2f})"
    # EV
    prob = match_state.get("Prob_ML", 0.0)
    ev   = prob * odd - 1.0
    if ev < EV_MIN:
        return False, f"EV_BAIXO({ev:+.3f})"
    league = match_state.get("League", "")
    if os.path.exists(LIGAS_PATH):
        with open(LIGAS_PATH, "r", encoding="utf-8") as f:
            ligas = [l.strip() for l in f.read().split(",") if l.strip()]
        if league not in ligas:
            return False, "LIGA_BLOQUEADA"
    return True, "APROVADO"


def log_paper_trade(match_state):
    log_file = "paper_trading_log_back_home_v2.csv"
    row = {
        "Date":       str(match_state.get("Date", "")),
        "League":     match_state.get("League", ""),
        "Home":       match_state.get("Home", ""),
        "Away":       match_state.get("Away", ""),
        "Odd_H_FT":   match_state.get("Odd_H_FT", 0.0),
        "Prob_ML":    match_state.get("Prob_ML", 0.0),
        "EV":         round(match_state.get("Prob_ML",0)*match_state.get("Odd_H_FT",1)-1, 4),
        "H_h_WR":     match_state.get("H_h_WR", 0.0),
        "A_a_loss_rate": match_state.get("A_a_loss_rate", 0.0),
        "ctx_xGOT_diff": match_state.get("ctx_xGOT_diff", 0.0),
        "Decision":   match_state.get("Decision", ""),
        "Reason":     match_state.get("Reason", ""),
        "Timestamp":  datetime.now().isoformat(),
    }
    df_row = pd.DataFrame([row])
    if not os.path.exists(log_file):
        df_row.to_csv(log_file, index=False)
    else:
        df_row.to_csv(log_file, mode="a", header=False, index=False)


def predict_and_evaluate_live(live_games_payload, df_historical):
    root = Path(__file__).resolve().parent
    if not all((root / p).exists() for p in [MODEL_PATH, SCALER_PATH, FEATURES_PATH]):
        return []
    model    = joblib.load(root / MODEL_PATH)
    scaler   = joblib.load(root / SCALER_PATH)
    features = joblib.load(root / FEATURES_PATH)

    df_hist = df_historical.copy()
    df_hist["Date"] = pd.to_datetime(df_hist["Date"], errors="coerce")
    df_hist = df_hist.dropna(subset=["Goals_H_FT","Goals_A_FT","Date","Home","Away"]).copy()
    if live_games_payload:
        fd = pd.to_datetime(live_games_payload[0].get("Date", datetime.now().date())).date()
        df_hist = df_hist[df_hist["Date"].dt.date < fd].copy()
    df_hist = df_hist.sort_values("Date").reset_index(drop=True)

    stat_cols = ["Goals_H_FT","Goals_A_FT","xGOT_H_FT","xGOT_A_FT",
                 "xGOT_Faced_H_FT","xGOT_Faced_A_FT","Goals_Prevented_H_FT","Goals_Prevented_A_FT",
                 "Big_Chances_H_FT","Big_Chances_A_FT","Shots_On_Target_H_FT","Shots_Inside_Box_H_FT",
                 "Shots_On_Target_A_FT","Shots_Inside_Box_A_FT","Possession_H_FT","Possession_A_FT"]
    for c in stat_cols:
        df_hist[c] = pd.to_numeric(df_hist[c], errors="coerce").fillna(0.0) if c in df_hist.columns else 0.0

    df_hist["_won"] = (df_hist["Goals_H_FT"] > df_hist["Goals_A_FT"]).astype(float)

    # Vista HOME
    dh = df_hist[["Date","Home","Goals_H_FT","Goals_A_FT","xGOT_H_FT","xGOT_Faced_H_FT",
                  "Goals_Prevented_H_FT","Big_Chances_H_FT","Big_Chances_A_FT",
                  "Shots_On_Target_H_FT","Shots_Inside_Box_H_FT","Possession_H_FT","_won"]].copy()
    dh.columns = ["Date","Team","Gf","Gc","xGOT_scored","xGOT_faced","GP",
                  "BC_scored","BC_conceded","SoT","SiB","Poss","won"]
    dh_s = dh.sort_values(["Team","Date"]).reset_index(drop=True)
    for col, name in [("Gf","h_Gf"),("Gc","h_Gc"),("xGOT_scored","h_xGOT"),
                      ("xGOT_faced","h_xGOT_faced"),("GP","h_GP"),("BC_scored","h_BC"),
                      ("SoT","h_SoT"),("SiB","h_SiB"),("Poss","h_Poss"),("won","h_WR")]:
        dh_s[name] = dh_s.groupby("Team")[col].transform(_decay_roll)

    # Vista AWAY
    da = df_hist[["Date","Away","Goals_A_FT","Goals_H_FT","xGOT_A_FT","xGOT_Faced_A_FT",
                  "Goals_Prevented_A_FT","Big_Chances_A_FT","Big_Chances_H_FT",
                  "Shots_On_Target_A_FT","Shots_Inside_Box_A_FT","Possession_A_FT","_won"]].copy()
    da.columns = ["Date","Team","Gf","Gc","xGOT_scored","xGOT_faced","GP",
                  "BC_scored","BC_conceded","SoT","SiB","Poss","lost"]
    da_s = da.sort_values(["Team","Date"]).reset_index(drop=True)
    for col, name in [("Gf","a_Gf"),("Gc","a_Gc"),("xGOT_scored","a_xGOT"),
                      ("xGOT_faced","a_xGOT_faced"),("GP","a_GP"),("BC_scored","a_BC"),
                      ("SoT","a_SoT"),("SiB","a_SiB"),("Poss","a_Poss"),("lost","a_loss_rate")]:
        da_s[name] = da_s.groupby("Team")[col].transform(_decay_roll)

    h_latest = dh_s.groupby("Team")[["h_Gf","h_Gc","h_xGOT","h_xGOT_faced","h_GP",
                                      "h_BC","h_SoT","h_SiB","h_Poss","h_WR"]].last()
    a_latest = da_s.groupby("Team")[["a_Gf","a_Gc","a_xGOT","a_xGOT_faced","a_GP",
                                      "a_BC","a_SoT","a_SiB","a_Poss","a_loss_rate"]].last()

    liga_rate = df_hist.groupby("League")["_won"].mean().to_dict()

    evaluated = []
    for g in live_games_payload:
        ng = normalize_live_data(g)
        home, away = ng["Home"], ng["Away"]
        if home not in h_latest.index or away not in a_latest.index:
            continue

        sh = h_latest.loc[home]
        sa = a_latest.loc[away]

        for k, v in sh.items():
            ng[f"H_{k}"] = float(v) if not pd.isna(v) else 0.0
        for k, v in sa.items():
            ng[f"A_{k}"] = float(v) if not pd.isna(v) else 0.0

        # Diferenças contextuais
        ng["ctx_xGOT_diff"]     = ng["H_h_xGOT"]    - ng["A_a_xGOT"]
        ng["ctx_BC_diff"]       = ng["H_h_BC"]       - ng["A_a_BC"]
        ng["ctx_WR_diff"]       = ng["H_h_WR"]       - ng["A_a_loss_rate"]
        ng["ctx_def_diff"]      = ng["H_h_GP"]       - ng["A_a_GP"]
        ng["ctx_Gc_diff"]       = ng["A_a_Gc"]       - ng["H_h_Gc"]
        ng["attack_vs_defense"] = ng["H_h_xGOT"]     * ng["A_a_Gc"]

        # Mercado
        odd_h = ng["Odd_H_FT"] or np.nan
        odd_d = ng["Odd_D_FT"] or np.nan
        odd_a = ng["Odd_A_FT"] or np.nan
        if all(not pd.isna(o) and o > 1 for o in [odd_h, odd_d, odd_a]):
            _s = 1/odd_h + 1/odd_d + 1/odd_a
            ng["mkt_prob_home"] = (1/odd_h) / _s
            ng["mkt_prob_away"] = (1/odd_a) / _s
            ng["imp_prob_home"] = 1/odd_h
            ng["mkt_home_edge"] = ng["mkt_prob_home"] - ng["mkt_prob_away"]
        else:
            ng["mkt_prob_home"] = ng["mkt_prob_away"] = ng["imp_prob_home"] = ng["mkt_home_edge"] = np.nan

        ng["h2h_home_wr"] = np.nan
        ng["liga_home_wr"] = liga_rate.get(ng["League"], 0.46)

        row_mat = pd.DataFrame([{col: ng.get(col, 0.0) or 0.0 for col in features}]).fillna(0.0)
        ng["Prob_ML"] = float(model.predict_proba(scaler.transform(row_mat))[0, 1])

        apostar, reason = check_entry_conditions(ng)
        ng["Decision"] = "APOSTA" if apostar else "SKIP"
        ng["Reason"]   = reason
        if apostar:
            log_paper_trade(ng)
        evaluated.append(ng)

    return evaluated
