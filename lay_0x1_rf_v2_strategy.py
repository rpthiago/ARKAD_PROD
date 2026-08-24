"""lay_0x1_rf_v2_strategy.py — Lay 0x1 v2 (Refatorado & 100% Alinhado ao GEMINI.md) | ARKAD PROD"""
import os, re, unicodedata, joblib
import numpy as np, pandas as pd
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MODEL_PATH    = str(ROOT / "modelo_lay_0x1_rf_v2.pkl")
SCALER_PATH   = str(ROOT / "scaler_lay_0x1_rf_v2.pkl")
FEATURES_PATH = str(ROOT / "features_lay_0x1_rf_v2.pkl")

COMMISSION  = 0.05
EV_MIN      = 0.03
PROB_MIN    = 0.85
ODD_MIN     = 6.00
ODD_MAX     = 16.00

def _canon(s):
    if pd.isna(s) or not s: return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

def _ev_lay(prob, odd):
    return prob * (1 - COMMISSION) - (1 - prob) * (odd - 1)

def _decay_roll_grouped(df, group_col, val_col, window=6, alpha=0.25):
    g = df.groupby(group_col)[val_col]
    numer = np.zeros(len(df)); count = np.zeros(len(df)); wsum = 0.0
    for j in range(window):
        sj = g.shift(j)
        ej = np.exp(-alpha * j)
        m = sj.notna().to_numpy()
        numer += np.where(m, np.nan_to_num(sj.to_numpy()) * ej, 0.0)
        count += m
        wsum += ej
    res = numer / wsum
    res[count < 3] = np.nan
    return pd.Series(res, index=df.index)

def check_entry_conditions(ms):
    odd = ms.get("Odd_CS_0x1_Lay") or ms.get("Odd_0x1_Lay") or ms.get("Odd_CS_0x1") or ms.get("Odd_0x1_FT") or 0.0
    if pd.isna(odd) or odd < ODD_MIN or odd > ODD_MAX:
        return False, "ODD_FORA_FAIXA"
    prob = ms.get("Prob_ML", 0) or 0.0
    if pd.isna(prob) or prob < PROB_MIN:
        return False, f"PROB_BAIXA({prob*100:.1f}%)"
    ev = ms.get("ev_lay", 0) or 0.0
    if ev < EV_MIN:
        return False, f"EV_BAIXO({ev:+.3f})"
    return True, "APROVADO"

def predict_and_evaluate_live(live_games_payload, df_historical):
    if not (os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH) and os.path.exists(FEATURES_PATH)):
        return []
    if df_historical is None or not isinstance(df_historical, pd.DataFrame) or df_historical.empty or "Date" not in df_historical.columns:
        return []

    model    = joblib.load(MODEL_PATH)
    scaler   = joblib.load(SCALER_PATH)
    features = joblib.load(FEATURES_PATH)

    dates_live = [pd.to_datetime(g.get("Date")) for g in live_games_payload if g.get("Date")]
    ref_date = min(dates_live) if dates_live else pd.to_datetime(datetime.now().date())

    df_hist = df_historical.copy()
    df_hist["Date"] = pd.to_datetime(df_hist["Date"], errors="coerce")
    df_hist = df_hist[df_hist["Date"] < ref_date].copy()
    df_hist = df_hist.dropna(subset=["Goals_H_FT", "Goals_A_FT", "Date", "Home", "Away"]).copy()
    df_hist = df_hist.sort_values("Date", kind="mergesort").reset_index(drop=True)

    df_hist["won_H"] = (df_hist["Goals_H_FT"] > df_hist["Goals_A_FT"]).astype(float)
    df_hist["won_A"] = (df_hist["Goals_A_FT"] > df_hist["Goals_H_FT"]).astype(float)
    df_hist["shut_H"] = (df_hist["Goals_A_FT"] == 0).astype(float)
    df_hist["score1_A"] = (df_hist["Goals_A_FT"] == 1).astype(float)

    df_hist["c_Home"] = df_hist["Home"].map(_canon)
    df_hist["c_Away"] = df_hist["Away"].map(_canon)

    # 1. Vista HOME
    dh = df_hist[["Date", "c_Home", "Goals_H_FT", "won_H", "shut_H"]].copy().rename(columns={"c_Home": "Team"})
    dh = dh.sort_values(["Team", "Date"], kind="mergesort").reset_index(drop=True)
    dh["H_h_WR"] = _decay_roll_grouped(dh, "Team", "won_H")
    dh["H_h_goals_rate"] = _decay_roll_grouped(dh, "Team", "Goals_H_FT")
    dh["H_h_shut_rate"] = _decay_roll_grouped(dh, "Team", "shut_H")
    home_last = dh.groupby("Team")[["H_h_WR", "H_h_goals_rate", "H_h_shut_rate"]].last().reset_index()

    # 2. Vista AWAY
    da = df_hist[["Date", "c_Away", "Goals_A_FT", "won_A", "score1_A"]].copy().rename(columns={"c_Away": "Team"})
    da = da.sort_values(["Team", "Date"], kind="mergesort").reset_index(drop=True)
    da["A_a_WR"] = _decay_roll_grouped(da, "Team", "won_A")
    da["A_a_goals_rate"] = _decay_roll_grouped(da, "Team", "Goals_A_FT")
    da["A_a_score1_rate"] = _decay_roll_grouped(da, "Team", "score1_A")
    away_last = da.groupby("Team")[["A_a_WR", "A_a_goals_rate", "A_a_score1_rate"]].last().reset_index()

    evaluated = []
    for g in live_games_payload:
        home   = str(g.get("Home") or g.get("HomeTeam") or "")
        away   = str(g.get("Away") or g.get("AwayTeam") or "")
        league = str(g.get("League") or g.get("Liga") or "")
        date_v = pd.to_datetime(g.get("Date") or datetime.now().date())

        c_h = _canon(home); c_a = _canon(away)
        sh_df = home_last[home_last["Team"] == c_h]
        sa_df = away_last[away_last["Team"] == c_a]

        odd_0x1 = pd.to_numeric(g.get("Odd_CS_0x1_Lay") or g.get("Odd_0x1_Lay") or g.get("Odd_CS_0x1") or g.get("Odd_0x1_FT") or g.get("Odd_0x1_FT") or g.get("Odd_0x1_Back") or g.get("Odd_0x1") or np.nan, errors="coerce")
        odd_h = pd.to_numeric(g.get("Odd_H_FT") or g.get("Odd_H_Back") or g.get("Odd_H") or np.nan, errors="coerce")
        odd_a = pd.to_numeric(g.get("Odd_A_FT") or g.get("Odd_A_Back") or g.get("Odd_A") or np.nan, errors="coerce")

        if pd.isna(odd_0x1) or odd_0x1 <= 0: continue

        ms = {"Home": home, "Away": away, "League": league, "Date": date_v, "Time": g.get("Time", ""),
              "Odd_0x1_FT": odd_0x1, "Odd_0x1_Lay": odd_0x1, "Odd_CS_0x1_Lay": odd_0x1, "Odd_H_FT": odd_h, "Odd_A_FT": odd_a}

        if sh_df.empty or sa_df.empty:
            ms["Decision"] = "SKIP"; ms["Reason"] = "TIME_SEM_HISTORICO_MANDO"; ms["Prob_ML"] = np.nan; ms["ev_lay"] = np.nan
            evaluated.append(ms); continue

        sh = sh_df.iloc[0]; sa = sa_df.iloc[0]
        ms["H_h_WR"] = sh.get("H_h_WR", np.nan)
        ms["H_h_goals_rate"] = sh.get("H_h_goals_rate", np.nan)
        ms["H_h_shut_rate"] = sh.get("H_h_shut_rate", np.nan)
        ms["A_a_WR"] = sa.get("A_a_WR", np.nan)
        ms["A_a_goals_rate"] = sa.get("A_a_goals_rate", np.nan)
        ms["A_a_score1_rate"] = sa.get("A_a_score1_rate", np.nan)

        if pd.isna(ms.get("H_h_WR")) or pd.isna(ms.get("A_a_WR")):
            ms["Decision"] = "SKIP"; ms["Reason"] = "FORMA_MANDO_INSUFICIENTE"; ms["Prob_ML"] = np.nan; ms["ev_lay"] = np.nan
            evaluated.append(ms); continue

        ms["spread_forca"] = ms["H_h_WR"] - ms["A_a_WR"]
        ms["home_strength_x_away_weakness"] = ms["H_h_goals_rate"] * ms["A_a_score1_rate"]
        ms["mkt_prob_0x1"] = 1.0 / odd_0x1 if odd_0x1 > 0 else np.nan
        ms["mkt_edge_signal"] = (1.0 / odd_h if pd.notna(odd_h) and odd_h>0 else 0.5) - ms["mkt_prob_0x1"]

        has_all = True
        row_dict = {}
        for col in features:
            val = ms.get(col)
            if val is None or pd.isna(val): has_all = False; break
            row_dict[col] = float(val)

        if not has_all:
            ms["Decision"] = "SKIP"; ms["Reason"] = "METRICAS_AUSENTES"; ms["Prob_ML"] = np.nan; ms["ev_lay"] = np.nan
            evaluated.append(ms); continue

        row_mat = pd.DataFrame([row_dict])[features]
        ms["Prob_ML"] = float(model.predict_proba(scaler.transform(row_mat))[0, 1])
        ms["ev_lay"]  = _ev_lay(ms["Prob_ML"], odd_0x1)

        apostar, reason = check_entry_conditions(ms)
        ms["Decision"] = "APOSTA" if apostar else "SKIP"
        ms["Reason"]   = reason
        evaluated.append(ms)

    return evaluated
