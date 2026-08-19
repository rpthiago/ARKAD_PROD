"""lay_under25_rf_v2_strategy.py — Lay Under 2.5 v2 | ROI OOS +6.9% | 20491 picks | ROI>0 4/4"""
import os, pandas as pd, numpy as np, joblib
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MODEL_PATH    = str(ROOT / "modelo_lay_under25_rf_v2.pkl")
SCALER_PATH   = str(ROOT / "scaler_lay_under25_rf_v2.pkl")
FEATURES_PATH = str(ROOT / "features_lay_under25_rf_v2.pkl")

COMMISSION      = 0.05
EV_MIN          = 0.02
ODD_MIN         = 1.5
ODD_MAX         = 2.8
LIGA_OVER25_MIN = 0.5
ODD_COL         = "Odd_Under25_FT"
ODD_COLS_FALLBACK = ['Odd_Under25_FT', 'Odd_Under_25', 'Odd_Under_2.5', 'Odd_Under_25_Back', 'Odd_Under_2_5', 'Odd_U25', 'B365<2.5']


def _ev_lay(prob, odd):
    return prob * (1 - COMMISSION) - (1 - prob) * (odd - 1)


def _decay_roll_grouped(df, group_col, val_col, window=6, alpha=0.25, min_g=3):
    """Média decaída vetorizada — idêntica a shift(1)+rolling(window).apply(pesos),
    ~40x mais rápida (evita .apply por grupo). Só produz valor com janela completa."""
    g = df.groupby(group_col)[val_col]
    numer = np.zeros(len(df)); count = np.zeros(len(df)); wsum = 0.0
    for j in range(window):
        sj = g.shift(1 + j)
        ej = np.exp(-alpha * j)
        m = sj.notna().to_numpy()
        numer += np.where(m, np.nan_to_num(sj.to_numpy()) * ej, 0.0)
        count += m
        wsum += ej
    res = numer / wsum
    res[count < window] = np.nan
    return pd.Series(res, index=df.index)


def _get_odd(g):
    for col in [ODD_COL] + ODD_COLS_FALLBACK:
        v = pd.to_numeric(g.get(col, np.nan), errors="coerce")
        if not pd.isna(v) and v > 0:
            return v, col
    return np.nan, None


def check_entry_conditions(ms):
    odd = ms.get("_odd_u25", 0) or 0.0
    if pd.isna(odd) or odd < ODD_MIN or odd > ODD_MAX:
        return False, "ODD_FORA_FAIXA"
    prob = ms.get("Prob_ML", 0) or 0.0
    ev = _ev_lay(prob, odd)
    if ev < EV_MIN:
        return False, f"EV_BAIXO({ev:+.3f})"
    liga_rate = ms.get("liga_over25_rate", None)
    if liga_rate is not None and LIGA_OVER25_MIN > 0 and liga_rate < LIGA_OVER25_MIN:
        return False, f"LIGA_DEFENSIVA({liga_rate:.2f})"
    return True, "APROVADO"


def log_paper_trade(ms):
    row = {k: ms.get(k, "") for k in
            ["Date","League","Home","Away","_odd_u25","Prob_ML",
             "total_xGOT","total_Gf","liga_over25_rate","mkt_prob_u25","Decision","Reason"]}
    row["ev_lay"] = round(_ev_lay(ms.get("Prob_ML", 0) or 0, ms.get("_odd_u25", 1) or 1), 4)
    row["Timestamp"] = datetime.now().isoformat()
    f = "paper_trading_log_lay_under25.csv"
    pd.DataFrame([row]).to_csv(f, mode="a", header=not os.path.exists(f), index=False)


def predict_and_evaluate_live(live_games_payload, df_historical):
    if not (os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH) and os.path.exists(FEATURES_PATH)):
        return []

    model    = joblib.load(MODEL_PATH)
    scaler   = joblib.load(SCALER_PATH)
    features = joblib.load(FEATURES_PATH)

    df_hist = df_historical.copy()
    df_hist["Date"] = pd.to_datetime(df_hist["Date"], errors="coerce")
    df_hist = df_hist.dropna(subset=["Goals_H_FT","Goals_A_FT","Date","Home","Away"]).copy()
    df_hist = df_hist.sort_values("Date").reset_index(drop=True)

    if live_games_payload:
        first_date = pd.to_datetime(live_games_payload[0].get("Date") or datetime.now().date()).date()
        df_hist = df_hist[df_hist["Date"].dt.date < first_date].copy()

    stat_cols = ["Goals_H_FT","Goals_A_FT","xGOT_H_FT","xGOT_A_FT",
                 "xGOT_Faced_H_FT","xGOT_Faced_A_FT",
                 "Goals_Prevented_H_FT","Goals_Prevented_A_FT",
                 "Big_Chances_H_FT","Big_Chances_A_FT",
                 "Shots_On_Target_H_FT","Shots_On_Target_A_FT",
                 "Possession_H_FT","Possession_A_FT"]
    for c in stat_cols:
        df_hist[c] = pd.to_numeric(df_hist.get(c, 0), errors="coerce").fillna(0.0) if c in df_hist.columns else 0.0

    df_hist["_over25"] = ((df_hist["Goals_H_FT"] + df_hist["Goals_A_FT"]) >= 3).astype(float)

    # Vista HOME
    dh = df_hist[["Date","Home","Goals_H_FT","Goals_A_FT","xGOT_H_FT","xGOT_Faced_H_FT",
                  "Goals_Prevented_H_FT","Big_Chances_H_FT","Shots_On_Target_H_FT","Possession_H_FT","_over25"]].copy()
    dh["won"] = (dh["Goals_H_FT"] > dh["Goals_A_FT"]).astype(float)
    dh = dh.rename(columns={"Home":"Team"})
    dh = dh.sort_values(["Team","Date"]).reset_index(drop=True)
    for col, nm in [("Goals_H_FT","h_Gf"),("Goals_A_FT","h_Gc"),("xGOT_H_FT","h_xGOT"),
                    ("xGOT_Faced_H_FT","h_xGOT_faced"),("Goals_Prevented_H_FT","h_GP"),
                    ("Big_Chances_H_FT","h_BC"),("Shots_On_Target_H_FT","h_SoT"),
                    ("Possession_H_FT","h_Poss"),("won","h_WR"),("_over25","h_over25_rate")]:
        dh[nm] = _decay_roll_grouped(dh, "Team", col)
    h_feats = ["h_Gf","h_Gc","h_xGOT","h_xGOT_faced","h_GP","h_BC","h_SoT","h_Poss","h_WR","h_over25_rate"]
    home_last = dh.groupby("Team")[h_feats].last().reset_index()

    # Vista AWAY
    da = df_hist[["Date","Away","Goals_A_FT","Goals_H_FT","xGOT_A_FT","xGOT_Faced_A_FT",
                  "Goals_Prevented_A_FT","Big_Chances_A_FT","Shots_On_Target_A_FT","Possession_A_FT","_over25"]].copy()
    da["won"] = (da["Goals_A_FT"] > da["Goals_H_FT"]).astype(float)
    da = da.rename(columns={"Away":"Team"})
    da = da.sort_values(["Team","Date"]).reset_index(drop=True)
    for col, nm in [("Goals_A_FT","a_Gf"),("Goals_H_FT","a_Gc"),("xGOT_A_FT","a_xGOT"),
                    ("xGOT_Faced_A_FT","a_xGOT_faced"),("Goals_Prevented_A_FT","a_GP"),
                    ("Big_Chances_A_FT","a_BC"),("Shots_On_Target_A_FT","a_SoT"),
                    ("Possession_A_FT","a_Poss"),("won","a_WR"),("_over25","a_over25_rate")]:
        da[nm] = _decay_roll_grouped(da, "Team", col)
    a_feats = ["a_Gf","a_Gc","a_xGOT","a_xGOT_faced","a_GP","a_BC","a_SoT","a_Poss","a_WR","a_over25_rate"]
    away_last = da.groupby("Team")[a_feats].last().reset_index()

    # Liga over25 rate
    df_lig = df_hist[["Date","League","_over25"]].sort_values(["League","Date"]).reset_index(drop=True)
    df_lig["liga_over25_rate"] = df_lig.groupby("League")["_over25"].transform(
        lambda x: x.shift(1).rolling(100, min_periods=20).mean())
    liga_last = df_lig.groupby("League")["liga_over25_rate"].last().to_dict()

    evaluated = []
    for g in live_games_payload:
        home   = str(g.get("Home") or g.get("HomeTeam") or "")
        away   = str(g.get("Away") or g.get("AwayTeam") or "")
        league = str(g.get("League") or g.get("Liga") or "")
        date_v = pd.to_datetime(g.get("Date") or datetime.now().date())

        sh = home_last[home_last["Team"] == home]
        sa = away_last[away_last["Team"] == away]
        if sh.empty or sa.empty:
            continue
        sh, sa = sh.iloc[0], sa.iloc[0]

        odd_val, _ = _get_odd(g)
        if pd.isna(odd_val) or odd_val <= 0:
            continue

        odd_h = pd.to_numeric(g.get("Odd_H_FT") or g.get("Odd_H_Back") or np.nan, errors="coerce")
        odd_d = pd.to_numeric(g.get("Odd_D_FT") or np.nan, errors="coerce")
        odd_a = pd.to_numeric(g.get("Odd_A_FT") or g.get("Odd_A_Back") or np.nan, errors="coerce")

        ms = {"Home": home, "Away": away, "League": league, "Date": date_v,
              "Time": g.get("Time", ""), "_odd_u25": odd_val}

        for col in h_feats:
            ms["H_" + col] = sh.get(col, np.nan)
        for col in a_feats:
            ms["A_" + col] = sa.get(col, np.nan)

        ms["total_xGOT"]       = (ms.get("H_h_xGOT",0) or 0) + (ms.get("A_a_xGOT",0) or 0)
        ms["total_Gf"]         = (ms.get("H_h_Gf",0) or 0)   + (ms.get("A_a_Gf",0) or 0)
        ms["total_Gc"]         = (ms.get("H_h_Gc",0) or 0)    + (ms.get("A_a_Gc",0) or 0)
        ms["total_BC"]         = (ms.get("H_h_BC",0) or 0)    + (ms.get("A_a_BC",0) or 0)
        ms["total_SoT"]        = (ms.get("H_h_SoT",0) or 0)   + (ms.get("A_a_SoT",0) or 0)
        ms["total_def_weak"]   = (ms.get("H_h_xGOT_faced",0) or 0) + (ms.get("A_a_xGOT_faced",0) or 0)
        h_o25 = ms.get("H_h_over25_rate", 0) or 0
        a_o25 = ms.get("A_a_over25_rate", 0) or 0
        ms["over25_rate_prod"] = h_o25 * a_o25
        ms["over25_rate_mean"] = (h_o25 + a_o25) / 2
        gc_safe = ms["total_Gc"] if ms["total_Gc"] > 0 else 0.5
        ms["attack_vs_defense"] = ms["total_Gf"] / gc_safe

        ms["mkt_prob_u25"]    = 1.0 / odd_val if odd_val > 0 else np.nan
        ms["mkt_prob_over25"] = 1.0 - ms["mkt_prob_u25"] if ms["mkt_prob_u25"] else np.nan
        ms["mkt_overvalue"]   = ms["over25_rate_mean"] - ms["mkt_prob_over25"] if ms["mkt_prob_over25"] else np.nan
        ms["mkt_prob_u25_norm"] = np.nan
        if not (pd.isna(odd_h) or pd.isna(odd_d) or pd.isna(odd_a)):
            _ov = 1/odd_h + 1/odd_d + 1/odd_a
            ms["mkt_prob_u25_norm"] = ms["mkt_prob_u25"] / _ov if _ov > 0 else np.nan

        ms["liga_over25_rate"] = liga_last.get(league, np.nan)
        ms["h2h_over25_rate"]  = np.nan

        row_mat = pd.DataFrame([{col: ms.get(col, 0.0) or 0.0 for col in features}]).fillna(0.0)
        ms["Prob_ML"] = float(model.predict_proba(scaler.transform(row_mat))[0, 1])
        ms["ev_lay"]  = _ev_lay(ms["Prob_ML"], odd_val)

        apostar, reason = check_entry_conditions(ms)
        ms["Decision"] = "APOSTA" if apostar else "SKIP"
        ms["Reason"]   = reason
        if apostar:
            log_paper_trade(ms)
        evaluated.append(ms)

    return evaluated
