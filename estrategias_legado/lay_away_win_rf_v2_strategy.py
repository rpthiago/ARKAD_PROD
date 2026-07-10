"""lay_away_win_rf_v2_strategy.py — Lay Away Win v2 | ROI OOS +9.6% | 17070 picks | ROI>0 4/4"""
import os, pandas as pd, numpy as np, joblib
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MODEL_PATH    = str(ROOT / "modelo_lay_away_win_rf_v2.pkl")
SCALER_PATH   = str(ROOT / "scaler_lay_away_win_rf_v2.pkl")
FEATURES_PATH = str(ROOT / "features_lay_away_win_rf_v2.pkl")

COMMISSION        = 0.05
EV_MIN            = 0.03
ODD_MIN           = 1.4
ODD_MAX           = 2.5
CTX_WR_DIFF_MIN   = 0.05
LIGA_LAY_RATE_MIN = 0.6
MKT_OVERVALUE_MIN = 0.0


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


def check_entry_conditions(ms):
    odd = ms.get("Odd_A_FT", 0) or 0.0
    if pd.isna(odd) or odd < ODD_MIN or odd > ODD_MAX:
        return False, "ODD_FORA_FAIXA"
    prob = ms.get("Prob_ML", 0) or 0.0
    ev = _ev_lay(prob, odd)
    if ev < EV_MIN:
        return False, f"EV_BAIXO({ev:+.3f})"
    ctx_wr = ms.get("ctx_WR_diff", None)
    if ctx_wr is not None and CTX_WR_DIFF_MIN > 0 and ctx_wr < CTX_WR_DIFF_MIN:
        return False, f"FORMA_VISITANTE_OK({ctx_wr:+.2f})"
    liga_rate = ms.get("liga_lay_rate", None)
    if liga_rate is not None and LIGA_LAY_RATE_MIN > 0 and liga_rate < LIGA_LAY_RATE_MIN:
        return False, f"LIGA_DESFAVORAVEL({liga_rate:.2f})"
    mkt_ov = ms.get("mkt_overvalue", None)
    if mkt_ov is not None and MKT_OVERVALUE_MIN > 0 and mkt_ov < MKT_OVERVALUE_MIN:
        return False, f"MKT_OVERVALUE_BAIXO({mkt_ov:+.2f})"
    return True, "APROVADO"


def log_paper_trade(ms):
    row = {k: ms.get(k, "") for k in
            ["Date","League","Home","Away","Odd_A_FT","Prob_ML",
             "H_h_WR","A_a_WR","ctx_WR_diff","mkt_overvalue","Decision","Reason"]}
    row["ev_lay"] = round(_ev_lay(ms.get("Prob_ML", 0) or 0, ms.get("Odd_A_FT", 1) or 1), 4)
    row["Timestamp"] = datetime.now().isoformat()
    f = "paper_trading_log_lay_away_win.csv"
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

    # Vista HOME
    dh = df_hist[["Date","Home","Goals_H_FT","Goals_A_FT","xGOT_H_FT","xGOT_Faced_H_FT",
                  "Goals_Prevented_H_FT","Big_Chances_H_FT","Shots_On_Target_H_FT","Possession_H_FT"]].copy()
    dh["won"] = (dh["Goals_H_FT"] > dh["Goals_A_FT"]).astype(float)
    dh = dh.rename(columns={"Home":"Team"})
    dh = dh.sort_values(["Team","Date"]).reset_index(drop=True)
    for col, nm in [("Goals_H_FT","h_Gf"),("Goals_A_FT","h_Gc"),("xGOT_H_FT","h_xGOT"),
                    ("xGOT_Faced_H_FT","h_xGOT_faced"),("Goals_Prevented_H_FT","h_GP"),
                    ("Big_Chances_H_FT","h_BC"),("Shots_On_Target_H_FT","h_SoT"),
                    ("Possession_H_FT","h_Poss"),("won","h_WR")]:
        dh[nm] = _decay_roll_grouped(dh, "Team", col)
    h_feats = ["h_Gf","h_Gc","h_xGOT","h_xGOT_faced","h_GP","h_BC","h_SoT","h_Poss","h_WR"]
    home_last = dh.groupby("Team")[h_feats].last().reset_index()

    # Vista AWAY
    da = df_hist[["Date","Away","Goals_A_FT","Goals_H_FT","xGOT_A_FT","xGOT_Faced_A_FT",
                  "Goals_Prevented_A_FT","Big_Chances_A_FT","Shots_On_Target_A_FT","Possession_A_FT"]].copy()
    da["won"] = (da["Goals_A_FT"] > da["Goals_H_FT"]).astype(float)
    da = da.rename(columns={"Away":"Team"})
    da = da.sort_values(["Team","Date"]).reset_index(drop=True)
    for col, nm in [("Goals_A_FT","a_Gf"),("Goals_H_FT","a_Gc"),("xGOT_A_FT","a_xGOT"),
                    ("xGOT_Faced_A_FT","a_xGOT_faced"),("Goals_Prevented_A_FT","a_GP"),
                    ("Big_Chances_A_FT","a_BC"),("Shots_On_Target_A_FT","a_SoT"),
                    ("Possession_A_FT","a_Poss"),("won","a_WR")]:
        da[nm] = _decay_roll_grouped(da, "Team", col)
    a_feats = ["a_Gf","a_Gc","a_xGOT","a_xGOT_faced","a_GP","a_BC","a_SoT","a_Poss","a_WR"]
    away_last = da.groupby("Team")[a_feats].last().reset_index()

    # Liga lay rate (visitante nao vence)
    df_hist["target_lay"] = (df_hist["Goals_A_FT"] <= df_hist["Goals_H_FT"]).astype(float)
    df_lig = df_hist[["Date","League","target_lay"]].sort_values(["League","Date"]).reset_index(drop=True)
    df_lig["liga_lay_rate"] = df_lig.groupby("League")["target_lay"].transform(
        lambda x: x.shift(1).rolling(50, min_periods=10).mean())
    liga_last = df_lig.groupby("League")["liga_lay_rate"].last().to_dict()

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

        odd_h = pd.to_numeric(g.get("Odd_H_FT") or g.get("Odd_H_Back") or np.nan, errors="coerce")
        odd_d = pd.to_numeric(g.get("Odd_D_FT") or np.nan, errors="coerce")
        odd_a = pd.to_numeric(g.get("Odd_A_FT") or g.get("Odd_A_Back") or np.nan, errors="coerce")

        if pd.isna(odd_a) or odd_a <= 0:
            continue

        ms = {
            "Home": home, "Away": away, "League": league, "Date": date_v,
            "Time": g.get("Time", ""),
            "Odd_H_FT": odd_h, "Odd_D_FT": odd_d, "Odd_A_FT": odd_a,
        }

        for col in h_feats:
            ms["H_" + col] = sh.get(col, np.nan)
        for col in a_feats:
            ms["A_" + col] = sa.get(col, np.nan)

        ms["ctx_WR_diff"]    = (ms.get("H_h_WR", 0) or 0) - (ms.get("A_a_WR", 0) or 0)
        ms["ctx_xGOT_diff"]  = (ms.get("H_h_xGOT", 0) or 0) - (ms.get("A_a_xGOT", 0) or 0)
        ms["ctx_BC_diff"]    = (ms.get("H_h_BC", 0) or 0) - (ms.get("A_a_BC", 0) or 0)
        ms["def_weakness_A"] = (ms.get("A_a_xGOT_faced", 0) or 0) - (ms.get("A_a_GP", 0) or 0)
        ms["home_pressure"]  = (ms.get("H_h_xGOT", 0) or 0) * (ms.get("A_a_xGOT_faced", 0) or 0)

        if not (pd.isna(odd_h) or pd.isna(odd_d) or pd.isna(odd_a)):
            _s = 1/odd_h + 1/odd_d + 1/odd_a
            ms["mkt_prob_away"] = (1/odd_a) / _s if _s > 0 else np.nan
            ms["imp_prob_away"] = 1/odd_a
        else:
            ms["mkt_prob_away"] = np.nan
            ms["imp_prob_away"] = 1/odd_a if not pd.isna(odd_a) else np.nan

        a_wr = ms.get("A_a_WR", np.nan)
        ms["mkt_overvalue"] = (
            (ms["mkt_prob_away"] - a_wr)
            if not (pd.isna(ms.get("mkt_prob_away")) or pd.isna(a_wr)) else np.nan
        )

        ms["h2h_lay_rate"]  = np.nan
        ms["liga_lay_rate"] = liga_last.get(league, np.nan)

        row_mat = pd.DataFrame([{col: ms.get(col, 0.0) or 0.0 for col in features}]).fillna(0.0)
        ms["Prob_ML"] = float(model.predict_proba(scaler.transform(row_mat))[0, 1])
        ms["ev_lay"]  = _ev_lay(ms["Prob_ML"], ms.get("Odd_A_FT", 0) or 0.0)

        apostar, reason = check_entry_conditions(ms)
        ms["Decision"] = "APOSTA" if apostar else "SKIP"
        ms["Reason"]   = reason
        if apostar:
            log_paper_trade(ms)
        evaluated.append(ms)

    return evaluated
