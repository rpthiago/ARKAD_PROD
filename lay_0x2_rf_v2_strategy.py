"""lay_0x2_rf_v2_strategy.py — Lay 0x2 v2 | ROI OOS +31.4% | 50281 picks | ROI>0 4/4"""
import os, pandas as pd, numpy as np, joblib
import unicodedata, re
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MODEL_PATH    = str(ROOT / "modelo_lay_0x2_rf_v2.pkl")
SCALER_PATH   = str(ROOT / "scaler_lay_0x2_rf_v2.pkl")
FEATURES_PATH = str(ROOT / "features_lay_0x2_rf_v2.pkl")

COMMISSION       = 0.05
EV_MIN           = 0.02
ODD_MIN          = 6.0
ODD_MAX          = 20.0
LIGA_0X2_RATE_MAX = 0.2
MKT_PROB_MAX     = 0.15
ODD_COL          = "Odd_CS_0x2"

BLACKLIST_LIGAS = [
    "ENGLAND 5", "BELGIUM 1", "WALES 1", "ITALY 2", "ECUADOR 1",
    "VENEZUELA 1", "FRANCE CUP", "JAPAN 2", "NETHERLANDS 1"
]


def _canon(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


def _ev_lay(prob, odd):
    return prob * (1 - COMMISSION) - (1 - prob) * (odd - 1)


def _decay_roll_grouped_unshifted(df, group_col, val_col, window=6, alpha=0.25):
    """Média decaída vetorizada sem shift — adequada para o live onde
    o jogo atual não está no histórico e precisamos do rolling incluindo o último jogo."""
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
    league = ms.get("League")
    if league in BLACKLIST_LIGAS:
        return False, f"LIGA_BLACKLIST({league})"
    
    odd_h = ms.get("Odd_H_FT") or 0.0
    if pd.isna(odd_h) or odd_h > 3.00:
        return False, f"ODD_CASA_ALTA({odd_h:.2f})"

    odd = ms.get("Odd_CS_0x2_Lay") or ms.get("Odd_CS_0x2") or 0.0
    if pd.isna(odd) or odd < ODD_MIN or odd > ODD_MAX:
        return False, "ODD_FORA_FAIXA"
    prob = ms.get("Prob_ML", 0) or 0.0
    ev = _ev_lay(prob, odd)
    if ev < EV_MIN:
        return False, f"EV_BAIXO({ev:+.3f})"
    liga_rate = ms.get("liga_0x2_rate", None)
    # Liga fora da base historica (rate NaN/None) => fora do universo validado; skip.
    if liga_rate is None or pd.isna(liga_rate):
        return False, "LIGA_FORA_UNIVERSO"
    if LIGA_0X2_RATE_MAX > 0 and liga_rate >= LIGA_0X2_RATE_MAX:
        return False, f"LIGA_DEFENSIVA({liga_rate:.2f})"
    mkt_prob = ms.get("mkt_prob_0x2", None)
    if mkt_prob is not None and MKT_PROB_MAX > 0 and mkt_prob >= MKT_PROB_MAX:
        return False, f"MERCADO_CARO({mkt_prob:.3f})"
    return True, "APROVADO"


def log_paper_trade(ms):
    row = {k: ms.get(k, "") for k in
            ["Date","League","Home","Away","Odd_CS_0x2_Lay","Prob_ML",
             "total_xGOT","total_Gf","liga_0x2_rate","mkt_prob_0x2","Decision","Reason"]}
    row["ev_lay"] = round(_ev_lay(ms.get("Prob_ML", 0) or 0, ms.get("Odd_CS_0x2_Lay", 1) or 1), 4)
    row["Timestamp"] = datetime.now().isoformat()
    f = "paper_trading_log_lay_0x2.csv"
    pd.DataFrame([row]).to_csv(f, mode="a", header=not os.path.exists(f), index=False)


def predict_and_evaluate_live(live_games_payload, df_historical):
    if not (os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH) and os.path.exists(FEATURES_PATH)):
        return []
    if df_historical is None or not isinstance(df_historical, pd.DataFrame) or df_historical.empty or "Date" not in df_historical.columns:
        return []

    model    = joblib.load(MODEL_PATH)
    scaler   = joblib.load(SCALER_PATH)
    features = joblib.load(FEATURES_PATH)

    df_hist = df_historical
    if live_games_payload:
        teams = set()
        for g in live_games_payload:
            teams.add(str(g.get("Home") or g.get("HomeTeam") or ""))
            teams.add(str(g.get("Away") or g.get("AwayTeam") or ""))
        teams = {t for t in teams if t}
        if teams and "Home" in df_hist.columns and "Away" in df_hist.columns:
            df_hist = df_hist[df_hist["Home"].isin(teams) | df_hist["Away"].isin(teams)]

    df_hist = df_hist.copy()
    df_hist["Date"] = pd.to_datetime(df_hist["Date"], errors="coerce")
    df_hist = df_hist.dropna(subset=["Goals_H_FT","Goals_A_FT","Date","Home","Away"]).copy()
    df_hist = df_hist.sort_values("Date", kind="mergesort").reset_index(drop=True)

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

    df_hist["_0x2_flag"] = ((df_hist["Goals_H_FT"] == 0) & (df_hist["Goals_A_FT"] == 2)).astype(float)

    # Vista HOME
    dh = df_hist[["Date","Home","Goals_H_FT","Goals_A_FT","xGOT_H_FT","xGOT_Faced_H_FT",
                  "Goals_Prevented_H_FT","Big_Chances_H_FT","Shots_On_Target_H_FT","Possession_H_FT","_0x2_flag"]].copy()
    dh["won"] = (dh["Goals_H_FT"] > dh["Goals_A_FT"]).astype(float)
    dh = dh.rename(columns={"Home":"Team"})
    dh = dh.sort_values(["Team","Date"], kind="mergesort").reset_index(drop=True)
    for col, nm in [("Goals_H_FT","h_Gf"),("Goals_A_FT","h_Gc"),("xGOT_H_FT","h_xGOT"),
                    ("xGOT_Faced_H_FT","h_xGOT_faced"),("Goals_Prevented_H_FT","h_GP"),
                    ("Big_Chances_H_FT","h_BC"),("Shots_On_Target_H_FT","h_SoT"),
                    ("Possession_H_FT","h_Poss"),("won","h_WR"),("_0x2_flag","h_0x2_rate")]:
        dh[nm] = _decay_roll_grouped_unshifted(dh, "Team", col)
    h_feats = ["h_Gf","h_Gc","h_xGOT","h_xGOT_faced","h_GP","h_BC","h_SoT","h_Poss","h_WR","h_0x2_rate"]
    home_last = dh.groupby("Team")[h_feats].last().reset_index()

    # Vista AWAY
    da = df_hist[["Date","Away","Goals_A_FT","Goals_H_FT","xGOT_A_FT","xGOT_Faced_A_FT",
                  "Goals_Prevented_A_FT","Big_Chances_A_FT","Shots_On_Target_A_FT","Possession_A_FT","_0x2_flag"]].copy()
    da["won"] = (da["Goals_A_FT"] > da["Goals_H_FT"]).astype(float)
    da = da.rename(columns={"Away":"Team"})
    da = da.sort_values(["Team","Date"], kind="mergesort").reset_index(drop=True)
    for col, nm in [("Goals_A_FT","a_Gf"),("Goals_H_FT","a_Gc"),("xGOT_A_FT","a_xGOT"),
                    ("xGOT_Faced_A_FT","a_xGOT_faced"),("Goals_Prevented_A_FT","a_GP"),
                    ("Big_Chances_A_FT","a_BC"),("Shots_On_Target_A_FT","a_SoT"),
                    ("Possession_A_FT","a_Poss"),("won","a_WR"),("_0x2_flag","a_0x2_rate")]:
        da[nm] = _decay_roll_grouped_unshifted(da, "Team", col)
    a_feats = ["a_Gf","a_Gc","a_xGOT","a_xGOT_faced","a_GP","a_BC","a_SoT","a_Poss","a_WR","a_0x2_rate"]
    away_last = da.groupby("Team")[a_feats].last().reset_index()

    # Liga 0x2 rate
    df_hist["_tgt"] = (~((df_hist["Goals_H_FT"] == 0) & (df_hist["Goals_A_FT"] == 2))).astype(float)
    df_lig = df_hist[["Date","League","_0x2_flag"]].sort_values(["League","Date"], kind="mergesort").reset_index(drop=True)
    df_lig["liga_0x2_rate"] = df_lig.groupby("League")["_0x2_flag"].transform(
        lambda x: x.shift(1).rolling(100, min_periods=20).mean())
    liga_last = df_lig.groupby("League")["liga_0x2_rate"].last().to_dict()

    evaluated = []
    for g in live_games_payload:
        home   = str(g.get("Home") or g.get("HomeTeam") or "")
        away   = str(g.get("Away") or g.get("AwayTeam") or "")
        league = str(g.get("League") or g.get("Liga") or "")
        date_v = pd.to_datetime(g.get("Date") or datetime.now().date())

        sh = home_last[home_last["Team"].map(_canon) == _canon(home)]
        sa = away_last[away_last["Team"].map(_canon) == _canon(away)]
        if sh.empty or sa.empty:
            continue
        sh, sa = sh.iloc[0], sa.iloc[0]

        odd_val = pd.to_numeric(g.get("Odd_CS_0x2_Lay") or g.get("Odd_CS_0x2") or np.nan, errors="coerce")
        if pd.isna(odd_val) or odd_val <= 0:
            continue

        odd_h = pd.to_numeric(g.get("Odd_H_FT") or g.get("Odd_H_Back") or np.nan, errors="coerce")
        odd_d = pd.to_numeric(g.get("Odd_D_FT") or np.nan, errors="coerce")
        odd_a = pd.to_numeric(g.get("Odd_A_FT") or g.get("Odd_A_Back") or np.nan, errors="coerce")

        ms = {
            "Home": home, "Away": away, "League": league, "Date": date_v,
            "Time": g.get("Time", ""),
            "Odd_CS_0x2_Lay": odd_val,
            "Odd_H_FT": odd_h,
        }

        for col in h_feats:
            ms["H_" + col] = sh.get(col, np.nan)
        for col in a_feats:
            ms["A_" + col] = sa.get(col, np.nan)

        ms["total_xGOT"]     = (ms.get("H_h_xGOT", 0) or 0) + (ms.get("A_a_xGOT", 0) or 0)
        ms["total_Gf"]       = (ms.get("H_h_Gf", 0) or 0)   + (ms.get("A_a_Gf", 0) or 0)
        ms["total_BC"]       = (ms.get("H_h_BC", 0) or 0)    + (ms.get("A_a_BC", 0) or 0)
        ms["total_SoT"]      = (ms.get("H_h_SoT", 0) or 0)   + (ms.get("A_a_SoT", 0) or 0)
        ms["total_def_weak"] = (ms.get("H_h_Gc", 0) or 0)    + (ms.get("A_a_Gc", 0) or 0)
        ms["weaker_gk"]      = (ms.get("H_h_xGOT_faced", 0) or 0) + (ms.get("A_a_xGOT_faced", 0) or 0)
        ms["h2h_0x2_rate_raw"] = (ms.get("H_h_0x2_rate", 0) or 0) * (ms.get("A_a_0x2_rate", 0) or 0)
        ms["attack_imbalance"] = (
            abs((ms.get("H_h_Gf", 0) or 0) - (ms.get("H_h_Gc", 0) or 0)) +
            abs((ms.get("A_a_Gf", 0) or 0) - (ms.get("A_a_Gc", 0) or 0))
        )

        ms["mkt_prob_0x2"] = 1.0 / odd_val if odd_val > 0 else np.nan
        ms["mkt_prob_0x2_norm"] = np.nan
        if not (pd.isna(odd_h) or pd.isna(odd_d) or pd.isna(odd_a)):
            _ov = 1/odd_h + 1/odd_d + 1/odd_a
            ms["mkt_prob_0x2_norm"] = ms["mkt_prob_0x2"] / _ov if _ov > 0 else np.nan

        ms["liga_0x2_rate"] = liga_last.get(league, np.nan)
        ms["h2h_0x2_rate"]  = np.nan

        row_dict = {col: ms.get(col, np.nan) for col in features}
        if any(pd.isna(v) for v in row_dict.values()):
            continue
        row_mat = pd.DataFrame([row_dict])
        
        ms["Prob_ML"] = float(model.predict_proba(scaler.transform(row_mat))[0, 1])
        ms["ev_lay"]  = _ev_lay(ms["Prob_ML"], odd_val)

        apostar, reason = check_entry_conditions(ms)
        ms["Decision"] = "APOSTA" if apostar else "SKIP"
        ms["Reason"]   = reason
        if apostar:
            log_paper_trade(ms)
        evaluated.append(ms)

    return evaluated
