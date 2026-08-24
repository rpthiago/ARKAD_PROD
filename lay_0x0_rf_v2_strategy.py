"""lay_0x0_rf_v2_strategy.py — Lay 0x0 v2 (Refatorado & 100% Alinhado ao GEMINI.md) | ARKAD PROD"""
import os, re, unicodedata, joblib
import numpy as np, pandas as pd
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MODEL_PATH    = str(ROOT / "modelo_lay_0x0_rf_v2.pkl")
SCALER_PATH   = str(ROOT / "scaler_lay_0x0_rf_v2.pkl")
FEATURES_PATH = str(ROOT / "features_lay_0x0_rf_v2.pkl")

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
    odd = ms.get("Odd_CS_0x0_Lay") or ms.get("Odd_0x0_Lay") or ms.get("Odd_CS_0x0") or ms.get("Odd_0x0_FT") or 0.0
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

    stat_cols = ["Goals_H_FT", "Goals_A_FT", "xGOT_H_FT", "xGOT_A_FT", "xGOT_Faced_H_FT", "xGOT_Faced_A_FT",
                 "Goals_Prevented_H_FT", "Goals_Prevented_A_FT", "Big_Chances_H_FT", "Big_Chances_A_FT",
                 "Shots_On_Target_H_FT", "Shots_On_Target_A_FT", "Possession_H_FT", "Possession_A_FT"]
    for c in stat_cols:
        df_hist[c] = pd.to_numeric(df_hist.get(c, 0), errors="coerce").fillna(0.0) if c in df_hist.columns else 0.0

    df_hist["_0x0_flag"] = ((df_hist["Goals_H_FT"] == 0) & (df_hist["Goals_A_FT"] == 0)).astype(float)
    df_hist["c_Home"] = df_hist["Home"].map(_canon)
    df_hist["c_Away"] = df_hist["Away"].map(_canon)

    # 1. Vista HOME
    dh = df_hist[["Date", "c_Home", "Goals_H_FT", "Goals_A_FT", "xGOT_H_FT", "xGOT_Faced_H_FT",
                  "Goals_Prevented_H_FT", "Big_Chances_H_FT", "Shots_On_Target_H_FT", "Possession_H_FT", "_0x0_flag"]].copy()
    dh["won"] = (dh["Goals_H_FT"] > dh["Goals_A_FT"]).astype(float)
    dh = dh.rename(columns={"c_Home": "Team"})
    dh = dh.sort_values(["Team", "Date"], kind="mergesort").reset_index(drop=True)
    for col, nm in [("Goals_H_FT", "h_Gf"), ("Goals_A_FT", "h_Gc"), ("xGOT_H_FT", "h_xGOT"),
                    ("xGOT_Faced_H_FT", "h_xGOT_faced"), ("Goals_Prevented_H_FT", "h_GP"),
                    ("Big_Chances_H_FT", "h_BC"), ("Shots_On_Target_H_FT", "h_SoT"),
                    ("Possession_H_FT", "h_Poss"), ("won", "h_WR"), ("_0x0_flag", "h_0x0_rate")]:
        dh[nm] = _decay_roll_grouped(dh, "Team", col)
    h_feats = ["h_Gf", "h_Gc", "h_xGOT", "h_xGOT_faced", "h_GP", "h_BC", "h_SoT", "h_Poss", "h_WR", "h_0x0_rate"]
    home_last = dh.groupby("Team")[h_feats].last().reset_index()

    # 2. Vista AWAY
    da = df_hist[["Date", "c_Away", "Goals_A_FT", "Goals_H_FT", "xGOT_A_FT", "xGOT_Faced_A_FT",
                  "Goals_Prevented_A_FT", "Big_Chances_A_FT", "Shots_On_Target_A_FT", "Possession_A_FT", "_0x0_flag"]].copy()
    da["won"] = (da["Goals_A_FT"] > da["Goals_H_FT"]).astype(float)
    da = da.rename(columns={"c_Away": "Team"})
    da = da.sort_values(["Team", "Date"], kind="mergesort").reset_index(drop=True)
    for col, nm in [("Goals_A_FT", "a_Gf"), ("Goals_H_FT", "a_Gc"), ("xGOT_A_FT", "a_xGOT"),
                    ("xGOT_Faced_A_FT", "a_xGOT_faced"), ("Goals_Prevented_A_FT", "a_GP"),
                    ("Big_Chances_A_FT", "a_BC"), ("Shots_On_Target_A_FT", "a_SoT"),
                    ("Possession_A_FT", "a_Poss"), ("won", "a_WR"), ("_0x0_flag", "a_0x0_rate")]:
        da[nm] = _decay_roll_grouped(da, "Team", col)
    a_feats = ["a_Gf", "a_Gc", "a_xGOT", "a_xGOT_faced", "a_GP", "a_BC", "a_SoT", "a_Poss", "a_WR", "a_0x0_rate"]
    away_last = da.groupby("Team")[a_feats].last().reset_index()

    # 3. Liga e H2H
    df_lig = df_hist[["Date", "League", "_0x0_flag"]].sort_values(["League", "Date"], kind="mergesort").reset_index(drop=True)
    df_lig["liga_0x0_rate"] = df_lig.groupby("League")["_0x0_flag"].transform(lambda x: x.shift(1).rolling(100, min_periods=20).mean())
    liga_last = df_lig.dropna(subset=["liga_0x0_rate"]).groupby("League")["liga_0x0_rate"].last().to_dict()

    df_hist["h2h_pair"] = [tuple(sorted(x)) for x in zip(df_hist["c_Home"], df_hist["c_Away"])]
    df_h2h = df_hist[["Date", "h2h_pair", "_0x0_flag"]].sort_values(["h2h_pair", "Date"], kind="mergesort").reset_index(drop=True)
    df_h2h["h2h_0x0_rate"] = df_h2h.groupby("h2h_pair")["_0x0_flag"].transform(lambda x: x.shift(1).rolling(8, min_periods=2).mean())
    h2h_last = df_h2h.dropna(subset=["h2h_0x0_rate"]).groupby("h2h_pair")["h2h_0x0_rate"].last().to_dict()

    evaluated = []
    for g in live_games_payload:
        home   = str(g.get("Home") or g.get("HomeTeam") or "")
        away   = str(g.get("Away") or g.get("AwayTeam") or "")
        league = str(g.get("League") or g.get("Liga") or "")
        date_v = pd.to_datetime(g.get("Date") or datetime.now().date())

        c_h = _canon(home); c_a = _canon(away)
        sh_df = home_last[home_last["Team"] == c_h]
        sa_df = away_last[away_last["Team"] == c_a]

        odd_0x0 = pd.to_numeric(g.get("Odd_CS_0x0_Lay") or g.get("Odd_0x0_Lay") or g.get("Odd_CS_0x0") or g.get("Odd_0x0_FT") or g.get("Odd_0x0_FT") or g.get("Odd_0x0_Back") or g.get("Odd_0x0") or np.nan, errors="coerce")
        odd_h = pd.to_numeric(g.get("Odd_H_FT") or g.get("Odd_H_Back") or g.get("Odd_H") or np.nan, errors="coerce")
        odd_a = pd.to_numeric(g.get("Odd_A_FT") or g.get("Odd_A_Back") or g.get("Odd_A") or np.nan, errors="coerce")

        if pd.isna(odd_0x0) or odd_0x0 <= 0: continue

        ms = {"Home": home, "Away": away, "League": league, "Date": date_v, "Time": g.get("Time", ""),
              "Odd_0x0_FT": odd_0x0, "Odd_0x0_Lay": odd_0x0, "Odd_H_FT": odd_h, "Odd_A_FT": odd_a}

        if sh_df.empty or sa_df.empty:
            ms["Decision"] = "SKIP"; ms["Reason"] = "TIME_SEM_HISTORICO_MANDO"; ms["Prob_ML"] = np.nan; ms["ev_lay"] = np.nan
            evaluated.append(ms); continue

        sh = sh_df.iloc[0]; sa = sa_df.iloc[0]
        for col in h_feats: ms["H_" + col] = sh.get(col, np.nan)
        for col in a_feats: ms["A_" + col] = sa.get(col, np.nan)

        if pd.isna(ms.get("H_h_WR")) or pd.isna(ms.get("A_a_WR")):
            ms["Decision"] = "SKIP"; ms["Reason"] = "FORMA_MANDO_INSUFICIENTE"; ms["Prob_ML"] = np.nan; ms["ev_lay"] = np.nan
            evaluated.append(ms); continue

        ms["total_xGOT"] = ms.get("H_h_xGOT", np.nan) + ms.get("A_a_xGOT", np.nan)
        ms["total_Gf"] = ms.get("H_h_Gf", np.nan) + ms.get("A_a_Gf", np.nan)
        ms["total_BC"] = ms.get("H_h_BC", np.nan) + ms.get("A_a_BC", np.nan)
        ms["total_SoT"] = ms.get("H_h_SoT", np.nan) + ms.get("A_a_SoT", np.nan)
        ms["total_def_weak"] = (ms.get("H_h_Gc", 0) or 0) + (ms.get("A_a_Gc", 0) or 0)
        ms["weaker_gk"] = min(ms.get("H_h_GP", 0) or 0, ms.get("A_a_GP", 0) or 0)
        ms["attack_imbalance"] = abs((ms.get("H_h_Gf", 0) or 0) - (ms.get("A_a_Gf", 0) or 0))

        ms["mkt_prob_0x0"] = 1.0 / odd_0x0 if odd_0x0 > 0 else np.nan
        _ov = (1/odd_h if pd.notna(odd_h) and odd_h>0 else 0) + (1/odd_a if pd.notna(odd_a) and odd_a>0 else 0) + (1/odd_0x0 if odd_0x0>0 else 0)
        ms["mkt_prob_0x0_norm"] = ms["mkt_prob_0x0"] / _ov if _ov > 0 else np.nan
        ms["liga_0x0_rate"] = liga_last.get(league, np.nan)
        
        pair = tuple(sorted([c_h, c_a]))
        ms["h2h_0x0_rate"] = h2h_last.get(pair, np.nan)
        ms["h2h_0x0_rate_raw"] = ms["h2h_0x0_rate"]

        has_all = True
        row_dict = {}
        for col in features:
            val = ms.get(col)
            if val is None or pd.isna(val): has_all = False; break
            row_dict[col] = float(val)

        if not has_all:
            ms["Decision"] = "SKIP"; ms["Reason"] = "METRICAS_AUSENTES_OU_PRIMEIRO_ENCONTRO"; ms["Prob_ML"] = np.nan; ms["ev_lay"] = np.nan
            evaluated.append(ms); continue

        row_mat = pd.DataFrame([row_dict])[features]
        ms["Prob_ML"] = float(model.predict_proba(scaler.transform(row_mat))[0, 1])
        ms["ev_lay"]  = _ev_lay(ms["Prob_ML"], odd_0x0)

        apostar, reason = check_entry_conditions(ms)
        ms["Decision"] = "APOSTA" if apostar else "SKIP"
        ms["Reason"]   = reason
        evaluated.append(ms)

    return evaluated
