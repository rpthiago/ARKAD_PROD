"""lay_under25_fs_strategy.py — Lay under25 (FOOTYSTATS) | ROI OOS +3.3% | 67161 picks | ROI>0 3/4"""
import os, pandas as pd, numpy as np, joblib
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MODEL_PATH    = str(ROOT / "modelo_lay_under25_fs.pkl")
SCALER_PATH   = str(ROOT / "scaler_lay_under25_fs.pkl")
FEATURES_PATH = str(ROOT / "features_lay_under25_fs.pkl")
COMMISSION = 0.05
EV_MIN     = 0.02
ODD_MIN, ODD_MAX = 1.5, 2.8
ODD_COL = "Odd_Under25_FT"
SPECS = [('Gf', 'Goals_H_FT', 'Goals_A_FT'), ('Gc', 'Goals_A_FT', 'Goals_H_FT'), ('xg', 'xG_H', 'xG_A'), ('sot', 'ShotsOnTarget_H', 'ShotsOnTarget_A'), ('poss', 'Possession_H', 'Possession_A'), ('da', 'DangerousAttacks_H', 'DangerousAttacks_A'), ('corn', 'Corners_H', 'Corners_A')]


def _ev_lay(prob, odd):
    return prob * (1 - COMMISSION) - (1 - prob) * (odd - 1)


def _decay_roll_grouped(df, grp, col, window=6, alpha=0.25, min_g=3):
    g = df.groupby(grp)[col]
    numer = np.zeros(len(df)); cnt = np.zeros(len(df)); ws = 0.0
    for j in range(window):
        sj = g.shift(1+j); ej = np.exp(-alpha*j); m = sj.notna().to_numpy()
        numer += np.where(m, np.nan_to_num(sj.to_numpy())*ej, 0.0); cnt += m; ws += ej
    r = numer/ws; r[cnt < window] = np.nan
    return pd.Series(r, index=df.index)


def check_entry_conditions(ms):
    odd = ms.get(ODD_COL, 0) or 0.0
    if pd.isna(odd) or odd < ODD_MIN or odd > ODD_MAX:
        return False, "ODD_FORA_FAIXA"
    ev = _ev_lay(ms.get("Prob_ML", 0) or 0.0, odd)
    if ev < EV_MIN:
        return False, f"EV_BAIXO({ev:+.3f})"
    return True, "APROVADO"


def log_paper_trade(ms):
    row = {k: ms.get(k, "") for k in ["Date","League","Home","Away",ODD_COL,"Prob_ML",
            "ctx_WR_diff","total_xg","liga_rate","mkt_prob_sel","Decision","Reason"]}
    row["ev_lay"] = round(_ev_lay(ms.get("Prob_ML", 0) or 0, ms.get(ODD_COL, 1) or 1), 4)
    row["Timestamp"] = datetime.now().isoformat()
    f = "paper_trading_log_lay_under25_fs.csv"
    pd.DataFrame([row]).to_csv(f, mode="a", header=not os.path.exists(f), index=False)


def predict_and_evaluate_live(live_games_payload, df_historical):
    """df_historical DEVE ser a base FOOTYSTATS."""
    if not (os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH) and os.path.exists(FEATURES_PATH)):
        return []
    model = joblib.load(MODEL_PATH); scaler = joblib.load(SCALER_PATH); features = joblib.load(FEATURES_PATH)

    df = df_historical.copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date","Home","Away","Goals_H_FT","Goals_A_FT"]).sort_values("Date").reset_index(drop=True)
    if live_games_payload:
        fd = pd.to_datetime(live_games_payload[0].get("Date") or datetime.now().date()).date()
        df = df[df["Date"].dt.date < fd].copy()
    gh = pd.to_numeric(df["Goals_H_FT"], errors="coerce"); ga = pd.to_numeric(df["Goals_A_FT"], errors="coerce")

    dh = pd.DataFrame({"Team": df["Home"], "Date": df["Date"], "won": (gh > ga).astype(float)})
    da = pd.DataFrame({"Team": df["Away"], "Date": df["Date"], "won": (ga > gh).astype(float)})
    for n, hc, ac in SPECS:
        dh[n] = pd.to_numeric(df[hc], errors="coerce").fillna(0.0).values
        da[n] = pd.to_numeric(df[ac], errors="coerce").fillna(0.0).values
    dh = dh.sort_values(["Team","Date"]); da = da.sort_values(["Team","Date"])
    for n, _, _ in SPECS:
        dh["r_"+n] = _decay_roll_grouped(dh, "Team", n)
        da["r_"+n] = _decay_roll_grouped(da, "Team", n)
    dh["r_WR"] = _decay_roll_grouped(dh, "Team", "won")
    da["r_WR"] = _decay_roll_grouped(da, "Team", "won")
    hlast = dh.groupby("Team").last(); alast = da.groupby("Team").last()

    df["_t"] = ((gh + ga) >= 3).astype(float)
    th = df.groupby("Home")["_t"].apply(lambda s: s.shift(1).rolling(6, min_periods=3).mean().iloc[-1] if len(s) else np.nan)
    ta = df.groupby("Away")["_t"].apply(lambda s: s.shift(1).rolling(6, min_periods=3).mean().iloc[-1] if len(s) else np.nan)
    dl = df.groupby("League")["_t"].apply(lambda s: s.shift(1).rolling(100, min_periods=20).mean().iloc[-1] if len(s) else np.nan)

    evaluated = []
    for g in live_games_payload:
        home = str(g.get("Home") or ""); away = str(g.get("Away") or ""); league = str(g.get("League") or "")
        if home not in hlast.index or away not in alast.index:
            continue
        odd = pd.to_numeric(g.get(ODD_COL) or np.nan, errors="coerce")
        if pd.isna(odd) or odd <= 0:
            continue
        sh = hlast.loc[home]; sa = alast.loc[away]
        ms = {"Home": home, "Away": away, "League": league,
              "Date": pd.to_datetime(g.get("Date") or datetime.now().date()),
              "Time": g.get("Time", ""), ODD_COL: float(odd)}
        for n, _, _ in SPECS:
            ms["H_"+n] = sh.get("r_"+n, np.nan); ms["A_"+n] = sa.get("r_"+n, np.nan)
        ms["H_WR"] = sh.get("r_WR", np.nan); ms["A_WR"] = sa.get("r_WR", np.nan)
        ms["H_mrate"] = th.get(home, np.nan); ms["A_mrate"] = ta.get(away, np.nan)
        ms["ctx_WR_diff"]   = (ms.get("H_WR",0) or 0) - (ms.get("A_WR",0) or 0)
        ms["total_xg"]      = (ms.get("H_xg",0) or 0) + (ms.get("A_xg",0) or 0)
        ms["total_Gf"]      = (ms.get("H_Gf",0) or 0) + (ms.get("A_Gf",0) or 0)
        ms["total_Gc"]      = (ms.get("H_Gc",0) or 0) + (ms.get("A_Gc",0) or 0)
        ms["total_da"]      = (ms.get("H_da",0) or 0) + (ms.get("A_da",0) or 0)
        ms["total_corn"]    = (ms.get("H_corn",0) or 0) + (ms.get("A_corn",0) or 0)
        ms["home_pressure"] = (ms.get("H_xg",0) or 0) * (ms.get("A_Gc",0) or 0)
        ms["mrate_mean"]    = ((ms.get("H_mrate",0) or 0) + (ms.get("A_mrate",0) or 0)) / 2
        ms["mkt_prob_sel"]  = 1.0 / odd if odd > 0 else np.nan
        ms["liga_rate"]     = dl.get(league, np.nan)

        row_mat = pd.DataFrame([{c: ms.get(c, 0.0) or 0.0 for c in features}]).fillna(0.0)
        ms["Prob_ML"] = float(model.predict_proba(scaler.transform(row_mat))[0, 1])
        ms["ev_lay"]  = _ev_lay(ms["Prob_ML"], float(odd))
        ok, reason = check_entry_conditions(ms)
        ms["Decision"] = "APOSTA" if ok else "SKIP"; ms["Reason"] = reason
        if ok: log_paper_trade(ms)
        evaluated.append(ms)
    return evaluated
