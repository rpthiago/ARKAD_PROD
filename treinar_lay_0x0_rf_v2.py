"""
treinar_lay_0x0_rf_v2.py — Lay 0x0 v2
=======================================
Hipótese: Laya o placar 0x0 quando ambos os times têm alto poder ofensivo
e/ou defesas porosas — o mercado superestima a chance de empate sem gols.

P&L LAY: ganhou = +(1-commission) se jogo tiver ≥1 gol; perdeu = -(odd-1).
Base rate: ~92% dos jogos têm ao menos 1 gol — mas odd 0x0 precisa compensar
o risco residual (EV filtro estrito).

Critério de aprovação: ROI > 0 em >= 3 dos últimos 4 meses OOS.

Backbone: XGBoost (auditoria Fable 5 jul/2026). No harness com odd lay REAL +
odd_lay>=10, XGB deu +24% vs +15% do LGBM, ganhando nas 2 metades (bootstrap
pareado p=0.009). Mesmas features, mesmo walk-forward, mesma aprovação — só o
classificador mudou. Fallback: LGBM -> GradientBoosting.
"""

import sys, os, warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
import joblib
from pathlib import Path
from datetime import datetime

from sklearn.preprocessing import StandardScaler
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import roc_auc_score

try:
    import lightgbm as lgb
    HAS_LGB = True
except ImportError:
    HAS_LGB = False

try:
    import xgboost as xgb
    HAS_XGB = True
except ImportError:
    HAS_XGB = False

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)

COMMISSION   = 0.05
EV_MIN       = 0.02        # EV mínimo real LAY — mais conservador (base rate alta → margem pequena)
ODD_MIN      = 6.0         # CS 0x0 mercado Betfair
ODD_MAX      = 16.0
WARMUP_MESES = 12
WINDOW       = 6
DECAY_ALPHA  = 0.25
MIN_GAMES    = 3

# Filtros contextuais
LIGA_0X0_RATE_MAX = 0.12   # só ligas onde ≤12% dos jogos terminam 0x0 (= ligas goleadoras)
MKT_PROB_MAX      = 0.10   # mercado implica ≤10% de chance de 0x0 (busca onde ele exagera)
TOTAL_XGOT_MIN    = 0.00   # desativado por padrão

ODD_COLS = ["Odd_CS_0x0_Lay", "Odd_CS_0x0"]   # tentar nesta ordem


def ts():
    return datetime.now().strftime("%H:%M:%S")


def _decay_roll(series, window=WINDOW):
    weights = np.exp(-DECAY_ALPHA * np.arange(window)[::-1])
    def _wm(arr):
        n = len(arr)
        if n < MIN_GAMES:
            return np.nan
        w = weights[-n:]
        return np.dot(arr, w) / w.sum()
    return series.shift(1).rolling(window, min_periods=MIN_GAMES).apply(_wm, raw=True)


def build_features(df_raw: pd.DataFrame, odd_col: str):
    # Trabalhar num frame slim com apenas as colunas necessárias para evitar OOM nos merges
    keep_cols = [
        "Date","League","Home","Away",
        "Goals_H_FT","Goals_A_FT",
        "xGOT_H_FT","xGOT_A_FT","xGOT_Faced_H_FT","xGOT_Faced_A_FT",
        "Goals_Prevented_H_FT","Goals_Prevented_A_FT",
        "Big_Chances_H_FT","Big_Chances_A_FT",
        "Shots_On_Target_H_FT","Shots_On_Target_A_FT",
        "Possession_H_FT","Possession_A_FT",
        "Odd_H_FT","Odd_D_FT","Odd_A_FT",
        odd_col,
    ]
    keep_cols = [c for c in keep_cols if c in df_raw.columns]
    df = df_raw[keep_cols].copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.sort_values("Date", kind="mergesort").reset_index(drop=True)

    stat_cols = ["Goals_H_FT","Goals_A_FT","xGOT_H_FT","xGOT_A_FT",
                 "xGOT_Faced_H_FT","xGOT_Faced_A_FT",
                 "Goals_Prevented_H_FT","Goals_Prevented_A_FT",
                 "Big_Chances_H_FT","Big_Chances_A_FT",
                 "Shots_On_Target_H_FT","Shots_On_Target_A_FT",
                 "Possession_H_FT","Possession_A_FT"]
    for c in stat_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0) if c in df.columns else 0.0

    for c in ["Odd_H_FT","Odd_D_FT","Odd_A_FT"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Target LAY 0x0: jogo tem ao menos 1 gol (= 0x0 NÃO acontece)
    df["target"]    = ((df["Goals_H_FT"] + df["Goals_A_FT"]) > 0).astype(int)
    df["_0x0_flag"] = (1 - df["target"]).astype(float)
    df["_won_H"]    = (df["Goals_H_FT"] > df["Goals_A_FT"]).astype(float)
    df["_won_A"]    = (df["Goals_A_FT"] > df["Goals_H_FT"]).astype(float)
    df["_month"]    = df["Date"].dt.to_period("M")

    print(f"  Base rate Lay 0x0 (>=1 gol): {df['target'].mean():.1%}")

    # ── Taxa de 0x0 por liga e H2H — calcular ANTES dos joins, em frames mínimos ──
    df["h2h_key"] = ["|".join(sorted([str(h), str(a)]))
                     for h, a in zip(df["Home"], df["Away"])]

    # Liga
    df_lig_s = df[["Date","League","_0x0_flag"]].sort_values(["League","Date"], kind="mergesort").reset_index(drop=True)
    df_lig_s["liga_0x0_rate"] = df_lig_s.groupby("League")["_0x0_flag"].transform(
        lambda x: x.shift(1).rolling(100, min_periods=20).mean())
    liga_rate_map = dict(zip(
        zip(df_lig_s["Date"].astype(str), df_lig_s["League"]),
        df_lig_s["liga_0x0_rate"]
    ))

    # H2H
    df_h2h_s = df[["Date","h2h_key","_0x0_flag"]].sort_values(["h2h_key","Date"], kind="mergesort").reset_index(drop=True)
    df_h2h_s["h2h_0x0_rate"] = df_h2h_s.groupby("h2h_key")["_0x0_flag"].transform(
        lambda x: x.shift(1).rolling(8, min_periods=2).mean())
    h2h_rate_map = dict(zip(
        zip(df_h2h_s["Date"].astype(str), df_h2h_s["h2h_key"]),
        df_h2h_s["h2h_0x0_rate"]
    ))
    del df_lig_s, df_h2h_s

    # ── Vista HOME: forma do mandante EM CASA ─────────────────────────────────
    dh = df[["Date","Home","Goals_H_FT","Goals_A_FT","xGOT_H_FT","xGOT_Faced_H_FT",
             "Goals_Prevented_H_FT","Big_Chances_H_FT","Shots_On_Target_H_FT",
             "Possession_H_FT","_won_H","_0x0_flag"]].copy()
    dh.columns = ["Date","Team","Gf","Gc","xGOT","xGOT_faced","GP","BC","SoT","Poss","won","_0x0"]
    dh = dh.sort_values(["Team","Date"], kind="mergesort")   # mantém índice de df p/ features as-of (leak-free)
    for col, name in [("Gf","h_Gf"),("Gc","h_Gc"),("xGOT","h_xGOT"),
                      ("xGOT_faced","h_xGOT_faced"),("GP","h_GP"),
                      ("BC","h_BC"),("SoT","h_SoT"),("Poss","h_Poss"),("won","h_WR"),
                      ("_0x0","h_0x0_rate")]:
        dh[name] = dh.groupby("Team")[col].transform(_decay_roll)

    # ── Vista AWAY: forma do visitante FORA ───────────────────────────────────
    da = df[["Date","Away","Goals_A_FT","Goals_H_FT","xGOT_A_FT","xGOT_Faced_A_FT",
             "Goals_Prevented_A_FT","Big_Chances_A_FT","Shots_On_Target_A_FT",
             "Possession_A_FT","_won_A","_0x0_flag"]].copy()
    da.columns = ["Date","Team","Gf","Gc","xGOT","xGOT_faced","GP","BC","SoT","Poss","won","_0x0"]
    da = da.sort_values(["Team","Date"], kind="mergesort")   # mantém índice de df p/ features as-of (leak-free)
    for col, name in [("Gf","a_Gf"),("Gc","a_Gc"),("xGOT","a_xGOT"),
                      ("xGOT_faced","a_xGOT_faced"),("GP","a_GP"),
                      ("BC","a_BC"),("SoT","a_SoT"),("Poss","a_Poss"),("won","a_WR"),
                      ("_0x0","a_0x0_rate")]:
        da[name] = da.groupby("Team")[col].transform(_decay_roll)

    h_feats = ["h_Gf","h_Gc","h_xGOT","h_xGOT_faced","h_GP","h_BC","h_SoT","h_Poss","h_WR","h_0x0_rate"]
    a_feats = ["a_Gf","a_Gc","a_xGOT","a_xGOT_faced","a_GP","a_BC","a_SoT","a_Poss","a_WR","a_0x0_rate"]
    # AS-OF (leak-free): valor por-linha alinhado ao índice de df — NÃO usa .last() (fim de base)
    for name in h_feats:
        df["H_" + name] = dh[name].reindex(df.index)
    for name in a_feats:
        df["A_" + name] = da[name].reindex(df.index)
    del dh, da

    # ── Derivadas: poder ofensivo combinado e risco de 0x0 ────────────────────
    df["total_xGOT"]       = df["H_h_xGOT"]       + df["A_a_xGOT"]
    df["total_Gf"]         = df["H_h_Gf"]          + df["A_a_Gf"]
    df["total_BC"]         = df["H_h_BC"]           + df["A_a_BC"]
    df["total_SoT"]        = df["H_h_SoT"]          + df["A_a_SoT"]
    df["total_def_weak"]   = df["H_h_Gc"]           + df["A_a_Gc"]
    df["weaker_gk"]        = df["H_h_xGOT_faced"]  + df["A_a_xGOT_faced"]
    df["h2h_0x0_rate_raw"] = df["H_h_0x0_rate"]    * df["A_a_0x0_rate"]
    df["attack_imbalance"] = (df["H_h_Gf"] - df["H_h_Gc"]).abs() + (df["A_a_Gf"] - df["A_a_Gc"]).abs()

    # ── Mercado: prob implícita de 0x0 ────────────────────────────────────────
    df["_odd_0x0"]      = pd.to_numeric(df[odd_col], errors="coerce")
    df["mkt_prob_0x0"]  = 1.0 / df["_odd_0x0"].replace(0, np.nan)
    df["mkt_prob_0x0_norm"] = np.nan
    if all(c in df.columns for c in ["Odd_H_FT","Odd_D_FT","Odd_A_FT"]):
        _over = (1/df["Odd_H_FT"].replace(0,np.nan) +
                 1/df["Odd_D_FT"].replace(0,np.nan) +
                 1/df["Odd_A_FT"].replace(0,np.nan))
        df["mkt_prob_0x0_norm"] = df["mkt_prob_0x0"] / _over

    # ── Mapear taxas pré-computadas (sem merge pesado) ────────────────────────
    date_str = df["Date"].astype(str)
    df["liga_0x0_rate"] = [liga_rate_map.get((d, l), np.nan)
                           for d, l in zip(date_str, df["League"])]
    df["h2h_0x0_rate"]  = [h2h_rate_map.get((d, k), np.nan)
                           for d, k in zip(date_str, df["h2h_key"])]

    FEATURE_COLS = [
        "H_h_Gf","H_h_Gc","H_h_xGOT","H_h_xGOT_faced","H_h_GP","H_h_BC","H_h_SoT","H_h_Poss","H_h_WR","H_h_0x0_rate",
        "A_a_Gf","A_a_Gc","A_a_xGOT","A_a_xGOT_faced","A_a_GP","A_a_BC","A_a_SoT","A_a_Poss","A_a_WR","A_a_0x0_rate",
        "total_xGOT","total_Gf","total_BC","total_SoT","total_def_weak","weaker_gk","h2h_0x0_rate_raw","attack_imbalance",
        "mkt_prob_0x0","mkt_prob_0x0_norm",
        "liga_0x0_rate","h2h_0x0_rate",
    ]
    FEATURE_COLS = [c for c in FEATURE_COLS if c in df.columns]
    return df, FEATURE_COLS


def walk_forward(df, feat_cols, odd_col):
    meses = sorted(df["_month"].unique())
    oos_meses = meses[WARMUP_MESES:]
    print(f"  Walk-forward: {len(meses)} meses | {len(oos_meses)} OOS | {len(feat_cols)} features")

    oos_records = []
    for mes in oos_meses:
        df_tr  = df[df["_month"] < mes].dropna(subset=feat_cols+["target","_odd"])
        df_oos = df[df["_month"] == mes].dropna(subset=feat_cols+["target","_odd"])
        df_oos = df_oos[(df_oos["_odd"] >= ODD_MIN) & (df_oos["_odd"] <= ODD_MAX)]

        if len(df_tr) < 200 or len(df_oos) < 5:
            continue
        y_tr = df_tr["target"].values
        if y_tr.sum() == 0 or y_tr.sum() == len(y_tr):
            continue

        scaler = StandardScaler()
        X_tr  = scaler.fit_transform(df_tr[feat_cols].fillna(0))
        X_oos = scaler.transform(df_oos[feat_cols].fillna(0))

        model = _build_model()
        model.fit(X_tr, y_tr)
        probs = model.predict_proba(X_oos)[:, 1]
        odds  = df_oos["_odd"].values

        # TRUE LAY EV
        ev_arr = probs * (1 - COMMISSION) - (1 - probs) * (odds - 1)
        mask   = ev_arr > EV_MIN

        if LIGA_0X0_RATE_MAX > 0 and "liga_0x0_rate" in df_oos.columns:
            mask = mask & (df_oos["liga_0x0_rate"].fillna(1.0).values < LIGA_0X0_RATE_MAX)
        if MKT_PROB_MAX > 0 and "mkt_prob_0x0" in df_oos.columns:
            mask = mask & (df_oos["mkt_prob_0x0"].fillna(1.0).values < MKT_PROB_MAX)

        n_picks = mask.sum()
        if n_picks < 3:
            continue

        pnl = 0.0
        for i in np.where(mask)[0]:
            ganhou = bool(df_oos["target"].iloc[i])
            pnl += (1 - COMMISSION) if ganhou else -(odds[i] - 1)

        try:
            auc = roc_auc_score(df_oos["target"].values, probs)
        except Exception:
            auc = 0.5

        roi = pnl / n_picks
        oos_records.append({"mes":str(mes),"picks":int(n_picks),"pnl":round(pnl,3),
                            "roi":round(roi,4),"ev_medio":round(ev_arr[mask].mean(),4),"auc":round(auc,3)})
        print(f"    {mes}: picks={n_picks:>4} | roi={roi:>+6.1%} | ev={ev_arr[mask].mean():>+5.3f} | auc={auc:.3f}")

    return pd.DataFrame(oos_records)


def _build_model():
    # Backbone XGBoost (auditoria Fable 5 jul/2026: +24% vs +15% do LGBM no 0x0,
    # ganha nas 2 metades, bootstrap pareado p=0.009). Config EXATA a validada.
    if HAS_XGB:
        base = xgb.XGBClassifier(n_estimators=250, max_depth=4, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8, reg_lambda=1.0,
            random_state=42, verbosity=0, eval_metric="logloss")
    elif HAS_LGB:
        base = lgb.LGBMClassifier(n_estimators=300, max_depth=4, learning_rate=0.05,
            num_leaves=20, min_child_samples=20, subsample=0.8, colsample_bytree=0.8,
            class_weight="balanced", random_state=42, verbose=-1)
    else:
        from sklearn.ensemble import GradientBoostingClassifier
        base = GradientBoostingClassifier(n_estimators=200, max_depth=3, random_state=42)
    return CalibratedClassifierCV(base, cv=3, method="isotonic")


def _gen_strategy(feat_cols, roi_oos, picks_oos, roi_pos, odd_col):
    feat_repr = repr(feat_cols)
    code = f'''"""lay_0x0_rf_v2_strategy.py — Lay 0x0 v2 | ROI OOS {roi_oos:+.1%} | {picks_oos} picks | ROI>0 {roi_pos}/4"""
import os, pandas as pd, numpy as np, joblib
import unicodedata, re
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MODEL_PATH    = str(ROOT / "modelo_lay_0x0_rf_v2.pkl")
SCALER_PATH   = str(ROOT / "scaler_lay_0x0_rf_v2.pkl")
FEATURES_PATH = str(ROOT / "features_lay_0x0_rf_v2.pkl")

COMMISSION       = {COMMISSION}
EV_MIN           = {EV_MIN}
ODD_MIN          = {ODD_MIN}
ODD_MAX          = {ODD_MAX}
LIGA_0X0_RATE_MAX = {LIGA_0X0_RATE_MAX}
MKT_PROB_MAX     = {MKT_PROB_MAX}
ODD_COL          = "{odd_col}"


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
    odd = ms.get("Odd_CS_0x0_Lay") or ms.get("Odd_CS_0x0") or 0.0
    if pd.isna(odd) or odd < ODD_MIN or odd > ODD_MAX:
        return False, "ODD_FORA_FAIXA"
    prob = ms.get("Prob_ML", 0) or 0.0
    ev = _ev_lay(prob, odd)
    if ev < EV_MIN:
        return False, f"EV_BAIXO({{ev:+.3f}})"
    liga_rate = ms.get("liga_0x0_rate", None)
    # Liga fora da base historica (rate NaN/None) => fora do universo validado; skip.
    if liga_rate is None or pd.isna(liga_rate):
        return False, "LIGA_FORA_UNIVERSO"
    if LIGA_0X0_RATE_MAX > 0 and liga_rate >= LIGA_0X0_RATE_MAX:
        return False, f"LIGA_DEFENSIVA({{liga_rate:.2f}})"
    mkt_prob = ms.get("mkt_prob_0x0", None)
    if mkt_prob is not None and MKT_PROB_MAX > 0 and mkt_prob >= MKT_PROB_MAX:
        return False, f"MERCADO_CARO({{mkt_prob:.3f}})"
    return True, "APROVADO"


def log_paper_trade(ms):
    row = {{k: ms.get(k, "") for k in
            ["Date","League","Home","Away","Odd_CS_0x0_Lay","Prob_ML",
             "total_xGOT","total_Gf","liga_0x0_rate","mkt_prob_0x0","Decision","Reason"]}}
    row["ev_lay"] = round(_ev_lay(ms.get("Prob_ML", 0) or 0, ms.get("Odd_CS_0x0_Lay", 1) or 1), 4)
    row["Timestamp"] = datetime.now().isoformat()
    f = "paper_trading_log_lay_0x0.csv"
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

    df_hist["_0x0_flag"] = ((df_hist["Goals_H_FT"] == 0) & (df_hist["Goals_A_FT"] == 0)).astype(float)

    # Vista HOME
    dh = df_hist[["Date","Home","Goals_H_FT","Goals_A_FT","xGOT_H_FT","xGOT_Faced_H_FT",
                  "Goals_Prevented_H_FT","Big_Chances_H_FT","Shots_On_Target_H_FT","Possession_H_FT","_0x0_flag"]].copy()
    dh["won"] = (dh["Goals_H_FT"] > dh["Goals_A_FT"]).astype(float)
    dh = dh.rename(columns={{"Home":"Team"}})
    dh = dh.sort_values(["Team","Date"], kind="mergesort").reset_index(drop=True)
    for col, nm in [("Goals_H_FT","h_Gf"),("Goals_A_FT","h_Gc"),("xGOT_H_FT","h_xGOT"),
                    ("xGOT_Faced_H_FT","h_xGOT_faced"),("Goals_Prevented_H_FT","h_GP"),
                    ("Big_Chances_H_FT","h_BC"),("Shots_On_Target_H_FT","h_SoT"),
                    ("Possession_H_FT","h_Poss"),("won","h_WR"),("_0x0_flag","h_0x0_rate")]:
        dh[nm] = _decay_roll_grouped_unshifted(dh, "Team", col)
    h_feats = ["h_Gf","h_Gc","h_xGOT","h_xGOT_faced","h_GP","h_BC","h_SoT","h_Poss","h_WR","h_0x0_rate"]
    home_last = dh.groupby("Team")[h_feats].last().reset_index()

    # Vista AWAY
    da = df_hist[["Date","Away","Goals_A_FT","Goals_H_FT","xGOT_A_FT","xGOT_Faced_A_FT",
                  "Goals_Prevented_A_FT","Big_Chances_A_FT","Shots_On_Target_A_FT","Possession_A_FT","_0x0_flag"]].copy()
    da["won"] = (da["Goals_A_FT"] > da["Goals_H_FT"]).astype(float)
    da = da.rename(columns={{"Away":"Team"}})
    da = da.sort_values(["Team","Date"], kind="mergesort").reset_index(drop=True)
    for col, nm in [("Goals_A_FT","a_Gf"),("Goals_H_FT","a_Gc"),("xGOT_A_FT","a_xGOT"),
                    ("xGOT_Faced_A_FT","a_xGOT_faced"),("Goals_Prevented_A_FT","a_GP"),
                    ("Big_Chances_A_FT","a_BC"),("Shots_On_Target_A_FT","a_SoT"),
                    ("Possession_A_FT","a_Poss"),("won","a_WR"),("_0x0_flag","a_0x0_rate")]:
        da[nm] = _decay_roll_grouped_unshifted(da, "Team", col)
    a_feats = ["a_Gf","a_Gc","a_xGOT","a_xGOT_faced","a_GP","a_BC","a_SoT","a_Poss","a_WR","a_0x0_rate"]
    away_last = da.groupby("Team")[a_feats].last().reset_index()

    # Liga 0x0 rate
    df_hist["_tgt"] = ((df_hist["Goals_H_FT"] + df_hist["Goals_A_FT"]) > 0).astype(float)
    df_lig = df_hist[["Date","League","_0x0_flag"]].sort_values(["League","Date"], kind="mergesort").reset_index(drop=True)
    df_lig["liga_0x0_rate"] = df_lig.groupby("League")["_0x0_flag"].transform(
        lambda x: x.shift(1).rolling(100, min_periods=20).mean())
    liga_last = df_lig.groupby("League")["liga_0x0_rate"].last().to_dict()

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

        odd_val = pd.to_numeric(g.get("Odd_CS_0x0_Lay") or g.get("Odd_CS_0x0") or np.nan, errors="coerce")
        if pd.isna(odd_val) or odd_val <= 0:
            continue

        odd_h = pd.to_numeric(g.get("Odd_H_FT") or g.get("Odd_H_Back") or np.nan, errors="coerce")
        odd_d = pd.to_numeric(g.get("Odd_D_FT") or np.nan, errors="coerce")
        odd_a = pd.to_numeric(g.get("Odd_A_FT") or g.get("Odd_A_Back") or np.nan, errors="coerce")

        ms = {{
            "Home": home, "Away": away, "League": league, "Date": date_v,
            "Time": g.get("Time", ""),
            "Odd_CS_0x0_Lay": odd_val,
        }}

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
        ms["h2h_0x0_rate_raw"] = (ms.get("H_h_0x0_rate", 0) or 0) * (ms.get("A_a_0x0_rate", 0) or 0)
        ms["attack_imbalance"] = (
            abs((ms.get("H_h_Gf", 0) or 0) - (ms.get("H_h_Gc", 0) or 0)) +
            abs((ms.get("A_a_Gf", 0) or 0) - (ms.get("A_a_Gc", 0) or 0))
        )

        ms["mkt_prob_0x0"] = 1.0 / odd_val if odd_val > 0 else np.nan
        ms["mkt_prob_0x0_norm"] = np.nan
        if not (pd.isna(odd_h) or pd.isna(odd_d) or pd.isna(odd_a)):
            _ov = 1/odd_h + 1/odd_d + 1/odd_a
            ms["mkt_prob_0x0_norm"] = ms["mkt_prob_0x0"] / _ov if _ov > 0 else np.nan

        ms["liga_0x0_rate"] = liga_last.get(league, np.nan)
        ms["h2h_0x0_rate"]  = np.nan

        row_dict = {{col: ms.get(col, np.nan) for col in features}}
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
'''
    (ROOT / "lay_0x0_rf_v2_strategy.py").write_text(code, encoding="utf-8")
    print(f"[{ts()}] Estratégia gerada: lay_0x0_rf_v2_strategy.py")


if __name__ == "__main__":
    csv_path = ROOT / "Bases_de_Dados_API_FutPythonTrader_Bet365.csv"
    print(f"[{ts()}] Carregando base...")
    raw = pd.read_csv(csv_path, low_memory=False)

    # Detectar coluna de odd 0x0
    odd_col = None
    for cand in ODD_COLS:
        if cand in raw.columns and pd.to_numeric(raw[cand], errors="coerce").notna().sum() > 100:
            odd_col = cand
            break

    if odd_col is None:
        print(f"ERRO: nenhuma coluna de odd 0x0 encontrada. Colunas disponíveis (CS):")
        cs_cols = [c for c in raw.columns if "CS" in c.upper() or "0x0" in c.lower()]
        for c in cs_cols[:20]:
            print(f"  {c}")
        sys.exit(1)

    print(f"[{ts()}] Usando coluna odd: {odd_col}")
    print(f"[{ts()}] Computando features...")
    df, feat_cols = build_features(raw, odd_col)

    df["_odd"] = pd.to_numeric(df[odd_col], errors="coerce")
    df = df[(df["_odd"] >= ODD_MIN) & (df["_odd"] <= ODD_MAX) & df["_odd"].notna()].copy()
    print(f"[{ts()}] {len(df):,} jogos na faixa odd {ODD_MIN}-{ODD_MAX} | Base rate: {df['target'].mean():.1%}")

    if len(df) < 500:
        print("Jogos insuficientes — coluna de odd vazia ou faixa errada."); sys.exit(1)

    print(f"\n[{ts()}] Walk-forward OOS (EV > {EV_MIN:.0%}, odd {ODD_MIN}-{ODD_MAX}):")
    df_oos = walk_forward(df, feat_cols, odd_col)

    if df_oos.empty:
        print("Nenhum OOS válido."); sys.exit(1)

    ultimos4  = df_oos.tail(4)
    roi_pos   = (ultimos4["roi"] > 0).sum()
    roi_med   = df_oos["roi"].mean()
    picks_tot = df_oos["picks"].sum()
    pnl_tot   = df_oos["pnl"].sum()

    print(f"\n{'='*60}")
    print(f"  Lay 0x0 v2")
    print(f"  OOS: {len(df_oos)} meses | ROI médio: {roi_med:+.1%} | P&L: {pnl_tot:+.1f} u")
    print(f"  ROI>0: {roi_pos}/4 últimos meses")
    aprovado = roi_pos >= 3
    print(f"  Veredito: {'APROVADO [OK]' if aprovado else 'REPROVADO [X]'} (ROI>0 em {roi_pos}/4)")
    print(f"{'='*60}")

    df_oos.to_csv(ROOT / "lay_0x0_rf_v2_oos.csv", index=False)

    if not aprovado:
        print("\nReprovado — modelo não gerado."); sys.exit(0)

    print(f"\n[{ts()}] Treinando modelo final em todos os dados...")
    df_f = df.dropna(subset=feat_cols + ["target"])
    scaler_f = StandardScaler()
    X_f = scaler_f.fit_transform(df_f[feat_cols].fillna(0))
    y_f = df_f["target"].values

    if HAS_XGB:
        base_f = xgb.XGBClassifier(n_estimators=250, max_depth=4, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8, reg_lambda=1.0,
            random_state=42, verbosity=0, eval_metric="logloss")
    elif HAS_LGB:
        base_f = lgb.LGBMClassifier(n_estimators=400, max_depth=4, learning_rate=0.04,
            num_leaves=20, min_child_samples=20, subsample=0.8, colsample_bytree=0.8,
            class_weight="balanced", random_state=42, verbose=-1)
    else:
        from sklearn.ensemble import GradientBoostingClassifier
        base_f = GradientBoostingClassifier(n_estimators=300, max_depth=3, random_state=42)

    model_f = CalibratedClassifierCV(base_f, cv=5, method="isotonic")
    model_f.fit(X_f, y_f)

    joblib.dump(model_f,   ROOT / "modelo_lay_0x0_rf_v2.pkl")
    joblib.dump(scaler_f,  ROOT / "scaler_lay_0x0_rf_v2.pkl")
    joblib.dump(feat_cols, ROOT / "features_lay_0x0_rf_v2.pkl")
    print(f"[{ts()}] Salvos: modelo / scaler / features _lay_0x0_rf_v2.pkl")

    _gen_strategy(feat_cols, roi_med, picks_tot, roi_pos, odd_col)
    print(f"[{ts()}] Done!")
