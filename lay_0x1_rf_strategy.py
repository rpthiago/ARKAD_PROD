"""
lay_0x1_rf_strategy.py — Lay 0x1 via Random Forest on-the-fly
Mesmo pipeline do Painel Quant, interface igual aos metodos do Hub Central.
Treina RF nos dados historicos e preve para os jogos do dia.
"""
import os
import numpy as np
import pandas as pd
import difflib
import unicodedata
import re
from datetime import datetime

# Arquivos de dados auxiliares
FOOTSTATS_PATH = "Resultados_2026_Full.csv"


def _clean_name(name):
    name = str(name).lower()
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", name)


def _merge_footstats_features(live_list, df_fs):
    """Tenta enriquecer os jogos ao vivo com features da base Footstats."""
    if df_fs is None or df_fs.empty:
        return live_list

    df_fs = df_fs.copy()
    df_fs["Date"] = pd.to_datetime(df_fs["Date"], errors="coerce")
    df_fs["_home_key"] = df_fs["Home"].map(_clean_name)
    df_fs["_away_key"] = df_fs["Away"].map(_clean_name)
    df_fs["_date_key"] = df_fs["Date"].dt.strftime("%Y-%m-%d")

    enriched = []
    for g in live_list:
        home_key = _clean_name(g.get("Home", ""))
        away_key = _clean_name(g.get("Away", ""))
        date_str = str(g.get("Date", ""))[:10]

        # Match by team names + date
        match = df_fs[
            (df_fs["_home_key"] == home_key) &
            (df_fs["_away_key"] == away_key) &
            (df_fs["_date_key"] == date_str)
        ]
        if match.empty:
            # Try without date
            match = df_fs[
                (df_fs["_home_key"] == home_key) &
                (df_fs["_away_key"] == away_key)
            ]
        if match.empty:
            # Fuzzy fallback
            best_score = 0
            for idx, row in df_fs.iterrows():
                h_score = difflib.SequenceMatcher(None, home_key, row.get("_home_key", "")).ratio()
                a_score = difflib.SequenceMatcher(None, away_key, row.get("_away_key", "")).ratio()
                score = (h_score + a_score) / 2
                if score > best_score and score > 0.75:
                    best_score = score
                    match = df_fs.loc[[idx]]

        if not match.empty:
            row = match.iloc[0]
            g["DangerousAttacks_H"] = row.get("DangerousAttacks_H", np.nan)
            g["DangerousAttacks_A"] = row.get("DangerousAttacks_A", np.nan)
            g["xG_H"] = row.get("xG_H", np.nan)
            g["xG_A"] = row.get("xG_A", np.nan)
            g["Goals_H_FT"] = row.get("Goals_H_FT", np.nan)
            g["Goals_A_FT"] = row.get("Goals_A_FT", np.nan)
        enriched.append(g)

    return enriched


def normalize_live_data(live_payload):
    normalized = {}
    normalized["Home"] = live_payload.get("Home") or live_payload.get("HomeTeam") or ""
    normalized["Away"] = live_payload.get("Away") or live_payload.get("AwayTeam") or ""
    normalized["League"] = live_payload.get("League") or live_payload.get("Liga") or ""
    normalized["Time"] = live_payload.get("Time") or ""
    date_val = live_payload.get("Date") or live_payload.get("Data_Jogo") or datetime.now().date()
    normalized["Date"] = pd.to_datetime(date_val)

    normalized["Odd_CS_0x1_Lay"] = pd.to_numeric(
        live_payload.get("Odd_CS_0x1_Lay") or live_payload.get("Odd_CS_0x1") or np.nan,
        errors="coerce")
    normalized["Odd_CS_0x1_Back"] = pd.to_numeric(
        live_payload.get("Odd_CS_0x1_Back") or np.nan, errors="coerce")
    normalized["Odd_H_FT"] = pd.to_numeric(
        live_payload.get("Odd_H_FT") or live_payload.get("Odd_H_Back") or np.nan, errors="coerce")
    normalized["Odd_A_FT"] = pd.to_numeric(
        live_payload.get("Odd_A_FT") or live_payload.get("Odd_A_Back") or np.nan, errors="coerce")

    # Keep raw values from payload for RF features
    normalized["DangerousAttacks_H"] = pd.to_numeric(
        live_payload.get("DangerousAttacks_H", np.nan), errors="coerce")
    normalized["DangerousAttacks_A"] = pd.to_numeric(
        live_payload.get("DangerousAttacks_A", np.nan), errors="coerce")
    normalized["xG_H"] = pd.to_numeric(
        live_payload.get("xG_H", live_payload.get("xG_H_FT", np.nan)), errors="coerce")
    normalized["xG_A"] = pd.to_numeric(
        live_payload.get("xG_A", live_payload.get("xG_A_FT", np.nan)), errors="coerce")

    return normalized


def check_entry_conditions(match_state):
    odd_lay = match_state.get("Odd_CS_0x1_Lay") or 0.0
    if pd.isna(odd_lay) or odd_lay < 6.6 or odd_lay > 13.2:
        return False, "ODD_FORA_FAIXA"

    prob_ml = match_state.get("Prob_ML", 0.0)
    if prob_ml < 0.92:
        return False, "PROB_BAIXA"

    # Blacklist de segundas divisões/ligas under com significância estatística
    league = str(match_state.get("League", "")).strip().upper()
    blacklist = {"BRAZIL 2", "FRANCE 2", "ENGLAND 2", "SPAIN 2", "PORTUGAL 1"}
    if league in blacklist:
        return False, "LIGA_BLOQUEADA"

    return True, "APROVADO"


def predict_and_evaluate_live(live_games_payload, df_historical):
    from sklearn.ensemble import RandomForestClassifier

    if not live_games_payload:
        return []

    # 1. Normalize live games
    live_list = [normalize_live_data(g) for g in live_games_payload]

    # 2. Try to enrich with Footstats features
    try:
        if os.path.exists(FOOTSTATS_PATH):
            df_fs = pd.read_csv(FOOTSTATS_PATH, low_memory=False)
            live_list = _merge_footstats_features(live_list, df_fs)
    except Exception:
        pass

    # 3. Build training data from historical base
    df_hist = df_historical.copy()
    df_hist["Date"] = pd.to_datetime(df_hist["Date"], errors="coerce")
    df_hist = df_hist.dropna(subset=["Goals_H_FT", "Goals_A_FT", "Date", "Home", "Away"]).copy()
    df_hist = df_hist.sort_values("Date").reset_index(drop=True)

    # Filter history to before live games
    if live_list:
        first_date = pd.to_datetime(live_list[0].get("Date") or datetime.now().date()).date()
        df_hist = df_hist[df_hist["Date"].dt.date < first_date].copy()

    if len(df_hist) < 200:
        return []

    # 4. Combine history + live for feature computation (shift ensures no look-ahead)
    # FIX: Prefer Odd_CS_0x1_Lay from historical to match live data source.
    # Odd_CS_0x1 in historical base may be Back price, not Lay — mismatch with
    # check_entry_conditions which uses Odd_CS_0x1_Lay exclusively.
    odd_cs_hist_col = "Odd_CS_0x1_Lay" if "Odd_CS_0x1_Lay" in df_hist.columns else "Odd_CS_0x1"
    cols_hist = ["Home", "Away", "Date", "Goals_H_FT", "Goals_A_FT",
                 "Odd_H_FT", "Odd_A_FT", odd_cs_hist_col]
    for c in cols_hist:
        if c not in df_hist.columns:
            df_hist[c] = np.nan

    df_comb = df_hist[cols_hist].copy()
    if odd_cs_hist_col != "Odd_CS_0x1":
        df_comb.rename(columns={odd_cs_hist_col: "Odd_CS_0x1"}, inplace=True)
    df_comb["Source"] = "hist"

    # Add live games
    live_rows = []
    for g in live_list:
        live_rows.append({
            "Home": g["Home"], "Away": g["Away"],
            "Date": pd.to_datetime(g["Date"]),
            "Goals_H_FT": np.nan, "Goals_A_FT": np.nan,
            "Odd_H_FT": g.get("Odd_H_FT") or 2.0,
            "Odd_A_FT": g.get("Odd_A_FT") or 2.0,
            "Odd_CS_0x1": g.get("Odd_CS_0x1_Lay") or g.get("Odd_CS_0x1") or np.nan,
            "DangerousAttacks_H": g.get("DangerousAttacks_H"),
            "DangerousAttacks_A": g.get("DangerousAttacks_A"),
            "xG_H": g.get("xG_H"),
            "xG_A": g.get("xG_A"),
            "League": g.get("League", ""),
            "Time": g.get("Time", ""),
            "Source": "live",
        })

    df_live = pd.DataFrame(live_rows)

    # 5. Compute features
    df_full = pd.concat([df_comb, df_live], ignore_index=True)
    df_full["Datetime"] = df_full["Date"]
    df_full = df_full.sort_values("Datetime").reset_index(drop=True)

    df_full["Odd_H_FT"] = pd.to_numeric(df_full["Odd_H_FT"], errors="coerce").fillna(2.0)
    df_full["Odd_A_FT"] = pd.to_numeric(df_full["Odd_A_FT"], errors="coerce").fillna(2.0)
    df_full["Spread_Forca"] = (1.0 / df_full["Odd_H_FT"]) - (1.0 / df_full["Odd_A_FT"])

    # Footstats features (try to compute if columns exist)
    has_fs = "DangerousAttacks_H" in df_full.columns

    if has_fs and df_full["DangerousAttacks_H"].notna().sum() > 10:
        df_full["DangerousAttacks_H"] = pd.to_numeric(df_full["DangerousAttacks_H"], errors="coerce")
        df_full["DangerousAttacks_A"] = pd.to_numeric(df_full["DangerousAttacks_A"], errors="coerce")
        df_full["xG_H"] = pd.to_numeric(df_full["xG_H"], errors="coerce")
        df_full["xG_A"] = pd.to_numeric(df_full["xG_A"], errors="coerce")

        df_full["MA_Dang_H"] = df_full.groupby("Home")["DangerousAttacks_H"].transform(
            lambda x: x.shift(1).rolling(5, min_periods=1).mean()).fillna(35)
        df_full["MA_Dang_A"] = df_full.groupby("Away")["DangerousAttacks_A"].transform(
            lambda x: x.shift(1).rolling(5, min_periods=1).mean()).fillna(35)
        df_full["MA_xGF"] = df_full.groupby("Home")["xG_H"].transform(
            lambda x: x.shift(1).rolling(5, min_periods=1).mean())
        df_full["MA_xGC"] = df_full.groupby("Home")["xG_A"].transform(
            lambda x: x.shift(1).rolling(5, min_periods=1).mean())
        df_full["xG_Ratio_H"] = (df_full["MA_xGF"] / (df_full["MA_xGC"] + 0.01)).fillna(1.0)

        features = ["Spread_Forca", "MA_Dang_H", "MA_Dang_A", "xG_Ratio_H"]
    else:
        features = ["Spread_Forca"]

    df_full.replace([np.inf, -np.inf], np.nan, inplace=True)

    # 6. Train on history, predict on live
    df_train = df_full[df_full["Source"] == "hist"].dropna(subset=features + ["Goals_H_FT"])
    df_pred = df_full[df_full["Source"] == "live"].dropna(subset=features)

    if df_pred.empty or len(df_train) < 100:
        return []

    # Label: Lose if exact score 0x1 happens
    y_train = (~((df_train["Goals_H_FT"] == 0) & (df_train["Goals_A_FT"] == 1))).astype(int)
    if y_train.sum() < 10:
        return []

    model = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42, n_jobs=-1)
    model.fit(df_train[features], y_train)
    probs = model.predict_proba(df_pred[features])[:, 1]

    # 7. Build results in Hub Central format
    evaluated = []
    for i in range(len(df_pred)):
        pred_row = df_pred.iloc[i]
        confidence = probs[i]

        odd_lay = pred_row.get("Odd_CS_0x1", np.nan)
        if pd.isna(odd_lay):
            # Try from original live list
            if i < len(live_list):
                odd_lay = live_list[i].get("Odd_CS_0x1_Lay") or live_list[i].get("Odd_CS_0x1") or np.nan

        result = {
            "Home": pred_row.get("Home", live_list[i]["Home"] if i < len(live_list) else ""),
            "Away": pred_row.get("Away", live_list[i]["Away"] if i < len(live_list) else ""),
            "League": pred_row.get("League", live_list[i].get("League", "") if i < len(live_list) else ""),
            "Time": pred_row.get("Time", live_list[i].get("Time", "") if i < len(live_list) else ""),
            "Date": pred_row["Date"] if "Date" in pred_row else datetime.now(),
            "Odd_CS_0x1_Lay": odd_lay,
            "Prob_ML": float(confidence),
            "Odd_H_FT": pred_row.get("Odd_H_FT", 2.0),
            "Odd_A_FT": pred_row.get("Odd_A_FT", 2.0),
        }

        apostar, reason = check_entry_conditions(result)
        result["Decision"] = "APOSTA" if apostar else "SKIP"
        result["Reason"] = reason

        evaluated.append(result)

    return evaluated
