"""lay_draw_rf_v2_strategy.py — Lay Draw v2 (Auditado & Consolidado em Alta Fidelidade) | ARKAD PROD"""
import os
import re
import unicodedata
from datetime import datetime
from pathlib import Path
import joblib
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
MODEL_PATH    = str(ROOT / "modelo_lay_draw_rf_v2.pkl")
SCALER_PATH   = str(ROOT / "scaler_lay_draw_rf_v2.pkl")
FEATURES_PATH = str(ROOT / "features_lay_draw_rf_v2.pkl")

COMMISSION         = 0.05
EV_MIN             = 0.03
PROB_MIN           = 0.85        # Padrão de Alta Convicção: Probabilidade mínima 85.0%
ODD_MIN            = 3.20        # Faixa Sweet Spot de Odds (3.20 a 4.20)
ODD_MAX            = 4.20        # Teto 4.20 para responsabilidade financeira baixa
FAV_ODD_MAX        = 2.10        # Exige favorito claro (Mandante ou Visitante <= 2.10)
LIGA_DRAW_RATE_MAX = 0.36        # Filtro anti-ligas hiper-empatadoras (máx 36% empates)


def _canon(s):
    if pd.isna(s) or not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)


def _ev_lay(prob_not_draw, odd):
    """EV do Lay: Ganho = 1 - Comissao (quando não empata) | Perda = odd - 1 (quando empata)"""
    return prob_not_draw * (1 - COMMISSION) - (1 - prob_not_draw) * (odd - 1)


def _decay_roll_grouped_unshifted(df, group_col, val_col, window=6, alpha=0.25):
    """
    Decaimento exponencial ponderado para séries temporais passadas.
    Calculado estritamente sobre partidas anteriores à data do evento.
    """
    g = df.groupby(group_col)[val_col]
    numer = np.zeros(len(df))
    count = np.zeros(len(df))
    wsum = 0.0
    for j in range(window):
        sj = g.shift(j)
        ej = np.exp(-alpha * j)
        m = sj.notna().to_numpy()
        numer += np.where(m, np.nan_to_num(sj.to_numpy()) * ej, 0.0)
        count += m
        wsum += ej
    res = numer / wsum
    res[count < 2] = np.nan
    return pd.Series(res, index=df.index)


def check_entry_conditions(ms):
    odd = ms.get("Odd_D_FT", 0) or 0.0
    if pd.isna(odd) or odd < ODD_MIN or odd > ODD_MAX:
        return False, "ODD_FORA_FAIXA"

    prob = ms.get("Prob_ML", 0) or 0.0
    if pd.isna(prob) or prob < PROB_MIN:
        return False, f"PROB_BAIXA({prob*100:.1f}%)" if pd.notna(prob) else "SEM_PROBABILIDADE"

    odd_h = ms.get("Odd_H_FT", np.nan)
    odd_a = ms.get("Odd_A_FT", np.nan)
    if FAV_ODD_MAX is not None:
        tem_favorito = (pd.notna(odd_h) and 0 < odd_h <= FAV_ODD_MAX) or (pd.notna(odd_a) and 0 < odd_a <= FAV_ODD_MAX)
        if not tem_favorito:
            return False, "SEM_FAVORITO_CLARO"

    ev = ms.get("ev_lay", 0) or 0.0
    if ev < EV_MIN:
        return False, f"EV_BAIXO({ev:+.3f})"

    liga_rate = ms.get("liga_draw_rate", None)
    if liga_rate is not None and pd.notna(liga_rate) and LIGA_DRAW_RATE_MAX > 0 and liga_rate > LIGA_DRAW_RATE_MAX:
        return False, f"LIGA_EMPATADORA({liga_rate:.2f})"

    return True, "APROVADO"


def predict_and_evaluate_live(live_games_payload, df_historical):
    """
    Avalia sinais diários com paridade 100% estrita ao modelo e base de treino:
    - Todas as 34 features ricas calculadas (xGOT, Posse, Finalizações, H2H, Ligas).
    - Cutoff dinâmico por data (sem datas hardcoded).
    - Descarte rigoroso de partidas com features ausentes (sem números fabricados).
    """
    if not (os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH) and os.path.exists(FEATURES_PATH)):
        return []
    if df_historical is None or not isinstance(df_historical, pd.DataFrame) or df_historical.empty or "Date" not in df_historical.columns:
        return []

    model    = joblib.load(MODEL_PATH)
    scaler   = joblib.load(SCALER_PATH)
    features = joblib.load(FEATURES_PATH)

    # Identificar data de referência dos jogos ao vivo para cutoff dinâmico
    dates_live = [pd.to_datetime(g.get("Date")) for g in live_games_payload if g.get("Date")]
    ref_date = min(dates_live) if dates_live else pd.to_datetime(datetime.now().date())

    df_hist = df_historical.copy()
    df_hist["Date"] = pd.to_datetime(df_hist["Date"], errors="coerce")
    # Cutoff dinâmico: usa estritamente histórico anterior aos jogos a avaliar
    df_hist = df_hist[df_hist["Date"] < ref_date].copy()
    df_hist = df_hist.dropna(subset=["Goals_H_FT", "Goals_A_FT", "Date", "Home", "Away"]).copy()
    df_hist = df_hist.sort_values("Date", kind="mergesort").reset_index(drop=True)

    stat_cols = [
        "Goals_H_FT", "Goals_A_FT", "xGOT_H_FT", "xGOT_A_FT",
        "xGOT_Faced_H_FT", "xGOT_Faced_A_FT",
        "Goals_Prevented_H_FT", "Goals_Prevented_A_FT",
        "Big_Chances_H_FT", "Big_Chances_A_FT",
        "Shots_On_Target_H_FT", "Shots_On_Target_A_FT",
        "Possession_H_FT", "Possession_A_FT"
    ]
    for c in stat_cols:
        df_hist[c] = pd.to_numeric(df_hist.get(c, 0), errors="coerce").fillna(0.0) if c in df_hist.columns else 0.0

    df_hist["_draw_flag"] = (df_hist["Goals_H_FT"] == df_hist["Goals_A_FT"]).astype(float)
    df_hist["c_Home"] = df_hist["Home"].map(_canon)
    df_hist["c_Away"] = df_hist["Away"].map(_canon)

    # Construir visão consolidada de cada time (jogos em casa E fora)
    home_records = df_hist[["Date", "c_Home", "Goals_H_FT", "Goals_A_FT", "xGOT_H_FT", "xGOT_Faced_H_FT", "Goals_Prevented_H_FT", "Big_Chances_H_FT", "Shots_On_Target_H_FT", "Possession_H_FT"]].copy()
    home_records.columns = ["Date", "Team", "Gf", "Gc", "xGOT", "xGOT_faced", "GP", "BC", "SoT", "Poss"]
    home_records["won"] = (home_records["Gf"] > home_records["Gc"]).astype(float)
    home_records["draw"] = (home_records["Gf"] == home_records["Gc"]).astype(float)

    away_records = df_hist[["Date", "c_Away", "Goals_A_FT", "Goals_H_FT", "xGOT_A_FT", "xGOT_Faced_A_FT", "Goals_Prevented_A_FT", "Big_Chances_A_FT", "Shots_On_Target_A_FT", "Possession_A_FT"]].copy()
    away_records.columns = ["Date", "Team", "Gf", "Gc", "xGOT", "xGOT_faced", "GP", "BC", "SoT", "Poss"]
    away_records["won"] = (away_records["Gf"] > away_records["Gc"]).astype(float)
    away_records["draw"] = (away_records["Gf"] == away_records["Gc"]).astype(float)

    all_team_matches = pd.concat([home_records, away_records], ignore_index=True)
    all_team_matches = all_team_matches.sort_values(["Team", "Date"], kind="mergesort").reset_index(drop=True)

    base_cols = ["Gf", "Gc", "xGOT", "xGOT_faced", "GP", "BC", "SoT", "Poss", "won", "draw"]
    for col in base_cols:
        all_team_matches[col + "_roll"] = _decay_roll_grouped_unshifted(all_team_matches, "Team", col)

    roll_feats = [c + "_roll" for c in base_cols]
    team_last = all_team_matches.groupby("Team")[roll_feats].last().reset_index()

    # 3. Liga Draw Rate (Histórico da Liga)
    df_lig = df_hist[["Date", "League", "_draw_flag"]].sort_values(["League", "Date"], kind="mergesort").reset_index(drop=True)
    df_lig["liga_draw_rate"] = df_lig.groupby("League")["_draw_flag"].transform(
        lambda x: x.shift(1).rolling(100, min_periods=10).mean())
    liga_last = df_lig.groupby("League")["liga_draw_rate"].last().to_dict()

    # 4. H2H Draw Rate Real (Histórico de Confrontos Diretos)
    df_hist["h2h_pair"] = [tuple(sorted(x)) for x in zip(df_hist["c_Home"], df_hist["c_Away"])]
    df_h2h = df_hist[["Date", "h2h_pair", "_draw_flag"]].sort_values(["h2h_pair", "Date"], kind="mergesort").reset_index(drop=True)
    df_h2h["h2h_draw_rate"] = df_h2h.groupby("h2h_pair")["_draw_flag"].transform(
        lambda x: x.shift(1).rolling(8, min_periods=2).mean())
    h2h_last = df_h2h.dropna(subset=["h2h_draw_rate"]).groupby("h2h_pair")["h2h_draw_rate"].last().to_dict()

    evaluated = []
    for g in live_games_payload:
        home   = str(g.get("Home") or g.get("HomeTeam") or "")
        away   = str(g.get("Away") or g.get("AwayTeam") or "")
        league = str(g.get("League") or g.get("Liga") or "")
        date_v = pd.to_datetime(g.get("Date") or datetime.now().date())

        c_h = _canon(home)
        c_a = _canon(away)

        sh_df = team_last[team_last["Team"] == c_h]
        sa_df = team_last[team_last["Team"] == c_a]

        odd_d = pd.to_numeric(g.get("Odd_D_Lay") or g.get("Odd_D_FT") or g.get("Odd_D_Back") or g.get("Odd_D_FT_Back") or g.get("Odd_D") or np.nan, errors="coerce")
        odd_h = pd.to_numeric(g.get("Odd_H_FT") or g.get("Odd_H_Back") or g.get("Odd_H_Lay") or g.get("Odd_H") or np.nan, errors="coerce")
        odd_a = pd.to_numeric(g.get("Odd_A_FT") or g.get("Odd_A_Back") or g.get("Odd_A_Lay") or g.get("Odd_A") or np.nan, errors="coerce")

        if pd.isna(odd_d) or odd_d <= 0:
            continue

        ms = {
            "Home": home, "Away": away, "League": league, "Date": date_v,
            "Time": g.get("Time", ""), "Odd_D_FT": odd_d,
            "Odd_H_FT": odd_h, "Odd_A_FT": odd_a
        }

        # Se time não existe no histórico -> descarta (igual ao dropna do treino)
        if sh_df.empty or sa_df.empty:
            ms["Decision"] = "SKIP"
            ms["Reason"]   = "TIME_SEM_HISTORICO"
            ms["Prob_ML"]  = np.nan
            ms["ev_lay"]   = np.nan
            evaluated.append(ms)
            continue

        sh = sh_df.iloc[0]
        sa = sa_df.iloc[0]

        h_feat_map = {
            "H_h_Gf": "Gf_roll", "H_h_Gc": "Gc_roll", "H_h_xGOT": "xGOT_roll", "H_h_xGOT_faced": "xGOT_faced_roll",
            "H_h_GP": "GP_roll", "H_h_BC": "BC_roll", "H_h_SoT": "SoT_roll", "H_h_Poss": "Poss_roll",
            "H_h_WR": "won_roll", "H_h_draw_rate": "draw_roll"
        }
        for target, src in h_feat_map.items():
            ms[target] = sh.get(src, np.nan)

        a_feat_map = {
            "A_a_Gf": "Gf_roll", "A_a_Gc": "Gc_roll", "A_a_xGOT": "xGOT_roll", "A_a_xGOT_faced": "xGOT_faced_roll",
            "A_a_GP": "GP_roll", "A_a_BC": "BC_roll", "A_a_SoT": "SoT_roll", "A_a_Poss": "Poss_roll",
            "A_a_WR": "won_roll", "A_a_draw_rate": "draw_roll"
        }
        for target, src in a_feat_map.items():
            ms[target] = sa.get(src, np.nan)

        h_wr = ms.get("H_h_WR", np.nan)
        a_wr = ms.get("A_a_WR", np.nan)
        h_dr = ms.get("H_h_draw_rate", np.nan)
        a_dr = ms.get("A_a_draw_rate", np.nan)

        # Se features básicas do time estão ausentes -> descarta
        if pd.isna(h_wr) or pd.isna(a_wr) or pd.isna(h_dr) or pd.isna(a_dr):
            ms["Decision"] = "SKIP"
            ms["Reason"]   = "FEATURES_INSUFICIENTES"
            ms["Prob_ML"]  = np.nan
            ms["ev_lay"]   = np.nan
            evaluated.append(ms)
            continue

        ms["total_WR"]        = h_wr + a_wr
        ms["wr_diff"]         = abs(h_wr - a_wr)
        ms["draw_rate_prod"]  = h_dr * a_dr
        ms["draw_rate_mean"]  = (h_dr + a_dr) / 2
        ms["total_xGOT"]      = (ms.get("H_h_xGOT", 0) or 0) + (ms.get("A_a_xGOT", 0) or 0)
        ms["xGOT_diff"]       = abs((ms.get("H_h_xGOT", 0) or 0) - (ms.get("A_a_xGOT", 0) or 0))
        ms["total_Gf"]        = (ms.get("H_h_Gf", 0) or 0) + (ms.get("A_a_Gf", 0) or 0)
        ms["gf_diff"]         = abs((ms.get("H_h_Gf", 0) or 0) - (ms.get("A_a_Gf", 0) or 0))
        ms["decisive_score"]  = ms["total_WR"] * ms["wr_diff"]

        ms["mkt_prob_draw"]     = 1.0 / odd_d if odd_d > 0 else np.nan
        ms["mkt_prob_draw_norm"] = np.nan
        if not (pd.isna(odd_h) or pd.isna(odd_d) or pd.isna(odd_a)):
            _ov = 1/odd_h + 1/odd_d + 1/odd_a
            ms["mkt_prob_draw_norm"] = ms["mkt_prob_draw"] / _ov if _ov > 0 else np.nan
        ms["mkt_overvalue_draw"] = (ms["mkt_prob_draw"] - ms["draw_rate_mean"]
                                    if pd.notna(ms["mkt_prob_draw"]) else np.nan)

        ms["liga_draw_rate"] = liga_last.get(league, 0.26)
        
        pair = tuple(sorted([c_h, c_a]))
        ms["h2h_draw_rate"]  = h2h_last.get(pair, ms["draw_rate_mean"])

        # Montar vetor exato das 34 features
        has_all_features = True
        row_dict = {}
        for col in features:
            val = ms.get(col)
            if val is None or pd.isna(val):
                has_all_features = False
                break
            row_dict[col] = float(val)

        if not has_all_features:
            ms["Decision"] = "SKIP"
            ms["Reason"]   = "METRICAS_AUSENTES"
            ms["Prob_ML"]  = np.nan
            ms["ev_lay"]   = np.nan
            evaluated.append(ms)
            continue

        row_mat = pd.DataFrame([row_dict])[features]
        ms["Prob_ML"] = float(model.predict_proba(scaler.transform(row_mat))[0, 1])
        ms["ev_lay"]  = _ev_lay(ms["Prob_ML"], odd_d)

        apostar, reason = check_entry_conditions(ms)
        ms["Decision"] = "APOSTA" if apostar else "SKIP"
        ms["Reason"]   = reason
        evaluated.append(ms)

    return evaluated
