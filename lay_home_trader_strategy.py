# === LAY HOME TRADER - Estrategia de Primeiro Gol ===
# Entrada: LAY no mandante (pre-live)
# Saida:   Visitante faz 1o gol = +60% | Mandante faz 1o gol = -100% | 0x0 HT = +25%
# Modelo:  mesmo do Lay Home Features (modelo_Lay_Home_Features.pkl)
# Parametros otimizados via walk-forward 5.5 anos

import os
import pandas as pd
import numpy as np
import joblib
from datetime import datetime

MODEL_PATH = "modelo_Lay_Home_Features.pkl"
SCALER_PATH = "scaler_Lay_Home_Features.pkl"
FEATURES_PATH = "features_Lay_Home_Features.pkl"
LIGAS_PATH = "ligas_Lay_Home_Trader.txt"

# Ligas excluidas (comprovadamente negativas no walk-forward):
# CROATIA 1, ARGENTINA 1, FRANCE 2, FRANCE 3, SPAIN 2, EUROPA CHAMPIONS LEAGUE, CHINA 1


def normalize_live_data(live_payload):
    normalized = {}

    normalized['Home'] = live_payload.get('Home') or live_payload.get('HomeTeam') or ''
    normalized['Away'] = live_payload.get('Away') or live_payload.get('AwayTeam') or ''
    normalized['League'] = live_payload.get('League') or live_payload.get('Liga') or ''
    normalized['Time'] = live_payload.get('Time') or ''

    date_val = live_payload.get('Date') or live_payload.get('Data_Jogo') or datetime.now().date()
    normalized['Date'] = pd.to_datetime(date_val)

    # Odds direcionais (Back) — usadas apenas para features Odds_Asymmetry e Ratio_HA
    normalized['Odd_H_FT'] = pd.to_numeric(
        live_payload.get('Odd_H_FT') or live_payload.get('Odd_H_Back') or np.nan, errors='coerce')
    normalized['Odd_A_FT'] = pd.to_numeric(
        live_payload.get('Odd_A_FT') or live_payload.get('Odd_A_Back') or np.nan, errors='coerce')
    normalized['Odd_H_Lay'] = pd.to_numeric(
        live_payload.get('Odd_H_Lay') or live_payload.get('Odd_H_FT') or live_payload.get('Odd_H_Back') or np.nan, errors='coerce')

    stats_cols = ['Total_Shots_H_FT', 'Total_Shots_A_FT',
                  'Shots_On_Target_H_FT', 'Shots_On_Target_A_FT',
                  'xG_H_FT', 'xG_A_FT']
    for c in stats_cols:
        normalized[c] = pd.to_numeric(live_payload.get(c, 0.0), errors='coerce')

    return normalized


CUSTOM_PROB_THRESHOLD = 0.52
CUSTOM_USE_WHITELIST = False


def check_entry_conditions(match_state):
    odd_back = match_state.get('Odd_H_FT') or 0.0
    if pd.isna(odd_back) or odd_back < 1.40 or odd_back > 2.50:
        return False, "ODD_FORA_FAIXA"

    prob_ml = match_state.get('Prob_ML', 0.0)
    prob_cutoff = globals().get('CUSTOM_PROB_THRESHOLD', 0.56)
    use_whitelist = globals().get('CUSTOM_USE_WHITELIST', True)

    if prob_ml < prob_cutoff:
        return False, "PROB_BAIXA"

    if use_whitelist:
        league = match_state.get('League', '')
        if os.path.exists(LIGAS_PATH):
            with open(LIGAS_PATH, 'r', encoding='utf-8') as f:
                ligas_whitelisted = [l.strip() for l in f.read().split(',') if l.strip()]
            if league not in ligas_whitelisted:
                return False, "LIGA_BLOQUEADA"

    return True, "APROVADO"


def predict_and_evaluate_live(live_games_payload, df_historical):
    if not (os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH) and os.path.exists(FEATURES_PATH)):
        return []

    model = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)
    features = joblib.load(FEATURES_PATH)

    df_hist = df_historical.copy()
    df_hist['Date'] = pd.to_datetime(df_hist['Date'], errors='coerce')
    df_hist = df_hist.dropna(subset=['Goals_H_FT', 'Goals_A_FT', 'Date', 'Home', 'Away']).copy()

    if live_games_payload:
        first_game_date = pd.to_datetime(live_games_payload[0].get('Date') or datetime.now().date()).date()
        df_hist = df_hist[df_hist['Date'].dt.date < first_game_date].copy()

    df_hist = df_hist.sort_values('Date').reset_index(drop=True)

    # Renomeia as colunas se vierem da base full de 2026
    rename_map = {
        'Shots_H': 'Total_Shots_H_FT',
        'Shots_A': 'Total_Shots_A_FT',
        'ShotsOnTarget_H': 'Shots_On_Target_H_FT',
        'ShotsOnTarget_A': 'Shots_On_Target_A_FT',
        'xG_H': 'xG_H_FT',
        'xG_A': 'xG_A_FT',
    }
    df_hist = df_hist.rename(columns=rename_map)

    stats_cols = ['Total_Shots_H_FT', 'Total_Shots_A_FT',
                  'Shots_On_Target_H_FT', 'Shots_On_Target_A_FT',
                  'xG_H_FT', 'xG_A_FT']
    for c in stats_cols:
        df_hist[c] = pd.to_numeric(df_hist[c], errors='coerce').fillna(0.0)

    df_h = df_hist[['Date', 'Home', 'Goals_H_FT', 'Total_Shots_H_FT',
                     'Shots_On_Target_H_FT', 'xG_H_FT']].copy()
    df_h.columns = ['Date', 'Team', 'Goals_Scored', 'Shots', 'Shots_On_Target', 'xG']

    df_a = df_hist[['Date', 'Away', 'Goals_A_FT', 'Total_Shots_A_FT',
                     'Shots_On_Target_A_FT', 'xG_A_FT']].copy()
    df_a.columns = ['Date', 'Team', 'Goals_Scored', 'Shots', 'Shots_On_Target', 'xG']

    df_teams = pd.concat([df_h, df_a], ignore_index=True)
    df_teams = df_teams.sort_values('Date').reset_index(drop=True)

    df_teams['Roll_Goals_5'] = df_teams.groupby('Team')['Goals_Scored'].transform(
        lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    df_teams['Roll_Shots_5'] = df_teams.groupby('Team')['Shots'].transform(
        lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    df_teams['Roll_SOT_3'] = df_teams.groupby('Team')['Shots_On_Target'].transform(
        lambda x: x.shift(1).rolling(3, min_periods=1).mean())
    df_teams['Roll_SOT_10'] = df_teams.groupby('Team')['Shots_On_Target'].transform(
        lambda x: x.shift(1).rolling(10, min_periods=1).mean())
    df_teams['Roll_xG_5'] = df_teams.groupby('Team')['xG'].transform(
        lambda x: x.shift(1).rolling(5, min_periods=1).mean())

    latest_stats = df_teams.groupby('Team').last().reset_index()

    evaluated_games = []
    for g in live_games_payload:
        norm_g = normalize_live_data(g)

        if pd.isna(norm_g.get('Odd_H_Lay')):
            continue

        home = norm_g['Home']
        away = norm_g['Away']

        stats_h = latest_stats[latest_stats['Team'] == home]
        stats_a = latest_stats[latest_stats['Team'] == away]

        if stats_h.empty or stats_a.empty:
            continue

        sh = stats_h.iloc[0]
        sa = stats_a.iloc[0]

        norm_g['H_Roll_Goals_5'] = sh['Roll_Goals_5']
        norm_g['H_Roll_Shots_5'] = sh['Roll_Shots_5']
        norm_g['H_Roll_SOT_3'] = sh['Roll_SOT_3']
        norm_g['H_Roll_SOT_10'] = sh['Roll_SOT_10']
        norm_g['H_Roll_xG_5'] = sh['Roll_xG_5']

        norm_g['A_Roll_Goals_5'] = sa['Roll_Goals_5']
        norm_g['A_Roll_Shots_5'] = sa['Roll_Shots_5']
        norm_g['A_Roll_SOT_3'] = sa['Roll_SOT_3']
        norm_g['A_Roll_SOT_10'] = sa['Roll_SOT_10']
        norm_g['A_Roll_xG_5'] = sa['Roll_xG_5']

        norm_g['H_SOT_Momentum'] = norm_g['H_Roll_SOT_3'] - norm_g['H_Roll_SOT_10']
        norm_g['xG_Dominance_Away'] = norm_g['A_Roll_xG_5'] - norm_g['H_Roll_xG_5']
        norm_g['Odds_Asymmetry'] = (1 / (norm_g['Odd_H_FT'] + 1e-10)) - (1 / (norm_g['Odd_A_FT'] + 1e-10))
        norm_g['SOT_Sum_5'] = norm_g['H_Roll_SOT_3'] + norm_g['A_Roll_SOT_3']
        norm_g['Ratio_HA'] = norm_g['Odd_H_FT'] / (norm_g['Odd_A_FT'] + 1e-10)

        row_mat = pd.DataFrame([{col: norm_g.get(col, 0.0) for col in features}])
        row_mat = row_mat.fillna(0.0)
        prob_ml = model.predict_proba(scaler.transform(row_mat))[0, 1]

        norm_g['Prob_ML'] = prob_ml

        apostar, reason = check_entry_conditions(norm_g)
        norm_g['Decision'] = 'APOSTA' if apostar else 'SKIP'
        norm_g['Reason'] = reason

        evaluated_games.append(norm_g)

    return evaluated_games
