import os
import pandas as pd
import numpy as np
import joblib
from datetime import datetime

MODEL_PATH = "modelo_BTTS_Yes_Agressivo.pkl"
SCALER_PATH = "scaler_BTTS_Yes_Agressivo.pkl"
FEATURES_PATH = "features_BTTS_Yes_Agressivo.pkl"
LIGAS_PATH = "ligas_BTTS_Yes_Agressivo.txt"


def normalize_live_data(live_payload):
    normalized = {}

    normalized['Home'] = live_payload.get('Home') or live_payload.get('HomeTeam') or ''
    normalized['Away'] = live_payload.get('Away') or live_payload.get('AwayTeam') or ''
    normalized['League'] = live_payload.get('League') or live_payload.get('Liga') or ''
    normalized['Time'] = live_payload.get('Time') or ''

    date_val = live_payload.get('Date') or live_payload.get('Data_Jogo') or datetime.now().date()
    normalized['Date'] = pd.to_datetime(date_val)

    normalized['Odd_BTTS_Yes'] = pd.to_numeric(
        live_payload.get('Odd_BTTS_Yes') or live_payload.get('Odd_BTTS_Yes_Back') or np.nan, errors='coerce')
    normalized['Odd_BTTS_No'] = pd.to_numeric(
        live_payload.get('Odd_BTTS_No') or live_payload.get('Odd_BTTS_No_Back') or np.nan, errors='coerce')
    normalized['Odd_H_FT'] = pd.to_numeric(
        live_payload.get('Odd_H_FT') or live_payload.get('Odd_H_Back') or np.nan, errors='coerce')
    normalized['Odd_A_FT'] = pd.to_numeric(
        live_payload.get('Odd_A_FT') or live_payload.get('Odd_A_Back') or np.nan, errors='coerce')

    stats_cols = ['Total_Shots_H_FT', 'Total_Shots_A_FT',
                  'Shots_On_Target_H_FT', 'Shots_On_Target_A_FT',
                  'xG_H_FT', 'xG_A_FT',
                  'Goals_H_FT', 'Goals_A_FT']
    for c in stats_cols:
        normalized[c] = pd.to_numeric(live_payload.get(c, 0.0), errors='coerce')

    return normalized


def check_entry_conditions(match_state):
    odd_btts = match_state.get('Odd_BTTS_Yes') or 0.0
    if pd.isna(odd_btts) or odd_btts < 1.55 or odd_btts > 1.70:
        return False, "ODD_FORA_FAIXA"

    prob_ml = match_state.get('Prob_ML', 0.0)
    if prob_ml < 0.45:
        return False, "PROB_BAIXA"

    league = match_state.get('League', '')
    if os.path.exists(LIGAS_PATH):
        with open(LIGAS_PATH, 'r', encoding='utf-8') as f:
            ligas_whitelisted = [l.strip() for l in f.read().split(',') if l.strip()]
        if league not in ligas_whitelisted:
            return False, "LIGA_BLOQUEADA"

    return True, "APROVADO"


def log_paper_trade(match_state):
    log_file = "paper_trading_log_btts_yes.csv"

    row = {
        'Date': str(match_state.get('Date', '')),
        'League': match_state.get('League', ''),
        'Home': match_state.get('Home', ''),
        'Away': match_state.get('Away', ''),
        'Odd_BTTS_Yes': match_state.get('Odd_BTTS_Yes', 0.0),
        'Prob_ML': match_state.get('Prob_ML', 0.0),
        'Decision': match_state.get('Decision', ''),
        'Reason': match_state.get('Reason', ''),
        'BTTS_Rate_Sum_5': match_state.get('BTTS_Rate_Sum_5', 0.0),
        'Home_Conc_Away_Scored': match_state.get('Home_Conc_Away_Scored', 0.0),
        'xG_Chaos_Sum': match_state.get('xG_Chaos_Sum', 0.0),
        'Goals_Conceded_Sum_5': match_state.get('Goals_Conceded_Sum_5', 0.0),
        'Timestamp': datetime.now().isoformat()
    }

    df_row = pd.DataFrame([row])

    if not os.path.exists(log_file):
        df_row.to_csv(log_file, index=False)
    else:
        df_row.to_csv(log_file, mode='a', header=False, index=False)


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

    stats_cols = ['Total_Shots_H_FT', 'Total_Shots_A_FT',
                  'Shots_On_Target_H_FT', 'Shots_On_Target_A_FT',
                  'xG_H_FT', 'xG_A_FT',
                  'Goals_H_FT', 'Goals_A_FT']
    for c in stats_cols:
        df_hist[c] = pd.to_numeric(df_hist[c], errors='coerce').fillna(0.0)

    # Home perspective: scored + conceded
    df_h = df_hist[['Date', 'Home', 'Goals_H_FT', 'Goals_A_FT',
                     'xG_H_FT', 'xG_A_FT',
                     'Total_Shots_H_FT', 'Shots_On_Target_H_FT']].copy()
    df_h.columns = ['Date', 'Team', 'Goals_Scored', 'Goals_Conceded',
                     'xG_Scored', 'xG_Conceded',
                     'Shots', 'Shots_On_Target']

    # Away perspective: scored + conceded
    df_a = df_hist[['Date', 'Away', 'Goals_A_FT', 'Goals_H_FT',
                     'xG_A_FT', 'xG_H_FT',
                     'Total_Shots_A_FT', 'Shots_On_Target_A_FT']].copy()
    df_a.columns = ['Date', 'Team', 'Goals_Scored', 'Goals_Conceded',
                     'xG_Scored', 'xG_Conceded',
                     'Shots', 'Shots_On_Target']

    df_teams = pd.concat([df_h, df_a], ignore_index=True)
    df_teams = df_teams.sort_values(['Team', 'Date']).reset_index(drop=True)

    # Rolling metrics
    df_teams['BTTS_Flag'] = (
        (df_teams['Goals_Scored'] > 0) & (df_teams['Goals_Conceded'] > 0)
    ).astype(float)
    df_teams['BTTS_Rate_5'] = (
        df_teams.groupby('Team')['BTTS_Flag']
        .rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_teams['Roll_Goals_Scored_5'] = (
        df_teams.groupby('Team')['Goals_Scored']
        .rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_teams['Roll_Goals_Conceded_5'] = (
        df_teams.groupby('Team')['Goals_Conceded']
        .rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_teams['Roll_xG_Scored_5'] = (
        df_teams.groupby('Team')['xG_Scored']
        .rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_teams['Roll_xG_Conceded_5'] = (
        df_teams.groupby('Team')['xG_Conceded']
        .rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_teams['Roll_SoT_5'] = (
        df_teams.groupby('Team')['Shots_On_Target']
        .rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )

    latest_stats = df_teams.groupby('Team').last().reset_index()

    evaluated_games = []
    for g in live_games_payload:
        norm_g = normalize_live_data(g)

        home = norm_g['Home']
        away = norm_g['Away']

        sh = latest_stats[latest_stats['Team'] == home]
        sa = latest_stats[latest_stats['Team'] == away]

        if sh.empty or sa.empty:
            continue

        sh = sh.iloc[0]
        sa = sa.iloc[0]

        norm_g['H_BTTS_Rate_5'] = sh['BTTS_Rate_5']
        norm_g['A_BTTS_Rate_5'] = sa['BTTS_Rate_5']
        norm_g['H_Goals_Scored_5'] = sh['Roll_Goals_Scored_5']
        norm_g['H_Goals_Conceded_5'] = sh['Roll_Goals_Conceded_5']
        norm_g['A_Goals_Scored_5'] = sa['Roll_Goals_Scored_5']
        norm_g['A_Goals_Conceded_5'] = sa['Roll_Goals_Conceded_5']
        norm_g['H_xG_Scored_5'] = sh['Roll_xG_Scored_5']
        norm_g['H_xG_Conceded_5'] = sh['Roll_xG_Conceded_5']
        norm_g['A_xG_Scored_5'] = sa['Roll_xG_Scored_5']
        norm_g['A_xG_Conceded_5'] = sa['Roll_xG_Conceded_5']
        norm_g['H_SoT_5'] = sh['Roll_SoT_5']
        norm_g['A_SoT_5'] = sa['Roll_SoT_5']

        # Derived features
        norm_g['BTTS_Rate_Sum_5'] = norm_g['H_BTTS_Rate_5'] + norm_g['A_BTTS_Rate_5']
        norm_g['Goals_Scored_Sum_5'] = norm_g['H_Goals_Scored_5'] + norm_g['A_Goals_Scored_5']
        norm_g['Goals_Conceded_Sum_5'] = norm_g['H_Goals_Conceded_5'] + norm_g['A_Goals_Conceded_5']
        norm_g['Home_Conc_Away_Scored'] = norm_g['H_Goals_Conceded_5'] + norm_g['A_Goals_Scored_5']
        norm_g['xG_Scored_Sum_5'] = norm_g['H_xG_Scored_5'] + norm_g['A_xG_Scored_5']
        norm_g['xG_Conceded_Sum_5'] = norm_g['H_xG_Conceded_5'] + norm_g['A_xG_Conceded_5']
        norm_g['xG_Chaos_Sum'] = norm_g['xG_Scored_Sum_5'] + norm_g['xG_Conceded_Sum_5']
        norm_g['SoT_Sum_5'] = norm_g['H_SoT_5'] + norm_g['A_SoT_5']

        norm_g['Prob_Odd_BTTS_Yes'] = 1 / (norm_g['Odd_BTTS_Yes'] + 1e-10)
        norm_g['Prob_Odd_BTTS_No'] = 1 / (norm_g['Odd_BTTS_No'] + 1e-10)
        norm_g['Ratio_HA'] = norm_g['Odd_H_FT'] / (norm_g['Odd_A_FT'] + 1e-10)
        norm_g['Ratio_BTTS'] = norm_g['Odd_BTTS_Yes'] / (norm_g['Odd_BTTS_No'] + 1e-10)

        row_mat = pd.DataFrame([{col: norm_g.get(col, 0.0) for col in features}])
        row_mat = row_mat.fillna(0.0)
        prob_ml = model.predict_proba(scaler.transform(row_mat))[0, 1]

        norm_g['Prob_ML'] = prob_ml

        apostar, reason = check_entry_conditions(norm_g)
        norm_g['Decision'] = 'APOSTA' if apostar else 'SKIP'
        norm_g['Reason'] = reason

        if apostar:
            log_paper_trade(norm_g)

        evaluated_games.append(norm_g)

    return evaluated_games
