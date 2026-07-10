import os
import pandas as pd
import numpy as np
import joblib
from datetime import datetime

MODEL_PATH = "modelo_Lay_Away_Agressivo.pkl"
SCALER_PATH = "scaler_Lay_Away_Agressivo.pkl"
FEATURES_PATH = "features_Lay_Away_Agressivo.pkl"
LIGAS_PATH = "ligas_Lay_Away_Agressivo.txt"
COMMISSION = 0.05


def normalize_live_data(live_payload):
    normalized = {}

    normalized['Home'] = live_payload.get('Home') or live_payload.get('HomeTeam') or ''
    normalized['Away'] = live_payload.get('Away') or live_payload.get('AwayTeam') or ''
    normalized['League'] = live_payload.get('League') or live_payload.get('Liga') or ''
    normalized['Time'] = live_payload.get('Time') or ''

    date_val = live_payload.get('Date') or live_payload.get('Data_Jogo') or datetime.now().date()
    normalized['Date'] = pd.to_datetime(date_val)

    normalized['Odd_A_Lay'] = pd.to_numeric(
        live_payload.get('Odd_A_Lay') or np.nan, errors='coerce')
    normalized['Odd_H_Lay'] = pd.to_numeric(
        live_payload.get('Odd_H_Lay') or np.nan, errors='coerce')
    normalized['Odd_D_Lay'] = pd.to_numeric(
        live_payload.get('Odd_D_Lay') or np.nan, errors='coerce')
    normalized['Odd_A_Back'] = pd.to_numeric(
        live_payload.get('Odd_A_Back') or np.nan, errors='coerce')
    normalized['Odd_H_Back'] = pd.to_numeric(
        live_payload.get('Odd_H_Back') or np.nan, errors='coerce')

    stats_cols = ['Total_Shots_H_FT', 'Total_Shots_A_FT',
                  'Shots_On_Target_H_FT', 'Shots_On_Target_A_FT',
                  'xG_H_FT', 'xG_A_FT',
                  'Goals_H_FT', 'Goals_A_FT']
    for c in stats_cols:
        normalized[c] = pd.to_numeric(live_payload.get(c, 0.0), errors='coerce')

    return normalized


def check_entry_conditions(match_state):
    odd_lay = match_state.get('Odd_A_Lay') or 0.0
    if pd.isna(odd_lay) or odd_lay < 1.40 or odd_lay > 5.00:
        return False, "ODD_FORA_FAIXA"

    prob_ml = match_state.get('Prob_ML', 0.0)
    if prob_ml < 0.60:
        return False, "PROB_BAIXA"

    league = match_state.get('League', '')
    if os.path.exists(LIGAS_PATH):
        with open(LIGAS_PATH, 'r', encoding='utf-8') as f:
            ligas_whitelisted = [l.strip() for l in f.read().split(',') if l.strip()]
        if league not in ligas_whitelisted:
            return False, "LIGA_BLOQUEADA"

    return True, "APROVADO"


def log_paper_trade(match_state):
    log_file = "paper_trading_log_lay_away.csv"

    row = {
        'Date': str(match_state.get('Date', '')),
        'League': match_state.get('League', ''),
        'Home': match_state.get('Home', ''),
        'Away': match_state.get('Away', ''),
        'Odd_A_Lay': match_state.get('Odd_A_Lay', 0.0),
        'Prob_ML': match_state.get('Prob_ML', 0.0),
        'Decision': match_state.get('Decision', ''),
        'Reason': match_state.get('Reason', ''),
        'A_SoT_Decline': match_state.get('A_SoT_Decline', 0.0),
        'H_Resilience_5': match_state.get('H_Resilience_5', 0.0),
        'Resilience_Net': match_state.get('Resilience_Net', 0.0),
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

    # Home perspective
    df_h = df_hist[['Date', 'Home', 'Goals_H_FT', 'Goals_A_FT',
                     'Total_Shots_H_FT', 'Shots_On_Target_H_FT', 'xG_H_FT']].copy()
    df_h.columns = ['Date', 'Team', 'Goals_Scored', 'Goals_Conceded',
                     'Shots', 'Shots_On_Target', 'xG']
    df_h['Is_Home'] = 1

    # Away perspective
    df_a = df_hist[['Date', 'Away', 'Goals_A_FT', 'Goals_H_FT',
                     'Total_Shots_A_FT', 'Shots_On_Target_A_FT', 'xG_A_FT']].copy()
    df_a.columns = ['Date', 'Team', 'Goals_Scored', 'Goals_Conceded',
                     'Shots', 'Shots_On_Target', 'xG']
    df_a['Is_Home'] = 0

    df_teams = pd.concat([df_h, df_a], ignore_index=True)
    df_teams = df_teams.sort_values(['Team', 'Date']).reset_index(drop=True)

    df_teams['Home_NoLoss_Flag'] = ((df_teams['Is_Home'] == 1) & (df_teams['Goals_Scored'] >= df_teams['Goals_Conceded'])).astype(float)
    df_teams['Home_Resilience_5'] = (
        df_teams.groupby('Team')['Home_NoLoss_Flag']
        .rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_teams['Roll_SoT_3'] = (
        df_teams.groupby('Team')['Shots_On_Target']
        .rolling(3, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_teams['Roll_SoT_10'] = (
        df_teams.groupby('Team')['Shots_On_Target']
        .rolling(10, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_teams['Roll_xG_3'] = (
        df_teams.groupby('Team')['xG']
        .rolling(3, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_teams['Roll_xG_10'] = (
        df_teams.groupby('Team')['xG']
        .rolling(10, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_teams['Roll_Goals_Scored_5'] = (
        df_teams.groupby('Team')['Goals_Scored']
        .rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_teams['Roll_Goals_Conceded_5'] = (
        df_teams.groupby('Team')['Goals_Conceded']
        .rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_teams['Away_Loss_Flag'] = ((df_teams['Is_Home'] == 0) & (df_teams['Goals_Scored'] < df_teams['Goals_Conceded'])).astype(float)
    df_teams['Away_Loss_Rate_5'] = (
        df_teams.groupby('Team')['Away_Loss_Flag']
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

        norm_g['H_Resilience_5'] = sh['Home_Resilience_5']
        norm_g['H_SoT_3'] = sh['Roll_SoT_3']
        norm_g['H_SoT_10'] = sh['Roll_SoT_10']
        norm_g['H_xG_3'] = sh['Roll_xG_3']
        norm_g['H_xG_10'] = sh['Roll_xG_10']
        norm_g['H_Goals_Scored_5'] = sh['Roll_Goals_Scored_5']
        norm_g['H_Goals_Conceded_5'] = sh['Roll_Goals_Conceded_5']

        norm_g['A_SoT_3'] = sa['Roll_SoT_3']
        norm_g['A_SoT_10'] = sa['Roll_SoT_10']
        norm_g['A_xG_3'] = sa['Roll_xG_3']
        norm_g['A_xG_10'] = sa['Roll_xG_10']
        norm_g['A_Goals_Scored_5'] = sa['Roll_Goals_Scored_5']
        norm_g['A_Goals_Conceded_5'] = sa['Roll_Goals_Conceded_5']
        norm_g['A_Loss_Rate_5'] = sa['Away_Loss_Rate_5']

        norm_g['A_SoT_Decline'] = norm_g['A_SoT_3'] - norm_g['A_SoT_10']
        norm_g['A_xG_Decline'] = norm_g['A_xG_3'] - norm_g['A_xG_10']
        norm_g['H_Scored_A_Conceded'] = norm_g['H_Goals_Scored_5'] + norm_g['A_Goals_Conceded_5']
        norm_g['H_Conceded_A_Scored'] = norm_g['H_Goals_Conceded_5'] + norm_g['A_Goals_Scored_5']
        norm_g['Resilience_Net'] = norm_g['H_Resilience_5'] - norm_g['A_Loss_Rate_5']

        norm_g['Prob_Odd_A_Lay'] = 1 / (norm_g['Odd_A_Lay'] + 1e-10)
        norm_g['Prob_Odd_H_Lay'] = 1 / (norm_g['Odd_H_Lay'] + 1e-10)
        norm_g['Prob_Odd_D_Lay'] = 1 / (norm_g['Odd_D_Lay'] + 1e-10)
        norm_g['Ratio_HA_Back'] = (norm_g['Odd_H_Back'] + 1e-10) / (norm_g['Odd_A_Back'] + 1e-10)
        norm_g['Ratio_AH_Lay'] = norm_g['Odd_A_Lay'] / (norm_g['Odd_H_Lay'] + 1e-10)

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
