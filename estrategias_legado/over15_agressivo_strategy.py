import os
import pandas as pd
import numpy as np
import joblib
from datetime import datetime

MODEL_PATH = "modelo_Over15_Agressivo.pkl"
SCALER_PATH = "scaler_Over15_Agressivo.pkl"
FEATURES_PATH = "features_Over15_Agressivo.pkl"
LIGAS_PATH = "ligas_Over15_Agressivo.txt"


def normalize_live_data(live_payload):
    """
    Converte o dicionario/JSON recebido da API ao vivo da Bet365
    exatamente para a nomenclatura e tipagem do DataFrame historico.
    """
    normalized = {}

    normalized['Home'] = live_payload.get('Home') or live_payload.get('HomeTeam') or ''
    normalized['Away'] = live_payload.get('Away') or live_payload.get('AwayTeam') or ''
    normalized['League'] = live_payload.get('League') or live_payload.get('Liga') or ''
    normalized['Time'] = live_payload.get('Time') or ''

    date_val = live_payload.get('Date') or live_payload.get('Data_Jogo') or datetime.now().date()
    normalized['Date'] = pd.to_datetime(date_val)

    normalized['Odd_Over15_FT'] = pd.to_numeric(
        live_payload.get('Odd_Over15_FT') or live_payload.get('Odd_Over15_FT_Back') or np.nan, errors='coerce')
    normalized['Odd_Under15_FT'] = pd.to_numeric(
        live_payload.get('Odd_Under15_FT') or live_payload.get('Odd_Under15_FT_Back') or np.nan, errors='coerce')
    normalized['Odd_H_FT'] = pd.to_numeric(
        live_payload.get('Odd_H_FT') or live_payload.get('Odd_H_Back') or np.nan, errors='coerce')
    normalized['Odd_A_FT'] = pd.to_numeric(
        live_payload.get('Odd_A_FT') or live_payload.get('Odd_A_Back') or np.nan, errors='coerce')

    stats_cols = ['Total_Shots_H_FT', 'Total_Shots_A_FT',
                  'Shots_On_Target_H_FT', 'Shots_On_Target_A_FT',
                  'xG_H_FT', 'xG_A_FT',
                  'Goals_H_HT', 'Goals_A_HT',
                  'Goals_H_FT', 'Goals_A_FT']
    for c in stats_cols:
        normalized[c] = pd.to_numeric(live_payload.get(c, 0.0), errors='coerce')

    return normalized


def check_entry_conditions(match_state):
    """
    Decide se faz a entrada no Over 1.5 FT com base nas regras otimizadas do modelo.
    Retorna: (Apostar: bool, Motivo/Status: str)
    """
    odd_over = match_state.get('Odd_Over15_FT') or 0.0
    if pd.isna(odd_over) or odd_over < 1.25 or odd_over > 1.60:
        return False, "ODD_FORA_FAIXA"

    prob_ml = match_state.get('Prob_ML', 0.0)
    if prob_ml < 0.55:
        return False, "PROB_BAIXA"

    league = match_state.get('League', '')
    if os.path.exists(LIGAS_PATH):
        with open(LIGAS_PATH, 'r', encoding='utf-8') as f:
            ligas_whitelisted = [l.strip() for l in f.read().split(',') if l.strip()]
        if league not in ligas_whitelisted:
            return False, "LIGA_BLOQUEADA"

    return True, "APROVADO"


def log_paper_trade(match_state):
    """
    Grava os logs operacionais do metodo em Quarentena no arquivo
    paper_trading_log_over15.csv, incluindo os valores numericos exatos
    das features preditivas no gatilho para futura auditoria de data drift.
    """
    log_file = "paper_trading_log_over15.csv"

    row = {
        'Date': str(match_state.get('Date', '')),
        'League': match_state.get('League', ''),
        'Home': match_state.get('Home', ''),
        'Away': match_state.get('Away', ''),
        'Odd_Over15_FT': match_state.get('Odd_Over15_FT', 0.0),
        'Prob_ML': match_state.get('Prob_ML', 0.0),
        'Decision': match_state.get('Decision', ''),
        'Reason': match_state.get('Reason', ''),
        'HT_Goal_Consist_Sum_5': match_state.get('HT_Goal_Consist_Sum_5', 0.0),
        'CS_Inverted_Sum_5': match_state.get('CS_Inverted_Sum_5', 0.0),
        'Goals_Sum_5': match_state.get('Goals_Sum_5', 0.0),
        'Timestamp': datetime.now().isoformat()
    }

    df_row = pd.DataFrame([row])

    if not os.path.exists(log_file):
        df_row.to_csv(log_file, index=False)
    else:
        df_row.to_csv(log_file, mode='a', header=False, index=False)


def predict_and_evaluate_live(live_games_payload, df_historical):
    """
    Interface para integrar com o dashboard ou robo live.
    Carrega o modelo, normaliza, busca o historico e avalia a aposta.
    """
    if not (os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH) and os.path.exists(FEATURES_PATH)):
        return []

    model = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)
    features = joblib.load(FEATURES_PATH)

    df_hist = df_historical.copy()
    df_hist['Date'] = pd.to_datetime(df_hist['Date'], errors='coerce')
    df_hist = df_hist.dropna(subset=['Goals_H_FT', 'Goals_A_FT', 'Date', 'Home', 'Away']).copy()

    # Blindagem temporal preventiva — evita lookahead bias
    if live_games_payload:
        first_game_date = pd.to_datetime(live_games_payload[0].get('Date') or datetime.now().date()).date()
        df_hist = df_hist[df_hist['Date'].dt.date < first_game_date].copy()

    df_hist = df_hist.sort_values('Date').reset_index(drop=True)

    stats_cols = ['Total_Shots_H_FT', 'Total_Shots_A_FT',
                  'Shots_On_Target_H_FT', 'Shots_On_Target_A_FT',
                  'xG_H_FT', 'xG_A_FT',
                  'Goals_H_HT', 'Goals_A_HT',
                  'Goals_H_FT', 'Goals_A_FT']
    for c in stats_cols:
        df_hist[c] = pd.to_numeric(df_hist[c], errors='coerce').fillna(0.0)

    # Home perspective
    df_h = df_hist[['Date', 'Home', 'Goals_H_FT', 'Goals_H_HT',
                     'Total_Shots_H_FT', 'Shots_On_Target_H_FT', 'xG_H_FT']].copy()
    df_h.columns = ['Date', 'Team', 'Goals_Scored_FT', 'Goals_Scored_HT',
                     'Shots', 'Shots_On_Target', 'xG']

    # Away perspective
    df_a = df_hist[['Date', 'Away', 'Goals_A_FT', 'Goals_A_HT',
                     'Total_Shots_A_FT', 'Shots_On_Target_A_FT', 'xG_A_FT']].copy()
    df_a.columns = ['Date', 'Team', 'Goals_Scored_FT', 'Goals_Scored_HT',
                     'Shots', 'Shots_On_Target', 'xG']

    df_teams = pd.concat([df_h, df_a], ignore_index=True)
    df_teams = df_teams.sort_values('Date').reset_index(drop=True)

    # Rolling metrics — dados ja estao no passado pois df_hist foi filtrado < first_game_date
    df_teams['Roll_Goals_FT_5'] = (
        df_teams.groupby('Team')['Goals_Scored_FT']
        .rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_teams['Roll_Goals_HT_5'] = (
        df_teams.groupby('Team')['Goals_Scored_HT']
        .rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_teams['HT_Scored_Flag'] = (df_teams['Goals_Scored_HT'] > 0).astype(float)
    df_teams['HT_Goal_Consist_5'] = (
        df_teams.groupby('Team')['HT_Scored_Flag']
        .rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_teams['NoGoal_Flag'] = (df_teams['Goals_Scored_FT'] == 0).astype(float)
    df_teams['CS_Inverted_5'] = (
        df_teams.groupby('Team')['NoGoal_Flag']
        .rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_teams['Roll_xG_5'] = (
        df_teams.groupby('Team')['xG']
        .rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_teams['Roll_Shots_3'] = (
        df_teams.groupby('Team')['Shots']
        .rolling(3, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_teams['Roll_Shots_10'] = (
        df_teams.groupby('Team')['Shots']
        .rolling(10, min_periods=1).mean().reset_index(level=0, drop=True)
    )

    latest_stats = df_teams.groupby('Team').last().reset_index()

    evaluated_games = []
    for g in live_games_payload:
        norm_g = normalize_live_data(g)

        home = norm_g['Home']
        away = norm_g['Away']

        stats_h = latest_stats[latest_stats['Team'] == home]
        stats_a = latest_stats[latest_stats['Team'] == away]

        if stats_h.empty or stats_a.empty:
            continue

        sh = stats_h.iloc[0]
        sa = stats_a.iloc[0]

        norm_g['H_Roll_Goals_5'] = sh['Roll_Goals_FT_5']
        norm_g['H_Roll_Goals_HT_5'] = sh['Roll_Goals_HT_5']
        norm_g['H_HT_Goal_Consist_5'] = sh['HT_Goal_Consist_5']
        norm_g['H_CS_Inverted_5'] = sh['CS_Inverted_5']
        norm_g['H_Roll_xG_5'] = sh['Roll_xG_5']
        norm_g['H_Roll_Shots_3'] = sh['Roll_Shots_3']
        norm_g['H_Roll_Shots_10'] = sh['Roll_Shots_10']

        norm_g['A_Roll_Goals_5'] = sa['Roll_Goals_FT_5']
        norm_g['A_Roll_Goals_HT_5'] = sa['Roll_Goals_HT_5']
        norm_g['A_HT_Goal_Consist_5'] = sa['HT_Goal_Consist_5']
        norm_g['A_CS_Inverted_5'] = sa['CS_Inverted_5']
        norm_g['A_Roll_xG_5'] = sa['Roll_xG_5']
        norm_g['A_Roll_Shots_3'] = sa['Roll_Shots_3']
        norm_g['A_Roll_Shots_10'] = sa['Roll_Shots_10']

        # Derived features (matching training feature_engineering)
        norm_g['HT_Goal_Consist_Sum_5'] = norm_g['H_HT_Goal_Consist_5'] + norm_g['A_HT_Goal_Consist_5']
        norm_g['CS_Inverted_Sum_5'] = norm_g['H_CS_Inverted_5'] + norm_g['A_CS_Inverted_5']
        norm_g['Goals_Sum_5'] = norm_g['H_Roll_Goals_5'] + norm_g['A_Roll_Goals_5']
        norm_g['xG_Sum_5'] = norm_g['H_Roll_xG_5'] + norm_g['A_Roll_xG_5']
        norm_g['Total_Shots_Momentum'] = (
            (norm_g['H_Roll_Shots_3'] - norm_g['H_Roll_Shots_10']) +
            (norm_g['A_Roll_Shots_3'] - norm_g['A_Roll_Shots_10'])
        )

        norm_g['Prob_Odd_Over15_FT'] = 1 / (norm_g['Odd_Over15_FT'] + 1e-10)
        norm_g['Ratio_HA'] = norm_g['Odd_H_FT'] / (norm_g['Odd_A_FT'] + 1e-10)
        norm_g['Ratio_OverUnder'] = norm_g['Odd_Over15_FT'] / (norm_g['Odd_Under15_FT'] + 1e-10)

        # Build matrix row with exact features the model expects
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
