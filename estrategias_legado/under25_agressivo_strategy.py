import os
import pandas as pd
import numpy as np
import joblib
from datetime import datetime

MODEL_PATH = "modelo_Under25_Agressivo.pkl"
SCALER_PATH = "scaler_Under25_Agressivo.pkl"
FEATURES_PATH = "features_Under25_Agressivo.pkl"
LIGAS_PATH = "ligas_Under25_Agressivo.txt"


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

    normalized['Odd_Under25_FT'] = pd.to_numeric(
        live_payload.get('Odd_Under25_FT') or live_payload.get('Odd_Under25_FT_Back') or np.nan, errors='coerce')
    normalized['Odd_Over25_FT'] = pd.to_numeric(
        live_payload.get('Odd_Over25_FT') or live_payload.get('Odd_Over25_FT_Back') or np.nan, errors='coerce')
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
    """
    Decide se faz a entrada no Under 2.5 FT com base nas regras otimizadas do modelo.
    Retorna: (Apostar: bool, Motivo/Status: str)
    """
    odd_under = match_state.get('Odd_Under25_FT') or 0.0
    if pd.isna(odd_under) or odd_under < 1.50 or odd_under > 1.70:
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
    """
    Grava os logs operacionais do metodo em Quarentena no arquivo
    paper_trading_log_under25.csv, incluindo os valores numericos exatos
    das features preditivas no gatilho para futura auditoria de data drift.
    """
    log_file = "paper_trading_log_under25.csv"

    row = {
        'Date': str(match_state.get('Date', '')),
        'League': match_state.get('League', ''),
        'Home': match_state.get('Home', ''),
        'Away': match_state.get('Away', ''),
        'Odd_Under25_FT': match_state.get('Odd_Under25_FT', 0.0),
        'Prob_ML': match_state.get('Prob_ML', 0.0),
        'Decision': match_state.get('Decision', ''),
        'Reason': match_state.get('Reason', ''),
        'CS_Index_Sum_5': match_state.get('CS_Index_Sum_5', 0.0),
        'xG_Decline_Sum': match_state.get('xG_Decline_Sum', 0.0),
        'Low_SoT_Sum_5': match_state.get('Low_SoT_Sum_5', 0.0),
        'Goals_Conceded_Sum_5': match_state.get('Goals_Conceded_Sum_5', 0.0),
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

    # Home offensive perspective
    df_h = df_hist[['Date', 'Home', 'Goals_H_FT',
                     'Total_Shots_H_FT', 'Shots_On_Target_H_FT', 'xG_H_FT']].copy()
    df_h.columns = ['Date', 'Team', 'Goals_Scored',
                     'Shots', 'Shots_On_Target', 'xG']

    # Away offensive perspective
    df_a = df_hist[['Date', 'Away', 'Goals_A_FT',
                     'Total_Shots_A_FT', 'Shots_On_Target_A_FT', 'xG_A_FT']].copy()
    df_a.columns = ['Date', 'Team', 'Goals_Scored',
                     'Shots', 'Shots_On_Target', 'xG']

    # Home conceded = Away scored
    df_h_conc = df_hist[['Date', 'Home', 'Goals_A_FT',
                          'xG_A_FT', 'Shots_On_Target_A_FT']].copy()
    df_h_conc.columns = ['Date', 'Team', 'Goals_Conceded',
                          'xG_Conceded', 'SoT_Conceded']

    # Away conceded = Home scored
    df_a_conc = df_hist[['Date', 'Away', 'Goals_H_FT',
                          'xG_H_FT', 'Shots_On_Target_H_FT']].copy()
    df_a_conc.columns = ['Date', 'Team', 'Goals_Conceded',
                          'xG_Conceded', 'SoT_Conceded']

    df_off = pd.concat([df_h, df_a], ignore_index=True)
    df_off = df_off.sort_values(['Team', 'Date']).reset_index(drop=True)

    df_def = pd.concat([df_h_conc, df_a_conc], ignore_index=True)
    df_def = df_def.sort_values(['Team', 'Date']).reset_index(drop=True)

    # Rolling offensive metrics
    df_off['Roll_Goals_5'] = (
        df_off.groupby('Team')['Goals_Scored']
        .rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_off['Roll_xG_3'] = (
        df_off.groupby('Team')['xG']
        .rolling(3, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_off['Roll_xG_10'] = (
        df_off.groupby('Team')['xG']
        .rolling(10, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_off['Roll_SoT_5'] = (
        df_off.groupby('Team')['Shots_On_Target']
        .rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_off['Low_SoT_Flag'] = (df_off['Shots_On_Target'] < 3).astype(float)
    df_off['Low_SoT_Index_5'] = (
        df_off.groupby('Team')['Low_SoT_Flag']
        .rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )

    # Rolling defensive metrics
    df_def['Roll_Goals_Conceded_5'] = (
        df_def.groupby('Team')['Goals_Conceded']
        .rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )
    df_def['CS_Flag'] = (df_def['Goals_Conceded'] == 0).astype(float)
    df_def['CS_Index_5'] = (
        df_def.groupby('Team')['CS_Flag']
        .rolling(5, min_periods=1).mean().reset_index(level=0, drop=True)
    )

    latest_off = df_off.groupby('Team').last().reset_index()
    latest_def = df_def.groupby('Team').last().reset_index()

    evaluated_games = []
    for g in live_games_payload:
        norm_g = normalize_live_data(g)

        home = norm_g['Home']
        away = norm_g['Away']

        off_h = latest_off[latest_off['Team'] == home]
        off_a = latest_off[latest_off['Team'] == away]
        def_h = latest_def[latest_def['Team'] == home]
        def_a = latest_def[latest_def['Team'] == away]

        if off_h.empty or off_a.empty or def_h.empty or def_a.empty:
            continue

        oh = off_h.iloc[0]
        oa = off_a.iloc[0]
        dh = def_h.iloc[0]
        da = def_a.iloc[0]

        # Offensive rollings
        norm_g['H_Roll_Goals_5'] = oh['Roll_Goals_5']
        norm_g['A_Roll_Goals_5'] = oa['Roll_Goals_5']
        norm_g['H_Roll_xG_3'] = oh['Roll_xG_3']
        norm_g['H_Roll_xG_10'] = oh['Roll_xG_10']
        norm_g['A_Roll_xG_3'] = oa['Roll_xG_3']
        norm_g['A_Roll_xG_10'] = oa['Roll_xG_10']
        norm_g['H_Roll_SoT_5'] = oh['Roll_SoT_5']
        norm_g['A_Roll_SoT_5'] = oa['Roll_SoT_5']
        norm_g['H_Low_SoT_Index_5'] = oh['Low_SoT_Index_5']
        norm_g['A_Low_SoT_Index_5'] = oa['Low_SoT_Index_5']

        # Defensive rollings
        norm_g['H_Roll_Conceded_5'] = dh['Roll_Goals_Conceded_5']
        norm_g['A_Roll_Conceded_5'] = da['Roll_Goals_Conceded_5']
        norm_g['H_CS_Index_5'] = dh['CS_Index_5']
        norm_g['A_CS_Index_5'] = da['CS_Index_5']

        # Derived features
        norm_g['Goals_Conceded_Sum_5'] = norm_g['H_Roll_Conceded_5'] + norm_g['A_Roll_Conceded_5']
        norm_g['xG_Decline_H'] = norm_g['H_Roll_xG_3'] - norm_g['H_Roll_xG_10']
        norm_g['xG_Decline_A'] = norm_g['A_Roll_xG_3'] - norm_g['A_Roll_xG_10']
        norm_g['xG_Decline_Sum'] = norm_g['xG_Decline_H'] + norm_g['xG_Decline_A']
        norm_g['CS_Index_Sum_5'] = norm_g['H_CS_Index_5'] + norm_g['A_CS_Index_5']
        norm_g['Low_SoT_Sum_5'] = norm_g['H_Low_SoT_Index_5'] + norm_g['A_Low_SoT_Index_5']
        norm_g['Goals_Scored_Sum_5'] = norm_g['H_Roll_Goals_5'] + norm_g['A_Roll_Goals_5']
        norm_g['SoT_Sum_5'] = norm_g['H_Roll_SoT_5'] + norm_g['A_Roll_SoT_5']

        norm_g['Prob_Odd_Under25_FT'] = 1 / (norm_g['Odd_Under25_FT'] + 1e-10)
        norm_g['Prob_Odd_Over25_FT'] = 1 / (norm_g['Odd_Over25_FT'] + 1e-10)
        norm_g['Ratio_HA'] = norm_g['Odd_H_FT'] / (norm_g['Odd_A_FT'] + 1e-10)
        norm_g['Ratio_OverUnder'] = norm_g['Odd_Over25_FT'] / (norm_g['Odd_Under25_FT'] + 1e-10)
        norm_g['Ratio_UnderOver'] = norm_g['Odd_Under25_FT'] / (norm_g['Odd_Over25_FT'] + 1e-10)

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
