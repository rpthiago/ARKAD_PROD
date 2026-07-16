import os
import pandas as pd
import numpy as np
import joblib
from datetime import datetime

MODEL_PATH = "modelo_Lay_0x1_Agressivo.pkl"
SCALER_PATH = "scaler_Lay_0x1_Agressivo.pkl"
FEATURES_PATH = "features_Lay_0x1_Agressivo.pkl"
LIGAS_PATH = "ligas_Lay_0x1_Agressivo.txt"
COMMISSION = 0.05


def normalize_live_data(live_payload):
    normalized = {}
    normalized['Home'] = live_payload.get('Home') or live_payload.get('HomeTeam') or ''
    normalized['Away'] = live_payload.get('Away') or live_payload.get('AwayTeam') or ''
    normalized['League'] = live_payload.get('League') or live_payload.get('Liga') or ''
    normalized['Time'] = live_payload.get('Time') or ''
    date_val = live_payload.get('Date') or live_payload.get('Data_Jogo') or datetime.now().date()
    normalized['Date'] = pd.to_datetime(date_val)

    normalized['Odd_CS_0x1_Lay'] = pd.to_numeric(
        live_payload.get('Odd_CS_0x1_Lay') or live_payload.get('Odd_CS_0x1') or np.nan, errors='coerce')
    normalized['Odd_CS_0x1_Back'] = pd.to_numeric(
        live_payload.get('Odd_CS_0x1_Back') or np.nan, errors='coerce')
    normalized['Odd_H_Back'] = pd.to_numeric(
        live_payload.get('Odd_H_Back') or live_payload.get('Odd_H_FT') or np.nan, errors='coerce')
    normalized['Odd_A_Back'] = pd.to_numeric(
        live_payload.get('Odd_A_Back') or live_payload.get('Odd_A_FT') or np.nan, errors='coerce')

    stats_cols = ['Total_Shots_H_FT', 'Total_Shots_A_FT',
                  'Shots_On_Target_H_FT', 'Shots_On_Target_A_FT',
                  'xG_H_FT', 'xG_A_FT', 'Goals_H_FT', 'Goals_A_FT']
    for c in stats_cols:
        normalized[c] = pd.to_numeric(live_payload.get(c, 0.0), errors='coerce')
    return normalized


def check_entry_conditions(match_state):
    odd_lay = match_state.get('Odd_CS_0x1_Lay') or 0.0
    if pd.isna(odd_lay) or odd_lay < 16.00 or odd_lay > 18.00:
        return False, "ODD_FORA_FAIXA"
    prob_ml = match_state.get('Prob_ML', 0.0)
    if prob_ml < 0.75:
        return False, "PROB_BAIXA"
    league = match_state.get('League', '')
    if os.path.exists(LIGAS_PATH):
        with open(LIGAS_PATH, 'r', encoding='utf-8') as f:
            ligas_whitelisted = [l.strip() for l in f.read().split(',') if l.strip()]
        if league not in ligas_whitelisted:
            return False, "LIGA_BLOQUEADA"
    return True, "APROVADO"


def log_paper_trade(match_state):
    log_file = "paper_trading_log_lay_0x1.csv"
    row = {
        'Date': str(match_state.get('Date', '')),
        'League': match_state.get('League', ''),
        'Home': match_state.get('Home', ''),
        'Away': match_state.get('Away', ''),
        'Odd_CS_0x1_Lay': match_state.get('Odd_CS_0x1_Lay', 0.0),
        'Prob_ML': match_state.get('Prob_ML', 0.0),
        'Decision': match_state.get('Decision', ''),
        'Reason': match_state.get('Reason', ''),
        'H_Scored_1plus_5': match_state.get('H_Scored_1plus_5', 0.0),
        'Over15_Sum_5': match_state.get('Over15_Sum_5', 0.0),
        'Home_Aggression': match_state.get('Home_Aggression', 0.0),
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
                  'xG_H_FT', 'xG_A_FT', 'Goals_H_FT', 'Goals_A_FT']
    for c in stats_cols:
        if c in df_hist.columns:
            df_hist[c] = pd.to_numeric(df_hist[c], errors='coerce').fillna(0.0)
        else:
            df_hist[c] = 0.0

    df_h = df_hist[['Date', 'Home', 'Goals_H_FT', 'Goals_A_FT',
                     'Total_Shots_H_FT', 'Shots_On_Target_H_FT', 'xG_H_FT']].copy()
    df_h.columns = ['Date', 'Team', 'Goals_Scored', 'Goals_Conceded', 'Shots', 'Shots_On_Target', 'xG']
    df_h['Is_Home'] = 1

    df_a = df_hist[['Date', 'Away', 'Goals_A_FT', 'Goals_H_FT',
                     'Total_Shots_A_FT', 'Shots_On_Target_A_FT', 'xG_A_FT']].copy()
    df_a.columns = ['Date', 'Team', 'Goals_Scored', 'Goals_Conceded', 'Shots', 'Shots_On_Target', 'xG']
    df_a['Is_Home'] = 0

    df_teams = pd.concat([df_h, df_a], ignore_index=True)
    df_teams = df_teams.sort_values(['Team', 'Date']).reset_index(drop=True)

    # FIX: Adicionado shift(1) em todas as rolling windows para evitar data leakage.
    # Sem shift(1), a media da janela na linha i inclui o proprio jogo i (o resultado
    # que queremos prever). Com shift(1), cada janela usa apenas os jogos ANTERIORES ao jogo i.
    df_teams['Home_Scored_Flag'] = ((df_teams['Is_Home'] == 1) & (df_teams['Goals_Scored'] >= 1)).astype(float)
    df_teams['H_Scored_1plus_5'] = (
        df_teams.groupby('Team')['Home_Scored_Flag']
        .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    )
    df_teams['Over15_Flag'] = ((df_teams['Goals_Scored'] + df_teams['Goals_Conceded']) >= 2).astype(float)
    df_teams['Over15_Rate_5'] = (
        df_teams.groupby('Team')['Over15_Flag']
        .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    )
    df_teams['Roll_Goals_Scored_5'] = (
        df_teams.groupby('Team')['Goals_Scored']
        .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    )
    df_teams['Roll_Goals_Conceded_5'] = (
        df_teams.groupby('Team')['Goals_Conceded']
        .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    )
    df_teams['Roll_xG_5'] = (
        df_teams.groupby('Team')['xG']
        .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    )
    df_teams['Roll_SoT_5'] = (
        df_teams.groupby('Team')['Shots_On_Target']
        .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    )
    df_teams['CS_0x1_Flag'] = ((df_teams['Goals_Scored'] == 0) & (df_teams['Goals_Conceded'] == 1)).astype(float)
    df_teams['CS_0x1_Rate_5'] = (
        df_teams.groupby('Team')['CS_0x1_Flag']
        .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    )

    latest_stats = df_teams.groupby('Team').last().reset_index()

    # Fit a post-hoc probability calibrator on the historical data using df_historical
    calibrated_model = None
    try:
        if not df_hist.empty:
            # Separate df_teams back into home and away stats per match date
            df_h_stats = df_teams[df_teams['Is_Home'] == 1].copy()
            df_a_stats = df_teams[df_teams['Is_Home'] == 0].copy()
            
            h_cols = {
                'Team': 'Home',
                'H_Scored_1plus_5': 'H_Scored_1plus_5',
                'Over15_Rate_5': 'H_Over15_Rate_5',
                'Roll_Goals_Scored_5': 'H_Goals_Scored_5',
                'Roll_Goals_Conceded_5': 'H_Goals_Conceded_5',
                'Roll_xG_5': 'H_xG_5',
                'Roll_SoT_5': 'H_SoT_5',
                'CS_0x1_Rate_5': 'H_CS_0x1_Rate_5'
            }
            df_h_stats = df_h_stats.rename(columns=h_cols)
            
            a_cols = {
                'Team': 'Away',
                'Over15_Rate_5': 'A_Over15_Rate_5',
                'Roll_Goals_Scored_5': 'A_Goals_Scored_5',
                'Roll_Goals_Conceded_5': 'A_Goals_Conceded_5',
                'Roll_xG_5': 'A_xG_5',
                'Roll_SoT_5': 'A_SoT_5',
                'CS_0x1_Rate_5': 'A_CS_0x1_Rate_5'
            }
            df_a_stats = df_a_stats.rename(columns=a_cols)
            
            df_calib = df_hist.merge(df_h_stats[['Home', 'Date'] + [c for c in h_cols.values() if c not in ['Home', 'Team']]], on=['Home', 'Date'], how='inner')
            df_calib = df_calib.merge(df_a_stats[['Away', 'Date'] + [c for c in a_cols.values() if c not in ['Away', 'Team']]], on=['Away', 'Date'], how='inner')
            
            if not df_calib.empty:
                df_calib['Over15_Sum_5'] = df_calib['H_Over15_Rate_5'] + df_calib['A_Over15_Rate_5']
                df_calib['Goals_Sum_5'] = df_calib['H_Goals_Scored_5'] + df_calib['A_Goals_Scored_5']
                df_calib['xG_Sum_5'] = df_calib['H_xG_5'] + df_calib['A_xG_5']
                df_calib['CS_0x1_Sum_5'] = df_calib['H_CS_0x1_Rate_5'] + df_calib['A_CS_0x1_Rate_5']
                df_calib['Scored_Over15_Score'] = df_calib['H_Scored_1plus_5'] * df_calib['Over15_Sum_5']
                df_calib['Home_Aggression'] = df_calib['H_Scored_1plus_5'] * df_calib['H_xG_5']
                
                odd_cs_col = 'Odd_CS_0x1_Lay' if 'Odd_CS_0x1_Lay' in df_calib.columns else 'Odd_CS_0x1'
                odd_h_col = 'Odd_H_Back' if 'Odd_H_Back' in df_calib.columns else 'Odd_H_FT'
                odd_a_col = 'Odd_A_Back' if 'Odd_A_Back' in df_calib.columns else 'Odd_A_FT'
                
                df_calib['Odd_CS_0x1_Lay'] = pd.to_numeric(df_calib[odd_cs_col], errors='coerce').fillna(12.0)
                df_calib['Odd_H_Back'] = pd.to_numeric(df_calib[odd_h_col], errors='coerce').fillna(2.0)
                df_calib['Odd_A_Back'] = pd.to_numeric(df_calib[odd_a_col], errors='coerce').fillna(2.0)
                
                df_calib['Prob_Odd_CS_0x1_Lay'] = 1 / (df_calib['Odd_CS_0x1_Lay'] + 1e-10)
                df_calib['Prob_Odd_H_Back'] = 1 / (df_calib['Odd_H_Back'] + 1e-10)
                df_calib['Prob_Odd_A_Back'] = 1 / (df_calib['Odd_A_Back'] + 1e-10)
                df_calib['Ratio_HA'] = df_calib['Odd_H_Back'] / (df_calib['Odd_A_Back'] + 1e-10)
                
                y_calib = (~((df_calib["Goals_H_FT"] == 0) & (df_calib["Goals_A_FT"] == 1))).astype(int)
                
                num_zeros = (y_calib == 0).sum()
                num_ones = (y_calib == 1).sum()
                if num_zeros >= 5 and num_ones >= 5:
                    from sklearn.calibration import CalibratedClassifierCV
                    X_calib = df_calib[features].fillna(0.0)
                    X_calib_scaled = scaler.transform(X_calib)
                    calibrated_model = CalibratedClassifierCV(estimator=model, method='sigmoid', cv='prefit')
                    calibrated_model.fit(X_calib_scaled, y_calib)
    except Exception:
        pass

    evaluated_games = []
    for g in live_games_payload:
        norm_g = normalize_live_data(g)
        home = norm_g['Home']; away = norm_g['Away']
        sh = latest_stats[latest_stats['Team'] == home]
        sa = latest_stats[latest_stats['Team'] == away]
        if sh.empty or sa.empty:
            continue
        sh = sh.iloc[0]; sa = sa.iloc[0]

        norm_g['H_Scored_1plus_5'] = sh['H_Scored_1plus_5']
        norm_g['H_Over15_Rate_5'] = sh['Over15_Rate_5']
        norm_g['H_Goals_Scored_5'] = sh['Roll_Goals_Scored_5']
        norm_g['H_Goals_Conceded_5'] = sh['Roll_Goals_Conceded_5']
        norm_g['H_xG_5'] = sh['Roll_xG_5']
        norm_g['H_SoT_5'] = sh['Roll_SoT_5']
        norm_g['H_CS_0x1_Rate_5'] = sh['CS_0x1_Rate_5']
        norm_g['A_Over15_Rate_5'] = sa['Over15_Rate_5']
        norm_g['A_Goals_Scored_5'] = sa['Roll_Goals_Scored_5']
        norm_g['A_Goals_Conceded_5'] = sa['Roll_Goals_Conceded_5']
        norm_g['A_xG_5'] = sa['Roll_xG_5']
        norm_g['A_SoT_5'] = sa['Roll_SoT_5']
        norm_g['A_CS_0x1_Rate_5'] = sa['CS_0x1_Rate_5']

        norm_g['Over15_Sum_5'] = norm_g['H_Over15_Rate_5'] + norm_g['A_Over15_Rate_5']
        norm_g['Goals_Sum_5'] = norm_g['H_Goals_Scored_5'] + norm_g['A_Goals_Scored_5']
        norm_g['xG_Sum_5'] = norm_g['H_xG_5'] + norm_g['A_xG_5']
        norm_g['CS_0x1_Sum_5'] = norm_g['H_CS_0x1_Rate_5'] + norm_g['A_CS_0x1_Rate_5']
        norm_g['Scored_Over15_Score'] = norm_g['H_Scored_1plus_5'] * norm_g['Over15_Sum_5']
        norm_g['Home_Aggression'] = norm_g['H_Scored_1plus_5'] * norm_g['H_xG_5']

        norm_g['Prob_Odd_CS_0x1_Lay'] = 1 / (norm_g['Odd_CS_0x1_Lay'] + 1e-10)
        norm_g['Prob_Odd_H_Back'] = 1 / (norm_g['Odd_H_Back'] + 1e-10)
        norm_g['Prob_Odd_A_Back'] = 1 / (norm_g['Odd_A_Back'] + 1e-10)
        norm_g['Ratio_HA'] = norm_g['Odd_H_Back'] / (norm_g['Odd_A_Back'] + 1e-10)

        row_mat = pd.DataFrame([{col: norm_g.get(col, 0.0) for col in features}])
        row_mat = row_mat.fillna(0.0)
        row_scaled = scaler.transform(row_mat)
        if calibrated_model is not None:
            prob_ml = calibrated_model.predict_proba(row_scaled)[0, 1]
        else:
            prob_ml = model.predict_proba(row_scaled)[0, 1]
        norm_g['Prob_ML'] = prob_ml

        apostar, reason = check_entry_conditions(norm_g)
        norm_g['Decision'] = 'APOSTA' if apostar else 'SKIP'
        norm_g['Reason'] = reason
        if apostar:
            log_paper_trade(norm_g)
        evaluated_games.append(norm_g)
    return evaluated_games
