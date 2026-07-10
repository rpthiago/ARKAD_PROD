import os
import pandas as pd
import numpy as np
import joblib
from datetime import datetime

MODEL_PATH = "modelo_Lay_1x0_Agressivo.pkl"
SCALER_PATH = "scaler_Lay_1x0_Agressivo.pkl"
FEATURES_PATH = "features_Lay_1x0_Agressivo.pkl"
LIGAS_PATH = "ligas_Lay_1x0_Agressivo.txt"
COMMISSION = 0.05


def normalize_live_data(live_payload):
    n = {}
    n['Home'] = live_payload.get('Home', '')
    n['Away'] = live_payload.get('Away', '')
    n['League'] = live_payload.get('League', '')
    n['Time'] = live_payload.get('Time', '')
    n['Date'] = pd.to_datetime(live_payload.get('Date', datetime.now().date()))
    n['Odd_CS_1x0_Lay'] = pd.to_numeric(live_payload.get('Odd_CS_1x0_Lay') or live_payload.get('Odd_CS_1x0') or np.nan, errors='coerce')
    n['Odd_CS_1x0_Back'] = pd.to_numeric(live_payload.get('Odd_CS_1x0_Back', np.nan), errors='coerce')
    n['Odd_BTTS_Yes_Back'] = pd.to_numeric(live_payload.get('Odd_BTTS_Yes_Back', np.nan), errors='coerce')
    n['Odd_H_Back'] = pd.to_numeric(live_payload.get('Odd_H_Back') or live_payload.get('Odd_H_FT', np.nan), errors='coerce')
    n['Odd_A_Back'] = pd.to_numeric(live_payload.get('Odd_A_Back') or live_payload.get('Odd_A_FT', np.nan), errors='coerce')
    for c in ['Total_Shots_H_FT','Total_Shots_A_FT','Shots_On_Target_H_FT','Shots_On_Target_A_FT','xG_H_FT','xG_A_FT','Goals_H_FT','Goals_A_FT']:
        n[c] = pd.to_numeric(live_payload.get(c, 0.0), errors='coerce')
    return n


def check_entry_conditions(match_state):
    odd = match_state.get('Odd_CS_1x0_Lay', 0.0)
    if pd.isna(odd) or odd < 8.00 or odd > 18.00:
        return False, "ODD_FORA_FAIXA"
    prob_ml = match_state.get('Prob_ML', 0.0)
    if prob_ml < 0.60:
        return False, "PROB_BAIXA"
    ev = prob_ml * (1 - COMMISSION) - (1 - prob_ml) * (odd - 1)
    if ev <= 0:
        return False, f"EV_NEGATIVO({ev:+.3f})"
    if os.path.exists(LIGAS_PATH):
        with open(LIGAS_PATH, 'r', encoding='utf-8') as f:
            if match_state.get('League', '') not in [l.strip() for l in f.read().split(',') if l.strip()]:
                return False, "LIGA_BLOQUEADA"
    return True, "APROVADO"


def log_paper_trade(match_state):
    log_file = "paper_trading_log_lay_1x0.csv"
    row = {
        'Date': str(match_state.get('Date', '')), 'League': match_state.get('League', ''),
        'Home': match_state.get('Home', ''), 'Away': match_state.get('Away', ''),
        'Odd_CS_1x0_Lay': match_state.get('Odd_CS_1x0_Lay', 0.0),
        'Prob_ML': match_state.get('Prob_ML', 0.0),
        'Decision': match_state.get('Decision', ''), 'Reason': match_state.get('Reason', ''),
        'A_Scored_1plus_5': match_state.get('A_Scored_1plus_5', 0.0),
        'BTTS_Sum_5': match_state.get('BTTS_Sum_5', 0.0),
        'Home_Fragility': match_state.get('Home_Fragility', 0.0),
        'Timestamp': datetime.now().isoformat()
    }
    df_row = pd.DataFrame([row])
    if not os.path.exists(log_file): df_row.to_csv(log_file, index=False)
    else: df_row.to_csv(log_file, mode='a', header=False, index=False)


def predict_and_evaluate_live(live_games_payload, df_historical):
    if not (os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH) and os.path.exists(FEATURES_PATH)):
        return []
    model = joblib.load(MODEL_PATH); scaler = joblib.load(SCALER_PATH); features = joblib.load(FEATURES_PATH)

    df_hist = df_historical.copy()
    df_hist['Date'] = pd.to_datetime(df_hist['Date'], errors='coerce')
    df_hist = df_hist.dropna(subset=['Goals_H_FT', 'Goals_A_FT', 'Date', 'Home', 'Away']).copy()
    if live_games_payload:
        fd = pd.to_datetime(live_games_payload[0].get('Date', datetime.now().date())).date()
        df_hist = df_hist[df_hist['Date'].dt.date < fd].copy()
    df_hist = df_hist.sort_values('Date').reset_index(drop=True)

    sc = ['Total_Shots_H_FT','Total_Shots_A_FT','Shots_On_Target_H_FT','Shots_On_Target_A_FT','xG_H_FT','xG_A_FT','Goals_H_FT','Goals_A_FT']
    for c in sc: df_hist[c] = pd.to_numeric(df_hist[c], errors='coerce').fillna(0.0)

    df_h = df_hist[['Date','Home','Goals_H_FT','Goals_A_FT','Total_Shots_H_FT','Shots_On_Target_H_FT','xG_H_FT','xG_A_FT']].copy()
    df_h.columns = ['Date','Team','Goals_Scored','Goals_Conceded','Shots','Shots_On_Target','xG_Scored','xG_Conceded']; df_h['Is_Home'] = 1
    df_a = df_hist[['Date','Away','Goals_A_FT','Goals_H_FT','Total_Shots_A_FT','Shots_On_Target_A_FT','xG_A_FT','xG_H_FT']].copy()
    df_a.columns = ['Date','Team','Goals_Scored','Goals_Conceded','Shots','Shots_On_Target','xG_Scored','xG_Conceded']; df_a['Is_Home'] = 0

    df_teams = pd.concat([df_h, df_a], ignore_index=True).sort_values(['Team','Date']).reset_index(drop=True)

    # FIX: Adicionado shift(1) em todas as rolling windows para evitar data leakage.
    # Sem shift(1), a media da janela na linha i inclui o proprio jogo i (o resultado
    # que queremos prever). Com shift(1), cada janela usa apenas os jogos ANTERIORES ao jogo i.
    df_teams['Away_Scored_Flag'] = ((df_teams['Is_Home']==0)&(df_teams['Goals_Scored']>=1)).astype(float)
    df_teams['A_Scored_1plus_5'] = (
        df_teams.groupby('Team')['Away_Scored_Flag']
        .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    )
    df_teams['BTTS_Flag'] = ((df_teams['Goals_Scored']>0)&(df_teams['Goals_Conceded']>0)).astype(float)
    df_teams['BTTS_Rate_5'] = (
        df_teams.groupby('Team')['BTTS_Flag']
        .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    )
    df_teams['Over15_Flag'] = ((df_teams['Goals_Scored']+df_teams['Goals_Conceded'])>=2).astype(float)
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
    df_teams['Roll_xG_Scored_5'] = (
        df_teams.groupby('Team')['xG_Scored']
        .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    )
    df_teams['Roll_SoT_5'] = (
        df_teams.groupby('Team')['Shots_On_Target']
        .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    )
    df_teams['CS_1x0_Flag'] = ((df_teams['Goals_Scored']==1)&(df_teams['Goals_Conceded']==0)).astype(float)
    df_teams['CS_1x0_Rate_5'] = (
        df_teams.groupby('Team')['CS_1x0_Flag']
        .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    )
    df_teams['Home_Conceded_Flag'] = ((df_teams['Is_Home']==1)&(df_teams['Goals_Conceded']>=1)).astype(float)
    df_teams['H_Conceded_1plus_5'] = (
        df_teams.groupby('Team')['Home_Conceded_Flag']
        .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    )

    latest = df_teams.groupby('Team').last().reset_index()
    evaluated = []
    for g in live_games_payload:
        ng = normalize_live_data(g)
        h, a = ng['Home'], ng['Away']
        sh = latest[latest['Team']==h]; sa = latest[latest['Team']==a]
        if sh.empty or sa.empty: continue
        sh, sa = sh.iloc[0], sa.iloc[0]

        ng['A_Scored_1plus_5'] = sa['A_Scored_1plus_5']
        ng['H_Conceded_1plus_5'] = sh['H_Conceded_1plus_5']
        ng['H_BTTS_Rate_5'] = sh['BTTS_Rate_5']; ng['A_BTTS_Rate_5'] = sa['BTTS_Rate_5']
        ng['H_Over15_Rate_5'] = sh['Over15_Rate_5']; ng['A_Over15_Rate_5'] = sa['Over15_Rate_5']
        ng['H_Goals_Scored_5'] = sh['Roll_Goals_Scored_5']; ng['H_Goals_Conceded_5'] = sh['Roll_Goals_Conceded_5']
        ng['A_Goals_Scored_5'] = sa['Roll_Goals_Scored_5']; ng['A_Goals_Conceded_5'] = sa['Roll_Goals_Conceded_5']
        ng['H_xG_5'] = sh['Roll_xG_Scored_5']; ng['A_xG_5'] = sa['Roll_xG_Scored_5']
        ng['H_SoT_5'] = sh['Roll_SoT_5']; ng['A_SoT_5'] = sa['Roll_SoT_5']
        ng['H_CS_1x0_Rate_5'] = sh['CS_1x0_Rate_5']; ng['A_CS_1x0_Rate_5'] = sa['CS_1x0_Rate_5']

        ng['Away_Scored_Home_Leaked'] = ng['A_Scored_1plus_5'] + ng['H_Conceded_1plus_5']
        ng['BTTS_Sum_5'] = ng['H_BTTS_Rate_5'] + ng['A_BTTS_Rate_5']
        ng['Over15_Sum_5'] = ng['H_Over15_Rate_5'] + ng['A_Over15_Rate_5']
        ng['CS_1x0_Sum_5'] = ng['H_CS_1x0_Rate_5'] + ng['A_CS_1x0_Rate_5']
        ng['Away_Aggression'] = ng['A_Scored_1plus_5'] * ng['BTTS_Sum_5']
        ng['Home_Fragility'] = ng['H_Conceded_1plus_5'] * ng['H_Goals_Conceded_5']

        ng['Prob_Odd_CS_1x0_Lay'] = 1 / (ng['Odd_CS_1x0_Lay'] + 1e-10)
        ng['Prob_Odd_BTTS_Yes_Back'] = 1 / (ng['Odd_BTTS_Yes_Back'] + 1e-10)
        ng['Prob_Odd_H_Back'] = 1 / (ng['Odd_H_Back'] + 1e-10)
        ng['Prob_Odd_A_Back'] = 1 / (ng['Odd_A_Back'] + 1e-10)
        ng['Ratio_HA'] = ng['Odd_H_Back'] / (ng['Odd_A_Back'] + 1e-10)

        row_mat = pd.DataFrame([{col: ng.get(col, 0.0) for col in features}])
        row_mat = row_mat.fillna(0.0)
        ng['Prob_ML'] = model.predict_proba(scaler.transform(row_mat))[0, 1]
        apostar, reason = check_entry_conditions(ng)
        ng['Decision'] = 'APOSTA' if apostar else 'SKIP'; ng['Reason'] = reason
        if apostar: log_paper_trade(ng)
        evaluated.append(ng)
    return evaluated
