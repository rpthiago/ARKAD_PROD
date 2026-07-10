import os; import pandas as pd; import numpy as np; import joblib
from datetime import datetime

MODEL_PATH = "modelo_Back_Draw_Agressivo.pkl"
SCALER_PATH = "scaler_Back_Draw_Agressivo.pkl"
FEATURES_PATH = "features_Back_Draw_Agressivo.pkl"
LIGAS_PATH = "ligas_Back_Draw_Agressivo.txt"


def normalize_live_data(live_payload):
    n = {}
    n['Home'] = live_payload.get('Home', ''); n['Away'] = live_payload.get('Away', '')
    n['League'] = live_payload.get('League', ''); n['Time'] = live_payload.get('Time', '')
    n['Date'] = pd.to_datetime(live_payload.get('Date', datetime.now().date()))
    n['Odd_D_FT'] = pd.to_numeric(live_payload.get('Odd_D_FT') or live_payload.get('Odd_D_Back', np.nan), errors='coerce')
    n['Odd_H_FT'] = pd.to_numeric(live_payload.get('Odd_H_FT') or live_payload.get('Odd_H_Back', np.nan), errors='coerce')
    n['Odd_A_FT'] = pd.to_numeric(live_payload.get('Odd_A_FT') or live_payload.get('Odd_A_Back', np.nan), errors='coerce')
    for c in ['Total_Shots_H_FT','Total_Shots_A_FT','Shots_On_Target_H_FT','Shots_On_Target_A_FT','xG_H_FT','xG_A_FT','Goals_H_FT','Goals_A_FT']:
        n[c] = pd.to_numeric(live_payload.get(c, 0.0), errors='coerce')
    return n


def check_entry_conditions(match_state):
    odd = match_state.get('Odd_D_FT', 0.0)
    if pd.isna(odd) or odd < 3.50 or odd > 4.20: return False, "ODD_FORA_FAIXA"
    if match_state.get('Prob_ML', 0.0) < 0.50: return False, "PROB_BAIXA"
    if os.path.exists(LIGAS_PATH):
        with open(LIGAS_PATH, 'r', encoding='utf-8') as f:
            if match_state.get('League', '') not in [l.strip() for l in f.read().split(',') if l.strip()]:
                return False, "LIGA_BLOQUEADA"
    return True, "APROVADO"


def log_paper_trade(match_state):
    log_file = "paper_trading_log_back_draw.csv"
    row = {
        'Date': str(match_state.get('Date', '')), 'League': match_state.get('League', ''),
        'Home': match_state.get('Home', ''), 'Away': match_state.get('Away', ''),
        'Odd_D_FT': match_state.get('Odd_D_FT', 0.0), 'Prob_ML': match_state.get('Prob_ML', 0.0),
        'Decision': match_state.get('Decision', ''), 'Reason': match_state.get('Reason', ''),
        'Draw_Rate_Sum_5': match_state.get('Draw_Rate_Sum_5', 0.0),
        'Mediocrity_Score': match_state.get('Mediocrity_Score', 0.0),
        'xG_Diff_Abs': match_state.get('xG_Diff_Abs', 0.0),
        'Timestamp': datetime.now().isoformat()
    }
    df_row = pd.DataFrame([row])
    if not os.path.exists(log_file): df_row.to_csv(log_file, index=False)
    else: df_row.to_csv(log_file, mode='a', header=False, index=False)


def predict_and_evaluate_live(live_games_payload, df_historical):
    if not (os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH) and os.path.exists(FEATURES_PATH)):
        return []
    model = joblib.load(MODEL_PATH); scaler = joblib.load(SCALER_PATH); features = joblib.load(FEATURES_PATH)

    df_hist = df_historical.copy(); df_hist['Date'] = pd.to_datetime(df_hist['Date'], errors='coerce')
    df_hist = df_hist.dropna(subset=['Goals_H_FT','Goals_A_FT','Date','Home','Away']).copy()
    if live_games_payload:
        fd = pd.to_datetime(live_games_payload[0].get('Date', datetime.now().date())).date()
        df_hist = df_hist[df_hist['Date'].dt.date < fd].copy()
    df_hist = df_hist.sort_values('Date').reset_index(drop=True)

    for c in ['Total_Shots_H_FT','Total_Shots_A_FT','Shots_On_Target_H_FT','Shots_On_Target_A_FT','xG_H_FT','xG_A_FT','Goals_H_FT','Goals_A_FT']:
        df_hist[c] = pd.to_numeric(df_hist[c], errors='coerce').fillna(0.0)

    df_h = df_hist[['Date','Home','Goals_H_FT','Goals_A_FT','Total_Shots_H_FT','Shots_On_Target_H_FT','xG_H_FT','xG_A_FT']].copy()
    df_h.columns = ['Date','Team','Goals_Scored','Goals_Conceded','Shots','Shots_On_Target','xG_Scored','xG_Conceded']; df_h['Is_Home']=1
    df_a = df_hist[['Date','Away','Goals_A_FT','Goals_H_FT','Total_Shots_A_FT','Shots_On_Target_A_FT','xG_A_FT','xG_H_FT']].copy()
    df_a.columns = ['Date','Team','Goals_Scored','Goals_Conceded','Shots','Shots_On_Target','xG_Scored','xG_Conceded']; df_a['Is_Home']=0

    df_teams = pd.concat([df_h,df_a],ignore_index=True).sort_values(['Team','Date']).reset_index(drop=True)

    df_teams['Draw_Flag'] = (df_teams['Goals_Scored']==df_teams['Goals_Conceded']).astype(float)
    df_teams['Draw_Rate_5'] = df_teams.groupby('Team')['Draw_Flag'].rolling(5,min_periods=1).mean().reset_index(level=0,drop=True)
    df_teams['Under25_Flag'] = ((df_teams['Goals_Scored']+df_teams['Goals_Conceded'])<3).astype(float)
    df_teams['Under25_Rate_5'] = df_teams.groupby('Team')['Under25_Flag'].rolling(5,min_periods=1).mean().reset_index(level=0,drop=True)
    df_teams['Roll_Goals_Scored_5'] = df_teams.groupby('Team')['Goals_Scored'].rolling(5,min_periods=1).mean().reset_index(level=0,drop=True)
    df_teams['Roll_Goals_Conceded_5'] = df_teams.groupby('Team')['Goals_Conceded'].rolling(5,min_periods=1).mean().reset_index(level=0,drop=True)
    df_teams['Roll_xG_Scored_5'] = df_teams.groupby('Team')['xG_Scored'].rolling(5,min_periods=1).mean().reset_index(level=0,drop=True)
    df_teams['Roll_xG_Conceded_5'] = df_teams.groupby('Team')['xG_Conceded'].rolling(5,min_periods=1).mean().reset_index(level=0,drop=True)
    df_teams['Roll_SoT_5'] = df_teams.groupby('Team')['Shots_On_Target'].rolling(5,min_periods=1).mean().reset_index(level=0,drop=True)

    latest = df_teams.groupby('Team').last().reset_index()
    evaluated = []
    for g in live_games_payload:
        ng = normalize_live_data(g); h=ng['Home']; a=ng['Away']
        sh=latest[latest['Team']==h]; sa=latest[latest['Team']==a]
        if sh.empty or sa.empty: continue
        sh=sh.iloc[0]; sa=sa.iloc[0]

        ng['H_Draw_Rate_5']=sh['Draw_Rate_5']; ng['A_Draw_Rate_5']=sa['Draw_Rate_5']
        ng['H_Under25_Rate_5']=sh['Under25_Rate_5']; ng['A_Under25_Rate_5']=sa['Under25_Rate_5']
        ng['H_Goals_Scored_5']=sh['Roll_Goals_Scored_5']; ng['H_Goals_Conceded_5']=sh['Roll_Goals_Conceded_5']
        ng['A_Goals_Scored_5']=sa['Roll_Goals_Scored_5']; ng['A_Goals_Conceded_5']=sa['Roll_Goals_Conceded_5']
        ng['H_xG_Scored_5']=sh['Roll_xG_Scored_5']; ng['H_xG_Conceded_5']=sh['Roll_xG_Conceded_5']
        ng['A_xG_Scored_5']=sa['Roll_xG_Scored_5']; ng['A_xG_Conceded_5']=sa['Roll_xG_Conceded_5']
        ng['H_SoT_5']=sh['Roll_SoT_5']; ng['A_SoT_5']=sa['Roll_SoT_5']

        ng['xG_Diff_Abs']=abs(ng['H_xG_Scored_5']-ng['A_xG_Scored_5'])
        ng['xG_Diff_Abs_Def']=abs(ng['H_xG_Conceded_5']-ng['A_xG_Conceded_5'])
        ng['Draw_Rate_Sum_5']=ng['H_Draw_Rate_5']+ng['A_Draw_Rate_5']
        ng['Under25_Sum_5']=ng['H_Under25_Rate_5']+ng['A_Under25_Rate_5']
        ng['Goals_Sum_5']=ng['H_Goals_Scored_5']+ng['A_Goals_Scored_5']
        ng['SoT_Sum_5']=ng['H_SoT_5']+ng['A_SoT_5']
        ng['Mediocrity_Score']=(ng['Draw_Rate_Sum_5']*ng['Under25_Sum_5'])/(ng['xG_Diff_Abs']+1)

        ng['Prob_Odd_D_FT']=1/(ng['Odd_D_FT']+1e-10)
        ng['Prob_Odd_H_FT']=1/(ng['Odd_H_FT']+1e-10)
        ng['Prob_Odd_A_FT']=1/(ng['Odd_A_FT']+1e-10)
        ng['Ratio_DH']=ng['Odd_D_FT']/(ng['Odd_H_FT']+1e-10)
        ng['Ratio_DA']=ng['Odd_D_FT']/(ng['Odd_A_FT']+1e-10)

        row_mat = pd.DataFrame([{col: ng.get(col,0.0) for col in features}])
        ng['Prob_ML'] = model.predict_proba(scaler.transform(row_mat))[0,1]
        apostar, reason = check_entry_conditions(ng)
        ng['Decision']='APOSTA' if apostar else 'SKIP'; ng['Reason']=reason
        if apostar: log_paper_trade(ng)
        evaluated.append(ng)
    return evaluated
