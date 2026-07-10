import os
import pandas as pd
import numpy as np
import joblib
from datetime import datetime

# Define paths for assets
MODEL_PATH = "modelo_Lay_Home_Features.pkl"
SCALER_PATH = "scaler_Lay_Home_Features.pkl"
FEATURES_PATH = "features_Lay_Home_Features.pkl"
LIGAS_PATH = "ligas_Lay_Home_Features.txt"

# 1. ADAPTER: Normalização de dados ao vivo
def normalize_live_data(live_payload):
    """
    Converte o dicionário/JSON recebido da API ao vivo da Betfair/Bet365
    exatamente para a nomenclatura e tipagem do DataFrame histórico.
    """
    normalized = {}
    
    # Text fields
    normalized['Home'] = live_payload.get('Home') or live_payload.get('HomeTeam') or ''
    normalized['Away'] = live_payload.get('Away') or live_payload.get('AwayTeam') or ''
    normalized['League'] = live_payload.get('League') or live_payload.get('Liga') or ''
    normalized['Time'] = live_payload.get('Time') or ''
    
    # Parse Date
    date_val = live_payload.get('Date') or live_payload.get('Data_Jogo') or datetime.now().date()
    normalized['Date'] = pd.to_datetime(date_val)
    
    # Map Betfair Exchange back/lay to historical pre-match Odd_H_FT
    normalized['Odd_H_FT'] = pd.to_numeric(live_payload.get('Odd_H_FT') or live_payload.get('Odd_H_Back') or live_payload.get('Odd_H_Lay') or np.nan, errors='coerce')
    normalized['Odd_A_FT'] = pd.to_numeric(live_payload.get('Odd_A_FT') or live_payload.get('Odd_A_Back') or live_payload.get('Odd_A_Lay') or np.nan, errors='coerce')
    
    # Specific Lay Home Odd
    normalized['Odd_H_Lay'] = pd.to_numeric(live_payload.get('Odd_H_Lay') or normalized['Odd_H_FT'] or np.nan, errors='coerce')
    
    # In-Play statistics if available (defaults to 0.0)
    stats_cols = ['Total_Shots_H_FT', 'Total_Shots_A_FT', 'Shots_On_Target_H_FT', 'Shots_On_Target_A_FT', 'xG_H_FT', 'xG_A_FT']
    for c in stats_cols:
        normalized[c] = pd.to_numeric(live_payload.get(c, 0.0), errors='coerce')
        
    return normalized

# 2. FONTE ÚNICA DA VERDADE: Condição de Entrada Única
def check_entry_conditions(match_state):
    """
    Decide se faz a entrada no Lay Home com base nas regras otimizadas do modelo.
    Retorna: (Apostar: bool, Motivo/Status: str)
    """
    # Trava de Odds Lay: entre 1.30 e 5.00 (conforme parametrização campeã)
    odd_lay = match_state.get('Odd_H_Lay') or match_state.get('Odd_H_FT') or 0.0
    if pd.isna(odd_lay) or odd_lay < 1.30 or odd_lay > 5.00:
        return False, "ODD_FORA_FAIXA"
        
    # Threshold de probabilidade do modelo: >= 0.75
    prob_ml = match_state.get('Prob_ML', 0.0)
    if prob_ml < 0.75:
        return False, "PROB_BAIXA"
        
    # Ligas Whitelist
    league = match_state.get('League', '')
    if os.path.exists(LIGAS_PATH):
        with open(LIGAS_PATH, 'r', encoding='utf-8') as f:
            ligas_whitelisted = [l.strip() for l in f.read().split(',') if l.strip()]
        if league not in ligas_whitelisted:
            return False, "LIGA_BLOQUEADA"
            
    return True, "APROVADO"

# 3. AUDITORIA: Lógica de gravação no paper_trading_log.csv
def log_paper_trade(match_state):
    """
    Grava os logs operacionais do método em Quarentena no arquivo paper_trading_log.csv,
    incluindo os valores numéricos exatos das features preditivas no gatilho.
    """
    log_file = "paper_trading_log.csv"
    
    row = {
        'Date': str(match_state.get('Date', '')),
        'League': match_state.get('League', ''),
        'Home': match_state.get('Home', ''),
        'Away': match_state.get('Away', ''),
        'Odd_H_FT': match_state.get('Odd_H_FT', 0.0),
        'Odd_H_Lay': match_state.get('Odd_H_Lay', 0.0),
        'Prob_ML': match_state.get('Prob_ML', 0.0),
        'Decision': match_state.get('Decision', ''),
        'Reason': match_state.get('Reason', ''),
        'H_SOT_Momentum': match_state.get('H_SOT_Momentum', 0.0),
        'xG_Dominance_Away': match_state.get('xG_Dominance_Away', 0.0),
        'Odds_Asymmetry': match_state.get('Odds_Asymmetry', 0.0),
        'Timestamp': datetime.now().isoformat()
    }
    
    df_row = pd.DataFrame([row])
    
    if not os.path.exists(log_file):
        df_row.to_csv(log_file, index=False)
    else:
        df_row.to_csv(log_file, mode='a', header=False, index=False)
        
# 4. PREDICT INTERFACE (Live Pipeline)
def predict_and_evaluate_live(live_games_payload, df_historical):
    """
    Interface para integrar com o dashboard ou robô live.
    Carrega o modelo, normaliza, busca o histórico e avalia a aposta.
    """
    if not (os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH) and os.path.exists(FEATURES_PATH)):
        return []
        
    model = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)
    features = joblib.load(FEATURES_PATH)
    
    # Compute rolling averages on the historical database to fetch team states
    # This is equivalent to get_latest_rolling_averages
    df_hist = df_historical.copy()
    df_hist['Date'] = pd.to_datetime(df_hist['Date'], errors='coerce')
    df_hist = df_hist.dropna(subset=['Goals_H_FT', 'Goals_A_FT', 'Date', 'Home', 'Away']).copy()
    
    # Filtragem temporal preventiva para evitar lookahead bias e ignorar jogos não finalizados de hoje
    if live_games_payload:
        first_game_date = pd.to_datetime(live_games_payload[0].get('Date') or datetime.now().date()).date()
        df_hist = df_hist[df_hist['Date'].dt.date < first_game_date].copy()
        
    df_hist = df_hist.sort_values('Date').reset_index(drop=True)
    
    stats_cols = ['Total_Shots_H_FT', 'Total_Shots_A_FT', 'Shots_On_Target_H_FT', 'Shots_On_Target_A_FT', 'xG_H_FT', 'xG_A_FT']
    for c in stats_cols:
        df_hist[c] = pd.to_numeric(df_hist.get(c, np.nan), errors='coerce').fillna(0.0)

    df_h = df_hist[['Date', 'Home', 'Goals_H_FT', 'Total_Shots_H_FT', 'Shots_On_Target_H_FT', 'xG_H_FT']].copy()
    df_h.columns = ['Date', 'Team', 'Goals_Scored', 'Shots', 'Shots_On_Target', 'xG']
    
    df_a = df_hist[['Date', 'Away', 'Goals_A_FT', 'Total_Shots_A_FT', 'Shots_On_Target_A_FT', 'xG_A_FT']].copy()
    df_a.columns = ['Date', 'Team', 'Goals_Scored', 'Shots', 'Shots_On_Target', 'xG']

    df_teams = pd.concat([df_h, df_a], ignore_index=True)
    df_teams = df_teams.sort_values('Date').reset_index(drop=True)

    df_teams['Roll_Goals_5'] = df_teams.groupby('Team')['Goals_Scored'].transform(lambda x: x.rolling(5, min_periods=1).mean())
    df_teams['Roll_Shots_5'] = df_teams.groupby('Team')['Shots'].transform(lambda x: x.rolling(5, min_periods=1).mean())
    df_teams['Roll_SOT_3'] = df_teams.groupby('Team')['Shots_On_Target'].transform(lambda x: x.rolling(3, min_periods=1).mean())
    df_teams['Roll_SOT_10'] = df_teams.groupby('Team')['Shots_On_Target'].transform(lambda x: x.rolling(10, min_periods=1).mean())
    df_teams['Roll_xG_5'] = df_teams.groupby('Team')['xG'].transform(lambda x: x.rolling(5, min_periods=1).mean())

    latest_stats = df_teams.groupby('Team').last().reset_index()
    
    evaluated_games = []
    for g in live_games_payload:
        norm_g = normalize_live_data(g)
        
        home = norm_g['Home']
        away = norm_g['Away']
        
        # Merge rolling features from historical state
        stats_h = latest_stats[latest_stats['Team'] == home]
        stats_a = latest_stats[latest_stats['Team'] == away]
        
        if stats_h.empty or stats_a.empty:
            # Skip if teams have no history
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
        
        # Calculate derived features
        norm_g['H_SOT_Momentum'] = norm_g['H_Roll_SOT_3'] - norm_g['H_Roll_SOT_10']
        norm_g['xG_Dominance_Away'] = norm_g['A_Roll_xG_5'] - norm_g['H_Roll_xG_5']
        norm_g['Odds_Asymmetry'] = (1 / (norm_g['Odd_H_FT'] + 1e-10)) - (1 / (norm_g['Odd_A_FT'] + 1e-10))
        norm_g['SOT_Sum_5'] = norm_g['H_Roll_SOT_3'] + norm_g['A_Roll_SOT_3']
        norm_g['Ratio_HA'] = norm_g['Odd_H_FT'] / (norm_g['Odd_A_FT'] + 1e-10)
        
        # Build matrix row
        row_mat = pd.DataFrame([{col: norm_g.get(col, 0.0) for col in features}])
        row_mat = row_mat.fillna(0.0)
        prob_ml = model.predict_proba(scaler.transform(row_mat))[0, 1]
        
        norm_g['Prob_ML'] = prob_ml
        
        # Evaluate decision
        apostar, reason = check_entry_conditions(norm_g)
        norm_g['Decision'] = 'APOSTA' if apostar else 'SKIP'
        norm_g['Reason'] = reason
        
        # Audit logging (since it is in quarantine, always logs triggers)
        if apostar:
            log_paper_trade(norm_g)
            
        evaluated_games.append(norm_g)
        
    return evaluated_games
