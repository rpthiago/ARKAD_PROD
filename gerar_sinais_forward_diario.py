"""
gerar_sinais_forward_diario.py — Gerador Diário de Sinais de Forward Paper Trading (ARKAD)

Regras Estritas GEMINI.md:
1. Somente métodos ativos/aprovados para observação: Lay Under 1.5 FT (XGBoost).
2. Sem re-injeção de métodos mortos (Lay 0x0, Lay 0x3, Lay 2x2, Lay Draw, etc.).
3. Stake Zero genuína: stake = 0.0 e tipo_registro = 'OBSERVACAO_STAKE_ZERO'.
4. Desacoplamento Pré vs Pós: Esta rotina SÓ gera sinais pré-jogo com status PENDENTE.
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FORWARD_LOG_PATH = ROOT / "paper_trading_forward_setembro_2026.csv"
FORWARD_LOG_EXCEL = ROOT / "paper_trading_forward_setembro_2026.xlsx"

def load_upcoming_games(target_date_str=None):
    """Carrega jogos do feed Betfair FRESH ou pasta Apostas_Diarias."""
    fresh_path = ROOT / "Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH.csv"
    if not fresh_path.exists():
        fresh_path = ROOT / "scratch" / "dataset_leak_free_features.parquet"
        
    if str(fresh_path).endswith('.csv'):
        df = pd.read_csv(fresh_path)
    else:
        df = pd.read_parquet(fresh_path)
        
    df['Date'] = pd.to_datetime(df['Date'])
    
    if target_date_str:
        tgt_dt = pd.to_datetime(target_date_str).date()
        df = df[df['Date'].dt.date == tgt_dt].copy()
    else:
        # Se nenhuma data for passada, pega a data de hoje
        today_dt = datetime.now().date()
        df = df[df['Date'].dt.date == today_dt].copy()
        
    return df

def _extract_horario(row_obj):
    for key in ['horario', 'Horario', 'Hora', 'Time', 'time', 'Horario_Entrada']:
        val = row_obj.get(key) if hasattr(row_obj, 'get') else getattr(row_obj, key, None)
        if val is not None and pd.notna(val):
            val_str = str(val).strip()
            if val_str and val_str.lower() not in ('nan', 'none', 'null', ''):
                return val_str[:5]
    return ''

def generate_forward_signals(df_games):
    """Gera entradas de Paper Trading estritamente com status PENDENTE e stake zero."""
    signals = []
    if df_games.empty:
        return pd.DataFrame(signals)
        
    from estrategia_lay_under15 import avaliar_jogo_lay_under15
    
    for idx, row in df_games.iterrows():
        game_date = row['Date'].strftime('%Y-%m-%d')
        league = str(row.get('League') or row.get('Liga') or 'Desconhecida')
        home_team = str(row.get('Home_Team') or row.get('Home') or row.get('Mandante') or 'Home')
        away_team = str(row.get('Away_Team') or row.get('Away') or row.get('Visitante') or 'Away')
        match_name = f"{home_team} x {away_team}"
        game_time = _extract_horario(row)
        
        # 1. LAY UNDER 1.5 FT (XGBoost EV >= 5%) [Oficial ARKAD Forward]
        try:
            eval_u15 = avaliar_jogo_lay_under15(row.to_dict(), ev_threshold=0.05)
            if eval_u15.get('aplica'):
                odd_u15_lay = eval_u15['odd_lay']
                signals.append({
                    'data': game_date,
                    'horario': game_time,
                    'liga': league,
                    'jogo': match_name,
                    'metodo': 'Lay Under 1.5 FT (XGBoost)',
                    'mercado': 'Under15_FT',
                    'lado': 'lay',
                    'odd_execucao': odd_u15_lay,
                    'prob_estimada': round(eval_u15['prob_estimada'], 4),
                    'ev': round(eval_u15['ev'], 4),
                    'stake_planejada': 0.0, # Observação Stake-Zero
                    'tipo_registro': 'OBSERVACAO_STAKE_ZERO',
                    'status': 'PENDENTE',
                    'resultado': None,
                    'pnl_unidades': 0.0,
                    'pnl_dolar': 0.0
                })
        except Exception:
            pass

    return pd.DataFrame(signals)

def run_forward_campaign():
    print("=== GERAÇÃO DE SINAIS PAPER TRADING (OBSERVAÇÃO STAKE-ZERO: LAY UNDER 1.5 FT) ===", flush=True)
    df_games = load_upcoming_games()
    df_signals = generate_forward_signals(df_games)
    
    if len(df_signals) > 0:
        if FORWARD_LOG_PATH.exists():
            df_hist = pd.read_csv(FORWARD_LOG_PATH)
            # Evita duplicatas pelo par (data, jogo, metodo)
            df_combined = pd.concat([df_hist, df_signals], ignore_index=True).drop_duplicates(subset=['data', 'jogo', 'metodo'], keep='last')
        else:
            df_combined = df_signals
            
        df_combined.to_csv(FORWARD_LOG_PATH, index=False)
        try:
            df_combined.to_excel(FORWARD_LOG_EXCEL, index=False)
        except Exception:
            pass
        print(f"[OK] {len(df_signals)} sinais gerados com sucesso e gravados como PENDENTE / Stake-Zero!", flush=True)
    else:
        print("[INFO] Nenhum jogo atendeu aos critérios de EV >= 5% para a data de hoje.", flush=True)

if __name__ == "__main__":
    run_forward_campaign()
