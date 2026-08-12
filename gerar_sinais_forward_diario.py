"""
gerar_sinais_forward_diario.py — Gerador Diário de Sinais de Forward Paper Trading (Ago - Set 2026)

Executa diariamente a varredura nos jogos do dia com as odds reais da Betfair Exchange,
gerando as entradas de Paper Trading em estrita sincronia com o Backtest Master.
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, date
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FORWARD_LOG_PATH = ROOT / "paper_trading_forward_setembro_2026.csv"
FORWARD_LOG_EXCEL = ROOT / "paper_trading_forward_setembro_2026.xlsx"

def load_upcoming_games(target_date_str=None):
    """Carrega jogos do dia do feed Betfair FRESH ou pasta Apostas_Diarias."""
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
        df = df[df['Date'] >= '2026-08-01'].copy()
        
    return df

def generate_forward_signals(df_games):
    """Gera entradas de Paper Trading para os métodos alinhados com o Backtest Master."""
    signals = []
    
    for idx, row in df_games.iterrows():
        game_date = row['Date'].strftime('%Y-%m-%d')
        league = str(row.get('League') or row.get('Liga') or 'Desconhecida')
        home_team = str(row.get('Home_Team') or row.get('Home') or row.get('Mandante') or 'Home')
        away_team = str(row.get('Away_Team') or row.get('Away') or row.get('Visitante') or 'Away')
        match_name = f"{home_team} x {away_team}"
        
        # Odds Executáveis Betfair Exchange
        odd_d_lay = float(row.get('Odd_D_Lay', 0.0) or 0.0)
        odd_o25_back = float(row.get('Odd_Over25_FT_Back', 0.0) or 0.0)
        odd_under25_back = float(row.get('Odd_Under25_FT_Back', 0.0) or 0.0)
        odd_btts_lay = float(row.get('Odd_BTTS_Yes_Lay', 0.0) or 0.0)
        odd_cs00_lay = float(row.get('Odd_CS_0x0_Lay', 0.0) or 0.0)
        odd_cs03_lay = float(row.get('Odd_CS_0x3_Lay', 0.0) or 0.0)
        xg_a_r5 = float(row.get('A_xGF_r5', row.get('Media_Gols_Pro_Visitante', 1.0)) or 1.0)
        
        # Sinais de Resultados Reais (se o jogo já tiver finalizado)
        gh = row.get('Goals_H_FT')
        ga = row.get('Goals_A_FT')
        is_finished = (gh is not None and ga is not None and not pd.isna(gh) and not pd.isna(ga) and gh >= 0 and ga >= 0)
        
        is_draw = (gh == ga) if is_finished else None
        is_over25 = ((gh + ga) > 2.5) if is_finished else None
        is_btts = (gh > 0 and ga > 0) if is_finished else None
        is_0x0 = (gh == 0 and ga == 0) if is_finished else None
        is_0x3 = (gh == 0 and ga == 3) if is_finished else None
        
        # 1. LAY 0x0 PROTEGIDO (Odd Lay <= 12.0)
        if 8.0 <= odd_cs00_lay <= 12.0:
            status = 'Finalizado' if is_finished else 'Pendente'
            res = ('GREEN' if not is_0x0 else 'RED') if is_finished else 'Pendente'
            pnl = (0.95 if not is_0x0 else -(odd_cs00_lay - 1.0)) if is_finished else 0.0
            signals.append({
                'data': game_date, 'liga': league, 'jogo': match_name,
                'metodo': 'Lay 0x0 Protegido', 'mercado': 'CS_0x0', 'lado': 'lay',
                'odd_execucao': odd_cs00_lay, 'stake': 100.0,
                'status': status, 'resultado': res,
                'pnl_unidades': pnl, 'pnl_dolar': pnl * 100.0
            })
            
        # 2. LAY DRAW ESTRUTURAL (Odd Lay <= 4.50)
        if 3.30 <= odd_d_lay <= 4.50:
            status = 'Finalizado' if is_finished else 'Pendente'
            res = ('GREEN' if not is_draw else 'RED') if is_finished else 'Pendente'
            pnl = (0.95 if not is_draw else -(odd_d_lay - 1.0)) if is_finished else 0.0
            signals.append({
                'data': game_date, 'liga': league, 'jogo': match_name,
                'metodo': 'Lay Draw Estrutural', 'mercado': '1X2_D', 'lado': 'lay',
                'odd_execucao': odd_d_lay, 'stake': 100.0,
                'status': status, 'resultado': res,
                'pnl_unidades': pnl, 'pnl_dolar': pnl * 100.0
            })
            
        # 3. OVER 2.5 BACK VALOR (Odd Back 1.80 - 2.60)
        if 1.80 <= odd_o25_back <= 2.60:
            status = 'Finalizado' if is_finished else 'Pendente'
            res = ('GREEN' if is_over25 else 'RED') if is_finished else 'Pendente'
            pnl = ((odd_o25_back - 1.0) if is_over25 else -1.0) if is_finished else 0.0
            signals.append({
                'data': game_date, 'liga': league, 'jogo': match_name,
                'metodo': 'Over 2.5 Back Valor', 'mercado': 'O25', 'lado': 'back',
                'odd_execucao': odd_o25_back, 'stake': 100.0,
                'status': status, 'resultado': res,
                'pnl_unidades': pnl, 'pnl_dolar': pnl * 100.0
            })
            
        # 4. BTTS LAY QUANT (Odd Lay 2.20 - 3.20)
        if 2.20 <= odd_btts_lay <= 3.20:
            status = 'Finalizado' if is_finished else 'Pendente'
            res = ('GREEN' if not is_btts else 'RED') if is_finished else 'Pendente'
            pnl = (0.95 if not is_btts else -(odd_btts_lay - 1.0)) if is_finished else 0.0
            signals.append({
                'data': game_date, 'liga': league, 'jogo': match_name,
                'metodo': 'BTTS Lay Quant', 'mercado': 'BTTS_Y', 'lado': 'lay',
                'odd_execucao': odd_btts_lay, 'stake': 100.0,
                'status': status, 'resultado': res,
                'pnl_unidades': pnl, 'pnl_dolar': pnl * 100.0
            })

        # 5. LAY 0x3 VISITANTE UNDER 2.5 + xG PROTECTED (Odd Under <= 1.85, Odd Lay 15-35, xG Visitante <= 1.10) [ROI +23.34%]
        if (0.0 < odd_under25_back <= 1.85) and (15.0 <= odd_cs03_lay <= 35.0) and (xg_a_r5 <= 1.10):
            status = 'Finalizado' if is_finished else 'Pendente'
            res = ('GREEN' if not is_0x3 else 'RED') if is_finished else 'Pendente'
            pnl = (0.95 if not is_0x3 else -(odd_cs03_lay - 1.0)) if is_finished else 0.0
            signals.append({
                'data': game_date, 'liga': league, 'jogo': match_name,
                'metodo': 'Lay 0x3 Visitante Under 2.5 (xG Protected)', 'mercado': 'CS_0x3', 'lado': 'lay',
                'odd_execucao': odd_cs03_lay, 'stake': 100.0,
                'status': status, 'resultado': res,
                'pnl_unidades': pnl, 'pnl_dolar': pnl * 100.0
            })

    df_signals = pd.DataFrame(signals)
    return df_signals

def run_forward_campaign():
    print("=== GERAÇÃO DE SINAIS PAPER TRADING (LAY 0x3 xG PROTECTED ROI +23.34%) ===", flush=True)
    df_games = load_upcoming_games()
    df_signals = generate_forward_signals(df_games)
    
    if len(df_signals) > 0:
        df_signals.to_csv(FORWARD_LOG_PATH, index=False)
        try:
            df_signals.to_excel(FORWARD_LOG_EXCEL, index=False)
        except Exception:
            pass
        print(f"[OK] {len(df_signals)} palpites gerados e alinhados com filtro de Proteção de Banca xG!", flush=True)

if __name__ == "__main__":
    run_forward_campaign()
