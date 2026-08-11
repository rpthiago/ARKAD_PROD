"""
gerar_sinais_forward_diario.py — Gerador Diário de Sinais de Forward Paper Trading (Ago - Set 2026)

Executa diariamente a varredura nos jogos do dia com as odds reais da Betfair Exchange,
gerando as entradas de Paper Trading para acompanhamento em tempo real até o final de Setembro.
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
        # Se não especificado, pega jogos a partir de Agosto de 2026 em diante (Campanha Ago-Set 2026)
        df = df[df['Date'] >= '2026-08-01'].copy()
        
    return df

def generate_forward_signals(df_games):
    """Gera entradas de Paper Trading para os métodos selecionados com travas estritas."""
    signals = []
    
    for idx, row in df_games.iterrows():
        game_date = row['Date'].strftime('%Y-%m-%d')
        league = row.get('League') or row.get('Liga') or 'Desconhecida'
        home_team = row.get('Home_Team') or row.get('Home') or row.get('Mandante') or 'Home'
        away_team = row.get('Away_Team') or row.get('Away') or row.get('Visitante') or 'Away'
        match_name = f"{home_team} x {away_team}"
        
        # Odds Executáveis Betfair Exchange
        odd_h_back = float(row.get('Odd_H_Back', 0.0) or 0.0)
        odd_h_lay = float(row.get('Odd_H_Lay', 0.0) or 0.0)
        
        odd_d_back = float(row.get('Odd_D_Back', 0.0) or 0.0)
        odd_d_lay = float(row.get('Odd_D_Lay', 0.0) or 0.0)
        
        odd_a_back = float(row.get('Odd_A_Back', 0.0) or 0.0)
        odd_a_lay = float(row.get('Odd_A_Lay', 0.0) or 0.0)
        
        odd_o25_back = float(row.get('Odd_Over25_FT_Back', 0.0) or 0.0)
        odd_o25_lay = float(row.get('Odd_Over25_FT_Lay', 0.0) or 0.0)
        
        odd_btts_back = float(row.get('Odd_BTTS_Yes_Back', 0.0) or 0.0)
        odd_btts_lay = float(row.get('Odd_BTTS_Yes_Lay', 0.0) or 0.0)
        
        odd_cs00_lay = float(row.get('Odd_CS_0x0_Lay', 0.0) or 0.0)
        odd_cs01_lay = float(row.get('Odd_CS_0x1_Lay', 0.0) or 0.0)
        
        # Sinais de Resultados Reais (se o jogo já tiver finalizado)
        is_home_win = row.get('is_home_win') if 'is_home_win' in row else (row.get('Goals_H_FT', -1) > row.get('Goals_A_FT', -1))
        is_draw = row.get('is_draw') if 'is_draw' in row else (row.get('Goals_H_FT', -1) == row.get('Goals_A_FT', -1) and row.get('Goals_H_FT', -1) >= 0)
        is_away_win = row.get('is_away_win') if 'is_away_win' in row else (row.get('Goals_A_FT', -1) > row.get('Goals_H_FT', -1))
        is_over25 = row.get('is_over25') if 'is_over25' in row else ((row.get('Goals_H_FT', 0) + row.get('Goals_A_FT', 0)) > 2.5)
        is_btts = row.get('is_btts') if 'is_btts' in row else (row.get('Goals_H_FT', 0) > 0 and row.get('Goals_A_FT', 0) > 0)
        is_0x0 = (row.get('Goals_H_FT', -1) == 0 and row.get('Goals_A_FT', -1) == 0)
        is_0x1 = (row.get('Goals_H_FT', -1) == 0 and row.get('Goals_A_FT', -1) == 1)
        
        # -------------------------------------------------------------
        # MÉTODO 1: LAY 0x0 PROTEGIDO (Odd Lay <= 12.0)
        # -------------------------------------------------------------
        if 8.0 <= odd_cs00_lay <= 12.0:
            pnl = 0.95 if not is_0x0 else -(odd_cs00_lay - 1.0)
            signals.append({
                'data': game_date, 'liga': league, 'jogo': match_name,
                'metodo': 'Lay 0x0 Protegido', 'mercado': 'CS_0x0', 'lado': 'lay',
                'odd_execucao': odd_cs00_lay, 'stake': 100.0,
                'status': 'Finalizado' if is_home_win is not None else 'Pendente',
                'resultado': 'GREEN' if (not is_0x0) else 'RED',
                'pnl_unidades': pnl, 'pnl_dolar': pnl * 100.0
            })
            
        # -------------------------------------------------------------
        # MÉTODO 2: LAY DRAW ESTRUTURAL (Odd Lay <= 4.5)
        # -------------------------------------------------------------
        if 3.30 <= odd_d_lay <= 4.50:
            pnl = 0.95 if not is_draw else -(odd_d_lay - 1.0)
            signals.append({
                'data': game_date, 'liga': league, 'jogo': match_name,
                'metodo': 'Lay Draw Estrutural', 'mercado': '1X2_D', 'lado': 'lay',
                'odd_execucao': odd_d_lay, 'stake': 100.0,
                'status': 'Finalizado' if is_draw is not None else 'Pendente',
                'resultado': 'GREEN' if (not is_draw) else 'RED',
                'pnl_unidades': pnl, 'pnl_dolar': pnl * 100.0
            })
            
        # -------------------------------------------------------------
        # MÉTODO 3: BTTS YES LAY (Discrepância Odd 2.20 - 3.20)
        # -------------------------------------------------------------
        if 2.20 <= odd_btts_lay <= 3.20:
            pnl = 0.95 if not is_btts else -(odd_btts_lay - 1.0)
            signals.append({
                'data': game_date, 'liga': league, 'jogo': match_name,
                'metodo': 'BTTS Lay Quant', 'mercado': 'BTTS_Y', 'lado': 'lay',
                'odd_execucao': odd_btts_lay, 'stake': 100.0,
                'status': 'Finalizado' if is_btts is not None else 'Pendente',
                'resultado': 'GREEN' if (not is_btts) else 'RED',
                'pnl_unidades': pnl, 'pnl_dolar': pnl * 100.0
            })
            
        # -------------------------------------------------------------
        # MÉTODO 4: OVER 2.5 BACK VALOR (Odd Back 1.80 - 2.60)
        # -------------------------------------------------------------
        if 1.80 <= odd_o25_back <= 2.60:
            pnl = (odd_o25_back - 1.0) if is_over25 else -1.0
            signals.append({
                'data': game_date, 'liga': league, 'jogo': match_name,
                'metodo': 'Over 2.5 Back Valor', 'mercado': 'O25', 'lado': 'back',
                'odd_execucao': odd_o25_back, 'stake': 100.0,
                'status': 'Finalizado' if is_over25 is not None else 'Pendente',
                'resultado': 'GREEN' if is_over25 else 'RED',
                'pnl_unidades': pnl, 'pnl_dolar': pnl * 100.0
            })
            
        # -------------------------------------------------------------
        # MÉTODO 5: LAY ZEBRA HOME/AWAY (Odd Lay <= 5.0)
        # -------------------------------------------------------------
        if 3.50 <= odd_a_lay <= 5.00:
            pnl = 0.95 if not is_away_win else -(odd_a_lay - 1.0)
            signals.append({
                'data': game_date, 'liga': league, 'jogo': match_name,
                'metodo': 'Lay Zebra Visitante', 'mercado': '1X2_A', 'lado': 'lay',
                'odd_execucao': odd_a_lay, 'stake': 100.0,
                'status': 'Finalizado' if is_away_win is not None else 'Pendente',
                'resultado': 'GREEN' if (not is_away_win) else 'RED',
                'pnl_unidades': pnl, 'pnl_dolar': pnl * 100.0
            })

    df_signals = pd.DataFrame(signals)
    return df_signals

def run_forward_campaign():
    print("=== INICIANDO GERAÇÃO DE SINAIS FORWARD PAPER TRADING (AGOSTO - SETEMBRO 2026) ===")
    df_games = load_upcoming_games()
    print(f"Total de jogos carregados no período: {len(df_games)}")
    
    df_signals = generate_forward_signals(df_games)
    print(f"Total de palpites/sinais de Paper Trading gerados: {len(df_signals)}")
    
    if len(df_signals) > 0:
        df_signals.to_csv(FORWARD_LOG_PATH, index=False)
        df_signals.to_excel(FORWARD_LOG_EXCEL, index=False)
        print(f"Arquivo CSV salvo em: {FORWARD_LOG_PATH}")
        print(f"Arquivo Excel salvo em: {FORWARD_LOG_EXCEL}")
        
        # Resumo por Método
        print("\n--- RESUMO DE DESEMPENHO DO FORWARD TEST POR MÉTODO ---")
        summary = df_signals.groupby('metodo').agg(
            apostas=('pnl_dolar', 'count'),
            lucro_total=('pnl_dolar', 'sum'),
            win_rate=('resultado', lambda x: (x == 'GREEN').mean() * 100.0)
        )
        summary['roi%'] = (summary['lucro_total'] / (summary['apostas'] * 100.0)) * 100.0
        print(summary.to_string())

if __name__ == "__main__":
    run_forward_campaign()
