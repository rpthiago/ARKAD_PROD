"""
rodar_jogos_hoje.py — Varredura e Execução de Jogos de Hoje para Paper Trading (ARKAD)

Uso:
    python rodar_jogos_hoje.py
    python rodar_jogos_hoje.py --data 2026-08-11
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, date
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FORWARD_LOG_PATH = ROOT / "paper_trading_forward_setembro_2026.csv"
FORWARD_LOG_EXCEL = ROOT / "paper_trading_forward_setembro_2026.xlsx"

# Cache global de xG das equipes baseado no histórico
_TEAM_XG_CACHE = {}

def _get_team_xg_cache():
    global _TEAM_XG_CACHE
    if _TEAM_XG_CACHE:
        return _TEAM_XG_CACHE
        
    fresh_path = ROOT / "Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH.csv"
    if not fresh_path.exists():
        fresh_path = ROOT / "scratch" / "dataset_leak_free_features.parquet"
        
    if fresh_path.exists():
        try:
            if str(fresh_path).endswith('.csv'):
                df = pd.read_csv(fresh_path)
            else:
                df = pd.read_parquet(fresh_path)
                
            away_col = 'Away' if 'Away' in df.columns else 'Away_Team'
            xg_col = 'A_xGF_r5' if 'A_xGF_r5' in df.columns else ('Media_Gols_Pro_Visitante' if 'Media_Gols_Pro_Visitante' in df.columns else None)
            
            if xg_col and away_col in df.columns:
                df[xg_col] = pd.to_numeric(df[xg_col], errors='coerce')
                # Pegar a média recente por time visitante
                _TEAM_XG_CACHE = df.groupby(away_col)[xg_col].last().dropna().to_dict()
        except Exception:
            _TEAM_XG_CACHE = {}
            
    return _TEAM_XG_CACHE

def fetch_today_games(target_date_str=None):
    if not target_date_str:
        target_date_str = datetime.now().strftime("%Y-%m-%d")
        
    print(f"[BUSCA] Buscando jogos estritamente para a data: {target_date_str}...", flush=True)
    
    # 1. Tentar carregar da API FutPythonTrader se token estiver presente
    try:
        from futpythontrader_client import get_daily_dataframe
        df_api = get_daily_dataframe(source="betfair", date_str=target_date_str)
        if df_api is not None and not df_api.empty:
            print(f"[API] {len(df_api)} jogos baixados da API Betfair em tempo real para {target_date_str}!", flush=True)
            return df_api, target_date_str
    except Exception as e:
        print(f"[INFO] API em tempo real nao disponivel ({e}). Buscando bases locais...", flush=True)

    # 2. Tentar buscar da pasta Apostas_Diarias (planilha Apostas_YYYYMMDD.xlsx)
    date_clean = target_date_str.replace("-", "")
    xlsx_path = ROOT / "Apostas_Diarias" / f"Apostas_{date_clean}.xlsx"
    if xlsx_path.exists():
        df_xlsx = pd.read_excel(xlsx_path)
        print(f"[XLSX] {len(df_xlsx)} jogos carregados da planilha local {xlsx_path.name}!", flush=True)
        return df_xlsx, target_date_str

    # 3. Fallback para base FRESH local FILTRADA APENAS PELA DATA EXATA
    fresh_path = ROOT / "Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH.csv"
    if not fresh_path.exists():
        fresh_path = ROOT / "scratch" / "dataset_leak_free_features.parquet"
        
    if fresh_path.exists():
        if str(fresh_path).endswith('.csv'):
            df_fresh = pd.read_csv(fresh_path)
        else:
            df_fresh = pd.read_parquet(fresh_path)
            
        df_fresh['Date'] = pd.to_datetime(df_fresh['Date'])
        tgt_dt = pd.to_datetime(target_date_str).date()
        sub = df_fresh[df_fresh['Date'].dt.date == tgt_dt].copy()
        
        if not sub.empty:
            print(f"[BASE LOCAL] {len(sub)} jogos filtrados da base local para a data {target_date_str}!", flush=True)
            return sub, target_date_str

    print(f"[INFO] Nenhum jogo encontrado na base de dados para a data {target_date_str}.", flush=True)
    return pd.DataFrame(), target_date_str

def _extract_horario(row_obj):
    for key in ['horario', 'Horario', 'Hora', 'Time', 'time', 'Horario_Entrada']:
        val = row_obj.get(key) if hasattr(row_obj, 'get') else getattr(row_obj, key, None)
        if val is not None and pd.notna(val):
            val_str = str(val).strip()
            if val_str and val_str.lower() not in ('nan', 'none', 'null', ''):
                return val_str[:5]
    return ''

def process_today_signals(df_games, date_str):
    if df_games.empty:
        return pd.DataFrame()
        
    signals = []
    today_str = datetime.now().strftime("%Y-%m-%d")
    is_today_or_future = (date_str >= today_str)
    team_xg_cache = _get_team_xg_cache()
    
    for idx, row in df_games.iterrows():
        league = str(row.get('League') or row.get('Liga') or 'Geral')
        home = str(row.get('Home_Team') or row.get('Home') or row.get('Mandante') or 'Mandante')
        away = str(row.get('Away_Team') or row.get('Away') or row.get('Visitante') or 'Visitante')
        match_name = f"{home} x {away}"
        
        # Horario do jogo
        game_time = _extract_horario(row)
        
        # Odds
        odd_h_back = float(row.get('Odd_H_Back', 0.0) or row.get('Odd_H_FT', 0.0) or row.get('Odd_H', 0.0) or 0.0)
        odd_a_back = float(row.get('Odd_A_Back', 0.0) or row.get('Odd_A_FT', 0.0) or row.get('Odd_A', 0.0) or 0.0)
        odd_d_lay = float(row.get('Odd_D_Lay', 0.0) or 0.0)
        odd_o25_back = float(row.get('Odd_Over25_FT_Back', 0.0) or 0.0)
        odd_under25_back = float(row.get('Odd_Under25_FT_Back', 0.0) or 0.0)
        odd_btts_lay = float(row.get('Odd_BTTS_Yes_Lay', 0.0) or 0.0)
        odd_cs00_lay = float(row.get('Odd_CS_0x0_Lay', 0.0) or 0.0)
        odd_cs03_lay = float(row.get('Odd_CS_0x3_Lay', 0.0) or 0.0)
        
        # Resolução de xG do Visitante (Live API / Daily XLSX / Cache Histórico de Desempenho do Time)
        raw_xg = row.get('A_xGF_r5') or row.get('Media_Gols_Pro_Visitante') or row.get('xG_Visitante')
        if raw_xg is not None and not pd.isna(raw_xg):
            xg_a_r5 = float(raw_xg)
        elif away in team_xg_cache:
            xg_a_r5 = float(team_xg_cache[away])
        else:
            xg_a_r5 = 1.0  # Conservador
            
        # Resultados se já finalizado no passado
        gh = row.get('Goals_H_FT')
        ga = row.get('Goals_A_FT')
        is_finished = (not is_today_or_future) and (gh is not None and ga is not None and not pd.isna(gh) and not pd.isna(ga) and gh >= 0 and ga >= 0)
        
        is_draw = (gh == ga) if is_finished else None
        is_over25 = ((gh + ga) > 2.5) if is_finished else None
        is_btts = (gh > 0 and ga > 0) if is_finished else None
        is_0x0 = (gh == 0 and ga == 0) if is_finished else None
        is_0x3 = (gh == 0 and ga == 3) if is_finished else None
        
        # 1. LAY UNDER 1.5 FT (XGBoost EV >= 5%) [Oficial ARKAD Forward / Stake-Zero]
        try:
            from estrategia_lay_under15 import avaliar_jogo_lay_under15
            eval_u15 = avaliar_jogo_lay_under15(row.to_dict(), ev_threshold=0.05)
            if eval_u15.get('aplica'):
                odd_u15_lay = eval_u15['odd_lay']
                signals.append({
                    'data': date_str,
                    'horario': game_time,
                    'liga': league,
                    'jogo': match_name,
                    'metodo': 'Lay Under 1.5 FT (XGBoost)',
                    'mercado': 'Under15_FT',
                    'lado': 'lay',
                    'odd_execucao': odd_u15_lay,
                    'prob_estimada': round(eval_u15['prob_estimada'], 4),
                    'ev': round(eval_u15['ev'], 4),
                    'stake_planejada': 0.0,
                    'tipo_registro': 'OBSERVACAO_STAKE_ZERO',
                    'status': 'PENDENTE',
                    'resultado': None,
                    'pnl_unidades': 0.0,
                    'pnl_dolar': 0.0
                })
        except Exception:
            pass

    return pd.DataFrame(signals)

def main():
    parser = argparse.ArgumentParser(description="Varredura de Jogos de Hoje para Paper Trading")
    parser.add_argument("--data", default=None, help="Data YYYY-MM-DD")
    args = parser.parse_args()

    df_games, date_str = fetch_today_games(args.data)
    if df_games.empty:
        print(f"[INFO] Nenhum jogo encontrado para a data {date_str}.", flush=True)
        return

    df_today = process_today_signals(df_games, date_str)
    
    print("\n=======================================================", flush=True)
    print(f" PALPITES DE PAPER TRADING — JOGOS DE {date_str}", flush=True)
    print("=======================================================", flush=True)
    if df_today.empty:
        print(f"Nenhum palpite gerado que atenda aos criterios de odds e liquidez para {date_str}.", flush=True)
    else:
        print(df_today[['liga', 'jogo', 'metodo', 'lado', 'odd_execucao', 'status', 'resultado']].to_string(), flush=True)
        
        # Atualizar planilha acumulada de Paper Trading
        if FORWARD_LOG_PATH.exists():
            df_hist = pd.read_csv(FORWARD_LOG_PATH)
            df_hist = df_hist[df_hist['data'] != date_str].copy()
            df_combined = pd.concat([df_hist, df_today], ignore_index=True)
        else:
            df_combined = df_today
            
        df_combined['pnl_unidades'] = df_combined['pnl_unidades'].fillna(df_combined['pnl_dolar'] / 100.0)
        
        df_combined.to_csv(FORWARD_LOG_PATH, index=False)
        try:
            df_combined.to_excel(FORWARD_LOG_EXCEL, index=False)
        except Exception:
            pass
            
        print(f"\n[SUCESSO] Planilha de Paper Trading atualizada! ({len(df_today)} palpites hoje, {len(df_combined)} no total)", flush=True)

if __name__ == "__main__":
    main()
