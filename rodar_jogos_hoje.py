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

def fetch_today_games(target_date_str=None):
    if not target_date_str:
        target_date_str = datetime.now().strftime("%Y-%m-%d")
        
    print(f"[BUSCA] Buscando jogos para a data: {target_date_str}...", flush=True)
    
    # 1. Tentar carregar da API FutPythonTrader se token estiver presente
    try:
        from futpythontrader_client import get_jogos_do_dia
        df_api = get_jogos_do_dia(source="betfair", date_str=target_date_str)
        if df_api is not None and not df_api.empty:
            print(f"[API] {len(df_api)} jogos baixados da API Betfair em tempo real!", flush=True)
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

    # 3. Fallback para base FRESH local
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
            print(f"[BASE LOCAL] {len(sub)} jogos filtrados da base local para {target_date_str}!", flush=True)
            return sub, target_date_str
        else:
            print(f"[INFO] Nenhum jogo na data exata {target_date_str}. Carregando jogos recentes...", flush=True)
            recent = df_fresh[df_fresh['Date'] >= '2026-08-01'].copy()
            return recent, "2026-08-11"

    print("[ERRO] Nenhuma base de dados encontrada.", flush=True)
    return pd.DataFrame(), target_date_str

def process_today_signals(df_games, date_str):
    signals = []
    
    for idx, row in df_games.iterrows():
        league = str(row.get('League') or row.get('Liga') or 'Geral')
        home = str(row.get('Home_Team') or row.get('Home') or row.get('Mandante') or 'Mandante')
        away = str(row.get('Away_Team') or row.get('Away') or row.get('Visitante') or 'Visitante')
        match_name = f"{home} x {away}"
        
        # Odds
        odd_h_lay = float(row.get('Odd_H_Lay', 0.0) or 0.0)
        odd_d_lay = float(row.get('Odd_D_Lay', 0.0) or 0.0)
        odd_a_lay = float(row.get('Odd_A_Lay', 0.0) or 0.0)
        odd_o25_back = float(row.get('Odd_Over25_FT_Back', 0.0) or 0.0)
        odd_btts_lay = float(row.get('Odd_BTTS_Yes_Lay', 0.0) or 0.0)
        odd_cs00_lay = float(row.get('Odd_CS_0x0_Lay', 0.0) or 0.0)
        
        # Resultados se já finalizado
        gh = row.get('Goals_H_FT')
        ga = row.get('Goals_A_FT')
        is_finished = (gh is not None and ga is not None and not pd.isna(gh) and not pd.isna(ga) and gh >= 0 and ga >= 0)
        
        is_draw = (gh == ga) if is_finished else None
        is_away_win = (ga > gh) if is_finished else None
        is_over25 = ((gh + ga) > 2.5) if is_finished else None
        is_btts = (gh > 0 and ga > 0) if is_finished else None
        is_0x0 = (gh == 0 and ga == 0) if is_finished else None
        
        # 1. LAY 0x0 PROTEGIDO (Odd Lay <= 12.0)
        if 8.0 <= odd_cs00_lay <= 12.0:
            status = 'Finalizado' if is_finished else 'Pendente'
            res = ('GREEN' if not is_0x0 else 'RED') if is_finished else 'Pendente'
            pnl = (0.95 if not is_0x0 else -(odd_cs00_lay - 1.0)) if is_finished else 0.0
            signals.append({
                'data': date_str, 'liga': league, 'jogo': match_name,
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
                'data': date_str, 'liga': league, 'jogo': match_name,
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
                'data': date_str, 'liga': league, 'jogo': match_name,
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
                'data': date_str, 'liga': league, 'jogo': match_name,
                'metodo': 'BTTS Lay Quant', 'mercado': 'BTTS_Y', 'lado': 'lay',
                'odd_execucao': odd_btts_lay, 'stake': 100.0,
                'status': status, 'resultado': res,
                'pnl_unidades': pnl, 'pnl_dolar': pnl * 100.0
            })

    return pd.DataFrame(signals)

def main():
    parser = argparse.ArgumentParser(description="Varredura de Jogos de Hoje para Paper Trading")
    parser.add_argument("--data", default=None, help="Data YYYY-MM-DD")
    args = parser.parse_args()

    df_games, date_str = fetch_today_games(args.data)
    if df_games.empty:
        print("[AVISO] Nenhum jogo encontrado para processar.", flush=True)
        return

    df_today = process_today_signals(df_games, date_str)
    
    print("\n=======================================================", flush=True)
    print(f" PALPITES DE PAPER TRADING — JOGOS DE {date_str}", flush=True)
    print("=======================================================", flush=True)
    if df_today.empty:
        print("Nenhum palpite gerado que atenda aos criterios de odds e liquidez hoje.", flush=True)
    else:
        print(df_today[['liga', 'jogo', 'metodo', 'lado', 'odd_execucao', 'status', 'resultado']].to_string(), flush=True)
        
        # Atualizar planilha acumulada de Paper Trading
        if FORWARD_LOG_PATH.exists():
            df_hist = pd.read_csv(FORWARD_LOG_PATH)
            df_combined = pd.concat([df_hist, df_today], ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=['data', 'jogo', 'metodo'], keep='last')
        else:
            df_combined = df_today
            
        # Garantir preenchimento de pnl_unidades
        df_combined['pnl_unidades'] = df_combined['pnl_unidades'].fillna(df_combined['pnl_dolar'] / 100.0)
        
        df_combined.to_csv(FORWARD_LOG_PATH, index=False)
        try:
            df_combined.to_excel(FORWARD_LOG_EXCEL, index=False)
        except Exception:
            pass
            
        print(f"\n[SUCESSO] Planilha de Paper Trading atualizada! ({len(df_today)} palpites hoje, {len(df_combined)} no total)", flush=True)

if __name__ == "__main__":
    main()
