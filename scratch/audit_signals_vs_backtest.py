"""
audit_signals_vs_backtest.py — Auditoria de Consistência: Sinais do Dia vs Backtest Master

Compara se os jogos e entradas gerados pelo motor de Sinais Diários (rodar_jogos_hoje.py / gerar_sinais_forward_diario.py)
são EXATAMENTE idênticos aos selecionados pelo Harness de Backtest para as mesmas datas.
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

def run_audit():
    print("=========================================================================", flush=True)
    print(" [AUDITORIA] CONSISTENCIA DE REGRAS: SINAIS DIARIOS VS BACKTEST MASTER", flush=True)
    print("=========================================================================\n", flush=True)
    
    # 1. Carregar os Sinais Gerados pelo Motor Diário (forward testing sheet)
    forward_csv = ROOT / "paper_trading_forward_setembro_2026.csv"
    if not forward_csv.exists():
        print(f"[ERRO] Arquivo de sinais {forward_csv.name} nao encontrado.")
        return
        
    df_signals = pd.read_csv(forward_csv)
    print(f"[OK] Sinais do Motor Diario carregados: {len(df_signals)} entradas no historico acumulado.", flush=True)
    
    # 2. Carregar a Base de Dados de Backtest (FRESH)
    fresh_csv = ROOT / "Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH.csv"
    if not fresh_csv.exists():
        fresh_csv = ROOT / "scratch" / "dataset_leak_free_features.parquet"
        
    if str(fresh_csv).endswith('.csv'):
        df_fresh = pd.read_csv(fresh_csv)
    else:
        df_fresh = pd.read_parquet(fresh_csv)
        
    df_fresh['Date'] = pd.to_datetime(df_fresh['Date']).dt.strftime('%Y-%m-%d')
    print(f"[OK] Base de Backtest carregada: {len(df_fresh)} partidas registradas de {df_fresh['Date'].min()} ate {df_fresh['Date'].max()}.\n", flush=True)
    
    # 3. Filtrar as datas comuns de teste (01/08/2026 a 06/08/2026)
    datas_teste = sorted(df_signals['data'].unique())
    print(f"[DATAS ANALISADAS]: {datas_teste}\n", flush=True)
    
    tot_sinais_auditados = 0
    tot_matches_identicos = 0
    
    for date_str in datas_teste:
        signals_date = df_signals[df_signals['data'] == date_str].copy()
        fresh_date = df_fresh[df_fresh['Date'] == date_str].copy()
        
        print(f"--- DATA: {date_str} ---", flush=True)
        print(f"  - Partidas na Base de Backtest: {len(fresh_date)}", flush=True)
        print(f"  - Entradas geradas pelo Motor de Sinais: {len(signals_date)}", flush=True)
        
        # Recriar as regras estritas do Backtest para a data
        backtest_picks = []
        for _, row in fresh_date.iterrows():
            home = str(row.get('Home_Team') or row.get('Home') or 'Home')
            away = str(row.get('Away_Team') or row.get('Away') or 'Away')
            match_name = f"{home} x {away}"
            
            odd_cs00_lay = float(row.get('Odd_CS_0x0_Lay', 0.0) or 0.0)
            odd_d_lay = float(row.get('Odd_D_Lay', 0.0) or 0.0)
            odd_o25_back = float(row.get('Odd_Over25_FT_Back', 0.0) or 0.0)
            odd_btts_lay = float(row.get('Odd_BTTS_Yes_Lay', 0.0) or 0.0)
            
            # Regras idênticas do Backtest
            if 8.0 <= odd_cs00_lay <= 12.0:
                backtest_picks.append({'jogo': match_name, 'metodo': 'Lay 0x0 Protegido', 'odd': odd_cs00_lay})
            if 3.30 <= odd_d_lay <= 4.50:
                backtest_picks.append({'jogo': match_name, 'metodo': 'Lay Draw Estrutural', 'odd': odd_d_lay})
            if 1.80 <= odd_o25_back <= 2.60:
                backtest_picks.append({'jogo': match_name, 'metodo': 'Over 2.5 Back Valor', 'odd': odd_o25_back})
            if 2.20 <= odd_btts_lay <= 3.20:
                backtest_picks.append({'jogo': match_name, 'metodo': 'BTTS Lay Quant', 'odd': odd_btts_lay})
                
        df_bt = pd.DataFrame(backtest_picks)
        
        # Comparar conjunto de pares (jogo, metodo)
        if not df_bt.empty and not signals_date.empty:
            set_bt = set(zip(df_bt['jogo'], df_bt['metodo']))
            set_sig = set(zip(signals_date['jogo'], signals_date['metodo']))
            
            intersection = set_bt.intersection(set_sig)
            diff_bt_only = set_bt - set_sig
            diff_sig_only = set_sig - set_bt
            
            tot_sinais_auditados += len(set_sig)
            tot_matches_identicos += len(intersection)
            
            taxa_conc = (len(intersection) / len(set_sig) * 100.0) if len(set_sig) > 0 else 100.0
            print(f"  - Concordancia entre Sinais e Backtest: {len(intersection)} / {len(set_sig)} ({taxa_conc:.1f}%)", flush=True)
            
            if diff_bt_only or diff_sig_only:
                print(f"  [DISCREPANCIAS ENCONTRADAS PARA {date_str}]:", flush=True)
                if diff_sig_only:
                    print(f"    - Apenas nos Sinais ({len(diff_sig_only)}): {list(diff_sig_only)[:3]}", flush=True)
                if diff_bt_only:
                    print(f"    - Apenas no Backtest ({len(diff_bt_only)}): {list(diff_bt_only)[:3]}", flush=True)
        else:
            print(f"  - Sem entradas geradas em uma das fontes.", flush=True)
        print("", flush=True)

    print("=========================================================================", flush=True)
    print(" RESULTADO FINAL DA AUDITORIA DE CONSISTENCIA", flush=True)
    print("=========================================================================", flush=True)
    taxa_global = (tot_matches_identicos / tot_sinais_auditados * 100.0) if tot_sinais_auditados > 0 else 0.0
    print(f" Total de Sinais Auditados: {tot_sinais_auditados}", flush=True)
    print(f" Total de Sinais Identicos ao Backtest: {tot_matches_identicos}", flush=True)
    print(f" Taxa de Fidelidade do Motor de Sinais: {taxa_global:.2f}%", flush=True)
    
    if taxa_global == 100.0:
        print("\n [STATUS DA AUDITORIA]: APROVADO COM PASSE PERFEITO (100.00% DE FIDELIDADE CONVERSIONAL)", flush=True)
    else:
        print(f"\n [STATUS DA AUDITORIA]: DISCREPANCIA DE {(100.0 - taxa_global):.2f}% DETECTADA.", flush=True)

if __name__ == "__main__":
    run_audit()
