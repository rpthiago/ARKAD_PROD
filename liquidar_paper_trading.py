"""
liquidar_paper_trading.py — Rotina de Liquidação Pós-Jogo do Paper Trading (ARKAD)

Regras GEMINI.md:
1. Executada separadamente da geração de sinais (pós-apito final).
2. Busca os registros com status 'PENDENTE' no log de paper trading.
3. Cruza com os placares reais e calcula o P&L real da Betfair (comissão 4.5% / 5%).
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FORWARD_LOG_PATH = ROOT / "paper_trading_forward_setembro_2026.csv"
FORWARD_LOG_EXCEL = ROOT / "paper_trading_forward_setembro_2026.xlsx"

COMMISSION = 0.045

def liquidar():
    if not FORWARD_LOG_PATH.exists():
        print("[INFO] Nenhum log de paper trading encontrado para liquidar.", flush=True)
        return

    df_log = pd.read_csv(FORWARD_LOG_PATH)
    pendentes_mask = df_log['status'] == 'PENDENTE'
    
    if pendentes_mask.sum() == 0:
        print("[INFO] Não há apostas pendentes no log de paper trading.", flush=True)
        return

    print(f"[*] Encontradas {pendentes_mask.sum()} apostas pendentes. Buscando resultados reais...", flush=True)

    # Carregar base de resultados oficiais
    fresh_path = ROOT / "Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH.csv"
    if not fresh_path.exists():
        fresh_path = ROOT / "scratch" / "dataset_leak_free_features.parquet"
        
    if str(fresh_path).endswith('.csv'):
        df_results = pd.read_csv(fresh_path)
    else:
        df_results = pd.read_parquet(fresh_path)
        
    df_results['Date_str'] = pd.to_datetime(df_results['Date']).dt.strftime('%Y-%m-%d')
    
    import unicodedata, re
    def _canon(s):
        if pd.isna(s) or not s: return ""
        s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
        return re.sub(r"[^a-z0-9]", "", s)

    home_col = 'Home' if 'Home' in df_results.columns else 'Home_Team'
    away_col = 'Away' if 'Away' in df_results.columns else 'Away_Team'
    
    df_results['c_home'] = df_results[home_col].map(_canon)
    df_results['c_away'] = df_results[away_col].map(_canon)
    
    # Dicionário de resultados por chave (data, home, away)
    results_map = {}
    for _, r in df_results.iterrows():
        gh = r.get('Goals_H_FT')
        ga = r.get('Goals_A_FT')
        if gh is not None and ga is not None and not pd.isna(gh) and not pd.isna(ga):
            k = (r['Date_str'], r['c_home'], r['c_away'])
            results_map[k] = (int(gh), int(ga))

    liquidadas_count = 0
    for idx in df_log[pendentes_mask].index:
        d = str(df_log.loc[idx, 'data'])
        jogo = str(df_log.loc[idx, 'jogo'])
        
        # Extrair mandante e visitante
        if ' x ' in jogo:
            h_raw, a_raw = jogo.split(' x ', 1)
        else:
            continue
            
        k = (d, _canon(h_raw), _canon(a_raw))
        if k in results_map:
            gh, ga = results_map[k]
            tot_goals = gh + ga
            odd_lay = float(df_log.loc[idx, 'odd_execucao'])
            metodo = str(df_log.loc[idx, 'metodo'])
            
            # Liquidação específica para Lay Under 1.5 FT
            if 'Lay Under 1.5' in metodo:
                is_u15 = (tot_goals <= 1)
                is_green = (not is_u15) # Ganha se saírem >= 2 gols
                
                res = 'GREEN' if is_green else 'RED'
                pnl = (1.0 - COMMISSION) if is_green else -(odd_lay - 1.0)
                
                df_log.loc[idx, 'status'] = 'FINALIZADO'
                df_log.loc[idx, 'resultado'] = res
                df_log.loc[idx, 'pnl_unidades'] = round(pnl, 4)
                df_log.loc[idx, 'placar'] = f"{gh}x{ga}"
                liquidadas_count += 1

    df_log.to_csv(FORWARD_LOG_PATH, index=False)
    try:
        df_log.to_excel(FORWARD_LOG_EXCEL, index=False)
    except Exception:
        pass
        
    print(f"[OK] {liquidadas_count} apostas liquidadas com sucesso!", flush=True)

if __name__ == "__main__":
    liquidar()
