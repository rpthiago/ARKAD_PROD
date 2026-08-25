"""
atualizar_feed_forward_diario.py — Pipeline de Enriquecimento Diário de Features (ARKAD Forward)

Objetivo:
1. Obter os jogos futuros/do dia (via API FutPythonTrader Betfair ou planilha Apostas_Diarias).
2. Calcular as probabilidades limpas de mercado (p_Over_clean, entropy, VARs) a partir das odds Betfair de hoje.
3. Acoplar as médias rolantes históricas leak-free dos times (H_xGF_r5, A_xGF_r5, etc.) e taxas da liga.
4. Salvar o feed diário pronto para o observador forward (observar_under15_forward.py).
"""

import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime, date
from pathlib import Path
import warnings
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)

ROOT = Path(__file__).resolve().parent
FEED_FORWARD_PATH = ROOT / "scratch" / "feed_forward_diario.parquet"
HIST_DATASET_PATH = ROOT / "scratch" / "dataset_leak_free_features.parquet"

def get_upcoming_fixtures(target_date_str=None):
    if not target_date_str:
        target_date_str = datetime.now().strftime("%Y-%m-%d")
        
    print(f"[*] Buscando fixtures para a data: {target_date_str}...", flush=True)
    
    # 1. Tentar API FutPythonTrader
    try:
        from futpythontrader_client import get_daily_dataframe
        df_api = get_daily_dataframe(source="betfair", date_str=target_date_str)
        if df_api is not None and not df_api.empty:
            print(f"[API] {len(df_api)} jogos obtidos da API Betfair para {target_date_str}!", flush=True)
            return df_api, target_date_str
    except Exception as e:
        print(f"[INFO] API Betfair indisponível ({e}). Tentando arquivos locais...", flush=True)

    # 2. Tentar pasta Apostas_Diarias
    date_clean = target_date_str.replace("-", "")
    xlsx_path = ROOT / "Apostas_Diarias" / f"Apostas_{date_clean}.xlsx"
    if xlsx_path.exists():
        df_xlsx = pd.read_excel(xlsx_path)
        print(f"[XLSX] {len(df_xlsx)} jogos carregados de {xlsx_path.name}!", flush=True)
        return df_xlsx, target_date_str

    # 3. Fallback: Base FRESH local
    fresh_path = ROOT / "Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH.csv"
    if fresh_path.exists():
        df_fresh = pd.read_csv(fresh_path, low_memory=False)
        df_fresh['Date'] = pd.to_datetime(df_fresh['Date'], errors='coerce')
        tgt_dt = pd.to_datetime(target_date_str).date()
        sub = df_fresh[df_fresh['Date'].dt.date == tgt_dt].copy()
        if not sub.empty:
            print(f"[LOCAL] {len(sub)} jogos encontrados na base local para {target_date_str}!", flush=True)
            return sub, target_date_str

    print(f"[AVISO] Nenhum jogo encontrado para {target_date_str}.")
    return pd.DataFrame(), target_date_str


def build_daily_features_feed(df_today, target_date_str):
    if df_today.empty:
        return pd.DataFrame()

    if not HIST_DATASET_PATH.exists():
        raise FileNotFoundError(f"Dataset histórico {HIST_DATASET_PATH} não encontrado.")

    df_hist = pd.read_parquet(HIST_DATASET_PATH)
    df_hist['Date'] = pd.to_datetime(df_hist['Date'])
    df_hist = df_hist.sort_values('Date')  # garante que .last() = jogo mais recente do time
    
    # 1. Normalizar nomes de colunas
    home_col = 'Home' if 'Home' in df_today.columns else ('Home_Team' if 'Home_Team' in df_today.columns else 'Mandante')
    away_col = 'Away' if 'Away' in df_today.columns else ('Away_Team' if 'Away_Team' in df_today.columns else 'Visitante')
    league_col = 'League' if 'League' in df_today.columns else 'Liga'

    df_today = df_today.copy()
    df_today['Home'] = df_today[home_col].astype(str)
    df_today['Away'] = df_today[away_col].astype(str)
    df_today['League'] = df_today[league_col].astype(str)
    df_today['Date'] = pd.to_datetime(target_date_str)

    # 2. Computar Features de Mercado com Odds Betfair de Hoje
    odd_h = pd.to_numeric(df_today.get('Odd_H_Back', df_today.get('Odd_H_FT', 2.0)), errors='coerce').fillna(2.0)
    odd_d = pd.to_numeric(df_today.get('Odd_D_Back', df_today.get('Odd_D_FT', 3.2)), errors='coerce').fillna(3.2)
    odd_a = pd.to_numeric(df_today.get('Odd_A_Back', df_today.get('Odd_A_FT', 2.0)), errors='coerce').fillna(2.0)
    odd_o25 = pd.to_numeric(df_today.get('Odd_Over25_FT_Back', 2.0), errors='coerce').fillna(2.0)
    odd_u25 = pd.to_numeric(df_today.get('Odd_Under25_FT_Back', 1.8), errors='coerce').fillna(1.8)
    odd_btts_y = pd.to_numeric(df_today.get('Odd_BTTS_Yes_Back', 1.9), errors='coerce').fillna(1.9)
    odd_btts_n = pd.to_numeric(df_today.get('Odd_BTTS_No_Back', 1.9), errors='coerce').fillna(1.9)

    p_h = 1.0 / np.maximum(1.001, odd_h)
    p_d = 1.0 / np.maximum(1.001, odd_d)
    p_a = 1.0 / np.maximum(1.001, odd_a)
    p_over = 1.0 / np.maximum(1.001, odd_o25)
    p_under = 1.0 / np.maximum(1.001, odd_u25)
    p_btts_y = 1.0 / np.maximum(1.001, odd_btts_y)
    p_btts_n = 1.0 / np.maximum(1.001, odd_btts_n)

    vig_1x2 = p_h + p_d + p_a
    df_today['p_H_clean'] = p_h / np.maximum(1e-5, vig_1x2)
    df_today['p_D_clean'] = p_d / np.maximum(1e-5, vig_1x2)
    df_today['p_A_clean'] = p_a / np.maximum(1e-5, vig_1x2)

    vig_ou = p_over + p_under
    df_today['p_Over_clean'] = p_over / np.maximum(1e-5, vig_ou)
    df_today['p_Under_clean'] = p_under / np.maximum(1e-5, vig_ou)

    df_today['entropy_1x2'] = -(df_today['p_H_clean'] * np.log(df_today['p_H_clean'] + 1e-9) +
                                df_today['p_D_clean'] * np.log(df_today['p_D_clean'] + 1e-9) +
                                df_today['p_A_clean'] * np.log(df_today['p_A_clean'] + 1e-9))

    eps = 1e-6
    df_today['VAR01'] = p_h / (p_d + eps)
    df_today['VAR02'] = p_h / (p_a + eps)
    df_today['VAR03'] = p_d / (p_h + eps)
    df_today['VAR04'] = p_d / (p_a + eps)
    df_today['VAR05'] = p_a / (p_h + eps)
    df_today['VAR06'] = p_a / (p_d + eps)
    df_today['VAR07'] = p_over / (p_under + eps)
    df_today['VAR08'] = p_under / (p_over + eps)
    df_today['VAR09'] = p_btts_y / (p_btts_n + eps)
    df_today['VAR10'] = p_btts_n / (p_btts_y + eps)
    df_today['VAR54'] = np.abs(p_h - p_a)
    df_today['VAR55'] = np.abs(p_h - p_d)
    df_today['VAR56'] = np.abs(p_d - p_a)

    # 3. Extrair as Últimas Médias Rolantes dos Times da Base Histórica (Shift 1)
    team_stat_cols = ['GF_r5', 'GA_r5', 'xGF_r5', 'xGA_r5', 'SoTF_r5', 'SoTA_r5', 'CornersF_r5', 'CornersA_r5']
    
    # SEM fabricacao (regra GEMINI.md #4): time/liga sem historico -> NaN -> o observador da SKIP.
    # Nao preencher com mediana (isso e "default magico" e faria o modelo pontuar dado inventado).
    new_cols = {}
    for c in team_stat_cols:
        h_dict = df_hist.groupby('Home')[f'H_{c}'].last().to_dict()
        new_cols[f'H_{c}'] = df_today['Home'].map(h_dict)

    for c in team_stat_cols:
        a_dict = df_hist.groupby('Away')[f'A_{c}'].last().to_dict()
        new_cols[f'A_{c}'] = df_today['Away'].map(a_dict)

    # 4. Extrair as Taxas de Liga
    liga_cols = ['liga_hw_rate', 'liga_draw_rate', 'liga_aw_rate', 'liga_o25_rate', 'liga_btts_rate', 'liga_0x0_rate']
    for lc in liga_cols:
        l_map = df_hist.groupby('League')[lc].last().to_dict()
        new_cols[lc] = df_today['League'].map(l_map)

    df_extra = pd.DataFrame(new_cols, index=df_today.index)
    df_today = pd.concat([df_today, df_extra], axis=1)

    return df_today


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default=None, help="Data YYYY-MM-DD")
    args = parser.parse_args()

    df_raw, date_str = get_upcoming_fixtures(args.data)
    if df_raw.empty:
        print("[INFO] Nenhuma fixture para processar.")
        return

    df_feed = build_daily_features_feed(df_raw, date_str)
    
    # Salvar feed do dia
    df_feed.to_parquet(FEED_FORWARD_PATH, index=False)
    print(f"[SUCESSO] Feed diário gerado e enriquecido com {len(df_feed)} jogos em: {FEED_FORWARD_PATH}")

    # Chamar observador honesto
    print("\n[*] Executando observador forward com o novo feed...")
    from observar_under15_forward import main as run_observador
    sys.argv = ['observar_under15_forward.py', '--feed', str(FEED_FORWARD_PATH)]
    run_observador()

if __name__ == "__main__":
    main()
