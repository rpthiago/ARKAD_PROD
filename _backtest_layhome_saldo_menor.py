"""
SCRIPT DE BACKTEST AUDITADO & LEAK-FREE - LAY HOME NO MÉTODO SALDO MENOR
ARKAD_PROD

Demonstra rigorosamente o impacto de:
1) Data Leakage (usar xG_FT pós-jogo vs Filtro Leak-Free 100% Pré-Jogo)
2) Odds Reais de Lay na Betfair (Odd_H_Lay real vs Odds teóricas de Back)
"""

import os
import sys
import pandas as pd
import numpy as np

def load_datasets():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    betfair_csv = os.path.join(base_dir, "Bases_de_Dados_API_FutPythonTrader_Betfair.csv")
    bet365_csv = os.path.join(base_dir, "Bases_de_Dados_API_FutPythonTrader_Bet365.csv")

    if os.path.exists(betfair_csv):
        print(f"[+] Lendo base real da Betfair: {os.path.basename(betfair_csv)}...")
        df = pd.read_csv(betfair_csv, low_memory=False)
        is_betfair_native = True
    elif os.path.exists(bet365_csv):
        print(f"[+] Lendo base primária Bet365: {os.path.basename(bet365_csv)}...")
        df = pd.read_csv(bet365_csv, low_memory=False)
        is_betfair_native = False
    else:
        raise FileNotFoundError("Base de dados não encontrada.")

    return df, is_betfair_native


def run_leak_free_audit():
    df, is_betfair = load_datasets()

    # Datas
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df.sort_values('Date').reset_index(drop=True)

    # Padronização de Odds
    df['Odd_H_Back'] = pd.to_numeric(df.get('Odd_H_Back') if 'Odd_H_Back' in df.columns else df.get('Odd_H_FT'), errors='coerce')
    df['Odd_A_Back'] = pd.to_numeric(df.get('Odd_A_Back') if 'Odd_A_Back' in df.columns else df.get('Odd_A_FT'), errors='coerce')
    df['Odd_H_Lay'] = pd.to_numeric(df.get('Odd_H_Lay'), errors='coerce') if 'Odd_H_Lay' in df.columns else np.nan

    gols_h = pd.to_numeric(df.get('Goals_H_FT'), errors='coerce')
    gols_a = pd.to_numeric(df.get('Goals_A_FT'), errors='coerce')

    df = df.dropna(subset=['Odd_H_Back', 'Odd_A_Back', 'Goals_H_FT', 'Goals_A_FT']).copy()
    df = df[(df['Odd_H_Back'] > 1.0) & (df['Odd_A_Back'] > 1.0)].copy()

    # Identificação da Zebra
    df['Is_Home_Zebra'] = df['Odd_H_Back'] > df['Odd_A_Back']
    df['Zebra_Odd'] = df['Odd_H_Back'].where(df['Is_Home_Zebra'], df['Odd_A_Back'])
    df['Fav_Odd'] = df['Odd_A_Back'].where(df['Is_Home_Zebra'], df['Odd_H_Back'])

    # Indicador de Vitória do Lay Home (Mandante não vence)
    df['LayHome_Green'] = gols_h <= gols_a

    # Filtros Pré-Jogo Purificados (Leak-Free: Sem xG pós-jogo)
    cond_odd_range = ((df['Fav_Odd'] >= 2.20) & (df['Fav_Odd'] <= 5.00)) | ((df['Zebra_Odd'] >= 2.20) & (df['Zebra_Odd'] <= 5.00))
    cond_zebra_prob = (1.0 / df['Zebra_Odd']) <= 0.45

    # 1. Base Leak-Free (Apenas filtros pré-jogo disponíveis antes da partida)
    df_leak_free = df[cond_odd_range & cond_zebra_prob].copy()

    # 2. Base Vazada (Inclui xG pós-jogo <= 2.0 que não existe pré-jogo)
    if 'xG_H_FT' in df.columns and 'xG_A_FT' in df.columns:
        xg_h = pd.to_numeric(df['xG_H_FT'], errors='coerce').fillna(0.0)
        xg_a = pd.to_numeric(df['xG_A_FT'], errors='coerce').fillna(0.0)
        df['Total_xG_FT'] = xg_h + xg_a
        df_vazada = df[cond_odd_range & cond_zebra_prob & (df['Total_xG_FT'] > 0) & (df['Total_xG_FT'] <= 2.0)].copy()
    else:
        df_vazada = pd.DataFrame()

    print("\n=========================================================================================================")
    print("           AUDITORIA DE VAZAMENTO DE DADOS (DATA LEAKAGE) E ODDS REAIS BETFAIR LAY                       ")
    print("=========================================================================================================")
    print(f"Total de Jogos no Dataset               : {len(df):,}")
    print(f"Jogos Selecionados LEAK-FREE (Pré-Jogo) : {len(df_leak_free):,} (WinRate real: {(df_leak_free['LayHome_Green'].mean()*100):.2f}%)")
    if not df_vazada.empty:
        print(f"Jogos Selecionados COM VAZAMENTO (xG FT): {len(df_vazada):,} (WinRate inflado: {(df_vazada['LayHome_Green'].mean()*100):.2f}%)")
    print("---------------------------------------------------------------------------------------------------------\n")

    # Métricas Financeiras na Base LEAK-FREE usando Odd Lay REAL da Betfair vs Odd Back Teórica
    liability = 100.0
    
    # Se odd lay nativa existir
    df_leak_free_valid_lay = df_leak_free.dropna(subset=['Odd_H_Lay']).copy()
    df_leak_free_valid_lay = df_leak_free_valid_lay[df_leak_free_valid_lay['Odd_H_Lay'] > 1.0].copy()

    # Lucro com Odd Back Teórica (Sem Fricção)
    df_leak_free['Lucro_Teorico'] = np.where(
        df_leak_free['LayHome_Green'],
        (liability / (df_leak_free['Odd_H_Back'] - 1.0)) * 0.95,
        -liability
    )

    # Lucro com Odd Lay REAL da Betfair (Com Spread Real do Mercado)
    if not df_leak_free_valid_lay.empty:
        df_leak_free_valid_lay['Lucro_Real_Betfair_Lay'] = np.where(
            df_leak_free_valid_lay['LayHome_Green'],
            (liability / (df_leak_free_valid_lay['Odd_H_Lay'] - 1.0)) * 0.95,
            -liability
        )

    # Resultados
    total_lf = len(df_leak_free)
    wr_lf = df_leak_free['LayHome_Green'].mean() * 100.0
    lucro_teorico_lf = df_leak_free['Lucro_Teorico'].sum()
    roi_teorico_lf = (lucro_teorico_lf / (total_lf * liability)) * 100.0

    print("--- 1. DESEMPENHO NA BASE LEAK-FREE (ODD BACK TEÓRICA) ---")
    print(f"Jogos: {total_lf:,} | WinRate: {wr_lf:.2f}% | Lucro: R$ {lucro_teorico_lf:,.2f} | ROI: {roi_teorico_lf:.2f}%\n")

    if not df_leak_free_valid_lay.empty:
        total_real = len(df_leak_free_valid_lay)
        wr_real = df_leak_free_valid_lay['LayHome_Green'].mean() * 100.0
        lucro_real = df_leak_free_valid_lay['Lucro_Real_Betfair_Lay'].sum()
        roi_real = (lucro_real / (total_real * liability)) * 100.0
        gap_medio = (df_leak_free_valid_lay['Odd_H_Lay'] - df_leak_free_valid_lay['Odd_H_Back']).mean()

        print("--- 2. DESEMPENHO NA BASE LEAK-FREE (ODD LAY REAL BETFAIR) ---")
        print(f"Jogos: {total_real:,} | WinRate: {wr_real:.2f}% | Gap Médio Lay-Back: +{gap_medio:.2f}")
        print(f"Lucro Real Líquido : R$ {lucro_real:,.2f}")
        print(f"ROI Real Betfair   : {roi_real:.2f}% (VEREDITO: {'POSITIVO' if roi_real > 0 else 'NEGATIVO (-EV)'})")
        print("---------------------------------------------------------------------------------------------------------\n")

    # Salvar resumo em CSV
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_audit = os.path.join(base_dir, "backtest_layhome_audit_leakfree.csv")
    df_leak_free.to_csv(output_audit, index=False)
    print(f"[OK] Auditoria salva em: {output_audit}")


if __name__ == "__main__":
    run_leak_free_audit()
