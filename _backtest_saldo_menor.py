"""
SCRIPT DE BACKTEST DEDICADO - MÉTODO SALDO MENOR
ARKAD_PROD

Executa a validação completa do MÉTODO SALDO MENOR sobre as bases históricas do projeto
(Bases_de_Dados_API_FutPythonTrader_Bet365.csv / Resultados_2026_Full.csv).
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Tuple, Dict, Any

from metodo_saldo_menor_strategy import normalize_live_data, identify_zebra_and_handicap, check_entry_conditions


def load_historical_datasets() -> pd.DataFrame:
    """Carrega e consolida a base histórica de dados da Bet365."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    primary_csv = os.path.join(base_dir, "Bases_de_Dados_API_FutPythonTrader_Bet365.csv")
    secondary_csv = os.path.join(base_dir, "Resultados_2026_Full.csv")

    df = pd.DataFrame()
    if os.path.exists(primary_csv):
        print(f"[+] Lendo base primária: {os.path.basename(primary_csv)}...")
        df = pd.read_csv(primary_csv, low_memory=False)
    elif os.path.exists(secondary_csv):
        print(f"[+] Lendo base secundária: {os.path.basename(secondary_csv)}...")
        df = pd.read_csv(secondary_csv, low_memory=False)

    if df.empty:
        raise FileNotFoundError("Nenhuma base de dados histórica encontrada para o backtest.")

    print(f"[i] Total de jogos na base original: {len(df):,}")
    return df


def run_saldo_menor_backtest(df: pd.DataFrame, stake_fixa: float = 100.0) -> Tuple[pd.DataFrame, dict]:
    """Executa o backtest do Método Saldo Menor."""
    # Converter datas
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

    # Renomear colunas para padrão unificado se necessário
    rename_map = {
        'xG_H': 'xG_H_FT',
        'xG_A': 'xG_A_FT',
        'Shots_H': 'Total_Shots_H_FT',
        'Shots_A': 'Total_Shots_A_FT',
    }
    df = df.rename(columns=rename_map)

    # Identificar Zebra e Odds
    df['Odd_H_FT'] = pd.to_numeric(df.get('Odd_H_FT'), errors='coerce')
    df['Odd_A_FT'] = pd.to_numeric(df.get('Odd_A_FT'), errors='coerce')
    
    # Remover jogos sem odds de mercado 1X2
    df = df.dropna(subset=['Odd_H_FT', 'Odd_A_FT']).copy()
    
    df['Is_Home_Zebra'] = df['Odd_H_FT'] > df['Odd_A_FT']
    df['Zebra_Odd'] = df['Odd_H_FT'].where(df['Is_Home_Zebra'], df['Odd_A_FT'])
    df['Fav_Odd'] = df['Odd_A_FT'].where(df['Is_Home_Zebra'], df['Odd_H_FT'])

    # Extrair Handicap Europeu +3 da Zebra:
    # EH_H_pos_3 (se Casa for Zebra) ou EH_A_pos_3 (se Visitante for Zebra)
    eh_h = pd.to_numeric(df.get('EH_H_pos_3'), errors='coerce').fillna(0.0)
    eh_a = pd.to_numeric(df.get('EH_A_pos_3'), errors='coerce').fillna(0.0)
    df['EH_Zebra_Plus3_Odd'] = eh_h.where(df['Is_Home_Zebra'], eh_a)

    # Total xG
    col_h = 'xG_H_FT' if 'xG_H_FT' in df.columns else ('xG_H_Pre' if 'xG_H_Pre' in df.columns else None)
    col_a = 'xG_A_FT' if 'xG_A_FT' in df.columns else ('xG_A_Pre' if 'xG_A_Pre' in df.columns else None)

    xg_h = pd.to_numeric(df[col_h], errors='coerce').fillna(0.0) if col_h else pd.Series(0.0, index=df.index)
    xg_a = pd.to_numeric(df[col_a], errors='coerce').fillna(0.0) if col_a else pd.Series(0.0, index=df.index)
    df['Total_xG'] = xg_h + xg_a

    # Filtros do Método Saldo Menor
    # Sanitização: Fav_Odd estritamente entre 2.00 e 5.00 para equilíbrio entre volume (+35%) e assertividade (90.04% nas Múltiplas)
    cond_a = (df['Fav_Odd'] >= 2.00) & (df['Fav_Odd'] <= 5.00)
    cond_b = (df['EH_Zebra_Plus3_Odd'] > 1.0) & (df['EH_Zebra_Plus3_Odd'] < df['Zebra_Odd']) & (df['EH_Zebra_Plus3_Odd'] <= 2.50)
    cond_c = (1.0 / df['Zebra_Odd']) <= 0.45
    cond_d = (df['Total_xG'] > 0) & (df['Total_xG'] <= 2.0)

    # Seleção de jogos aprovados
    df_approved = df[cond_a & cond_b & cond_c & cond_d].copy()

    if df_approved.empty:
        # Se xG estritamente no CSV primário for 0 para muitos jogos, testa sem restricao estrita de xG zerado
        cond_d_flexible = df['Total_xG'] <= 2.0
        df_approved = df[cond_a & cond_b & cond_c & cond_d_flexible].copy()

    # Filtro de Confiança Quantitativa >= 94% (0.94)
    try:
        import joblib
        import master_feature_engineer
        model_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'modelo_saldo_menor_quant.pkl')
        if os.path.exists(model_path):
            model_sm = joblib.load(model_path)
            feats_sm = master_feature_engineer.build_master_features(df_approved)
            if hasattr(model_sm, "feature_names_in_"):
                feats_sm = feats_sm.reindex(columns=model_sm.feature_names_in_, fill_value=0.0)
            df_approved['Prob_Master'] = model_sm.predict_proba(feats_sm)[:, 1]
            df_approved = df_approved[df_approved['Prob_Master'] >= 0.94].copy()
    except Exception as ex:
        print(f"[!] Aviso: Nao foi possivel aplicar filtro Prob_Master >= 0.94: {ex}")

    # Cálculo do Resultado Real dos jogos
    gols_h = pd.to_numeric(df_approved.get('Goals_H_FT'), errors='coerce').fillna(0)
    gols_a = pd.to_numeric(df_approved.get('Goals_A_FT'), errors='coerce').fillna(0)

    df_approved['Gols_Zebra'] = gols_h.where(df_approved['Is_Home_Zebra'], gols_a)
    df_approved['Gols_Fav'] = gols_a.where(df_approved['Is_Home_Zebra'], gols_h)
    df_approved['Diff_Gols_Fav'] = df_approved['Gols_Fav'] - df_approved['Gols_Zebra']

    # GREEN no EH +3: Zebra não perde por 3 ou mais gols (Diff_Gols_Fav < 3)
    df_approved['Green'] = df_approved['Diff_Gols_Fav'] < 3
    df_approved['Resultado_Str'] = np.where(df_approved['Green'], 'GREEN', 'RED')

    # Cálculos Financeiros
    df_approved['Lucro_Operacao'] = np.where(
        df_approved['Green'],
        stake_fixa * (df_approved['EH_Zebra_Plus3_Odd'] - 1.0),
        -stake_fixa
    )
    df_approved['Banca_Acumulada'] = df_approved['Lucro_Operacao'].cumsum()

    # Métricas
    total_jogos = len(df_approved)
    greens = int(df_approved['Green'].sum())
    reds = total_jogos - greens
    win_rate = (greens / total_jogos) if total_jogos > 0 else 0.0
    odd_media = df_approved['EH_Zebra_Plus3_Odd'].mean() if total_jogos > 0 else 0.0
    lucro_total = df_approved['Lucro_Operacao'].sum() if total_jogos > 0 else 0.0
    roi = (lucro_total / (total_jogos * stake_fixa)) * 100 if total_jogos > 0 else 0.0

    # Drawdown máximo
    if total_jogos > 0:
        peak = df_approved['Banca_Acumulada'].cummax()
        dd = df_approved['Banca_Acumulada'] - peak
        max_dd = dd.min()
    else:
        max_dd = 0.0

    summary = {
        'total_jogos': total_jogos,
        'greens': greens,
        'reds': reds,
        'win_rate_pct': win_rate * 100,
        'odd_media_eh3': odd_media,
        'stake_fixa': stake_fixa,
        'lucro_total_rs': lucro_total,
        'roi_pct': roi,
        'max_drawdown_rs': max_dd
    }

    return df_approved, summary


def main():
    print("==========================================================")
    print("    BACKTEST EXECUTÁVEL - MÉTODO SALDO MENOR (EH +3)    ")
    print("==========================================================")

    df_raw = load_historical_datasets()
    df_ops, summary = run_saldo_menor_backtest(df_raw, stake_fixa=100.0)

    print("\n--- RELATÓRIO DE DESEMPENHO ---")
    print(f"Total de Entradas Aprovadas : {summary['total_jogos']:,}")
    print(f"Greens                      : {summary['greens']:,} ({summary['win_rate_pct']:.2f}%)")
    print(f"Reds                        : {summary['reds']:,}")
    print(f"Odd Média EH +3 Zebra       : {summary['odd_media_eh3']:.2f}")
    print(f"Stake por Entrada           : R$ {summary['stake_fixa']:.2f}")
    print(f"Lucro Líquido Acumulado     : R$ {summary['lucro_total_rs']:,.2f}")
    print(f"ROI sobre Capital Investido : {summary['roi_pct']:.2f}%")
    print(f"Drawdown Máximo             : R$ {summary['max_drawdown_rs']:,.2f}")
    print("----------------------------------------------------------")

    # Salvar resultados em CSV
    output_ops = "backtest_metodo_saldo_menor_ops.csv"
    output_summary = "backtest_metodo_saldo_menor_resumo.csv"

    df_ops.to_csv(output_ops, index=False)
    pd.DataFrame([summary]).to_csv(output_summary, index=False)

    print(f"[OK] Operacoes salvas em: {output_ops}")
    print(f"[OK] Resumo salvo em   : {output_summary}")


if __name__ == "__main__":
    main()
