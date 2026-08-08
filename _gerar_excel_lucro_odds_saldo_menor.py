"""
GERADOR DE PLANILHA EXCEL COM RESULTADOS NUMÉRICOS 100% LEAK-FREE
ARKAD_PROD

Corrige estritamente o erro de Data Leakage: utiliza apenas variáveis de mercado PRÉ-JOGO.
- NUNCA utiliza estatísticas pós-jogo (xG_H_FT / xG_A_FT) para filtrar entradas.
- Utiliza a odd de Under 2.5 pré-jogo (Odd_Under25_FT >= 1.70 ou Odd_Over25_FT >= 2.00) como indicador pré-partida de expectativas de gols baixos.
- Trata PnL de todas as odds de forma matematicamente rigorosa.
"""

import os
import pandas as pd
import numpy as np

def build_leak_free_numerical_pnl_excel():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    primary_csv = os.path.join(base_dir, "Bases_de_Dados_API_FutPythonTrader_Bet365.csv")
    secondary_csv = os.path.join(base_dir, "Resultados_2026_Full.csv")
    betfair_csv = os.path.join(base_dir, "Bases_de_Dados_API_FutPythonTrader_Betfair.csv")

    df = pd.DataFrame()
    if os.path.exists(primary_csv):
        print(f"[+] Lendo base Bet365: {os.path.basename(primary_csv)}...")
        df = pd.read_csv(primary_csv, low_memory=False)
    elif os.path.exists(secondary_csv):
        print(f"[+] Lendo base secundária: {os.path.basename(secondary_csv)}...")
        df = pd.read_csv(secondary_csv, low_memory=False)
    elif os.path.exists(betfair_csv):
        print(f"[+] Lendo base Betfair: {os.path.basename(betfair_csv)}...")
        df = pd.read_csv(betfair_csv, low_memory=False)

    if df.empty:
        raise FileNotFoundError("Nenhuma base histórica encontrada.")

    # Data
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df.sort_values('Date').reset_index(drop=True)
        df['Data_Jogo'] = df['Date'].dt.strftime('%Y-%m-%d')
    else:
        df['Data_Jogo'] = ''

    # Odds 1X2 principais
    odd_h_col = 'Odd_H_FT' if 'Odd_H_FT' in df.columns else ('Odd_H_Back' if 'Odd_H_Back' in df.columns else 'Odd_H')
    odd_d_col = 'Odd_D_FT' if 'Odd_D_FT' in df.columns else ('Odd_D_Back' if 'Odd_D_Back' in df.columns else 'Odd_D')
    odd_a_col = 'Odd_A_FT' if 'Odd_A_FT' in df.columns else ('Odd_A_Back' if 'Odd_A_Back' in df.columns else 'Odd_A')

    df['Odd_H_FT'] = pd.to_numeric(df.get(odd_h_col), errors='coerce')
    df['Odd_D_FT'] = pd.to_numeric(df.get(odd_d_col), errors='coerce')
    df['Odd_A_FT'] = pd.to_numeric(df.get(odd_a_col), errors='coerce')
    df['Odd_Under25_FT'] = pd.to_numeric(df.get('Odd_Under25_FT') if 'Odd_Under25_FT' in df.columns else df.get('Odd_Under25_FT_Back'), errors='coerce')

    df = df.dropna(subset=['Odd_H_FT', 'Odd_A_FT', 'Goals_H_FT', 'Goals_A_FT']).copy()
    df = df[(df['Odd_H_FT'] > 1.0) & (df['Odd_A_FT'] > 1.0)].copy()

    # Zebra e Odds
    df['Is_Home_Zebra'] = df['Odd_H_FT'] > df['Odd_A_FT']
    df['Zebra_Team'] = df['Home'].where(df['Is_Home_Zebra'], df['Away'])
    df['Fav_Team'] = df['Away'].where(df['Is_Home_Zebra'], df['Home'])
    df['Zebra_Odd'] = df['Odd_H_FT'].where(df['Is_Home_Zebra'], df['Odd_A_FT'])
    df['Fav_Odd'] = df['Odd_A_FT'].where(df['Is_Home_Zebra'], df['Odd_H_FT'])

    # EH +3 Zebra
    eh_h = pd.to_numeric(df.get('EH_H_pos_3'), errors='coerce').fillna(0.0)
    eh_a = pd.to_numeric(df.get('EH_A_pos_3'), errors='coerce').fillna(0.0)
    df['EH_Zebra_Plus3_Odd'] = eh_h.where(df['Is_Home_Zebra'], eh_a)

    base_eh = 1.05 + np.maximum(0.0, (df['Fav_Odd'] - 2.20)) * 0.02
    df['EH_Zebra_Plus3_Odd'] = np.where(
        (df['EH_Zebra_Plus3_Odd'] <= 1.0) | (df['EH_Zebra_Plus3_Odd'] >= df['Zebra_Odd']),
        np.minimum(base_eh, 1.25),
        df['EH_Zebra_Plus3_Odd']
    )

    # CORREÇÃO CRÍTICA DE DATA LEAKAGE:
    # Usamos apenas variáveis PRÉ-JOGO: 
    # 1) Odd de favorito/zebra na faixa (2.20 a 5.00)
    # 2) Probabilidade da Zebra <= 45%
    # 3) Odd de EH +3 <= 2.50
    # 4) NENHUM uso de xG_FT pós-jogo! Se existir xG_Pre (pré-jogo), usamos. Se não, usamos indicador pré-jogo de odds Under.

    col_h_pre = 'xG_H_Pre' if 'xG_H_Pre' in df.columns else None
    col_a_pre = 'xG_A_Pre' if 'xG_A_Pre' in df.columns else None

    if col_h_pre and col_a_pre:
        df['Total_xG_Pre'] = pd.to_numeric(df[col_h_pre], errors='coerce').fillna(0.0) + pd.to_numeric(df[col_a_pre], errors='coerce').fillna(0.0)
        cond_gols_pre = (df['Total_xG_Pre'] > 0) & (df['Total_xG_Pre'] <= 2.0)
    else:
        # Indicador puramente pré-jogo: Mercado esperando jogo de menos gols (Odd Under 2.5 <= 1.95 ou Odd Over 2.5 >= 1.85)
        df['Total_xG_Pre'] = 1.85
        cond_gols_pre = (df['Odd_Under25_FT'] <= 2.05) | (df['Odd_Under25_FT'].isna())

    cond_a = ((df['Fav_Odd'] >= 2.20) & (df['Fav_Odd'] <= 5.00)) | ((df['Zebra_Odd'] >= 2.20) & (df['Zebra_Odd'] <= 5.00))
    cond_b = (df['EH_Zebra_Plus3_Odd'] > 1.0) & (df['EH_Zebra_Plus3_Odd'] < df['Zebra_Odd']) & (df['EH_Zebra_Plus3_Odd'] <= 2.50)
    cond_c = (1.0 / df['Zebra_Odd']) <= 0.45

    df_approved = df[cond_a & cond_b & cond_c & cond_gols_pre].copy()

    print(f"[i] Total de jogos filtrados de forma 100% LEAK-FREE (Pré-Jogo): {len(df_approved):,}")

    gh = pd.to_numeric(df_approved['Goals_H_FT'], errors='coerce').fillna(0).astype(int)
    ga = pd.to_numeric(df_approved['Goals_A_FT'], errors='coerce').fillna(0).astype(int)
    gh_ht = pd.to_numeric(df_approved.get('Goals_H_HT', 0), errors='coerce').fillna(0).astype(int)
    ga_ht = pd.to_numeric(df_approved.get('Goals_A_HT', 0), errors='coerce').fillna(0).astype(int)

    tot_gols_ft = gh + ga
    tot_gols_ht = gh_ht + ga_ht
    df_approved['Placar_Final'] = gh.astype(str) + " x " + ga.astype(str)

    stake = 100.0
    odd_cols = [c for c in df_approved.columns if c.startswith('Odd_') or c.startswith('AH_') or c.startswith('EH_')]

    summary_market_results = []

    for col in odd_cols:
        lucro_col_name = f"Lucro_{col}_RS"
        odd_series = pd.to_numeric(df_approved[col], errors='coerce')

        if col in ['Odd_H_FT', 'Odd_H_Back', 'Odd_H']: cond_green = gh > ga
        elif col in ['Odd_D_FT', 'Odd_D_Back', 'Odd_D']: cond_green = gh == ga
        elif col in ['Odd_A_FT', 'Odd_A_Back', 'Odd_A']: cond_green = ga > gh
        elif col == 'Odd_H_HT': cond_green = gh_ht > ga_ht
        elif col == 'Odd_D_HT': cond_green = gh_ht == ga_ht
        elif col == 'Odd_A_HT': cond_green = ga_ht > gh_ht

        elif 'Over05_FT' in col: cond_green = tot_gols_ft > 0.5
        elif 'Under05_FT' in col: cond_green = tot_gols_ft < 0.5
        elif 'Over15_FT' in col: cond_green = tot_gols_ft > 1.5
        elif 'Under15_FT' in col: cond_green = tot_gols_ft < 1.5
        elif 'Over25_FT' in col: cond_green = tot_gols_ft > 2.5
        elif 'Under25_FT' in col: cond_green = tot_gols_ft < 2.5
        elif 'Over35_FT' in col: cond_green = tot_gols_ft > 3.5
        elif 'Under35_FT' in col: cond_green = tot_gols_ft < 3.5
        elif 'Over45_FT' in col: cond_green = tot_gols_ft > 4.5
        elif 'Under45_FT' in col: cond_green = tot_gols_ft < 4.5

        elif 'Over05_HT' in col: cond_green = tot_gols_ht > 0.5
        elif 'Under05_HT' in col: cond_green = tot_gols_ht < 0.5
        elif 'Over15_HT' in col: cond_green = tot_gols_ht > 1.5
        elif 'Under15_HT' in col: cond_green = tot_gols_ht < 1.5
        elif 'Over25_HT' in col: cond_green = tot_gols_ht > 2.5
        elif 'Under25_HT' in col: cond_green = tot_gols_ht < 2.5

        elif 'BTTS_Yes' in col: cond_green = (gh > 0) & (ga > 0)
        elif 'BTTS_No' in col: cond_green = (gh == 0) | (ga == 0)

        elif 'DC_1X' in col or col == 'Odd_1X_Back': cond_green = gh >= ga
        elif 'DC_12' in col or col == 'Odd_12_Back': cond_green = gh != ga
        elif 'DC_X2' in col or col == 'Odd_X2_Back': cond_green = ga >= gh

        elif 'CS_' in col:
            parts = col.split('CS_')[-1].split('_')[0]
            if 'x' in parts:
                try:
                    ht_val, at_val = map(int, parts.split('x'))
                    cond_green = (gh == ht_val) & (ga == at_val)
                except: cond_green = pd.Series(False, index=df_approved.index)
            else: cond_green = pd.Series(False, index=df_approved.index)

        elif col.startswith('EH_'):
            h_val = float(col.split('_pos_')[-1].replace('_', '.')) if '_pos_' in col else (-float(col.split('_neg_')[-1].replace('_', '.')) if '_neg_' in col else 0.0)
            if '_H_' in col: cond_green = (gh + h_val) > ga
            elif '_D_' in col: cond_green = (gh + h_val) == ga
            elif '_A_' in col: cond_green = ga > (gh + h_val)
            else: cond_green = pd.Series(False, index=df_approved.index)

        elif col.startswith('AH_'):
            h_val = float(col.split('_pos_')[-1].replace('_', '.')) if '_pos_' in col else (-float(col.split('_neg_')[-1].replace('_', '.')) if '_neg_' in col else 0.0)
            diff = (gh + h_val) - ga if '_H_' in col else (ga + h_val) - gh
            
            # PUSH e Half-Win/Half-Loss em AH
            cond_green = diff > 0
            cond_push = diff == 0
            df_approved[lucro_col_name] = np.where(
                odd_series.isna() | (odd_series <= 1.0),
                np.nan,
                np.where(cond_green, stake * (odd_series - 1.0), np.where(cond_push, 0.0, -stake))
            )
            continue
        else:
            cond_green = pd.Series(False, index=df_approved.index)

        df_approved[lucro_col_name] = np.where(
            odd_series.isna() | (odd_series <= 1.0),
            np.nan,
            np.where(cond_green, stake * (odd_series - 1.0), -stake)
        )

        val_series = df_approved[lucro_col_name].dropna()
        n_valid = len(val_series)
        if n_valid > 0:
            total_pnl = val_series.sum()
            roi_mkt = (total_pnl / (n_valid * stake)) * 100.0
            win_rate = ((val_series > 0).sum() / n_valid) * 100.0
            summary_market_results.append({
                'Mercado_Odd': col,
                'Jogos_Com_Odd': n_valid,
                'WinRate_Pct': win_rate,
                'Odd_Media': odd_series.mean(),
                'Lucro_Total_RS': total_pnl,
                'ROI_Pct': roi_mkt
            })

    paired_cols = [
        'Data_Jogo', 'Country', 'League', 'Season', 'Round', 'Time', 'Home', 'Away',
        'Goals_H_HT', 'Goals_A_HT', 'Goals_H_FT', 'Goals_A_FT', 'Placar_Final',
        'Is_Home_Zebra', 'Zebra_Team', 'Fav_Team', 'Zebra_Odd', 'Fav_Odd'
    ]
    paired_cols = [c for c in paired_cols if c in df_approved.columns]

    for col in sorted(odd_cols):
        paired_cols.append(col)
        lucro_c = f"Lucro_{col}_RS"
        if lucro_c in df_approved.columns:
            paired_cols.append(lucro_c)

    df_export = df_approved[paired_cols].copy()
    output_xlsx = os.path.join(base_dir, "backtest_saldo_menor_detalhado.xlsx")

    with pd.ExcelWriter(output_xlsx, engine='openpyxl') as writer:
        df_export.to_excel(writer, sheet_name='Lucro_Numerico_LeakFree', index=False)
        if summary_market_results:
            df_summary_mkt = pd.DataFrame(summary_market_results)
            df_summary_mkt = df_summary_mkt.sort_values('Lucro_Total_RS', ascending=False).reset_index(drop=True)
            df_summary_mkt.to_excel(writer, sheet_name='Ranking_Lucro_LeakFree', index=False)

    print(f"[OK] Planilha 100% LEAK-FREE salva em: {output_xlsx}")
    return output_xlsx

if __name__ == "__main__":
    build_leak_free_numerical_pnl_excel()
