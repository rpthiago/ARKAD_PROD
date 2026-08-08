"""
GERADOR ULTRA RÁPIDO DE PLANILHA EXCEL COM RESULTADOS INDIVIDUAIS DE TODAS AS ODDS
ARKAD_PROD
"""

import os
import pandas as pd
import numpy as np

def build_fast_excel():
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

    # Data
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df.sort_values('Date').reset_index(drop=True)
        df['Data_Jogo'] = df['Date'].dt.strftime('%Y-%m-%d')
    else:
        df['Data_Jogo'] = ''

    # Odds 1X2
    odd_h_col = 'Odd_H_FT' if 'Odd_H_FT' in df.columns else ('Odd_H_Back' if 'Odd_H_Back' in df.columns else 'Odd_H')
    odd_d_col = 'Odd_D_FT' if 'Odd_D_FT' in df.columns else ('Odd_D_Back' if 'Odd_D_Back' in df.columns else 'Odd_D')
    odd_a_col = 'Odd_A_FT' if 'Odd_A_FT' in df.columns else ('Odd_A_Back' if 'Odd_A_Back' in df.columns else 'Odd_A')

    df['Odd_H_FT'] = pd.to_numeric(df.get(odd_h_col), errors='coerce')
    df['Odd_D_FT'] = pd.to_numeric(df.get(odd_d_col), errors='coerce')
    df['Odd_A_FT'] = pd.to_numeric(df.get(odd_a_col), errors='coerce')

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

    # Total xG
    col_h = 'xG_H_FT' if 'xG_H_FT' in df.columns else ('xG_H_Pre' if 'xG_H_Pre' in df.columns else None)
    col_a = 'xG_A_FT' if 'xG_A_FT' in df.columns else ('xG_A_Pre' if 'xG_A_Pre' in df.columns else None)
    xg_h = pd.to_numeric(df[col_h], errors='coerce').fillna(0.0) if col_h else pd.Series(0.0, index=df.index)
    xg_a = pd.to_numeric(df[col_a], errors='coerce').fillna(0.0) if col_a else pd.Series(0.0, index=df.index)
    df['Total_xG'] = xg_h + xg_a

    # Filtros do Saldo Menor
    cond_a = ((df['Fav_Odd'] >= 2.20) & (df['Fav_Odd'] <= 5.00)) | ((df['Zebra_Odd'] >= 2.20) & (df['Zebra_Odd'] <= 5.00))
    cond_b = (df['EH_Zebra_Plus3_Odd'] > 1.0) & (df['EH_Zebra_Plus3_Odd'] < df['Zebra_Odd']) & (df['EH_Zebra_Plus3_Odd'] <= 2.50)
    cond_c = (1.0 / df['Zebra_Odd']) <= 0.45
    cond_d = (df['Total_xG'] > 0) & (df['Total_xG'] <= 2.0)

    df_approved = df[cond_a & cond_b & cond_c & cond_d].copy()
    if df_approved.empty:
        cond_d_flex = df['Total_xG'] <= 2.0
        df_approved = df[cond_a & cond_b & cond_c & cond_d_flex].copy()

    # Placares
    gh = pd.to_numeric(df_approved['Goals_H_FT'], errors='coerce').fillna(0).astype(int)
    ga = pd.to_numeric(df_approved['Goals_A_FT'], errors='coerce').fillna(0).astype(int)
    gh_ht = pd.to_numeric(df_approved.get('Goals_H_HT', 0), errors='coerce').fillna(0).astype(int)
    ga_ht = pd.to_numeric(df_approved.get('Goals_A_HT', 0), errors='coerce').fillna(0).astype(int)

    tot_gols_ft = gh + ga
    tot_gols_ht = gh_ht + ga_ht
    df_approved['Placar_Final'] = gh.astype(str) + " x " + ga.astype(str)

    # Identificar todas as odds
    odd_cols = [
        c for c in df_approved.columns 
        if (c.startswith('Odd_') or c.startswith('AH_') or c.startswith('EH_'))
    ]

    print(f"[+] Vetorizando resultados de {len(odd_cols)} odds...")

    # Vetorizar os cálculos de resultado de todas as odds de uma vez
    for col in odd_cols:
        res_col = f"Res_{col}"
        if col in ['Odd_H_FT', 'Odd_H_Back', 'Odd_H']: df_approved[res_col] = np.where(gh > ga, 'GREEN', 'RED')
        elif col in ['Odd_D_FT', 'Odd_D_Back', 'Odd_D']: df_approved[res_col] = np.where(gh == ga, 'GREEN', 'RED')
        elif col in ['Odd_A_FT', 'Odd_A_Back', 'Odd_A']: df_approved[res_col] = np.where(ga > gh, 'GREEN', 'RED')
        elif col == 'Odd_H_HT': df_approved[res_col] = np.where(gh_ht > ga_ht, 'GREEN', 'RED')
        elif col == 'Odd_D_HT': df_approved[res_col] = np.where(gh_ht == ga_ht, 'GREEN', 'RED')
        elif col == 'Odd_A_HT': df_approved[res_col] = np.where(ga_ht > gh_ht, 'GREEN', 'RED')

        elif 'Over05_FT' in col: df_approved[res_col] = np.where(tot_gols_ft > 0.5, 'GREEN', 'RED')
        elif 'Under05_FT' in col: df_approved[res_col] = np.where(tot_gols_ft < 0.5, 'GREEN', 'RED')
        elif 'Over15_FT' in col: df_approved[res_col] = np.where(tot_gols_ft > 1.5, 'GREEN', 'RED')
        elif 'Under15_FT' in col: df_approved[res_col] = np.where(tot_gols_ft < 1.5, 'GREEN', 'RED')
        elif 'Over25_FT' in col: df_approved[res_col] = np.where(tot_gols_ft > 2.5, 'GREEN', 'RED')
        elif 'Under25_FT' in col: df_approved[res_col] = np.where(tot_gols_ft < 2.5, 'GREEN', 'RED')
        elif 'Over35_FT' in col: df_approved[res_col] = np.where(tot_gols_ft > 3.5, 'GREEN', 'RED')
        elif 'Under35_FT' in col: df_approved[res_col] = np.where(tot_gols_ft < 3.5, 'GREEN', 'RED')
        elif 'Over45_FT' in col: df_approved[res_col] = np.where(tot_gols_ft > 4.5, 'GREEN', 'RED')
        elif 'Under45_FT' in col: df_approved[res_col] = np.where(tot_gols_ft < 4.5, 'GREEN', 'RED')

        elif 'Over05_HT' in col: df_approved[res_col] = np.where(tot_gols_ht > 0.5, 'GREEN', 'RED')
        elif 'Under05_HT' in col: df_approved[res_col] = np.where(tot_gols_ht < 0.5, 'GREEN', 'RED')
        elif 'Over15_HT' in col: df_approved[res_col] = np.where(tot_gols_ht > 1.5, 'GREEN', 'RED')
        elif 'Under15_HT' in col: df_approved[res_col] = np.where(tot_gols_ht < 1.5, 'GREEN', 'RED')
        elif 'Over25_HT' in col: df_approved[res_col] = np.where(tot_gols_ht > 2.5, 'GREEN', 'RED')
        elif 'Under25_HT' in col: df_approved[res_col] = np.where(tot_gols_ht < 2.5, 'GREEN', 'RED')

        elif 'BTTS_Yes' in col: df_approved[res_col] = np.where((gh > 0) & (ga > 0), 'GREEN', 'RED')
        elif 'BTTS_No' in col: df_approved[res_col] = np.where((gh == 0) | (ga == 0), 'GREEN', 'RED')

        elif 'DC_1X' in col or col == 'Odd_1X_Back': df_approved[res_col] = np.where(gh >= ga, 'GREEN', 'RED')
        elif 'DC_12' in col or col == 'Odd_12_Back': df_approved[res_col] = np.where(gh != ga, 'GREEN', 'RED')
        elif 'DC_X2' in col or col == 'Odd_X2_Back': df_approved[res_col] = np.where(ga >= gh, 'GREEN', 'RED')

        elif 'CS_' in col:
            parts = col.split('CS_')[-1].split('_')[0]
            if 'x' in parts:
                try:
                    ht_val, at_val = map(int, parts.split('x'))
                    df_approved[res_col] = np.where((gh == ht_val) & (ga == at_val), 'GREEN', 'RED')
                except: df_approved[res_col] = 'N/A'
            else: df_approved[res_col] = 'N/A'

        elif col.startswith('EH_'):
            h_val = float(col.split('_pos_')[-1].replace('_', '.')) if '_pos_' in col else (-float(col.split('_neg_')[-1].replace('_', '.')) if '_neg_' in col else 0.0)
            if '_H_' in col: df_approved[res_col] = np.where((gh + h_val) > ga, 'GREEN', 'RED')
            elif '_D_' in col: df_approved[res_col] = np.where((gh + h_val) == ga, 'GREEN', 'RED')
            elif '_A_' in col: df_approved[res_col] = np.where(ga > (gh + h_val), 'GREEN', 'RED')

        elif col.startswith('AH_'):
            h_val = float(col.split('_pos_')[-1].replace('_', '.')) if '_pos_' in col else (-float(col.split('_neg_')[-1].replace('_', '.')) if '_neg_' in col else 0.0)
            diff = (gh + h_val) - ga if '_H_' in col else (ga + h_val) - gh
            df_approved[res_col] = np.where(diff > 0, 'GREEN', np.where(diff == 0, 'PUSH', 'RED'))
        else:
            df_approved[res_col] = 'N/A'

    # Ordenar Pares Lado a Lado: Odd -> Res_Odd
    paired_cols = [
        'Data_Jogo', 'Country', 'League', 'Season', 'Round', 'Time', 'Home', 'Away',
        'Goals_H_HT', 'Goals_A_HT', 'Goals_H_FT', 'Goals_A_FT', 'Placar_Final',
        'Is_Home_Zebra', 'Zebra_Team', 'Fav_Team', 'Zebra_Odd', 'Fav_Odd', 'Total_xG'
    ]
    paired_cols = [c for c in paired_cols if c in df_approved.columns]

    for col in sorted(odd_cols):
        paired_cols.append(col)
        res_col = f"Res_{col}"
        if res_col in df_approved.columns:
            paired_cols.append(res_col)

    df_export = df_approved[paired_cols].copy()
    print(f"[+] Salvando {len(df_export.columns)} colunas em formato ultrarrápido...")

    output_xlsx = os.path.join(base_dir, "backtest_saldo_menor_detalhado.xlsx")
    with pd.ExcelWriter(output_xlsx, engine='openpyxl') as writer:
        df_export.to_excel(writer, sheet_name='Resultados_Todas_Odds', index=False)

    print(f"[OK] Planilha ultrarrápida gerada com sucesso em: {output_xlsx}")
    return output_xlsx

if __name__ == "__main__":
    build_fast_excel()
