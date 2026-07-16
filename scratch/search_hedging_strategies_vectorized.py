import pandas as pd
import numpy as np
import os

def load_and_prep_data():
    if not os.path.exists("betfair_historical.csv"):
        raise FileNotFoundError("betfair_historical.csv not found.")
        
    df = pd.read_csv("betfair_historical.csv")
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Goals_H_FT', 'Goals_A_FT', 'Odd_H_Back', 'Odd_D_Back', 'Odd_A_Back']).copy()
    df['GH'] = df['Goals_H_FT'].astype(int)
    df['GA'] = df['Goals_A_FT'].astype(int)
    return df

# --- VECTORIZED COMBO ANALYSIS ---

def analyze_combo_1_vectorized(df, odd_h_min=1.25, odd_h_max=1.85, ratio=0.15, commission=0.05):
    s_h = 100.0
    s_cs = s_h * ratio
    cs_col = 'Odd_CS_0x1_Lay'
    
    if cs_col not in df.columns:
        return None
        
    mask = (df['Odd_H_Back'] >= odd_h_min) & (df['Odd_H_Back'] <= odd_h_max) & (df[cs_col] > 1.0)
    sub = df[mask]
    n_bets = len(sub)
    if n_bets < 100:
        return None
        
    gh = sub['GH'].values
    ga = sub['GA'].values
    odd_h = sub['Odd_H_Back'].values
    odd_cs_lay = sub[cs_col].values
    
    home_win = gh > ga
    away_01 = (gh == 0) & (ga == 1)
    other = ~(home_win | away_01)
    
    pnl = np.zeros(n_bets)
    pnl[home_win] = s_h * (odd_h[home_win] - 1.0) * (1.0 - commission) + s_cs * (1.0 - commission)
    pnl[away_01] = -s_h - s_cs * (odd_cs_lay[away_01] - 1.0)
    pnl[other] = -s_h + s_cs * (1.0 - commission)
    
    total_pnl = pnl.sum()
    roi = total_pnl / (n_bets * s_h) * 100.0
    win_rate = (pnl > 0.0).sum() / n_bets * 100.0
    
    banca_history = 1000.0 + np.cumsum(pnl)
    peak = np.maximum.accumulate(banca_history)
    max_dd = ((peak - banca_history) / peak * 100.0).max()
    
    return {'bets': n_bets, 'win_rate': win_rate, 'pnl': total_pnl, 'roi': roi, 'max_dd': max_dd}

def analyze_combo_2_vectorized(df, odd_h_min=1.25, odd_h_max=1.85, ratio=0.15, commission=0.05):
    s_h = 100.0
    s_cs = s_h * ratio
    cs_col = 'Odd_CS_0x2_Lay'
    
    if cs_col not in df.columns:
        return None
        
    mask = (df['Odd_H_Back'] >= odd_h_min) & (df['Odd_H_Back'] <= odd_h_max) & (df[cs_col] > 1.0)
    sub = df[mask]
    n_bets = len(sub)
    if n_bets < 100:
        return None
        
    gh = sub['GH'].values
    ga = sub['GA'].values
    odd_h = sub['Odd_H_Back'].values
    odd_cs_lay = sub[cs_col].values
    
    home_win = gh > ga
    away_02 = (gh == 0) & (ga == 2)
    other = ~(home_win | away_02)
    
    pnl = np.zeros(n_bets)
    pnl[home_win] = s_h * (odd_h[home_win] - 1.0) * (1.0 - commission) + s_cs * (1.0 - commission)
    pnl[away_02] = -s_h - s_cs * (odd_cs_lay[away_02] - 1.0)
    pnl[other] = -s_h + s_cs * (1.0 - commission)
    
    total_pnl = pnl.sum()
    roi = total_pnl / (n_bets * s_h) * 100.0
    win_rate = (pnl > 0.0).sum() / n_bets * 100.0
    
    banca_history = 1000.0 + np.cumsum(pnl)
    peak = np.maximum.accumulate(banca_history)
    max_dd = ((peak - banca_history) / peak * 100.0).max()
    
    return {'bets': n_bets, 'win_rate': win_rate, 'pnl': total_pnl, 'roi': roi, 'max_dd': max_dd}

def analyze_combo_3_vectorized(df, odd_d_min=2.80, odd_d_max=4.20, ratio=0.20, commission=0.05):
    s_d = 100.0
    s_lay = s_d * ratio
    o10_col = 'Odd_CS_1x0_Lay'
    o01_col = 'Odd_CS_0x1_Lay'
    
    if o10_col not in df.columns or o01_col not in df.columns:
        return None
        
    mask = (df['Odd_D_Back'] >= odd_d_min) & (df['Odd_D_Back'] <= odd_d_max) & (df[o10_col] > 1.0) & (df[o01_col] > 1.0)
    sub = df[mask]
    n_bets = len(sub)
    if n_bets < 100:
        return None
        
    gh = sub['GH'].values
    ga = sub['GA'].values
    odd_d = sub['Odd_D_Back'].values
    o_10 = sub[o10_col].values
    o_01 = sub[o01_col].values
    
    draw = gh == ga
    win_10 = (gh == 1) & (ga == 0)
    win_01 = (gh == 0) & (ga == 1)
    other = ~(draw | win_10 | win_01)
    
    pnl = np.zeros(n_bets)
    pnl[draw] = s_d * (odd_d[draw] - 1.0) * (1.0 - commission) + 2.0 * s_lay * (1.0 - commission)
    pnl[win_10] = -s_d - s_lay * (o_10[win_10] - 1.0) + s_lay * (1.0 - commission)
    pnl[win_01] = -s_d + s_lay * (1.0 - commission) - s_lay * (o_01[win_01] - 1.0)
    pnl[other] = -s_d + 2.0 * s_lay * (1.0 - commission)
    
    total_pnl = pnl.sum()
    roi = total_pnl / (n_bets * s_d) * 100.0
    win_rate = (pnl > 0.0).sum() / n_bets * 100.0
    
    banca_history = 1000.0 + np.cumsum(pnl)
    peak = np.maximum.accumulate(banca_history)
    max_dd = ((peak - banca_history) / peak * 100.0).max()
    
    return {'bets': n_bets, 'win_rate': win_rate, 'pnl': total_pnl, 'roi': roi, 'max_dd': max_dd}

def analyze_combo_4_vectorized(df, odd_over15_max=1.35, ratio=0.25, commission=0.05):
    s_o = 100.0
    s_lay = s_o * ratio
    o15_col = 'Odd_Over15_FT_Back'
    o00_col = 'Odd_CS_0x0_Lay'
    o11_col = 'Odd_CS_1x1_Lay'
    
    if o15_col not in df.columns or o00_col not in df.columns or o11_col not in df.columns:
        return None
        
    mask = (df[o15_col] <= odd_over15_max) & (df[o00_col] > 1.0) & (df[o11_col] > 1.0) & (df[o15_col] > 1.0)
    sub = df[mask]
    n_bets = len(sub)
    if n_bets < 100:
        return None
        
    gh = sub['GH'].values
    ga = sub['GA'].values
    odd_o = sub[o15_col].values
    o_00 = sub[o00_col].values
    o_11 = sub[o11_col].values
    
    tot_goals = gh + ga
    over_15 = tot_goals >= 2
    
    draw_11 = (gh == 1) & (ga == 1)
    over_15_other = over_15 & (~draw_11)
    
    draw_00 = (gh == 0) & (ga == 0)
    under_15_other = (~over_15) & (~draw_00)
    
    pnl = np.zeros(n_bets)
    pnl[draw_11] = s_o * (odd_o[draw_11] - 1.0) * (1.0 - commission) + s_lay * (1.0 - commission) - s_lay * (o_11[draw_11] - 1.0)
    pnl[over_15_other] = s_o * (odd_o[over_15_other] - 1.0) * (1.0 - commission) + 2.0 * s_lay * (1.0 - commission)
    pnl[draw_00] = -s_o - s_lay * (o_00[draw_00] - 1.0) + s_lay * (1.0 - commission)
    pnl[under_15_other] = -s_o + 2.0 * s_lay * (1.0 - commission)
    
    total_pnl = pnl.sum()
    roi = total_pnl / (n_bets * s_o) * 100.0
    win_rate = (pnl > 0.0).sum() / n_bets * 100.0
    
    banca_history = 1000.0 + np.cumsum(pnl)
    peak = np.maximum.accumulate(banca_history)
    max_dd = ((peak - banca_history) / peak * 100.0).max()
    
    return {'bets': n_bets, 'win_rate': win_rate, 'pnl': total_pnl, 'roi': roi, 'max_dd': max_dd}


if __name__ == "__main__":
    df = load_and_prep_data()
    print(f"Running vectorized grid search on {len(df)} games...")
    
    print("\n" + "="*80)
    print("GRID SEARCH RESULTS - HEDGING COMBINATIONS (BETFAIR ODSS)")
    print("="*80)
    
    # 1. Combo 1
    best_roi_1 = -999
    best_params_1 = {}
    for h_max in [1.50, 1.60, 1.70, 1.80, 1.90]:
        for rat in [0.05, 0.10, 0.15, 0.20, 0.25, 0.30]:
            res = analyze_combo_1_vectorized(df, odd_h_min=1.25, odd_h_max=h_max, ratio=rat)
            if res and res['roi'] > best_roi_1 and res['bets'] >= 150:
                best_roi_1 = res['roi']
                best_params_1 = {'odd_h_max': h_max, 'ratio': rat, 'metrics': res}
    print("\n[Combo 1: Back Home + Lay 0x1]")
    if best_params_1:
        print(f"  Best Params: Max Odd Home = {best_params_1['odd_h_max']:.2f} | Lay Ratio = {best_params_1['ratio']:.2f}")
        m = best_params_1['metrics']
        print(f"  Bets: {m['bets']} | Win Rate: {m['win_rate']:.2f}% | P&L: R$ {m['pnl']:.2f} | ROI: {m['roi']:.2f}% | Max DD: {m['max_dd']:.2f}%")
    else:
        print("  No viable settings found.")
        
    # 2. Combo 2
    best_roi_2 = -999
    best_params_2 = {}
    for h_max in [1.50, 1.60, 1.70, 1.80, 1.90]:
        for rat in [0.05, 0.10, 0.15, 0.20, 0.25, 0.30]:
            res = analyze_combo_2_vectorized(df, odd_h_min=1.25, odd_h_max=h_max, ratio=rat)
            if res and res['roi'] > best_roi_2 and res['bets'] >= 150:
                best_roi_2 = res['roi']
                best_params_2 = {'odd_h_max': h_max, 'ratio': rat, 'metrics': res}
    print("\n[Combo 2: Back Home + Lay 0x2]")
    if best_params_2:
        print(f"  Best Params: Max Odd Home = {best_params_2['odd_h_max']:.2f} | Lay Ratio = {best_params_2['ratio']:.2f}")
        m = best_params_2['metrics']
        print(f"  Bets: {m['bets']} | Win Rate: {m['win_rate']:.2f}% | P&L: R$ {m['pnl']:.2f} | ROI: {m['roi']:.2f}% | Max DD: {m['max_dd']:.2f}%")
    else:
        print("  No viable settings found.")
        
    # 3. Combo 3
    best_roi_3 = -999
    best_params_3 = {}
    for d_max in [3.20, 3.40, 3.60, 3.80, 4.00]:
        for rat in [0.05, 0.10, 0.15, 0.20, 0.25]:
            res = analyze_combo_3_vectorized(df, odd_d_min=2.80, odd_d_max=d_max, ratio=rat)
            if res and res['roi'] > best_roi_3 and res['bets'] >= 150:
                best_roi_3 = res['roi']
                best_params_3 = {'odd_d_max': d_max, 'ratio': rat, 'metrics': res}
    print("\n[Combo 3: Back Draw + Lay 1x0 + Lay 0x1]")
    if best_params_3:
        print(f"  Best Params: Max Odd Draw = {best_params_3['odd_d_max']:.2f} | Lay Ratio = {best_params_3['ratio']:.2f}")
        m = best_params_3['metrics']
        print(f"  Bets: {m['bets']} | Win Rate: {m['win_rate']:.2f}% | P&L: R$ {m['pnl']:.2f} | ROI: {m['roi']:.2f}% | Max DD: {m['max_dd']:.2f}%")
    else:
        print("  No viable settings found.")
        
    # 4. Combo 4
    best_roi_4 = -999
    best_params_4 = {}
    for o_max in [1.20, 1.25, 1.30, 1.35]:
        for rat in [0.05, 0.10, 0.15, 0.20, 0.25, 0.30]:
            res = analyze_combo_4_vectorized(df, odd_over15_max=o_max, ratio=rat)
            if res and res['roi'] > best_roi_4 and res['bets'] >= 150:
                best_roi_4 = res['roi']
                best_params_4 = {'odd_over15_max': o_max, 'ratio': rat, 'metrics': res}
    print("\n[Combo 4: Back Over 1.5 + Lay 0x0 + Lay 1x1]")
    if best_params_4:
        print(f"  Best Params: Max Odd Over 1.5 = {best_params_4['odd_over15_max']:.2f} | Lay Ratio = {best_params_4['ratio']:.2f}")
        m = best_params_4['metrics']
        print(f"  Bets: {m['bets']} | Win Rate: {m['win_rate']:.2f}% | P&L: R$ {m['pnl']:.2f} | ROI: {m['roi']:.2f}% | Max DD: {m['max_dd']:.2f}%")
    else:
        print("  No viable settings found.")
