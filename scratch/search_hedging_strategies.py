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

def analyze_combo_1(df, odd_h_min=1.40, odd_h_max=1.85, ratio=0.15, commission=0.05):
    # Combo 1: Back Home (Stake=100) + Lay 0x1 (Stake = 100 * ratio)
    # We win if Home wins, hedge draws and other away wins, lose big on 0-1.
    sub = df[(df['Odd_H_Back'] >= odd_h_min) & (df['Odd_H_Back'] <= odd_h_max)].copy()
    
    cs_col = 'Odd_CS_0x1_Lay' # Note: we use Lay odd for Laying
    if cs_col not in sub.columns:
        return None
    sub = sub.dropna(subset=[cs_col]).copy()
    
    n_bets = len(sub)
    if n_bets < 100:
        return None
        
    s_h = 100.0
    s_cs = s_h * ratio
    
    pnl_arr = []
    greens = 0
    for idx, row in sub.iterrows():
        gh, ga = row['GH'], row['GA']
        odd_h = row['Odd_H_Back']
        odd_cs_lay = row[cs_col]
        
        if odd_cs_lay <= 1.0:
            continue
            
        # PnL calculations
        if gh > ga: # Home Win
            # Back Home wins, Lay 0x1 wins
            win_h = s_h * (odd_h - 1) * (1 - commission)
            win_cs = s_cs * (1 - commission)
            pnl = win_h + win_cs
            greens += 1
        elif gh == 0 and ga == 1: # Away 0-1
            # Back Home loses, Lay 0x1 loses (liability)
            loss_h = -s_h
            loss_cs = -s_cs * (odd_cs_lay - 1)
            pnl = loss_h + loss_cs
        else: # Draw or other Away Win
            # Back Home loses, Lay 0x1 wins
            loss_h = -s_h
            win_cs = s_cs * (1 - commission)
            pnl = loss_h + win_cs
            if pnl > 0:
                greens += 1
                
        pnl_arr.append(pnl)
        
    if not pnl_arr:
        return None
        
    pnl_arr = np.array(pnl_arr)
    total_pnl = pnl_arr.sum()
    total_invested = n_bets * s_h
    roi = total_pnl / total_invested * 100
    win_rate = (pnl_arr > 0).sum() / len(pnl_arr) * 100
    
    banca_history = 1000.0 + np.cumsum(pnl_arr)
    peak = np.maximum.accumulate(banca_history)
    drawdowns = (peak - banca_history) / peak * 100
    max_dd = drawdowns.max()
    
    return {
        'bets': len(pnl_arr),
        'win_rate': win_rate,
        'pnl': total_pnl,
        'roi': roi,
        'max_dd': max_dd
    }

def analyze_combo_2(df, odd_h_min=1.40, odd_h_max=1.85, ratio=0.15, commission=0.05):
    # Combo 2: Back Home (Stake=100) + Lay 0x2 (Stake = 100 * ratio)
    sub = df[(df['Odd_H_Back'] >= odd_h_min) & (df['Odd_H_Back'] <= odd_h_max)].copy()
    cs_col = 'Odd_CS_0x2_Lay'
    if cs_col not in sub.columns:
        return None
    sub = sub.dropna(subset=[cs_col]).copy()
    
    n_bets = len(sub)
    if n_bets < 100:
        return None
        
    s_h = 100.0
    s_cs = s_h * ratio
    pnl_arr = []
    
    for idx, row in sub.iterrows():
        gh, ga = row['GH'], row['GA']
        odd_h = row['Odd_H_Back']
        odd_cs_lay = row[cs_col]
        
        if odd_cs_lay <= 1.0:
            continue
            
        if gh > ga:
            pnl = s_h * (odd_h - 1) * (1 - commission) + s_cs * (1 - commission)
        elif gh == 0 and ga == 2:
            pnl = -s_h - s_cs * (odd_cs_lay - 1)
        else:
            pnl = -s_h + s_cs * (1 - commission)
        pnl_arr.append(pnl)
        
    pnl_arr = np.array(pnl_arr)
    total_pnl = pnl_arr.sum()
    roi = total_pnl / (len(pnl_arr) * s_h) * 100
    win_rate = (pnl_arr > 0).sum() / len(pnl_arr) * 100
    banca_history = 1000.0 + np.cumsum(pnl_arr)
    peak = np.maximum.accumulate(banca_history)
    max_dd = ((peak - banca_history) / peak * 100).max()
    
    return {'bets': len(pnl_arr), 'win_rate': win_rate, 'pnl': total_pnl, 'roi': roi, 'max_dd': max_dd}

def analyze_combo_3(df, odd_d_min=3.0, odd_d_max=4.20, ratio=0.20, commission=0.05):
    # Combo 3: Back Draw (Stake=100) + Lay 1x0 (Stake=100*ratio) + Lay 0x1 (Stake=100*ratio)
    # We win on draw, hedge 1-0 and 0-1, win on high-scoring wins.
    sub = df[(df['Odd_D_Back'] >= odd_d_min) & (df['Odd_D_Back'] <= odd_d_max)].copy()
    if 'Odd_CS_1x0_Lay' not in sub.columns or 'Odd_CS_0x1_Lay' not in sub.columns:
        return None
    sub = sub.dropna(subset=['Odd_CS_1x0_Lay', 'Odd_CS_0x1_Lay']).copy()
    
    n_bets = len(sub)
    if n_bets < 100:
        return None
        
    s_d = 100.0
    s_lay = s_d * ratio
    pnl_arr = []
    
    for idx, row in sub.iterrows():
        gh, ga = row['GH'], row['GA']
        odd_d = row['Odd_D_Back']
        o_10 = row['Odd_CS_1x0_Lay']
        o_01 = row['Odd_CS_0x1_Lay']
        
        if o_10 <= 1.0 or o_01 <= 1.0:
            continue
            
        if gh == ga: # Draw
            # Back Draw wins, both Lays win
            pnl = s_d * (odd_d - 1) * (1 - commission) + 2 * s_lay * (1 - commission)
        elif gh == 1 and ga == 0: # 1-0
            # Back Draw loses, Lay 1x0 loses, Lay 0x1 wins
            pnl = -s_d - s_lay * (o_10 - 1) + s_lay * (1 - commission)
        elif gh == 0 and ga == 1: # 0-1
            # Back Draw loses, Lay 1x0 wins, Lay 0x1 loses
            pnl = -s_d + s_lay * (1 - commission) - s_lay * (o_01 - 1)
        else: # High scoring win (2-0, 0-2, 2-1, 1-2, etc.)
            # Back Draw loses, both Lays win
            pnl = -s_d + 2 * s_lay * (1 - commission)
            
        pnl_arr.append(pnl)
        
    pnl_arr = np.array(pnl_arr)
    total_pnl = pnl_arr.sum()
    roi = total_pnl / (len(pnl_arr) * s_d) * 100
    win_rate = (pnl_arr > 0).sum() / len(pnl_arr) * 100
    banca_history = 1000.0 + np.cumsum(pnl_arr)
    peak = np.maximum.accumulate(banca_history)
    max_dd = ((peak - banca_history) / peak * 100).max()
    
    return {'bets': len(pnl_arr), 'win_rate': win_rate, 'pnl': total_pnl, 'roi': roi, 'max_dd': max_dd}

def analyze_combo_4(df, odd_over15_max=1.35, ratio=0.25, commission=0.05):
    # Combo 4: Back Over 1.5 (Stake=100) + Lay 0x0 (Stake=100*ratio) + Lay 1x1 (Stake=100*ratio)
    # We want goals. Over 1.5 covers 2+ goals. Lay 0x0 covers 0-0, Lay 1x1 covers 1-1.
    # Wait, if 0-0 happens: Over 1.5 loses (-100), Lay 0x0 loses (-ratio*(o_00-1)), Lay 1x1 wins (+ratio).
    # This is a very aggressive coverage strategy for high goal matches.
    if 'Odd_Over15_FT_Back' not in df.columns or 'Odd_CS_0x0_Lay' not in df.columns or 'Odd_CS_1x1_Lay' not in df.columns:
        return None
        
    sub = df[(df['Odd_Over15_FT_Back'] <= odd_over15_max)].copy()
    sub = sub.dropna(subset=['Odd_CS_0x0_Lay', 'Odd_CS_1x1_Lay']).copy()
    
    n_bets = len(sub)
    if n_bets < 100:
        return None
        
    s_o = 100.0
    s_lay = s_o * ratio
    pnl_arr = []
    
    for idx, row in sub.iterrows():
        gh, ga = row['GH'], row['GA']
        odd_o = row['Odd_Over15_FT_Back']
        o_00 = row['Odd_CS_0x0_Lay']
        o_11 = row['Odd_CS_1x1_Lay']
        
        if o_00 <= 1.0 or o_11 <= 1.0 or odd_o <= 1.0:
            continue
            
        tot_goals = gh + ga
        if tot_goals >= 2: # Over 1.5 wins
            if gh == 1 and ga == 1: # 1-1
                # Over 1.5 wins, Lay 0x0 wins, Lay 1x1 loses
                pnl = s_o * (odd_o - 1) * (1 - commission) + s_lay * (1 - commission) - s_lay * (o_11 - 1)
            else: # Other 2+ goals (2-0, 0-2, 2-1, 3-0, etc.)
                # Over 1.5 wins, both Lays win
                pnl = s_o * (odd_o - 1) * (1 - commission) + 2 * s_lay * (1 - commission)
        else: # Under 1.5 goals (1-0, 0-1, 0-0)
            if gh == 0 and ga == 0: # 0-0
                # Over 1.5 loses, Lay 0x0 loses, Lay 1x1 wins
                pnl = -s_o - s_lay * (o_00 - 1) + s_lay * (1 - commission)
            else: # 1-0 or 0-1
                # Over 1.5 loses, both Lays win
                pnl = -s_o + 2 * s_lay * (1 - commission)
                
        pnl_arr.append(pnl)
        
    pnl_arr = np.array(pnl_arr)
    total_pnl = pnl_arr.sum()
    roi = total_pnl / (len(pnl_arr) * s_o) * 100
    win_rate = (pnl_arr > 0).sum() / len(pnl_arr) * 100
    banca_history = 1000.0 + np.cumsum(pnl_arr)
    peak = np.maximum.accumulate(banca_history)
    max_dd = ((peak - banca_history) / peak * 100).max()
    
    return {'bets': len(pnl_arr), 'win_rate': win_rate, 'pnl': total_pnl, 'roi': roi, 'max_dd': max_dd}


if __name__ == "__main__":
    df = load_and_prep_data()
    print(f"Running grid search on {len(df)} games...")
    
    print("\n=== GRID SEARCH FOR COMBO 1 (Back Home + Lay 0x1) ===")
    best_roi = -999
    best_params = {}
    for h_max in [1.50, 1.60, 1.70, 1.80, 1.90]:
        for rat in [0.05, 0.10, 0.15, 0.20, 0.25, 0.30]:
            res = analyze_combo_1(df, odd_h_min=1.25, odd_h_max=h_max, ratio=rat)
            if res and res['roi'] > best_roi and res['bets'] >= 150:
                best_roi = res['roi']
                best_params = {'odd_h_max': h_max, 'ratio': rat, 'metrics': res}
    print("Best Combo 1:", best_params)
    
    print("\n=== GRID SEARCH FOR COMBO 2 (Back Home + Lay 0x2) ===")
    best_roi = -999
    best_params = {}
    for h_max in [1.50, 1.60, 1.70, 1.80, 1.90]:
        for rat in [0.05, 0.10, 0.15, 0.20, 0.25, 0.30]:
            res = analyze_combo_2(df, odd_h_min=1.25, odd_h_max=h_max, ratio=rat)
            if res and res['roi'] > best_roi and res['bets'] >= 150:
                best_roi = res['roi']
                best_params = {'odd_h_max': h_max, 'ratio': rat, 'metrics': res}
    print("Best Combo 2:", best_params)
    
    print("\n=== GRID SEARCH FOR COMBO 3 (Back Draw + Lay 1x0 + Lay 0x1) ===")
    best_roi = -999
    best_params = {}
    for d_max in [3.20, 3.40, 3.60, 3.80, 4.00]:
        for rat in [0.05, 0.10, 0.15, 0.20, 0.25]:
            res = analyze_combo_3(df, odd_d_min=2.80, odd_d_max=d_max, ratio=rat)
            if res and res['roi'] > best_roi and res['bets'] >= 150:
                best_roi = res['roi']
                best_params = {'odd_d_max': d_max, 'ratio': rat, 'metrics': res}
    print("Best Combo 3:", best_params)
    
    print("\n=== GRID SEARCH FOR COMBO 4 (Back Over 1.5 + Lay 0x0 + Lay 1x1) ===")
    best_roi = -999
    best_params = {}
    for o_max in [1.20, 1.25, 1.30, 1.35]:
        for rat in [0.05, 0.10, 0.15, 0.20, 0.25, 0.30]:
            res = analyze_combo_4(df, odd_over15_max=o_max, ratio=rat)
            if res and res['roi'] > best_roi and res['bets'] >= 150:
                best_roi = res['roi']
                best_params = {'odd_over15_max': o_max, 'ratio': rat, 'metrics': res}
    print("Best Combo 4:", best_params)
