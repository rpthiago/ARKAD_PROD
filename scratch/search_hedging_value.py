import pandas as pd
import numpy as np
import os

# --- FAST POISSON MATHEMATICS ---
K_FACTORIAL = np.array([1.0, 1.0, 2.0, 6.0, 24.0, 120.0, 720.0, 5040.0, 40320.0])
K_ARR = np.arange(9)

def fast_poisson_pmf(lam):
    if lam <= 0.0:
        res = np.zeros(9)
        res[0] = 1.0
        return res
    return (lam ** K_ARR) * np.exp(-lam) / K_FACTORIAL

def solve_total_expected_goals(p_under25):
    p_under25 = max(0.001, min(0.999, p_under25))
    low, high = 0.0, 15.0
    for _ in range(20):
        mid = (low + high) / 2
        val = np.exp(-mid) * (1.0 + mid + (mid**2)/2.0)
        if val > p_under25:
            low = mid
        else:
            high = mid
    return (low + high) / 2

def solve_weights(G, p_home):
    low, high = 0.0, 1.0
    for _ in range(15):
        mid = (low + high) / 2
        lam = G * mid
        mu = G * (1.0 - mid)
        
        px = fast_poisson_pmf(lam)
        py = fast_poisson_pmf(mu)
        grid = np.outer(px, py)
        
        p_h_calc = 0.0
        for h in range(9):
            for a in range(h):
                p_h_calc += grid[h, a]
                
        if p_h_calc < p_home:
            low = mid
        else:
            high = mid
    w = (low + high) / 2
    return G * w, G * (1.0 - w)

def get_poisson_probs_for_all(df):
    print("Pre-calculating Poisson probabilities for all matches...")
    n = len(df)
    lams = np.zeros(n)
    mus = np.zeros(n)
    
    o_h = df['Odd_H_Back'].values
    o_d = df['Odd_D_Back'].values
    o_a = df['Odd_A_Back'].values
    o_u = df['Odd_Under25_FT_Back'].values
    o_o = df['Odd_Over25_FT_Back'].values
    
    # Vectorized check for valid odds
    valid_mask = (o_h > 1.0) & (o_d > 1.0) & (o_a > 1.0) & (o_u > 1.0) & (o_o > 1.0)
    
    for i in range(n):
        if not valid_mask[i]:
            continue
        try:
            sum_mo = 1.0/o_h[i] + 1.0/o_d[i] + 1.0/o_a[i]
            p_h = (1.0/o_h[i]) / sum_mo
            
            sum_ou = 1.0/o_u[i] + 1.0/o_o[i]
            p_under = (1.0/o_u[i]) / sum_ou
            
            G = solve_total_expected_goals(p_under)
            lam, mu = solve_weights(G, p_h)
            lams[i] = lam
            mus[i] = mu
        except Exception:
            pass
            
    # Calculate specific Correct Score probabilities
    p_00 = np.zeros(n)
    p_10 = np.zeros(n)
    p_01 = np.zeros(n)
    p_11 = np.zeros(n)
    p_02 = np.zeros(n)
    p_20 = np.zeros(n)
    p_21 = np.zeros(n)
    p_12 = np.zeros(n)
    p_22 = np.zeros(n)
    p_home_win = np.zeros(n)
    p_draw = np.zeros(n)
    p_away_win = np.zeros(n)
    
    for i in range(n):
        if lams[i] == 0.0 and mus[i] == 0.0:
            continue
        px = fast_poisson_pmf(lams[i])
        py = fast_poisson_pmf(mus[i])
        grid = np.outer(px, py)
        
        p_00[i] = grid[0, 0]
        p_10[i] = grid[1, 0]
        p_01[i] = grid[0, 1]
        p_11[i] = grid[1, 1]
        p_02[i] = grid[0, 2]
        p_20[i] = grid[2, 0]
        p_21[i] = grid[2, 1]
        p_12[i] = grid[1, 2]
        p_22[i] = grid[2, 2]
        
        # Win sums
        for h in range(9):
            for a in range(9):
                p_val = grid[h, a]
                if h > a:
                    p_home_win[i] += p_val
                elif h < a:
                    p_away_win[i] += p_val
                else:
                    p_draw[i] += p_val
                    
    df['P_00'] = p_00
    df['P_10'] = p_10
    df['P_01'] = p_01
    df['P_11'] = p_11
    df['P_02'] = p_02
    df['P_20'] = p_20
    df['P_21'] = p_21
    df['P_12'] = p_12
    df['P_22'] = p_22
    df['P_H'] = p_home_win
    df['P_D'] = p_draw
    df['P_A'] = p_away_win
    
    return df

# --- VALUE HEDGING SIMULATION ---

def test_combo_1_value(df, ratio=0.15, min_ev=0.05, commission=0.05):
    # Combo 1: Back Home (100) + Lay 0x1 (100*ratio)
    cs_col = 'Odd_CS_0x1_Lay'
    if cs_col not in df.columns:
        return None
        
    mask = (df[cs_col] > 1.0) & (df['P_H'] > 0.0)
    sub = df[mask].copy()
    n_total = len(sub)
    
    s_h = 100.0
    s_cs = s_h * ratio
    
    # Calculate EV for each row
    p_h = sub['P_H'].values
    p_01 = sub['P_01'].values
    p_other = 1.0 - p_h - p_01
    
    odd_h = sub['Odd_H_Back'].values
    odd_cs_lay = sub[cs_col].values
    
    # PnL if results happen
    w_h = s_h * (odd_h - 1.0) * (1.0 - commission) + s_cs * (1.0 - commission)
    l_01 = -s_h - s_cs * (odd_cs_lay - 1.0)
    w_other = -s_h + s_cs * (1.0 - commission)
    
    ev = p_h * w_h + p_01 * l_01 + p_other * w_other
    
    # Apply EV filter (EV / s_h >= min_ev)
    valid_bets = ev / s_h >= min_ev
    if valid_bets.sum() < 50:
        return None
        
    # Run backtest on matched bets
    gh = sub['GH'].values[valid_bets]
    ga = sub['GA'].values[valid_bets]
    odd_h_b = odd_h[valid_bets]
    odd_cs_l = odd_cs_lay[valid_bets]
    
    n_bets = len(gh)
    pnl = np.zeros(n_bets)
    
    home_win = gh > ga
    away_01 = (gh == 0) & (ga == 1)
    other = ~(home_win | away_01)
    
    pnl[home_win] = s_h * (odd_h_b[home_win] - 1.0) * (1.0 - commission) + s_cs * (1.0 - commission)
    pnl[away_01] = -s_h - s_cs * (odd_cs_l[away_01] - 1.0)
    pnl[other] = -s_h + s_cs * (1.0 - commission)
    
    total_pnl = pnl.sum()
    roi = total_pnl / (n_bets * s_h) * 100.0
    win_rate = (pnl > 0.0).sum() / n_bets * 100.0
    
    banca_history = 1000.0 + np.cumsum(pnl)
    peak = np.maximum.accumulate(banca_history)
    max_dd = ((peak - banca_history) / peak * 100.0).max()
    
    return {'bets': n_bets, 'win_rate': win_rate, 'pnl': total_pnl, 'roi': roi, 'max_dd': max_dd}


def test_combo_3_value(df, ratio=0.20, min_ev=0.05, commission=0.05):
    # Combo 3: Back Draw (100) + Lay 1x0 (100*ratio) + Lay 0x1 (100*ratio)
    o10_col = 'Odd_CS_1x0_Lay'
    o01_col = 'Odd_CS_0x1_Lay'
    if o10_col not in df.columns or o01_col not in df.columns:
        return None
        
    mask = (df[o10_col] > 1.0) & (df[o01_col] > 1.0) & (df['P_D'] > 0.0)
    sub = df[mask].copy()
    n_total = len(sub)
    
    s_d = 100.0
    s_lay = s_d * ratio
    
    # Calculate EV
    p_d = sub['P_D'].values
    p_10 = sub['P_10'].values
    p_01 = sub['P_01'].values
    p_other = 1.0 - p_d - p_10 - p_01
    
    odd_d = sub['Odd_D_Back'].values
    o_10 = sub[o10_col].values
    o_01 = sub[o01_col].values
    
    w_draw = s_d * (odd_d - 1.0) * (1.0 - commission) + 2.0 * s_lay * (1.0 - commission)
    l_10 = -s_d - s_lay * (o_10 - 1.0) + s_lay * (1.0 - commission)
    l_01 = -s_d + s_lay * (1.0 - commission) - s_lay * (o_01 - 1.0)
    w_other = -s_d + 2.0 * s_lay * (1.0 - commission)
    
    ev = p_d * w_draw + p_10 * l_10 + p_01 * l_01 + p_other * w_other
    
    valid_bets = ev / s_d >= min_ev
    if valid_bets.sum() < 50:
        return None
        
    gh = sub['GH'].values[valid_bets]
    ga = sub['GA'].values[valid_bets]
    odd_d_b = odd_d[valid_bets]
    o_10_b = o_10[valid_bets]
    o_01_b = o_01[valid_bets]
    
    n_bets = len(gh)
    pnl = np.zeros(n_bets)
    
    draw = gh == ga
    win_10 = (gh == 1) & (ga == 0)
    win_01 = (gh == 0) & (ga == 1)
    other = ~(draw | win_10 | win_01)
    
    pnl[draw] = s_d * (odd_d_b[draw] - 1.0) * (1.0 - commission) + 2.0 * s_lay * (1.0 - commission)
    pnl[win_10] = -s_d - s_lay * (o_10_b[win_10] - 1.0) + s_lay * (1.0 - commission)
    pnl[win_01] = -s_d + s_lay * (1.0 - commission) - s_lay * (o_01_b[win_01] - 1.0)
    pnl[other] = -s_d + 2.0 * s_lay * (1.0 - commission)
    
    total_pnl = pnl.sum()
    roi = total_pnl / (n_bets * s_d) * 100.0
    win_rate = (pnl > 0.0).sum() / n_bets * 100.0
    banca_history = 1000.0 + np.cumsum(pnl)
    peak = np.maximum.accumulate(banca_history)
    max_dd = ((peak - banca_history) / peak * 100.0).max()
    
    return {'bets': n_bets, 'win_rate': win_rate, 'pnl': total_pnl, 'roi': roi, 'max_dd': max_dd}


if __name__ == "__main__":
    df_raw = pd.read_csv("betfair_historical.csv")
    df = get_poisson_probs_for_all(df_raw)
    
    print("\n" + "="*80)
    print("GRID SEARCH - VALUE-FILTERED HEDGING COMBINATIONS (EV >= 5%)")
    print("="*80)
    
    # Test Combo 1 with Value
    print("\n[Combo 1: Back Home + Lay 0x1 (Value-Filtered)]")
    best_roi_1 = -999
    best_params_1 = {}
    for rat in [0.05, 0.10, 0.15, 0.20, 0.25, 0.30]:
        for ev_thresh in [0.02, 0.05, 0.08]:
            res = test_combo_1_value(df, ratio=rat, min_ev=ev_thresh)
            if res and res['roi'] > best_roi_1:
                best_roi_1 = res['roi']
                best_params_1 = {'ratio': rat, 'ev': ev_thresh, 'metrics': res}
    if best_params_1:
        m = best_params_1['metrics']
        print(f"  Best Params: Lay Ratio = {best_params_1['ratio']:.2f} | EV Threshold = +{best_params_1['ev']*100:.0f}%")
        print(f"  Bets: {m['bets']} | Win Rate: {m['win_rate']:.2f}% | P&L: R$ {m['pnl']:.2f} | ROI: {m['roi']:.2f}% | Max DD: {m['max_dd']:.2f}%")
    else:
        print("  No viable settings found.")
        
    # Test Combo 3 with Value
    print("\n[Combo 3: Back Draw + Lay 1x0 + Lay 0x1 (Value-Filtered)]")
    best_roi_3 = -999
    best_params_3 = {}
    for rat in [0.05, 0.10, 0.15, 0.20, 0.25]:
        for ev_thresh in [0.02, 0.05, 0.08]:
            res = test_combo_3_value(df, ratio=rat, min_ev=ev_thresh)
            if res and res['roi'] > best_roi_3:
                best_roi_3 = res['roi']
                best_params_3 = {'ratio': rat, 'ev': ev_thresh, 'metrics': res}
    if best_params_3:
        m = best_params_3['metrics']
        print(f"  Best Params: Lay Ratio = {best_params_3['ratio']:.2f} | EV Threshold = +{best_params_3['ev']*100:.0f}%")
        print(f"  Bets: {m['bets']} | Win Rate: {m['win_rate']:.2f}% | P&L: R$ {m['pnl']:.2f} | ROI: {m['roi']:.2f}% | Max DD: {m['max_dd']:.2f}%")
    else:
        print("  No viable settings found.")
