import pandas as pd
import numpy as np
import ast
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

# --- IN-PLAY ODD ESTIMATOR ---
def estimate_inplay_odd_h(lam, mu, t, score_h, score_a):
    rem_fraction = max(0.0, (90.0 - t) / 90.0)
    if rem_fraction <= 0.0:
        return 1.01 if score_h > score_a else 1000.0
        
    lam_rem = lam * rem_fraction
    mu_rem = mu * rem_fraction
    
    px = fast_poisson_pmf(lam_rem)
    py = fast_poisson_pmf(mu_rem)
    grid = np.outer(px, py)
    
    target_diff = score_a - score_h
    p_win_h = 0.0
    for h_rem in range(9):
        for a_rem in range(9):
            if h_rem - a_rem > target_diff:
                p_win_h += grid[h_rem, a_rem]
                
    p_win_h = max(0.0001, min(0.9999, p_win_h))
    return 1.0 / p_win_h

# --- GOAL MINUTES PARSER ---
def parse_goal_minutes(val):
    if pd.isna(val) or val == "" or val == "nan" or str(val).strip() == "[]":
        return []
    try:
        val_str = str(val).strip()
        if not val_str.startswith('['):
            return [int(x.split('+')[0]) for x in val_str.split(',') if x.strip().split('+')[0].isdigit()]
        lst = ast.literal_eval(val_str)
        res = []
        for x in lst:
            x_str = str(x).split('+')[0]
            if x_str.isdigit():
                res.append(int(x_str))
        return sorted(res)
    except:
        return []

# --- BACKTEST CORE ---
def backtest_layhome_inplay(df, exit_minute=45, stop_loss_on_goal=True, commission=0.05, liability_stake=100.0):
    # Ensure columns exist
    df = df.copy()
    df['GH'] = df['Goals_H_FT'].astype(int)
    df['GA'] = df['Goals_A_FT'].astype(int)
    
    pnl_arr = []
    bets = 0
    greens = 0
    
    for idx, row in df.iterrows():
        o_h = float(row['Odd_H_Back'])
        o_u = float(row['Odd_Under25_FT_Back'])
        o_o = float(row['Odd_Over25_FT_Back'])
        o_d = float(row['Odd_D_Back'])
        o_a = float(row['Odd_A_Back'])
        
        if o_h <= 1.0 or o_u <= 1.0 or o_o <= 1.0 or o_d <= 1.0 or o_a <= 1.0:
            continue
            
        try:
            sum_mo = 1.0/o_h + 1.0/o_d + 1.0/o_a
            p_h = (1.0/o_h) / sum_mo
            sum_ou = 1.0/o_u + 1.0/o_o
            p_under = (1.0/o_u) / sum_ou
            G = solve_total_expected_goals(p_under)
            lam, mu = solve_weights(G, p_h)
        except Exception:
            continue
            
        g_min_h = parse_goal_minutes(row.get('Goals_Min_H', '[]'))
        g_min_a = parse_goal_minutes(row.get('Goals_Min_A', '[]'))
        
        all_goals = []
        for m in g_min_h: all_goals.append((m, 'H'))
        for m in g_min_a: all_goals.append((m, 'A'))
        all_goals = sorted(all_goals, key=lambda x: x[0])
        
        stake = liability_stake / (o_h - 1)
        pnl = 0.0
        exited = False
        
        goals_before_exit = [g for g in all_goals if g[0] <= exit_minute]
        
        if len(goals_before_exit) > 0:
            if stop_loss_on_goal:
                min_g, team = goals_before_exit[0]
                sh = 1 if team == 'H' else 0
                sa = 1 if team == 'A' else 0
                odd_inplay = estimate_inplay_odd_h(lam, mu, min_g, sh, sa)
                pnl = stake - (stake * o_h) / odd_inplay
                if pnl > 0:
                    pnl *= (1 - commission)
                exited = True
            else:
                is_home_win = row['GH'] > row['GA']
                if is_home_win:
                    pnl = -liability_stake
                else:
                    pnl = stake * (1.0 - commission)
                exited = True
                
        if not exited:
            odd_inplay = estimate_inplay_odd_h(lam, mu, exit_minute, 0, 0)
            pnl = stake - (stake * o_h) / odd_inplay
            if pnl > 0:
                pnl *= (1 - commission)
                
        pnl_arr.append(pnl)
        bets += 1
        if pnl > 0:
            greens += 1
            
    pnl_arr = np.array(pnl_arr)
    total_pnl = pnl_arr.sum()
    win_rate = greens / bets * 100 if bets > 0 else 0
    roi = total_pnl / (bets * liability_stake) * 100 if bets > 0 else 0
    
    banca_history = 1000.0 + np.cumsum(pnl_arr)
    peak = np.maximum.accumulate(banca_history)
    max_dd = ((peak - banca_history) / peak * 100.0).max() if len(banca_history) > 0 else 0
    
    return {
        'bets': bets,
        'win_rate': win_rate,
        'pnl': total_pnl,
        'roi': roi,
        'max_dd': max_dd
    }

if __name__ == "__main__":
    if not os.path.exists("betfair_historical.csv"):
        print("betfair_historical.csv not found.")
        exit(1)
        
    df = pd.read_csv("betfair_historical.csv")
    df_fav = df[(df['Odd_H_Back'] >= 1.40) & (df['Odd_H_Back'] <= 2.00)].copy()
    print(f"Loaded {len(df_fav)} matches with Home Favorite (Odd 1.40 - 2.00)")
    
    print("\n" + "="*80)
    print("BACKTEST: LAY HOME IN-PLAY CASH OUT STRATEGIES (CORRECTED)")
    print("="*80)
    
    # Test 1: Exit at Minute 45 if 0-0, stop-loss/take-profit on goal
    print("\n[Strategy 1: Exit at Minute 45 (or on Goal)]")
    res1 = backtest_layhome_inplay(df_fav, exit_minute=45, stop_loss_on_goal=True)
    print(f"  Bets: {res1['bets']} | Win Rate: {res1['win_rate']:.2f}%")
    print(f"  PnL: R$ {res1['pnl']:.2f} | ROI (on liability): {res1['roi']:.2f}% | Max DD: {res1['max_dd']:.2f}%")
    
    # Test 2: Exit at Minute 30 if 0-0, stop-loss/take-profit on goal
    print("\n[Strategy 2: Exit at Minute 30 (or on Goal)]")
    res2 = backtest_layhome_inplay(df_fav, exit_minute=30, stop_loss_on_goal=True)
    print(f"  Bets: {res2['bets']} | Win Rate: {res2['win_rate']:.2f}%")
    print(f"  PnL: R$ {res2['pnl']:.2f} | ROI: {res2['roi']:.2f}% | Max DD: {res2['max_dd']:.2f}%")
    
    # Test 3: Exit at Minute 45 if 0-0, NO stop-loss on goal (let run to FT if goal occurs)
    print("\n[Strategy 3: Exit at Minute 45 (if 0-0 only, else let run to Full Time)]")
    res3 = backtest_layhome_inplay(df_fav, exit_minute=45, stop_loss_on_goal=False)
    print(f"  Bets: {res3['bets']} | Win Rate: {res3['win_rate']:.2f}%")
    print(f"  PnL: R$ {res3['pnl']:.2f} | ROI: {res3['roi']:.2f}% | Max DD: {res3['max_dd']:.2f}%")
