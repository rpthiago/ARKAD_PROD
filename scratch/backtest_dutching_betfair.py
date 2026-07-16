import pandas as pd
import numpy as np
import os

# --- FAST POISSON MATHEMATICS (PURE NUMPY, NO SCIPY OVERHEAD) ---
K_FACTORIAL = np.array([1.0, 1.0, 2.0, 6.0, 24.0, 120.0, 720.0, 5040.0, 40320.0])
K_ARR = np.arange(9)

def fast_poisson_pmf(lam):
    # lam can be a float, handles lam=0 safely
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
        
        # Sum where h > a
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

def get_poisson_correct_scores(lam, mu):
    px = fast_poisson_pmf(lam)
    py = fast_poisson_pmf(mu)
    return np.outer(px, py)

# --- MAP ACTUAL GOALS TO CORRECT SCORE STRING ---
def get_actual_score_string(h, a):
    if pd.isna(h) or pd.isna(a):
        return None
    h, a = int(h), int(a)
    if h == a:
        if h <= 3:
            return f"{h}x{a}"
        else:
            return "Goleada_D"
    elif h > a:
        if h <= 3 and a <= 3:
            return f"{h}x{a}"
        else:
            return "Goleada_H"
    else: # h < a
        if h <= 3 and a <= 3:
            return f"{h}x{a}"
        else:
            return "Goleada_A"

# --- BACKTEST RUNNER ---
def run_dutching_backtest(
    df_raw,
    selections,
    commission=0.05,
    banca_inicial=1000.0,
    stake_total=100.0,
    min_ev=0.0,          # Filter by Poisson EV
    odd_under_max=None,  # Filter by Max Under 2.5 Odd
    odd_h_min=None,      # Home win odd range filters
    odd_h_max=None,
    use_poisson_filter=False
):
    df = df_raw.copy()
    
    # 1. Clean and parse data
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Goals_H_FT', 'Goals_A_FT', 'Odd_H_Back', 'Odd_D_Back', 'Odd_A_Back', 'Odd_Under25_FT_Back', 'Odd_Over25_FT_Back']).copy()
    df['GH'] = df['Goals_H_FT'].astype(int)
    df['GA'] = df['Goals_A_FT'].astype(int)
    
    # Map scorelines to actual score string
    df['Actual_CS'] = df.apply(lambda r: get_actual_score_string(r['GH'], r['GA']), axis=1)
    
    # Required CS columns
    cs_back_cols = [f"Odd_CS_{sel}_Back" for sel in selections]
    df = df.dropna(subset=cs_back_cols).copy()
    
    results = []
    banca = banca_inicial
    banca_history = [banca_inicial]
    
    # For metrics
    total_analyzed = len(df)
    bets_placed = 0
    greens = 0
    reds = 0
    total_invested = 0.0
    total_pnl = 0.0
    
    for idx, row in df.iterrows():
        # Get odds for selections
        odds = np.array([float(row[f"Odd_CS_{sel}_Back"]) for sel in selections])
        
        # Check if any odd is invalid (<= 1.0)
        if np.any(odds <= 1.0):
            continue
            
        # Implied probability of bookmaker (overround)
        T = np.sum(1.0 / odds)
        
        # If total probability is >= 1.0, Dutching is not viable/profitable
        if T >= 1.0:
            continue
            
        # --- APPLY FILTERS ---
        # 1. Under 2.5 Odd filter
        if odd_under_max is not None:
            if float(row['Odd_Under25_FT_Back']) > odd_under_max:
                continue
                
        # 2. Home Odd range filter
        if odd_h_min is not None and float(row['Odd_H_Back']) < odd_h_min:
            continue
        if odd_h_max is not None and float(row['Odd_H_Back']) > odd_h_max:
            continue
            
        # 3. Poisson Expected Value filter
        if use_poisson_filter:
            try:
                # Target fair probabilities
                sum_mo = 1.0/row['Odd_H_Back'] + 1.0/row['Odd_D_Back'] + 1.0/row['Odd_A_Back']
                p_h = (1.0/row['Odd_H_Back']) / sum_mo
                
                sum_ou = 1.0/row['Odd_Under25_FT_Back'] + 1.0/row['Odd_Over25_FT_Back']
                p_under = (1.0/row['Odd_Under25_FT_Back']) / sum_ou
                
                # Fit Poisson
                G = solve_total_expected_goals(p_under)
                lam, mu = solve_weights(G, p_h)
                grid = get_poisson_correct_scores(lam, mu)
                
                # Calculate probability of our selections
                p_model = 0.0
                for sel in selections:
                    if sel == "Goleada_H":
                        p_all_h = 0.0
                        for h in range(1, 9):
                            for a in range(h):
                                p_all_h += grid[h, a]
                        p_std_h = grid[1,0]+grid[2,0]+grid[2,1]+grid[3,0]+grid[3,1]+grid[3,2]
                        p_model += max(0.0, p_all_h - p_std_h)
                    elif sel == "Goleada_A":
                        p_all_a = 0.0
                        for a in range(1, 9):
                            for h in range(a):
                                p_all_a += grid[h, a]
                        p_std_a = grid[0,1]+grid[0,2]+grid[1,2]+grid[0,3]+grid[1,3]+grid[2,3]
                        p_model += max(0.0, p_all_a - p_std_a)
                    elif sel == "Goleada_D":
                        p_all_d = 0.0
                        for d in range(9):
                            p_all_d += grid[d, d]
                        p_std_d = grid[0,0]+grid[1,1]+grid[2,2]+grid[3,3]
                        p_model += max(0.0, p_all_d - p_std_d)
                    else:
                        h_s, a_s = map(int, sel.split('x'))
                        p_model += grid[h_s, a_s]
                        
                # Expected Value
                EV = (p_model / T) - 1.0
                if EV < min_ev:
                    continue
            except Exception:
                continue
        else:
            EV = 0.0
            
        # --- PLACE BET ---
        bets_placed += 1
        total_invested += stake_total
        
        # Calculate individual stakes
        stakes = stake_total / (odds * T)
        
        # Check result
        actual_cs = row['Actual_CS']
        is_green = actual_cs in selections
        
        if is_green:
            greens += 1
            gross_return = stake_total / T
            pnl = (gross_return - stake_total) * (1.0 - commission)
        else:
            reds += 1
            pnl = -stake_total
            
        banca += pnl
        total_pnl += pnl
        banca_history.append(banca)
        
        results.append({
            'Date': row['Date'],
            'League': row['League'],
            'Home': row['Home'],
            'Away': row['Away'],
            'Score': f"{row['GH']}-{row['GA']}",
            'Odds': [round(o, 2) for o in odds],
            'Total_Prob_T': round(T, 3),
            'EV': round(EV, 3) if use_poisson_filter else None,
            'Result': 'GREEN' if is_green else 'RED',
            'P&L': round(pnl, 2),
            'Banca': round(banca, 2)
        })
        
    df_results = pd.DataFrame(results)
    
    # Calculate drawdown
    if len(banca_history) > 1:
        banca_arr = np.array(banca_history)
        peak = np.maximum.accumulate(banca_arr)
        drawdowns = (peak - banca_arr) / peak * 100
        max_dd = drawdowns.max()
    else:
        max_dd = 0.0
        
    roi = (total_pnl / total_invested * 100) if total_invested > 0 else 0.0
    win_rate = (greens / bets_placed * 100) if bets_placed > 0 else 0.0
    
    return {
        'total_analyzed': total_analyzed,
        'bets_placed': bets_placed,
        'greens': greens,
        'reds': reds,
        'win_rate': win_rate,
        'total_invested': total_invested,
        'final_banca': banca,
        'pnl': total_pnl,
        'roi': roi,
        'max_dd': max_dd,
        'df_results': df_results
    }

# --- MAIN EXPERIMENTATION ---
if __name__ == "__main__":
    if not os.path.exists("betfair_historical.csv"):
        print("betfair_historical.csv not found. Please run inspect_betfair_columns.py first.")
        exit(1)
        
    print("Loading data...")
    df_raw = pd.read_csv("betfair_historical.csv")
    print(f"Loaded {len(df_raw)} raw games.")
    
    # Define Dutching Groups to test
    groups = {
        "Grupo Low-Score (0x0, 1x0, 0x1, 1x1)": ["0x0", "1x0", "0x1", "1x1"],
        "Grupo Home Favorite CS (1x0, 2x0, 2x1, 1x1)": ["1x0", "2x0", "2x1", "1x1"],
        "Grupo Draw Catch (0x0, 1x1, 2x2)": ["0x0", "1x1", "2x2"],
        "Grupo Favorite Clean Sheet (1x0, 2x0, 3x0)": ["1x0", "2x0", "3x0"],
    }
    
    print("\n" + "="*80)
    print("RUNNING OPTIMIZED CORRECT SCORE DUTCHING BACKTESTS ON BETFAIR EXCHANGE DATA")
    print("="*80)
    
    for name, sels in groups.items():
        print(f"\n--- {name} ---")
        print(f"Selections: {sels}")
        
        # Test 1: Flat Strategy (No Filters)
        res_flat = run_dutching_backtest(df_raw, sels, use_poisson_filter=False)
        print(f"Flat (No Filters):")
        print(f"  Bets: {res_flat['bets_placed']} / {res_flat['total_analyzed']} games")
        print(f"  Win Rate: {res_flat['win_rate']:.2f}% | Greens: {res_flat['greens']} | Reds: {res_flat['reds']}")
        print(f"  Net P&L: R$ {res_flat['pnl']:.2f} | ROI: {res_flat['roi']:.2f}% | Max Drawdown: {res_flat['max_dd']:.2f}%")
        
        # Test 2: Filter by Under 2.5 Odd <= 1.65 (Only low scoring games)
        res_under = run_dutching_backtest(df_raw, sels, odd_under_max=1.65, use_poisson_filter=False)
        print(f"Low Scoring Only (Odd Under 2.5 <= 1.65):")
        print(f"  Bets: {res_under['bets_placed']} / {res_under['total_analyzed']} games")
        print(f"  Win Rate: {res_under['win_rate']:.2f}% | Greens: {res_under['greens']} | Reds: {res_under['reds']}")
        print(f"  Net P&L: R$ {res_under['pnl']:.2f} | ROI: {res_under['roi']:.2f}% | Max Drawdown: {res_under['max_dd']:.2f}%")
        
        # Test 3: Value Dutching (Poisson EV > +5%)
        res_value = run_dutching_backtest(df_raw, sels, min_ev=0.05, use_poisson_filter=True)
        print(f"Value Dutching (Poisson EV >= +5%):")
        print(f"  Bets: {res_value['bets_placed']} / {res_value['total_analyzed']} games")
        print(f"  Win Rate: {res_value['win_rate']:.2f}% | Greens: {res_value['greens']} | Reds: {res_value['reds']}")
        print(f"  Net P&L: R$ {res_value['pnl']:.2f} | ROI: {res_value['roi']:.2f}% | Max Drawdown: {res_value['max_dd']:.2f}%")
        
        # Test 4: Combined Filter (Odd Under 2.5 <= 1.80 and Poisson EV >= +5%)
        res_comb = run_dutching_backtest(df_raw, sels, odd_under_max=1.80, min_ev=0.05, use_poisson_filter=True)
        print(f"Combined Filter (Under 2.5 <= 1.80 & EV >= +5%):")
        print(f"  Bets: {res_comb['bets_placed']} / {res_comb['total_analyzed']} games")
        print(f"  Win Rate: {res_comb['win_rate']:.2f}% | Greens: {res_comb['greens']} | Reds: {res_comb['reds']}")
        print(f"  Net P&L: R$ {res_comb['pnl']:.2f} | ROI: {res_comb['roi']:.2f}% | Max Drawdown: {res_comb['max_dd']:.2f}%")
        
        # Save results for the combined strategy
        if len(res_comb['df_results']) > 0:
            os.makedirs("scratch", exist_ok=True)
            res_comb['df_results'].to_csv(f"scratch/dutching_{sels[0]}_backtest_ops.csv", index=False)
            
            # Print performance by league
            print("  Top 5 leagues by Profit (Combined Strategy):")
            leagues = res_comb['df_results'].groupby('League')['P&L'].sum().sort_values(ascending=False).head(5)
            for lg, profit in leagues.items():
                print(f"    - {lg}: R$ {profit:.2f}")
