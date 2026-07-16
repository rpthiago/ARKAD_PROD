import pandas as pd
import numpy as np
from scipy.stats import poisson

def solve_total_expected_goals(p_under25):
    low, high = 0.0, 15.0
    for _ in range(20):
        mid = (low + high) / 2
        val = np.exp(-mid) * (1 + mid + (mid**2)/2)
        if val > p_under25:
            low = mid
        else:
            high = mid
    return (low + high) / 2

def solve_weights(G, p_home, max_goals=8):
    low, high = 0.0, 1.0
    for _ in range(15):
        mid = (low + high) / 2
        lam = G * mid
        mu = G * (1 - mid)
        
        # Calculate p_home
        px = poisson.pmf(np.arange(max_goals + 1), lam)
        py = poisson.pmf(np.arange(max_goals + 1), mu)
        grid = np.outer(px, py)
        
        p_h_calc = 0.0
        for h in range(max_goals + 1):
            for a in range(h):
                p_h_calc += grid[h, a]
                
        if p_h_calc < p_home:
            low = mid
        else:
            high = mid
    w = (low + high) / 2
    return G * w, G * (1 - w)

def get_poisson_correct_scores(lam, mu, max_goals=8):
    px = poisson.pmf(np.arange(max_goals + 1), lam)
    py = poisson.pmf(np.arange(max_goals + 1), mu)
    grid = np.outer(px, py)
    return grid

# Load dataset and test on 5 random games
df = pd.read_csv("b365_base_lean.csv")
# Filter games with all needed odds valid
df_valid = df.dropna(subset=[
    'Odd_H_FT', 'Odd_D_FT', 'Odd_A_FT', 
    'Odd_Under25_FT', 'Odd_Over25_FT',
    'Odd_CS_0x0', 'Odd_CS_1x0', 'Odd_CS_0x1', 'Odd_CS_1x1'
]).copy()

print(f"Total valid games for CS comparison: {len(df_valid)}")

np.random.seed(42)
sample = df_valid.sample(5)

for idx, row in sample.iterrows():
    print(f"\n--- Jogo: {row['Home']} x {row['Away']} ({row['League']}) ---")
    
    # 1. Match Odds & Over/Under odds
    o_h, o_d, o_a = row['Odd_H_FT'], row['Odd_D_FT'], row['Odd_A_FT']
    o_under, o_over = row['Odd_Under25_FT'], row['Odd_Over25_FT']
    
    # 2. Normalize probabilities
    sum_mo = 1/o_h + 1/o_d + 1/o_a
    p_h = (1/o_h) / sum_mo
    p_d = (1/o_d) / sum_mo
    p_a = (1/o_a) / sum_mo
    
    sum_ou = 1/o_under + 1/o_over
    p_under = (1/o_under) / sum_ou
    
    # 3. Solve Poisson
    G = solve_total_expected_goals(p_under)
    lam, mu = solve_weights(G, p_h)
    
    print(f"Odds: H={o_h}, D={o_d}, A={o_a} | U2.5={o_under}, O2.5={o_over}")
    print(f"Poisson Calculados: lam = {lam:.3f}, mu = {mu:.3f} | Total G = {G:.3f}")
    
    # 4. Correct score probabilities
    grid = get_poisson_correct_scores(lam, mu)
    
    # Compare with bookmaker correct score odds
    for cs_str, x, y in [("0x0", 0, 0), ("1x0", 1, 0), ("0x1", 0, 1), ("1x1", 1, 1)]:
        model_prob = grid[x, y]
        model_odd = 1 / model_prob if model_prob > 0 else np.nan
        real_odd = row[f'Odd_CS_{cs_str}']
        error_pct = (model_odd - real_odd) / real_odd * 100
        print(f" Placar {cs_str} -> Modelo: {model_prob*100:5.2f}% (Odd: {model_odd:5.2f}) | Real: Odd {real_odd:5.2f} | Diferenca: {error_pct:+.1f}%")
