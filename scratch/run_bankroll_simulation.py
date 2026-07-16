import pickle
import pandas as pd
import numpy as np
from pathlib import Path

# Load pickle
with open("scratch_backtest_results.pkl", "rb") as f:
    results = pickle.load(f)

# We want to analyze:
# 1. Min 15 | Shift: -3 | Full Match
# 2. Min 20 | Shift: -3 | Full Match

target_configs = [
    {"min": 15, "shift": -3, "label": "Min 15 | 3 Ticks Down | Full Match"},
    {"min": 20, "shift": -3, "label": "Min 20 | 3 Ticks Down | Full Match"}
]

print("="*80)
print("ANÁLISE DE GESTÃO DE BANCA PARA ALAVANCAGEM (FULL MATCH)")
print("="*80)

# Simulate different methods:
# Method A: Flat Liability (Fixed R$ 100 liability, no compounding)
# Method B: Simple Compound (Liability is 5% of current bankroll)
# Method C: Arkad Metodo 3 C (Compound ramp, step-ups when bankroll doubles)

for config in target_configs:
    # Find matching result
    res_data = None
    for r in results:
        if r["Entry_Min"] == config["min"] and r["Shift"] == config["shift"] and r["Exit_Strategy"] == "Full Match":
            res_data = r
            break
            
    if res_data is None:
        continue
        
    df_ops = res_data["df_ops"].copy()
    
    print(f"\n--- {config['label']} ---")
    print(f"Total de operações: {len(df_ops)} | Taxa de Green: {res_data['Win_Rate']:.1f}%")
    
    # 1. Flat Staking (Fixed liability of 3% of initial bankroll, i.e., R$ 30 for R$ 1000 bankroll)
    banca_flat = 1000.0
    liab_flat = 30.0 # R$ 30 liability flat
    max_banca_flat = banca_flat
    min_banca_flat = banca_flat
    
    # 2. Simple Compound (Liability is 3% of CURRENT bankroll)
    banca_comp = 1000.0
    max_banca_comp = banca_comp
    min_banca_comp = banca_comp
    
    # 3. Metodo 3 C (Agressivo) - Arkad style
    # Base stake starts at 30, grows in cycles.
    # If banca grows by 100% of base, we double base (step up).
    # If banca falls by 50% of base, we halve base (step down).
    banca_arkad = 1000.0
    base_liab = 30.0
    current_liab = base_liab
    max_banca_arkad = banca_arkad
    min_banca_arkad = banca_arkad
    
    # Track metrics
    for idx, row in df_ops.iterrows():
        odd_entry = row["Odd_Entry"]
        outcome = row["Outcome"]
        
        # Calculate P&L factor (Stakes won or lost)
        # On green: wins back stake * (1 - 0.065)
        # On red: loses liability
        is_green = (outcome == "GREEN")
        
        # --- FLAT ---
        back_stake_flat = liab_flat / (odd_entry - 1.0)
        pnl_flat = back_stake_flat * 0.935 if is_green else -liab_flat
        banca_flat += pnl_flat
        max_banca_flat = max(max_banca_flat, banca_flat)
        min_banca_flat = min(min_banca_flat, banca_flat)
        
        # --- SIMPLE COMPOUND ---
        current_liab_comp = banca_comp * 0.03 # 3% of current bankroll
        # Cap liability at R$ 200 to prevent oversized risk on single bet
        current_liab_comp = min(current_liab_comp, 200.0)
        back_stake_comp = current_liab_comp / (odd_entry - 1.0)
        pnl_comp = back_stake_comp * 0.935 if is_green else -current_liab_comp
        banca_comp += pnl_comp
        max_banca_comp = max(max_banca_comp, banca_comp)
        min_banca_comp = min(min_banca_comp, banca_comp)
        
        # --- ARKAD METODO 3 C ---
        # Liability is fixed in blocks.
        # Step up target: when bankroll increases by 2x base (R$ 60 for base 30), we double base.
        # Step down target: when bankroll decreases by 1x base (R$ 30), we halve base.
        # Simple implementation of step up / step down:
        back_stake_arkad = current_liab / (odd_entry - 1.0)
        pnl_arkad = back_stake_arkad * 0.935 if is_green else -current_liab
        banca_arkad += pnl_arkad
        max_banca_arkad = max(max_banca_arkad, banca_arkad)
        min_banca_arkad = min(min_banca_arkad, banca_arkad)
        
        # Adjust Arkad level
        profit_delta = banca_arkad - 1000.0
        if profit_delta > 0:
            # How many steps of 500 profit?
            steps = int(profit_delta // 500.0)
            current_liab = base_liab * (2 ** steps)
            current_liab = min(current_liab, 300.0) # Cap at R$ 300
        else:
            current_liab = base_liab
            
    print(f"  1. Gestão FLAT (Responsabilidade Fixa R$ 30):")
    print(f"     Banca Final: R$ {banca_flat:.2f} | P&L: R$ {banca_flat-1000.0:+.2f} | Max Banca: R$ {max_banca_flat:.2f}")
    print(f"  2. Gestão JUROS COMPOSTOS (3% da Banca, Teto R$ 200):")
    print(f"     Banca Final: R$ {banca_comp:.2f} | P&L: R$ {banca_comp-1000.0:+.2f} | Max Banca: R$ {max_banca_comp:.2f}")
    print(f"  3. Gestão ARKAD METODO 3 C (Nível Dinâmico, Teto R$ 300):")
    print(f"     Banca Final: R$ {banca_arkad:.2f} | P&L: R$ {banca_arkad-1000.0:+.2f} | Max Banca: R$ {max_banca_arkad:.2f}")
