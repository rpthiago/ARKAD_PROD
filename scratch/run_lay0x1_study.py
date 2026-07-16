import json
import pandas as pd
import numpy as np
import ast
import re
from pathlib import Path

# --- Betfair Odds Tick Scale ---
def get_bf_odds():
    odds = []
    odds.extend(np.arange(1.01, 2.0, 0.01))
    odds.extend(np.arange(2.0, 3.0, 0.02))
    odds.extend(np.arange(3.0, 4.0, 0.05))
    odds.extend(np.arange(4.0, 6.0, 0.1))
    odds.extend(np.arange(6.0, 10.0, 0.2))
    odds.extend(np.arange(10.0, 20.0, 0.5))
    odds.extend(np.arange(20.0, 30.0, 1.0))
    odds.extend(np.arange(30.0, 50.0, 2.0))
    odds.extend(np.arange(50.0, 100.0, 5.0))
    odds.extend(np.arange(100.0, 1000.0, 10.0))
    odds.append(1000.0)
    return [round(x, 2) for x in odds]

BF_ODDS = get_bf_odds()

def shift_ticks(odd, num_ticks):
    idx = min(range(len(BF_ODDS)), key=lambda i: abs(BF_ODDS[i] - odd))
    new_idx = idx + num_ticks
    new_idx = max(0, min(new_idx, len(BF_ODDS) - 1))
    return BF_ODDS[new_idx]

# --- Parse goal minutes ---
def _parse(v):
    if pd.isna(v):
        return []
    s = str(v).strip()
    if s in ("", "[]", "nan"):
        return []
    try:
        val = ast.literal_eval(s)
        if isinstance(val, list):
            res = []
            for x in val:
                val_clean = str(x).split("+")[0].strip()
                if val_clean.isdigit():
                    res.append(int(val_clean))
            return res
    except:
        pass
    try:
        return [int(m) for m in re.findall(r'\d+', s)]
    except:
        return []

# --- Load configuration ---
config_path = Path("config_universo_97.json")
with open(config_path, "r", encoding="utf-8") as f:
    cfg = json.load(f)

fm = cfg["runtime_data"]["filtros_metodo"]["Lay_CS_0x1_B365"]
LIGAS_PERMITIDAS = {l.upper() for l in fm["ligas_permitidas"]}
ODD_MIN = fm["odd_min"]
ODD_MAX = fm["odd_max"]

# Load Rodo cuts
rodos = []
for src in [cfg.get("filtros_rodo", []), cfg.get("filters", {}).get("filtros_rodo", [])]:
    for c in (src or []):
        rodos.append(c)

def _is_rodo(liga, metodo, odd):
    for cut in rodos:
        cut_leagues = set(cut.get("leagues", []))
        if cut.get("league"):
            cut_leagues.add(str(cut["league"]).upper())
        if cut_leagues and liga.upper() not in cut_leagues:
            continue
        me = cut.get("method_equals")
        mc = cut.get("method_contains")
        if me and str(me) != metodo:
            continue
        if mc and str(mc) not in metodo:
            continue
        omn = cut.get("odd_min")
        omx = cut.get("odd_max")
        if omn is not None and odd < float(omn):
            continue
        if omx is not None and odd > float(omx):
            continue
        return True
    return False

# --- Load dataset ---
base_csv = Path("C:/Users/thiag/OneDrive/Documentos/GitHub/DASHBOARD_ARKAD-1/Bases_de_Dados_API_FutPythonTrader_Bet365.csv")
print("Carregando dados históricos de Bet365...")
df = pd.read_csv(base_csv, low_memory=False)
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

# Filter by year (2025 and 2026) and drop na goals
df_clean = df[df["Date"].dt.year.isin([2025, 2026])].dropna(subset=["Goals_H_FT", "Goals_A_FT"]).copy()
df_clean["GH"] = df_clean["Goals_H_FT"].astype(int)
df_clean["GA"] = df_clean["Goals_A_FT"].astype(int)
df_clean["Liga"] = df_clean["League"].str.upper().str.strip()
df_clean["Jogo"] = df_clean["Home"].str.strip() + " x " + df_clean["Away"].str.strip()
df_clean = df_clean.sort_values("Date").reset_index(drop=True)

print(f"Total de jogos carregados: {len(df_clean)}")

# --- Run Simulation ---
# Strategies to evaluate:
# - Entry Min: [15, 20]
# - Tick Shift: [0, -3]
# - Exit: ["Full Match", "Rota 60"]
# We simulate flat liability of R$ 100 for liability-based staking, and flat stake of R$ 100 for comparison.
LIABILITY = 100.0
COMMISSION = 0.065

results = []

for entry_min in [15, 20]:
    decay = 0.78 if entry_min == 15 else 0.70
    
    for shift in [0, -3]:
        for exit_strategy in ["Full Match", "Rota 60"]:
            
            banca_liab = 1000.0 # Starting bankroll for liability-based
            total_entries = 0
            greens = 0
            reds = 0
            voids = 0
            pnl_liab_total = 0.0
            
            # For tracking operations
            ops_records = []
            
            for idx, row in df_clean.iterrows():
                liga = row["Liga"]
                odd_base = pd.to_numeric(row["Odd_CS_0x1"], errors="coerce")
                
                if pd.isna(odd_base):
                    continue
                
                # Whitelist & Odd Range filter
                if liga not in LIGAS_PERMITIDAS:
                    continue
                if odd_base < ODD_MIN or odd_base > ODD_MAX:
                    continue
                
                # Rodo filter
                if _is_rodo(liga, "Lay_CS_0x1_B365", odd_base):
                    continue
                
                # Parse goals timeline
                goals_h = _parse(row["Goals_Min_H"])
                goals_a = _parse(row["Goals_Min_A"])
                
                timeline = []
                for m in goals_h:
                    timeline.append({"team": "H", "min": m})
                for m in goals_a:
                    timeline.append({"team": "A", "min": m})
                timeline = sorted(timeline, key=lambda x: x["min"])
                
                # Check if goal was scored before or at entry minute
                goals_before_entry = [g for g in timeline if g["min"] <= entry_min]
                if goals_before_entry:
                    # Skip since score changed from 0-0 before entry
                    continue
                
                total_entries += 1
                
                # Calculate entry odd
                odd_entry = odd_base * decay
                if shift != 0:
                    odd_entry = shift_ticks(odd_entry, shift)
                if odd_entry <= 1.01:
                    odd_entry = 1.01
                
                # Stake & Liability
                # For liability-based: liability = LIABILITY, back stake = LIABILITY / (odd_entry - 1)
                back_stake = LIABILITY / (odd_entry - 1.0)
                
                # Evaluate outcome
                if exit_strategy == "Full Match":
                    # Full Match: only loses if final score is exactly 0-1
                    final_h = row["GH"]
                    final_a = row["GA"]
                    if final_h == 0 and final_a == 1:
                        # Red
                        pnl = -LIABILITY
                        reds += 1
                        outcome = "RED"
                    else:
                        # Green
                        pnl = back_stake * (1.0 - COMMISSION)
                        greens += 1
                        outcome = "GREEN"
                else:
                    # Rota 60 (Exit at Minute 60)
                    # Check goals up to minute 60
                    timeline_60 = [g for g in timeline if g["min"] <= 60]
                    score_h_60 = sum(1 for g in timeline_60 if g["team"] == "H")
                    score_a_60 = sum(1 for g in timeline_60 if g["team"] == "A")
                    
                    if score_h_60 > 0 or score_a_60 > 1:
                        # Green: 0x1 is impossible
                        pnl = back_stake * (1.0 - COMMISSION)
                        greens += 1
                        outcome = "GREEN"
                    else:
                        # Still 0x0 or 0x1 at minute 60, cashout!
                        if score_h_60 == 0 and score_a_60 == 0:
                            # 0-0 at minute 60: odd_exit = odd_base * 0.45
                            odd_exit = odd_base * 0.45
                        else:
                            # 0-1 at minute 60: odd_exit = odd_base * 0.20
                            odd_exit = odd_base * 0.20
                        
                        if odd_exit <= 1.01:
                            odd_exit = 1.01
                        
                        # Cashout P&L
                        pnl = back_stake * (1.0 - (odd_entry / odd_exit))
                        if pnl > 0:
                            greens += 1
                            outcome = f"GREEN_CASH({pnl:.2f})"
                        else:
                            reds += 1
                            outcome = f"RED_CASH({pnl:.2f})"
                
                banca_liab += pnl
                pnl_liab_total += pnl
                
                ops_records.append({
                    "Date": str(row["Date"])[:10],
                    "Jogo": row["Jogo"],
                    "Liga": liga,
                    "Odd_Base": odd_base,
                    "Odd_Entry": odd_entry,
                    "Outcome": outcome,
                    "PnL": pnl,
                    "Banca": banca_liab
                })
            
            wr = (greens / total_entries * 100.0) if total_entries > 0 else 0.0
            results.append({
                "Entry_Min": entry_min,
                "Decay": decay,
                "Shift": shift,
                "Exit_Strategy": exit_strategy,
                "Total_Entries": total_entries,
                "Greens": greens,
                "Reds": reds,
                "Win_Rate": wr,
                "PnL_Total": pnl_liab_total,
                "Banca_Final": banca_liab,
                "ROI_Banca_%": (banca_liab - 1000.0) / 1000.0 * 100.0,
                "df_ops": pd.DataFrame(ops_records)
            })

# Print comparison table
print("\n" + "="*80)
print(f"RESULTADO DAS SIMULAÇÕES (Banca Inicial R$ 1000 | Responsabilidade Flat R$ {LIABILITY})")
print("="*80)
for r in results:
    print(f"Min: {r['Entry_Min']} | Shift: {r['Shift']:+2d} | Exit: {r['Exit_Strategy']:<10} | N: {r['Total_Entries']:3d} | G: {r['Greens']:3d} | R: {r['Reds']:3d} | WR: {r['Win_Rate']:.1f}% | P&L: R$ {r['PnL_Total']:+7.2f} | Banca Final: R$ {r['Banca_Final']:.2f} ({r['ROI_Banca_%']:.1f}% ROI)")

# Save the detailed results as a JSON/CSV for further analysis
import pickle
with open("scratch_backtest_results.pkl", "wb") as f:
    pickle.dump(results, f)
