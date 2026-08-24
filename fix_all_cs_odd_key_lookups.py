import sys
sys.stdout.reconfigure(encoding='utf-8')
import os, re

strategies = [
    ("0x0", "lay_0x0_rf_v2_strategy.py"),
    ("0x1", "lay_0x1_rf_v2_strategy.py"),
    ("0x2", "lay_0x2_rf_v2_strategy.py"),
    ("1x0", "lay_1x0_rf_v2_strategy.py"),
    ("2x0", "lay_2x0_rf_v2_strategy.py")
]

for cs, strat_file in strategies:
    with open(strat_file, "r", encoding="utf-8") as f:
        code = f.read()
        
    # Corrigir check_entry_conditions
    code = re.sub(
        rf'odd\s*=\s*ms\.get\(["\']Odd_{cs}_Lay["\']\)\s*or\s*ms\.get\(["\']Odd_{cs}_FT["\']\)',
        f'odd = ms.get("Odd_CS_{cs}_Lay") or ms.get("Odd_{cs}_Lay") or ms.get("Odd_CS_{cs}") or ms.get("Odd_{cs}_FT")',
        code
    )
    
    # Corrigir busca na payload do loop
    code = re.sub(
        rf'odd_{cs}\s*=\s*pd\.to_numeric\(g\.get\(["\']Odd_{cs}_Lay["\']\)',
        f'odd_{cs} = pd.to_numeric(g.get("Odd_CS_{cs}_Lay") or g.get("Odd_{cs}_Lay") or g.get("Odd_CS_{cs}") or g.get("Odd_{cs}_FT")',
        code
    )
    
    with open(strat_file, "w", encoding="utf-8") as f:
        f.write(code)
        
    print(f"[+] Chaves de odd Betfair CS corrigidas em: {strat_file}")

print("=== ATUALIZAÇÃO CONCLUÍDA ===")
