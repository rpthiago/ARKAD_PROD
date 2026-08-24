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
        
    # Adicionar Odd_CS_{cs}_Lay no dicionario ms
    old_ms = f'"Odd_{cs}_FT": odd_{cs}, "Odd_{cs}_Lay": odd_{cs}'
    new_ms = f'"Odd_{cs}_FT": odd_{cs}, "Odd_{cs}_Lay": odd_{cs}, "Odd_CS_{cs}_Lay": odd_{cs}'
    if old_ms in code:
        code = code.replace(old_ms, new_ms)
        
    with open(strat_file, "w", encoding="utf-8") as f:
        f.write(code)
    print(f"[+] ms atualizado em: {strat_file}")

# Atualizar coleta_lay_cs_aovivo.py
with open("coleta_lay_cs_aovivo.py", "r", encoding="utf-8") as f:
    coleta_code = f.read()

coleta_code = coleta_code.replace(
    'odd = pd.to_numeric(g.get(cfg["odd_key"]) or np.nan, errors="coerce")',
    'odd = pd.to_numeric(g.get(cfg["odd_key"]) or g.get(f"Odd_{cfg[\'placar\'].replace(\'-\', \'x\')}_Lay") or g.get(f"Odd_{cfg[\'placar\'].replace(\'-\', \'x\')}_FT") or np.nan, errors="coerce")'
)

with open("coleta_lay_cs_aovivo.py", "w", encoding="utf-8") as f:
    f.write(coleta_code)
print("[+] coleta_lay_cs_aovivo.py atualizado!")
