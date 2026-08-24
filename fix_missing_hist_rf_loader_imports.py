import sys
sys.stdout.reconfigure(encoding='utf-8')
import os

target_pages = [
    "pages/9_🎯_Sinais_Lay_0x0.py",
    "pages/5_🎯_Sinais_Lay_0x1.py",
    "pages/7_🎯_Sinais_Lay_2x0.py",
    "pages/13_🎯_Sinais_Lay_0x2.py",
    "pages/18_🎯_Sinais_Lay_1x0.py"
]

for pfile in target_pages:
    with open(pfile, "r", encoding="utf-8") as f:
        content = f.read()
        
    if "import hist_rf_loader" not in content:
        # Adicionar import hist_rf_loader no bloco try de imports
        content = content.replace("import b365_data_utils", "import b365_data_utils\n    import hist_rf_loader")
        
        with open(pfile, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"[+] import hist_rf_loader adicionado em: {pfile}")
    else:
        print(f"[-] Já presente em: {pfile}")

print("=== CORREÇÃO CONCLUÍDA ===")
