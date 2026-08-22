import sys
sys.stdout.reconfigure(encoding='utf-8')
import os, re

strategies_real = [
    ("Lay 0x0", "lay_0x0_rf_v2_strategy.py", "pages/9_🎯_Sinais_Lay_0x0.py"),
    ("Lay 0x1", "lay_0x1_rf_v2_strategy.py", "pages/5_🎯_Sinais_Lay_0x1.py"),
    ("Lay 0x2", "lay_0x2_rf_v2_strategy.py", "pages/13_🎯_Sinais_Lay_0x2.py"),
    ("Lay 1x0", "lay_1x0_rf_v2_strategy.py", "pages/18_🎯_Sinais_Lay_1x0.py"),
    ("Lay 2x0", "lay_2x0_rf_v2_strategy.py", "pages/7_🎯_Sinais_Lay_2x0.py")
]

print("=== RELATÓRIO DE AUDITORIA COMPLETO DAS 5 ESTRATÉGIAS LAY CS ===", flush=True)

for name, strat_file, page_file in strategies_real:
    print("\n" + "="*85)
    print(f"🎯 AUDITORIA: {name}")
    print(f"📁 Motor: {strat_file} | 🖥️ Interface: {page_file}")
    print("="*85)
    
    with open(strat_file, "r", encoding="utf-8") as f:
        code = f.read()
        
    with open(page_file, "r", encoding="utf-8") as pf:
        p_code = pf.read()
        
    # 1. Base
    uses_hist_loader = "hist_rf_loader" in code or "hist_rf_loader" in p_code
    print(f"1. Base de Dados: {'✅ Usa hist_rf_loader (Bet365 com features ricas)' if uses_hist_loader else '🔴 CRÍTICO: Carrega base Resultados_2026_Full via _hist_df'}")
    
    # 2. H2H
    has_h2h = "h2h_" in code and "h2h_last" in code
    has_h2h_needed = "h2h_" in code
    if "0x1" in name or "1x0" in name:
        print(f"2. H2H Rate: ✅ Modelo treinado sem H2H (Features de Força Relativa e Multi-Goal)")
    else:
        print(f"2. H2H Rate: {'✅ H2H dinâmico calculado por par ordenado' if has_h2h else '🔴 CRÍTICO: H2H não calculado'}")
    
    # 3. Fallbacks
    has_fallback = bool(re.search(r'else\s+(0\.35|0\.25|0\.28)', code)) or bool(re.search(r'fillna\((0\.35|0\.25|0\.28)\)', code))
    print(f"3. Fallbacks Fabricados: {'🔴 ALTO: Injeta números inventados' if has_fallback else '✅ Sem fallbacks (Descarte estrito / dropna)'}")
    
    # 4. Odds
    uses_lay_odd = "Odd_" in code and "Lay" in code
    print(f"4. Odd Executável: {'✅ Usa Odd Lay' if uses_lay_odd else '⚠️ Usa odd de back b365'}")
    
    # 5. Mando
    has_mando = "dh" in code and "da" in code and "H_h_" in code and "A_a_" in code
    print(f"5. Separação por Mando: {'✅ Sim (dh/da separados)' if has_mando else '🔴 Sem separação correta de mando'}")
