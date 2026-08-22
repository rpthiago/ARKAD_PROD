import sys
sys.stdout.reconfigure(encoding='utf-8')
import os, re

strategies = [
    ("Lay 0x0", "lay_0x0_rf_v2_strategy.py", "pages/14_🎯_Sinais_Lay_0x0.py"),
    ("Lay 0x1", "lay_0x1_rf_v2_strategy.py", "pages/17_🎯_Sinais_Lay_0x1.py"),
    ("Lay 0x2", "lay_0x2_rf_v2_strategy.py", "pages/16_🎯_Sinais_Lay_0x2.py"),
    ("Lay 1x0", "lay_1x0_rf_v2_strategy.py", "pages/18_🎯_Sinais_Lay_1x0.py"),
    ("Lay 2x0", "lay_2x0_rf_v2_strategy.py", "pages/15_🎯_Sinais_Lay_2x0.py")
]

print("=== AUDITORIA ESTRUTURAL DAS 5 ESTRATÉGIAS LAY DE CORRECT SCORE ===", flush=True)

for name, strat_file, page_file in strategies:
    print("\n" + "="*80)
    print(f"🔍 AUDITANDO: {name} ({strat_file})")
    print("="*80)
    
    if not os.path.exists(strat_file):
        print(f"❌ Arquivo {strat_file} não encontrado na raiz!")
        continue
        
    with open(strat_file, "r", encoding="utf-8") as f:
        code = f.read()
        
    # 1. Checar se usa hist_rf_loader ou _hist_df
    uses_hist_loader = "hist_rf_loader" in code
    print(f"1. Base de Dados: {'✅ Usa hist_rf_loader (Bet365 com features ricas)' if uses_hist_loader else '🔴 CRÍTICO: Usa base legada/_hist_df (Resultados_2026_Full sem xGOT/Posse)'}")
    
    # 2. Checar Cutoff Hardcoded
    has_hardcoded_cutoff = bool(re.search(r'Date\s*<\s*["\']2026-08-01["\']', code))
    print(f"2. Cutoff de Data: {'🔴 CRÍTICO: Cutoff HARDCODED < 2026-08-01' if has_hardcoded_cutoff else '✅ Cutoff Dinâmico'}")
    
    # 3. Checar Fallbacks Fabricados
    has_fallback = "0.35" in code or "0.25" in code or "0.28" in code or "draw_rate_mean" in code
    print(f"3. Fabricação de Features: {'🔴 ALTO: Possui fallbacks fabricados (0.35/0.25/0.28)' if has_fallback else '✅ Sem Fallbacks (Descarte estrito / dropna)'}")
    
    # 4. Checar H2H
    has_h2h_calc = "h2h_pair" in code and "h2h_draw_rate" in code or "h2h_" in code
    print(f"4. H2H Rate: {'✅ H2H computado dinamicamente' if has_h2h_calc else '🔴 CRÍTICO: H2H setado como NaN->0.0'}")
    
    # 5. Checar Mando de Campo (Home vs Away)
    has_venue_split = "dh" in code and "da" in code and "H_h_" in code and "A_a_" in code
    print(f"5. Separação por Mando (dh/da): {'✅ Features específicas por mando' if has_venue_split else '🔴 Sem separação correta de mando'}")
    
    # 6. Checar se a página Streamlit existe e recarrega
    if os.path.exists(page_file):
        with open(page_file, "r", encoding="utf-8") as pf:
            p_code = pf.read()
        uses_loader_page = "hist_rf_loader" in p_code
        print(f"6. Página Streamlit ({page_file}): {'✅ Conectada ao hist_rf_loader' if uses_loader_page else '🔴 Carrega _hist_df legado'}")
    else:
        print(f"6. Página Streamlit: Arquivo {page_file} não encontrado")

