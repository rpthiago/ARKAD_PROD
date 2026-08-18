"""
ESTUDO DE VIABILIDADE E EXPECTATIVA MATEMÁTICA (EV) DO DESAFIO DE ALAVANCAGEM
ARKAD_PROD

Simulação de 1.000 Desafios de Alavancagem (R$ 500 -> R$ 1.000 com Saque e Reset)
para determinar se o modelo vale a pena matematicamente e financeiramente.
"""

import numpy as np
import pandas as pd

print("==========================================================================")
print("     ESTUDO DE VIABILIDADE: DESAFIO R$ 500 -> R$ 1.000 (SAQUE & RESET)")
print("==========================================================================\n")

def analisar_desafio(nome_metodo, win_rate_historico, n_greens_meta, odd_lay_media):
    N_DESAFIOS = 1000
    CAPITAL_TENTATIVA = 500.0
    COMISSAO = 0.05
    
    sucessos = 0
    fracassos = 0
    pnl_total = 0.0
    historico_pnl = []
    
    np.random.seed(42)
    
    for _ in range(N_DESAFIOS):
        saldo = CAPITAL_TENTATIVA
        conseguiu = True
        
        for g in range(n_greens_meta):
            # Sorteia se o jogo foi Green ou Red baseado no Win Rate
            is_green = (np.random.rand() <= win_rate_historico)
            
            if is_green:
                resp = saldo
                stake = resp / (odd_lay_media - 1.0)
                lucro = stake * (1.0 - COMISSAO)
                saldo += lucro
            else:
                conseguiu = False
                break
                
        if conseguiu:
            sucessos += 1
            lucro_desafio = saldo - CAPITAL_TENTATIVA  # Aprox R$ 500
            pnl_total += lucro_desafio
        else:
            fracassos += 1
            pnl_total -= CAPITAL_TENTATIVA
            
        historico_pnl.append(pnl_total)
        
    pct_sucesso = (sucessos / N_DESAFIOS) * 100.0
    pct_fracasso = (fracassos / N_DESAFIOS) * 100.0
    ev_por_tentativa = pnl_total / N_DESAFIOS
    
    print(f"METODO: {nome_metodo}")
    print(f"   Win Rate Histórico: {win_rate_historico*100:.2f}% | Meta de Greens Seguidos: {n_greens_meta}")
    print(f"   Total de Tentativas: {N_DESAFIOS:,}")
    print(f"   Desafios Concluídos com Sucesso: {sucessos:,} ({pct_sucesso:.2f}%)")
    print(f"   Desafios Perdidos (Red no Caminho): {fracassos:,} ({pct_fracasso:.2f}%)")
    print(f"   Lucro Acumulado Total em 1.000 Tentativas: R$ {pnl_total:,.2f}")
    print(f"   Expectativa Matemática (EV+) por Tentativa: R$ {ev_por_tentativa:,.2f}")
    print(f"   Status de Viabilidade: {'VALE MUITO A PENA (EV+ EXTREMO)' if ev_por_tentativa > 0 else 'NÃO VALE A PENA (EV-)'}\n")

analisar_desafio("Lay 0x3 Visitante (Under 2.5 xG Protected)", 0.9930, 16, 22.0)
analisar_desafio("Lay 2x2 Quant", 0.9470, 8, 11.0)

print("==========================================================================")
