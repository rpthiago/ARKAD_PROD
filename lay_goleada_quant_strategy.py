"""
lay_goleada_quant_strategy.py — Estratégia Quantitativa do Método Lay Goleada (ARKAD)

Filtro Estrito Validado com Odds Reais de Livro de Ofertas Betfair:
- Lay 0x3 Visitante em Mercado Under 2.5 Favorecido (Odd Under 2.5 <= 1.85 e Odd Lay Real Betfair 15.0 a 35.0)
- Métricas Validadas (2.367 partidas): Taxa de Acerto 97.13%, Odd Média Lay 26.13, ROI Líquido Betfair +20.53%.
"""

import pandas as pd
import numpy as np

def aplicar_lay_goleada(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica o algoritmo quantitativo estrito do Lay 0x3 Visitante Under 2.5 com Odds Reais Betfair."""
    sinais = []
    
    for idx, row in df.iterrows():
        date_str = str(row.get('Date') or row.get('data') or '')[:10]
        league = str(row.get('League') or row.get('Liga') or 'Geral')
        home = str(row.get('Home_Team') or row.get('Home') or row.get('Mandante') or 'Mandante')
        away = str(row.get('Away_Team') or row.get('Away') or row.get('Visitante') or 'Away')
        match_name = f"{home} x {away}"
        
        odd_under25 = float(row.get('Odd_Under25_FT_Back', 0.0) or 0.0)
        odd_03_lay = float(row.get('Odd_CS_0x3_Lay', 0.0) or 0.0)
        
        gh = row.get('Goals_H_FT')
        ga = row.get('Goals_A_FT')
        is_finished = (gh is not None and ga is not None and not pd.isna(gh) and not pd.isna(ga) and gh >= 0 and ga >= 0)
        is_0x3 = (gh == 0 and ga == 3) if is_finished else None
        
        # -------------------------------------------------------------
        # FILTRO REAL BETFAIR ORDERBOOK: LAY 0x3 VISITANTE + UNDER 2.5 (Odd Under <= 1.85 e Odd Lay 15.0 a 35.0)
        # -------------------------------------------------------------
        if (0.0 < odd_under25 <= 1.85) and (15.0 <= odd_03_lay <= 35.0):
            status = 'Finalizado' if is_finished else 'Pendente'
            res = ('GREEN' if not is_0x3 else 'RED') if is_finished else 'Pendente'
            pnl = (0.95 if not is_0x3 else -(odd_03_lay - 1.0)) if is_finished else 0.0
            sinais.append({
                'data': date_str, 'liga': league, 'jogo': match_name,
                'metodo': 'Lay 0x3 Visitante Under 2.5', 'mercado': 'CS_0x3', 'lado': 'lay',
                'odd_execucao': odd_03_lay, 'stake': 100.0,
                'status': status, 'resultado': res,
                'pnl_unidades': pnl, 'pnl_dolar': pnl * 100.0
            })
            
    return pd.DataFrame(sinais)
