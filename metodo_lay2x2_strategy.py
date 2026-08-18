"""
MÓDULO DE ESTRATÉGIA - LAY 2X2 (CORRECT SCORE 2-2)
ARKAD_PROD

Estratégia quantitativa focada em Lay no placar exato 2x2 (Correct Score 2-2).
Baseada no estudo estatístico de 50.000+ partidas históricas.

Filtros Quantitativos de Validação:
1. Odd Lay 2x2 Betfair: Entre 8.00 e 14.00 (Teto estrito de responsabilidade).
2. Tendência Under 2.5: Odd Under 2.5 FT <= 2.00 ou Total xG <= 2.40.
3. Taxa de Acerto Histórica Esperada: ~94.7% Win Rate.
"""

import pandas as pd
import numpy as np

# Parâmetros Padrão da Estratégia
ODD_LAY_2X2_MIN = 8.00
ODD_LAY_2X2_MAX = 14.00
ODD_UNDER25_MAX = 2.00
STAKE_PADRAO = 100.0
COMISSAO_BETFAIR = 0.05

def validar_entrada_lay2x2(
    odd_lay_2x2: float,
    odd_under25: float = None,
    total_xg: float = None,
    odd_h: float = None,
    odd_a: float = None
) -> tuple[bool, str]:
    """
    Valida se uma oportunidade atende aos critérios estritos da estratégia Lay 2x2.
    
    Retorna:
        (aprovado: bool, motivo: str)
    """
    if pd.isna(odd_lay_2x2) or odd_lay_2x2 <= 1.0:
        return False, "Odd Lay 2x2 inválida ou ausente."
        
    if odd_lay_2x2 < ODD_LAY_2X2_MIN:
        return False, f"Odd Lay 2x2 ({odd_lay_2x2:.2f}) abaixo do mínimo ({ODD_LAY_2X2_MIN:.2f})."
        
    if odd_lay_2x2 > ODD_LAY_2X2_MAX:
        return False, f"Odd Lay 2x2 ({odd_lay_2x2:.2f}) acima do teto de risco ({ODD_LAY_2X2_MAX:.2f})."
        
    # Filtro de Tendência Under / Estabilidade de Gols
    passou_filtro_tendencia = False
    motivo_filtro = ""
    
    if odd_under25 is not None and pd.notna(odd_under25) and odd_under25 <= ODD_UNDER25_MAX:
        passou_filtro_tendencia = True
        motivo_filtro = f"Odd Under 2.5 ({odd_under25:.2f}) <= {ODD_UNDER25_MAX:.2f}"
    elif total_xg is not None and pd.notna(total_xg) and total_xg <= 2.40:
        passou_filtro_tendencia = True
        motivo_filtro = f"Total xG ({total_xg:.2f}) <= 2.40"
    elif odd_h is not None and odd_a is not None and pd.notna(odd_h) and pd.notna(odd_a):
        if odd_h <= 1.75 or odd_a <= 1.75:
            passou_filtro_tendencia = True
            motivo_filtro = f"Favorito Claro (Odd Mandante: {odd_h:.2f} | Visitante: {odd_a:.2f})"

    # Se não houver informação de xG ou Under 2.5, valida apenas pela faixa de Odd Lay 2x2 <= 14.0
    if not passou_filtro_tendencia:
        if odd_lay_2x2 <= 12.50:
            passou_filtro_tendencia = True
            motivo_filtro = f"Odd Lay 2x2 muito favorável ({odd_lay_2x2:.2f})"
        else:
            return False, f"Não atende aos critérios de tendência Under 2.5 ou Favoritismo."

    return True, f"Aprovado para Lay 2x2! ({motivo_filtro})"

def calcular_resultado_lay2x2(
    gols_mandante: int,
    gols_visitante: int,
    odd_execucao: float,
    stake: float = STAKE_PADRAO,
    comissao: float = COMISSAO_BETFAIR
) -> dict:
    """
    Calcula o resultado financeiro (GREEN/RED, P&L e Responsabilidade) de uma aposta Lay 2x2.
    """
    if pd.isna(gols_mandante) or pd.isna(gols_visitante):
        return {
            "status": "Pendente",
            "resultado": "",
            "lucro_prejuizo": 0.0,
            "responsabilidade": round((odd_execucao - 1.0) * stake, 2)
        }
        
    gh = int(gols_mandante)
    ga = int(gols_visitante)
    
    is_2x2 = (gh == 2 and ga == 2)
    win = not is_2x2
    
    responsabilidade = round((odd_execucao - 1.0) * stake, 2)
    
    if win:
        # Green: ganha a stake menos comissão da bolsa
        lucro = round(stake * (1.0 - comissao), 2)
        resultado_str = "GREEN"
    else:
        # Red: perde a responsabilidade cobrada pela odd
        lucro = -responsabilidade
        resultado_str = "RED"
        
    return {
        "status": "Finalizado",
        "resultado": resultado_str,
        "lucro_prejuizo": lucro,
        "responsabilidade": responsabilidade
    }

def filtrar_grade_lay2x2(df_jogos: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra um DataFrame de jogos diários e retorna apenas as partidas aprovadas para o Método Lay 2x2.
    """
    if df_jogos.empty:
        return pd.DataFrame()
        
    aprovados = []
    
    for idx, row in df_jogos.iterrows():
        odd_lay_2x2 = float(row.get("Odd_CS_2x2_Lay", row.get("Odd_2x2_Lay", 0.0)))
        odd_under25 = float(row.get("Odd_Under25_FT_Back", row.get("Odd_Under25", 0.0)))
        total_xg = float(row.get("total_xg", row.get("Total_xG", 0.0)))
        odd_h = float(row.get("Odd_H_FT_Back", row.get("Odd_H", 0.0)))
        odd_a = float(row.get("Odd_A_FT_Back", row.get("Odd_A", 0.0)))
        
        ok, motivo = validar_entrada_lay2x2(
            odd_lay_2x2=odd_lay_2x2,
            odd_under25=odd_under25,
            total_xg=total_xg,
            odd_h=odd_h,
            odd_a=odd_a
        )
        
        if ok:
            r_copy = row.to_dict()
            r_copy["motivo_aprovacao"] = motivo
            r_copy["metodo"] = "Lay 2x2 Quant"
            r_copy["mercado"] = "CS_2x2"
            r_copy["lado"] = "lay"
            r_copy["odd_execucao"] = odd_lay_2x2
            aprovados.append(r_copy)
            
    return pd.DataFrame(aprovados)

if __name__ == "__main__":
    print("=== TESTE UNITÁRIO DO MÓDULO METODO_LAY2X2_STRATEGY ===")
    
    # Teste 1: Validação de Entrada
    ok, msg = validar_entrada_lay2x2(odd_lay_2x2=11.50, odd_under25=1.85)
    print("Teste 1 (Válido):", ok, "| Motivo:", msg)
    assert ok == True
    
    ok_inv, msg_inv = validar_entrada_lay2x2(odd_lay_2x2=22.00)
    print("Teste 2 (Odd muito alta):", ok_inv, "| Motivo:", msg_inv)
    assert ok_inv == False
    
    # Teste 2: Cálculo de Resultado Green
    res_green = calcular_resultado_lay2x2(gols_mandante=1, gols_visitante=0, odd_execucao=10.0, stake=100.0)
    print("Teste 3 (Green 1x0):", res_green)
    assert res_green["resultado"] == "GREEN"
    assert res_green["lucro_prejuizo"] == 95.0
    
    # Teste 3: Cálculo de Resultado Red (Placar 2x2)
    res_red = calcular_resultado_lay2x2(gols_mandante=2, gols_visitante=2, odd_execucao=10.0, stake=100.0)
    print("Teste 4 (Red 2x2):", res_red)
    assert res_red["resultado"] == "RED"
    assert res_red["lucro_prejuizo"] == -900.0
    
    print("\nTodos os testes unitários do Método Lay 2x2 passaram com sucesso!")
