"""
SIMULAÇÃO COMPLETA DE GESTÃO DE BANCA E RISCO DE RUÍNA - LAY 2X2
ARKAD_PROD

Estudo quantitativo de Monte Carlo (10.000 simulações de trajetória de banca)
para determinar o dimensionamento ideal de posição (Position Sizing) garantindo:
- Risco de Ruína <= 20.0%
- Retorno Financeiro Otimizado (CAGR / Crescimento Composto)
"""

import numpy as np
import pandas as pd
from scipy import stats

print("==========================================================================")
print("     SIMULAÇÃO DE MONTE CARLO - GESTÃO DE BANCA PARA LAY 2X2")
print("==========================================================================\n")

# Carregar amostragem histórica estendida da Betfair (50.000 partidas)
df = pd.read_csv("Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH.csv", low_memory=False)

df["Odd_CS_2x2_Lay"] = pd.to_numeric(df["Odd_CS_2x2_Lay"], errors="coerce")
df["gh"] = pd.to_numeric(df["Goals_H_FT"], errors="coerce")
df["ga"] = pd.to_numeric(df["Goals_A_FT"], errors="coerce")

df_sub = df[df["gh"].notna() & df["ga"].notna() & df["Odd_CS_2x2_Lay"].notna() & (df["Odd_CS_2x2_Lay"] >= 8.0) & (df["Odd_CS_2x2_Lay"] <= 18.0)].copy()
df_sub["is_2x2"] = (df_sub["gh"] == 2) & (df_sub["ga"] == 2)

n_sample = len(df_sub)
reds_count = df_sub["is_2x2"].sum()
greens_count = n_sample - reds_count
win_rate = (greens_count / n_sample)

odds_lay_sample = df_sub["Odd_CS_2x2_Lay"].values
is_red_sample = df_sub["is_2x2"].values

print(f"Amostra Historica Base: {n_sample:,} jogos")
print(f"Greens: {greens_count:,} | Reds: {reds_count:,} | Win Rate Historico: {win_rate*100:.2f}%\n")

# Parâmetros de Simulação de Monte Carlo
BANCA_INICIAL = 1000.0
N_TRADES_SIMULACAO = 500  # 500 operações sequenciais (aprox 6 meses de trading)
N_SIMULACOES = 10000     # 10.000 trajetórias independentes
COMISSAO_BETFAIR = 0.05
LIMITE_RUINA = 0.50      # Considera Ruína se a banca cair 50% (drawdown >= 50%)

# Perfis de Gestão de Banca a Testar
perfis_gestao = [
    # 1. Responsabilidade Fixa (% da Banca)
    {"nome": "Responsabilidade 2.5% (Kelly Conservador)", "tipo": "resp_pct", "val": 0.025},
    {"nome": "Responsabilidade 5.0% (Conservador)", "tipo": "resp_pct", "val": 0.050},
    {"nome": "Responsabilidade 7.5% (Moderado)", "tipo": "resp_pct", "val": 0.075},
    {"nome": "Responsabilidade 10.0% (Moderado-Agressivo)", "tipo": "resp_pct", "val": 0.100},
    {"nome": "Responsabilidade 12.5% (Agressivo)", "tipo": "resp_pct", "val": 0.125},
    {"nome": "Responsabilidade 15.0% (Ultra-Agressivo)", "tipo": "resp_pct", "val": 0.150},
    {"nome": "Responsabilidade 20.0% (Extremo)", "tipo": "resp_pct", "val": 0.200},
    
    # 2. Stake Fixa (% da Banca)
    {"nome": "Stake Fixa 1.0% da Banca", "tipo": "stake_pct", "val": 0.010},
    {"nome": "Stake Fixa 2.0% da Banca", "tipo": "stake_pct", "val": 0.020},
    {"nome": "Stake Fixa 3.0% da Banca", "tipo": "stake_pct", "val": 0.030},
    
    # 3. Valor Absoluto Fixo (R$ Fixo)
    {"nome": "Responsabilidade Fixa R$ 100", "tipo": "resp_abs", "val": 100.0},
    {"nome": "Responsabilidade Fixa R$ 200", "tipo": "resp_abs", "val": 200.0},
]

print("==========================================================================")
print("     RESULTADO DAS SIMULAÇÕES DE MONTE CARLO (10.000 TRAJETÓRIAS)")
print("==========================================================================")
print(f"{'Perfil de Gestão':<45} | {'Ruína %':<8} | {'Drawdown Máx %':<15} | {'Banca Média Final':<18} | {'Status Ruína <= 20%'}")
print("-" * 110)

resultados_sim = []

np.random.seed(42)

for perfil in perfis_gestao:
    ruinas = 0
    bancas_finais = []
    max_drawdowns = []
    
    for _ in range(N_SIMULACOES):
        banca = BANCA_INICIAL
        banca_peak = BANCA_INICIAL
        max_dd = 0.0
        ruiu = False
        
        # Sorteia 500 trades aleatórios com reposição da amostragem real
        indices = np.random.choice(n_sample, size=N_TRADES_SIMULACAO, replace=True)
        
        for idx in indices:
            odd_lay = odds_lay_sample[idx]
            is_red = is_red_sample[idx]
            
            # Calcula tamanho da aposta conforme o perfil
            if perfil["tipo"] == "resp_pct":
                resp = banca * perfil["val"]
                stake = resp / (odd_lay - 1.0)
            elif perfil["tipo"] == "stake_pct":
                stake = banca * perfil["val"]
                resp = stake * (odd_lay - 1.0)
            elif perfil["tipo"] == "resp_abs":
                resp = min(perfil["val"], banca * 0.90)  # Capped no saldo
                stake = resp / (odd_lay - 1.0)
                
            if is_red:
                banca -= resp
            else:
                banca += stake * (1.0 - COMISSAO_BETFAIR)
                
            # Atualiza pico e drawdown
            if banca > banca_peak:
                banca_peak = banca
            dd = (banca_peak - banca) / banca_peak
            if dd > max_dd:
                max_dd = dd
                
            # Verifica se atingiu limite de ruína (perda >= 50% do capital inicial ou da banca)
            if banca <= (BANCA_INICIAL * (1.0 - LIMITE_RUINA)):
                ruiu = True
                
        if ruiu or banca <= 10.0:
            ruinas += 1
            
        bancas_finais.append(banca)
        max_drawdowns.append(max_dd)
        
    pct_ruina = (ruinas / N_SIMULACOES) * 100.0
    mediana_banca = np.median(bancas_finais)
    media_banca = np.mean(bancas_finais)
    avg_max_dd = np.mean(max_drawdowns) * 100.0
    p95_max_dd = np.percentile(max_drawdowns, 95) * 100.0
    
    aprovado = "APROVADO" if pct_ruina <= 20.0 else "REPROVADO"
    
    resultados_sim.append({
        "perfil": perfil["nome"],
        "pct_ruina": pct_ruina,
        "media_banca": media_banca,
        "mediana_banca": mediana_banca,
        "avg_max_dd": avg_max_dd,
        "p95_max_dd": p95_max_dd,
        "aprovado": aprovado
    })
    
    print(f"{perfil['nome']:<45} | {pct_ruina:6.2f}%  | {p95_max_dd:13.2f}%  | R$ {mediana_banca:14.2f}  | {aprovado}")

print("==========================================================================\n")
