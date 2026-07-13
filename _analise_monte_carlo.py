import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import argparse

def simulate_monte_carlo(df_results, method_name, initial_bankroll=1000, n_simulations=10000, n_bets=1000, bet_type="LAY"):
    """
    Roda a simulação de Monte Carlo.
    df_results deve conter 'is_green' (bool) e 'Resolved_Odd' (float).
    """
    is_green_history = df_results['is_green'].values
    odd_history = df_results['Resolved_Odd'].values
    
    # Pre-calcula os multiplicadores de PnL para 1 unidade de aposta
    # Em Stake Fixa, 1 unidade = 1 unidade monetária (ex: R$ 10)
    # Em Juros Compostos, 1 unidade = % da banca (ex: 1%)
    if bet_type == "BACK":
        # Se Green, ganha (Odd - 1) * 0.95. Se Red, perde 1.0.
        pnl_units = np.where(is_green_history, (odd_history - 1) * 0.95, -1.0)
    else:
        # Se Green, ganha 0.95. Se Red, perde (Odd - 1).
        pnl_units = np.where(is_green_history, 0.95, -(odd_history - 1))
        
    n_historical = len(pnl_units)
    if n_historical == 0:
        print("Histórico vazio!")
        return

    print(f"[{method_name}] Iniciando {n_simulations} simulacoes de {n_bets} apostas...")
    
    # Gera índices aleatórios (com reposição) para criar as trajetórias
    # Shape: (n_simulations, n_bets)
    random_indices = np.random.randint(0, n_historical, size=(n_simulations, n_bets))
    simulated_pnl = pnl_units[random_indices]
    
    # ------------------------------------------------------------------
    # OTIMIZAÇÃO: Buscar a Stake Máxima para RoR <= 20%
    # ------------------------------------------------------------------
    # Vamos testar Stakes de 0.1% a 5.0% (em incrementos de 0.1%)
    stake_pcts = np.arange(0.001, 0.051, 0.001)
    
    best_stake = 0
    max_median_profit = -np.inf
    best_ror = 0
    
    for pct in stake_pcts:
        # Simulação de Juros Compostos
        # PnL array tem os ganhos/perdas por unidade.
        # Banca começa em 1.0 (100%). Multiplicador em cada aposta:
        # Bankroll_n = Bankroll_n-1 * (1 + pct * pnl_unit)
        
        # Cria a matriz de multiplicadores (n_simulations, n_bets)
        multipliers = 1.0 + (pct * simulated_pnl)
        
        # Trajetoria da banca
        # limitamos a 0 para não ter valores negativos no cumprod (banca quebrada)
        multipliers = np.maximum(multipliers, 0)
        
        bankroll_paths = initial_bankroll * np.cumprod(multipliers, axis=1)
        
        # Banca Final (ultimo elemento de cada caminho)
        final_bankrolls = bankroll_paths[:, -1]
        
        # RoR (Risco de Ruína): banca final menor que 5% da inicial
        # ou se a banca bateu em zero no meio do caminho
        ruined = np.any(bankroll_paths <= (initial_bankroll * 0.05), axis=1)
        ror = np.mean(ruined)
        
        median_final = np.median(final_bankrolls)
        
        if ror <= 0.20:
            if median_final > max_median_profit:
                max_median_profit = median_final
                best_stake = pct
                best_ror = ror
                
    print("\n" + "="*50)
    print(f" RESULTADO MONTE CARLO: {method_name}")
    print("="*50)
    print(f"Objetivo: Máxima rentabilidade com RoR <= 20%.")
    print(f"Stake Ideal Encontrada: {best_stake*100:.2f}% da Banca")
    print(f"RoR (Risco de Quebra) nesta Stake: {best_ror*100:.1f}%")
    print(f"Banca Inicial: R$ {initial_bankroll}")
    print(f"Mediana da Banca Final (após {n_bets} apostas): R$ {max_median_profit:.2f}")
    
    # Rodar os dados com a best_stake para estatísticas detalhadas
    multipliers = np.maximum(1.0 + (best_stake * simulated_pnl), 0)
    bankroll_paths = initial_bankroll * np.cumprod(multipliers, axis=1)
    final_bankrolls = bankroll_paths[:, -1]
    
    p05 = np.percentile(final_bankrolls, 5)
    p95 = np.percentile(final_bankrolls, 95)
    
    print(f"Pior Cenário (5% mais azarentos): R$ {p05:.2f}")
    print(f"Cenário Otimista (5% mais sortudos): R$ {p95:.2f}")
    print("="*50)

if __name__ == "__main__":
    # Exemplo simulando os dados reais auditados do Lay Home Trader
    # O user pediu para usar as médias: 56% ganho (gol away), 21.5% ganho (0x0 HT), -31% perda (gol home)
    # Taxa de acerto real = 76%. Vamos dividir as vitórias entre Gol Away e 0x0 HT.
    np.random.seed(42)
    n = 10000
    rand_vals = np.random.rand(n)
    
    # Probabilidades: 24% Red (Gol Home), 76% Green
    # Dentre os 76% Green, vamos assumir (baseado em futebol geral) que 40% é 0x0 HT e 36% é Gol Away.
    pnl_units = np.zeros(n)
    
    for i in range(n):
        if rand_vals[i] < 0.24:
            # RED: Mandante marcou. A gente fecha com Cash Out e perde em média 31% da responsabilidade.
            # Como a odd média de Back é 4.3 (Liability = 3.3), a perda em unidades de stake inicial é:
            # Perda = -0.31 * Liability = -0.31 * 3.3 = -1.02 unidades
            pnl_units[i] = -1.02
        elif rand_vals[i] < 0.64:
            # GREEN HT: 0x0 HT. Ganhamos 21.5% do nosso potencial lucro total (que seria 0.95 unidades)
            pnl_units[i] = 0.215
        else:
            # GREEN GOL: Visitante marcou. Ganhamos 56% do nosso lucro total
            pnl_units[i] = 0.56

    df_fake = pd.DataFrame({'is_green': rand_vals >= 0.24, 'Resolved_Odd': 4.30})
    
    # Substituir a geração interna pela nossa
    df_fake['pnl_units'] = pnl_units
    
    def simulate_monte_carlo_custom(pnl_units, method_name, initial_bankroll=1000, n_simulations=10000, n_bets=1000):
        print(f"[{method_name}] Iniciando {n_simulations} simulacoes de {n_bets} apostas...")
        n_historical = len(pnl_units)
        random_indices = np.random.randint(0, n_historical, size=(n_simulations, n_bets))
        simulated_pnl = pnl_units[random_indices]
        
        stake_pcts = np.arange(0.001, 0.051, 0.001)
        best_stake = 0
        max_median_profit = -np.inf
        best_ror = 0
        
        for pct in stake_pcts:
            # Aqui multiplicamos pela % da Banca
            multipliers = 1.0 + (pct * simulated_pnl)
            multipliers = np.maximum(multipliers, 0)
            bankroll_paths = initial_bankroll * np.cumprod(multipliers, axis=1)
            final_bankrolls = bankroll_paths[:, -1]
            ruined = np.any(bankroll_paths <= (initial_bankroll * 0.20), axis=1) # Risco de perder 80%
            ror = np.mean(ruined)
            median_final = np.median(final_bankrolls)
            
            if ror <= 0.20:
                if median_final > max_median_profit:
                    max_median_profit = median_final
                    best_stake = pct
                    best_ror = ror
                    
        print("\n" + "="*50)
        print(f" RESULTADO MONTE CARLO: {method_name}")
        print("="*50)
        print(f"Objetivo: Máxima rentabilidade com RoR <= 20%.")
        print(f"Stake Ideal Encontrada: {best_stake*100:.2f}% da Banca")
        print(f"RoR (Risco de Perder >80% Banca) nesta Stake: {best_ror*100:.1f}%")
        print(f"Banca Inicial: R$ {initial_bankroll}")
        print(f"Mediana da Banca Final (após {n_bets} apostas): R$ {max_median_profit:.2f}")
        
        if best_stake > 0:
            multipliers = np.maximum(1.0 + (best_stake * simulated_pnl), 0)
            bankroll_paths = initial_bankroll * np.cumprod(multipliers, axis=1)
            final_bankrolls = bankroll_paths[:, -1]
            p05 = np.percentile(final_bankrolls, 5)
            p95 = np.percentile(final_bankrolls, 95)
            print(f"Pior Cenário (5% mais azarentos): R$ {p05:.2f}")
            print(f"Cenário Otimista (5% mais sortudos): R$ {p95:.2f}")
        print("="*50)

    simulate_monte_carlo_custom(pnl_units, method_name="Lay Home Trader (Dados Planilha)")

    # =======================================================
    # PIOR CENÁRIO (STRESS TEST)
    # =======================================================
    # Reduzimos o Win Rate de 76% para 68%
    # Pioramos o Red de -31% para -45% (Atraso no Cash Out)
    # Reduzimos o lucro do Green Gol de 56% para 40%
    # Reduzimos o lucro do Green HT de 21.5% para 15%
    
    pnl_units_worst = np.zeros(n)
    for i in range(n):
        if rand_vals[i] < 0.32: # 32% de Reds agora (antes era 24%)
            # Perda = -0.45 * Liability(3.3) = -1.48 unidades
            pnl_units_worst[i] = -1.48
        elif rand_vals[i] < 0.72:
            pnl_units_worst[i] = 0.15
        else:
            pnl_units_worst[i] = 0.40

    simulate_monte_carlo_custom(pnl_units_worst, method_name="Lay Home Trader (PIOR CENÁRIO / STRESS TEST)")
