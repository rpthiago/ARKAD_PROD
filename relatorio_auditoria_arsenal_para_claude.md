# 📑 PROMPT E RELATÓRIO DE AUDITORIA QUANTITATIVA DO ARSENAL ARKAD PARA O CLAUDE

> **PROMPT DE COMANDO PARA ENVIAR AO CLAUDE:**
> "Claude, atue como um Quant Trader Senior e Engenheiro de Risco Esportivo. Faça uma auditoria profunda nos métodos do Arsenal ARKAD abaixo, analise a cronologia dos dados (Backtest In-Sample vs Paper Trading Out-of-Sample) e responda ao roteiro de perguntas estruturadas ao final."

---

## 📅 Cronologia Oficial dos Dados do Projeto ARKAD

1. **Etapa 1 - Backtest Histórico (In-Sample):**
   * **Início:** 01/01/2021 (ou 01/01/2024 para amostra recente de 50.000 jogos)
   * **Término:** 31/07/2026
   * **Amostra Analisada:** 50.964 partidas globais auditadas em base de dados estática.

2. **Etapa 2 - Paper Trading / Forward Testing (Out-of-Sample ao Vivo):**
   * **Início:** 01/08/2026
   * **Término:** 17/08/2026 (período atual em andamento)
   * **Amostra Monitorada:** 1.767 partidas reais capturadas diariamente pela API da Betfair com placares confirmados pelo usuário e ESPN API.

---

## 📊 Tabela Consolidada de Desempenho no Paper Trading (01/08 a 17/08/2026)

| Método | Jogos | Greens | Reds | Win Rate % | Lucro Stake R$100 | Lucro Liab R$200 | ROI % | Profit Factor | p-value |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Lay 0x0 Protegido | 195 | 144 | 51 | 73.85 | -34770.0 | -7212.22 | -178.31 | 0.28 | 0.120595 |
| Lay Draw Estrutural | 655 | 389 | 266 | 59.39 | -38410.0 | -26859.92 | -58.64 | 0.49 | 1.0 |
| Over 2.5 Back Valor | 478 | 198 | 280 | 41.42 | -12104.0 | -20735.09 | -25.32 | 0.61 | 1.0 |
| BTTS Lay Quant | 193 | 118 | 75 | 61.14 | 131.0 | 761.32 | 0.68 | 1.01 | 0.996384 |
| Lay 0x3 Visitante Under 2.5 (xG Protected) | 31 | 31 | 0 | 100.0 | 2945.0 | 229.39 | 95.0 | Infinito | 0.000134 |

---

## 🔍 Roteiro Estruturado de Perguntas para a Auditoria do Claude:

1. **Validação Quantitativa & p-value:**
   - Analisando a Win Rate do *Lay 0x3 Visitante* (100.0% - 31G/0R) e do *Lay 2x2 Quant* (95.2% - 415G/21R), o p-value confirma que a vantagem matemática (EV+) é estatisticamente significante?

2. **Diagnóstico dos Métodos Sob Estresse:**
   - Quais métodos do Arsenal devem ser mantidos em produção e quais devem passar por recalibragem de filtro (ex: Lay 0x0 e Lay Draw Estrutural)?

3. **Gestão de Banca & Risco de Ruína:**
   - Na simulação de Monte Carlo realizada com 10.000 trajetórias para um Risco de Ruína <= 20%, a alocação de **15.0% de Responsabilidade no Lay 0x3** e **6.50% no Lay 2x2** é a ideal para alavancagem por Juros Compostos?

4. **Plano de Execução Prático:**
   - Qual o melhor modelo operacional para executar o **Desafio de Alavancagem (R$ 500 -> R$ 1.000)** com saque e reset?
