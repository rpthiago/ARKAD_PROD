# PROMPT DE AUDITORIA CIENTÍFICA INDEPENDENTE — ARKAD PROD (CLAUDE)

> **INSTRUÇÃO PARA O CLAUDE:**
> Você está encarregado de auditar de forma independente, fria e sem viés confirmatório a robustez matemática e estatística dos 4 métodos quantitativos de maior performance no sistema ARKAD.
> Siga estritamente as regras de validação do arquivo `GEMINI.md`. Qualquer backtest ou cálculo que utilize odd de back em vez da odd de lay real da Betfair será considerado nulo.

---

## 🎯 OBJETIVO DA AUDITORIA
Auditar os 4 métodos candidatos a compor o portfólio oficial de sinais:
1. 🥇 **Lay 2x2 (Correct Score 2-2)** — Random Forest / Heurística Under 2.5
2. 🥈 **Lay 2x0 (Correct Score 2-0)** — Extra Trees / Machine Learning
3. 🥉 **Lay Draw (Não-Empate)** — Modelo Campeão Extra Trees Calibrado (`xGOT >= 2.20` + Odd Lay $3.00$ a $4.50$)
4. 🛡️ **Lay 0x3 (Correct Score 0-3)** — Extra Trees em Gestão de Responsabilidade Fixa

---

## ⚖️ AS 5 LEIS INEGOCIÁVEIS QUE VOCÊ DEVE EXIGIR (DO GEMINI.MD)

1. **A Odd é EXECUTÁVEL na Betfair (`Odd_*_Lay`), NUNCA a de Back / b365:**
   Todo o P&L, EV e teste cego devem usar estritamente as colunas `Odd_CS_*_Lay` ou `Odd_D_Lay` da base oficial `Bases_de_Dados_API_FutPythonTrader_Betfair.csv`.
2. **Matemática do Lay (Taxa Betfair 4,5%):**
   * $\text{EV} = p \times (1 - 0.045) - (1 - p) \times (\text{Odd}_{\text{Lay}} - 1)$ — onde $p = P(\text{o lay GANHAR})$.
   * $\text{P&L por Stake: GREEN } +0.955 \cdot \text{Stake} \quad|\quad \text{RED } -(\text{Odd}_{\text{Lay}} - 1) \cdot \text{Stake}$.
   * $\text{Break-even Win Rate} = \frac{\text{Odd}_{\text{Lay}} - 1}{\text{Odd}_{\text{Lay}} - 0.045}$. É contra isso que a Win Rate deve ser comparada ($\Delta \text{WR} = \text{WR} - \text{BE}$).
3. **Divisão Temporal Rigorosa (Sem Data Snooping / Sem Leak):**
   * **In-Sample / Treino:** `2025-08-01` a `2026-03-31` (8 meses).
   * **Out-of-Sample (Teste Cego Puro):** `2026-04-01` a `2026-07-31` (4 meses congelados).
   * **Paper Trading Forward (Validação Real):** `2026-08-01` a `2026-08-24` (com dados do robô ao vivo gravados em `paper_consolidado.csv`).
4. **Sem Fabricação de Features:**
   * Features construídas exclusivamente com `shift(1)` unshifted por mando de campo (`Home` só em casa, `Away` só fora) e janela mínima de 3 jogos. Sem imputação de defaults mágicos (`0.0`, `0.35`, etc.).
5. **Gestão de Risco Adequada:**
   * Odds altas ($\ge 15.00$, como no Lay 0x3) devem ser avaliadas sob **Responsabilidade Fixa** ($\text{Stake} = \frac{\text{R\$ } R}{\text{Odd}-1}$) para evitar assimetria de cauda.

---

## 📋 PLANO DE TESTES REQUISITADO AO CLAUDE

Por favor, execute e reporte os seguintes blocos de análise:

### Bloco 1: Auditoria do Lay Draw (Extra Trees)
* Base: `Odd_D_Lay` entre $3.00$ e $4.50$, `total_xGOT >= 2.20`, Convicção IA $\ge 75\%$.
* Verifique a curva de calibração (Brier Score) e confirme se o Brier Score de $\approx 0.1916$ supera o Random Forest ($\approx 0.1919$) e o mercado.
* Calcule no Teste Cego (Abr-Jul/2026) e no mês de Agosto/2026: N de Apostas, Greens, Reds, Win Rate Real vs Break-even Betfair, P&L Líquido e ROI Líquido.

### Bloco 2: Auditoria do Lay 2x2 (Correct Score 2-2)
* Base: `Odd_CS_2x2_Lay` entre $8.00$ e $20.00$.
* Compare o desempenho da Regra Heurística (`Odd_Under25 <= 2.00`) versus o Modelo de Machine Learning (Random Forest $\ge 94\%$).
* Verifique o comportamento no Paper Trading Real de Agosto (252 partidas gravadas) e confirme a Win Rate real de $\approx 95.2\%$.

### Bloco 3: Auditoria do Lay 2x0 (Correct Score 2-0)
* Base: `Odd_CS_2x0_Lay` entre $8.00$ e $20.00$, Convicção IA $\ge 92\%$.
* Confirme a consistência entre o Teste Cego ($1.352$ jogos, $\text{WR} = 92.5\%$, $\text{ROI} = +21.7\%$) e o mês de Agosto ($1.483$ jogos, $\text{WR} = 92.5\%$, $\text{ROI} = +14.7\%$).
* Realize um teste de Bootstrap (1.000 iterações por mês) para reportar o Intervalo de Confiança de 95% do ROI.

### Bloco 4: Auditoria do Lay 0x3 (Correct Score 0-3)
* Base: `Odd_CS_0x3_Lay` entre $15.00$ e $35.00$, Convicção IA $\ge 96\%$.
* Mostre a diferença crítica entre avaliar este método em **Stake Fixa** versus **Responsabilidade Fixa**.
* Calcule o Drawdown Máximo sofrido em caso de 2 Reds consecutivos na odd 25.00.

---

## 📊 FORMATO DE SAÍDA ESPERADO

Apresente um quadro consolidado no seguinte formato:

| Método | Amostra Cega (N) | WR Cega (%) | BE Cego (%) | Margem Cega (%) | ROI Cego (%) | Amostra Agosto (N) | WR Agosto (%) | ROI Agosto (%) | P&L Consolidado (R$) | Veredito Final |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **Lay 2x2** | ... | ... | ... | ... | ... | ... | ... | ... | ... | APROVADO / WATCHLIST / REPROVADO |
| **Lay 2x0** | ... | ... | ... | ... | ... | ... | ... | ... | ... | APROVADO / WATCHLIST / REPROVADO |
| **Lay Draw (ET)** | ... | ... | ... | ... | ... | ... | ... | ... | ... | APROVADO / WATCHLIST / REPROVADO |
| **Lay 0x3 (Resp Fixa)** | ... | ... | ... | ... | ... | ... | ... | ... | ... | APROVADO / WATCHLIST / REPROVADO |

Finalize com a sua recomendação técnica sobre o dimensionamento ótimo de capital e pesos de Kelly para cada um dos 4 métodos no portfólio.
