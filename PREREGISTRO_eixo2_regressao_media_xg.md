# 🔬 PRÉ-REGISTRO FORMAL — EIXO 2: REGRESSÃO À MÉDIA EM FINALIZAÇÃO (LUCK FADE VIA xG)
> **Status:** CONGELADO ANTES DOS RESULTADOS  
> **Data:** 2026-09-28  
> **Autoridade:** Regras de Engenharia do ARKAD (GEMINI.md) + PROMPT_GEMINI_metodos_com_xg.md

---

## 1. Regra Congelada em Uma Frase
> *"Apostar em LAY contra times que vêm de sequências extremas de sobre-desempenho em gols em relação ao xG produzido ($Gols - xG \ge +0,60$ por jogo nos últimos 5 jogos com $shift(1)$ estrito), explorando a sobre-reação e compressão de preço da Closing Line da Betfair Exchange."*

---

## 2. Mecanismo Causal (Por que o mercado erraria?)
1. **Saliência do Placar vs Invisibilidade do xG:** O mercado recreativo e grande parte do fluxo de apostas ancoram fortemente em resultados recentes de placar visível (times que vêm de vitórias com 2, 3 ou 4 gols marcados).
2. **Natureza Estocástica da Taxa de Conversão:** A literatura empírica de analytics de futebol demonstra que a conversão de finalizações em gols ($Gols / xG$) no nível de equipe possui um fortíssimo componente de variância/sorte no curto prazo (chutes desviados, golaços fora da área, falhas de goleiros).
3. **Sobre-reação de Preço:** Quando um time converte muito acima do seu xG por 3 a 5 jogos seguidos, sua odd de vitória encolhe (preço fica caro / odd de Lay fica comprimida). No momento em que a taxa de finalização regride à média, o time volta a marcar de acordo com suas chances reais, gerando valor sistemático no LAY.

---

## 3. Formulação Matemática das Features (Leak-Free)

Para cada equipe $T$ na partida da data $D$, calculamos o histórico dos últimos $K = 5$ jogos anteriores oficiais (com $shift(1)$ obrigatório):

1. **Sorte Ofensiva (Overperformance de Finalização):**
   $$\text{Luck}_{\text{off}}(T) = \frac{1}{K} \sum_{i=1}^K (\text{GF}_i - \text{xGF}_i)$$
   * $\text{Luck}_{\text{off}} > 0$: time marcou mais gols do que produziu em chances.
   * $\text{Luck}_{\text{off}} < 0$: time marcou menos gols do que produziu em chances.

2. **Sorte Defensiva (Underperformance do Adversário):**
   $$\text{Luck}_{\text{def}}(T) = \frac{1}{K} \sum_{i=1}^K (\text{GA}_i - \text{xGA}_i)$$
   * $\text{Luck}_{\text{def}} < 0$: time sofreu menos gols do que cedeu em chances (goleiro salvador / adversários erraram gols feitos).

3. **Sorte Líquida Total:**
   $$\text{Luck}_{\text{net}}(T) = \text{Luck}_{\text{off}}(T) - \text{Luck}_{\text{def}}(T)$$

4. **Diferencial no Confronto (Delta Luck):**
   $$\Delta \text{Luck} = \text{Luck}_{\text{net}}(\text{Mandante}) - \text{Luck}_{\text{net}}(\text{Visitante})$$

---

## 4. Sub-Hipóteses Testadas na Grade Pré-Registrada

| Código | Hipótese | Regra de Entrada | Mercado | Faixa de Odd Lay |
| :--- | :--- | :--- | :---: | :---: |
| **H2-A** | **Lay Mandante Sortudo** | $\text{Luck}_{\text{net}}(H) \ge +0,60$ e $\text{Luck}_{\text{net}}(A) \le 0,00$ | Lay Home | `Odd_H_Lay ∈ [1.50, 4.00]` |
| **H2-B** | **Lay Visitante Sortudo** | $\text{Luck}_{\text{net}}(A) \ge +0,60$ e $\text{Luck}_{\text{net}}(H) \le 0,00$ | Lay Away | `Odd_A_Lay ∈ [1.50, 4.00]` |
| **H2-C** | **Fade Conjunto Over 2.5** | $\text{Luck}_{\text{off}}(H) + \text{Luck}_{\text{off}}(A) \ge +1,00$ | Lay Over 2.5 | `Odd_Over25_Lay ∈ [1.60, 2.50]` |

*Thresholds vizinhos auditados obrigatoriamente para teste de estabilidade:* $\tau \in \{0,40;\ 0,60;\ 0,80\}$.

---

## 5. Janela de Dados e Restrições de Cobertura
* **Universo:** 32.070 partidas de 2026 em `estatisticas_jogos.csv` cruzadas com odds reais da Betfair (`FRESH3` + feeds diários).
* **Filtro de Cobertura:** Apenas partidas onde ambos os times possuem pelo menos $K=3$ jogos prévios com xG coletado.
* **Declarar:** Número de jogos qualificados vs. número de jogos descartados por ausência de xG na liga.

---

## 6. Critérios Rígidos de Decisão (Aprovação vs Reprovação)

1. **Aprovação para Watchlist Stake-Zero:**
   * $N \ge 200$ apostas na janela com xG.
   * **ROI sobre liability $> 0$** (comissão 5% da Betfair).
   * **Piso do IC95% (via Bootstrap Bloco-Dia com $\ge 1.000$ iterações) $> -1,0\%$**.
   * **Significância Estatística contra a Odd:** O termo $\Delta \text{Luck}$ deve ser estatisticamente significante ($p < 0,05$) na presença de $\log(\text{Odd})$ na regressão logística:  
     $$\text{logit}(P(\text{Vitória})) = \beta_0 + \beta_1 \log(\text{Odd}) + \beta_2 \Delta \text{Luck}$$
     com $\beta_2$ apontando na direção esperada.
   * **Monotonia nos Vizinhos:** O sinal não pode inverter ao trocar $\tau = 0,60$ por $0,40$ ou $0,80$.
2. **Reprovação Definitiva:**
   * Se $\beta_2$ não for significante ($p \ge 0,05$), provando que a Closing Line da Betfair já absorve o xG e não deixa resíduo explorável.
   * Se o ROI líquido sobre liability for negativo ($< 0\%$).
