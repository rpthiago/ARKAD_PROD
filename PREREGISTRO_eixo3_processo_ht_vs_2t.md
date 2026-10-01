# 🔬 PRÉ-REGISTRO FORMAL — EIXO 3: O PROCESSO DO 1º TEMPO (HT) COMO PREDITOR DO 2º TEMPO (2T)
> **Status:** CONGELADO ANTES DOS RESULTADOS  
> **Data:** 2026-09-28  
> **Autoridade:** Regras de Engenharia do ARKAD (GEMINI.md) + PROMPT_GEMINI_metodos_com_xg.md

---

## 1. Regra Congelada em Uma Frase
> *"Testar se o volume de xG e finalizações acumulado no 1º tempo (HT) possui poder preditivo estatisticamente significante sobre a ocorrência de gols no 2º tempo em jogos empatados em 0-0 no intervalo, após controlar pela Closing Line pré-jogo de Over 2.5."*

---

## 2. Mecanismo Causal
1. **Decaimento Temporal vs Acumulação de Pressão:** Em jogos 0-0 no intervalo, a odd de Over (Over 1.5 ou Over 2.5 FT) sobe fortemente por decaimento de tempo (45 minutos se passaram sem gols).
2. **Separação de Estados (Jogo Morto vs Jogo de Chances Desperdiçadas):**
   * Estado 1: 0-0 com xG 0,15 (nenhuma chance real, defesas intactas, ritmo lento).
   * Estado 2: 0-0 com xG 1,50 (bolas na trave, milagres do goleiro, defesas expostas).
3. **Se o mercado in-play de intervalo precificar o 0-0 apenas pelo tempo decorrido** e não pelo volume de perigo criado no 1T, existirá ineficiência (alpha) para apostas de gols no 2T (Over 1.5 FT ou Back Favorito).

---

## 3. Amostra e Cobertura Declarada (Lei 6)
* **Base:** `Bases_de_Dados_API_FutPythonTrader_Bet365.csv` (1998→2026).
* **Filtro Temporal:** Temporadas recentes 2024, 2025 e 2026 (onde a coleta de xG HT é ativa).
* **Filtro de Integridade:** Excluir zeros fabricados (manter apenas jogos onde `Total_Shots_HT > 0` e `xG_HT > 0`).
* **Universo Alvo:** Partidas com placar de **0-0 no intervalo** (`Goals_H_HT == 0` e `Goals_A_HT == 0`).

---

## 4. Modelos Econométricos Testados

1. **Modelo Logístico 1 (Gols no 2º Tempo):**
   $$\text{logit}(P(\text{Gols 2T} \ge 1)) = \beta_0 + \beta_1 \log(\text{Odd\_Over25\_Pre}) + \beta_2 \text{xG\_Tot\_HT} + \beta_3 \text{SoT\_Tot\_HT}$$

2. **Modelo Logístico 2 (2+ Gols no 2º Tempo):**
   $$\text{logit}(P(\text{Gols 2T} \ge 2)) = \beta_0 + \beta_1 \log(\text{Odd\_Over25\_Pre}) + \beta_2 \text{xG\_Tot\_HT} + \beta_3 \text{SoT\_Tot\_HT}$$

3. **Sub-Hipótese 3-B (Favorito Mandante Dominante em 0-0 HT):**
   * Filtro: `Odd_H <= 1.50` & `0-0 no HT` & `xG_H_HT >= 1.00`.
   * Alvo: Vitória do Favorito no FT (`Goals_H_FT > Goals_A_FT`).

---

## 5. Critérios Rígidos de Aprovação vs Reprovação
* **Aprovação:** $\beta_2 > 0$ com $p < 0,05$ na presença do preço pré-jogo de Over 2.5, com estabilidade temporal consistente em 2024, 2025 e 2026.
* **Reprovação:** Se $\beta_2$ não for significante ($p \ge 0,05$), provando que xG alto no 1T sem gols não aumenta a taxa de conversão do 2T (independência temporal do processo de Poisson).
