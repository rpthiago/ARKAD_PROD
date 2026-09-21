# PROMPT DE AUDITORIA CIENTÍFICA INDEPENDENTE — ARKAD PROD (CLAUDE)

> **INSTRUÇÃO PARA O CLAUDE:**
> Você está encarregado de auditar de forma independente, fria, cética e sem qualquer viés confirmatório a nova formulação de resgate do método **Lay 0x1 (denominado "Lay 0x1 Sniper")** desenvolvida pelo Antigravity para o sistema ARKAD.
> 
> **Contexto:** No passado, o Lay 0x1 genérico foi reprovado e arquivado (Hall of Shame do `GEMINI.md`) porque tomava prejuízo em super favoritos (odd de lay explodia para 40–60, criando liability assimétrica destrutiva) e em visitantes favoritos (onde 0-1 é o placar mais provável).
> O Antigravity realizou uma varredura exaustiva no ano de 2026 completo (12.999 jogos na base `FRESH3` + 3.932 jogos do gap recente 19/08 a 20/09) e propôs uma nova regra estrita para salvar o método.
> 
> Siga rigorosamente as **5 Leis Inegociáveis do `GEMINI.md`**. Qualquer cálculo com odd de back, sem comissão ou com imputação arbitrária invalida a auditoria.

---

## 🎯 AS REGRAS DO MÉTODO CANDIDATO: "LAY 0x1 SNIPER"

O método opera sob 4 filtros simultâneos com execução pré-jogo na Betfair Exchange:

1. **Mandante Favorito Moderado:** `Odd_H_Back` entre **`1.55` e `2.15`**.
   * *Mecanismo:* O mandante tem vantagem de campo e volume ofensivo para marcar pelo menos 1 gol (quebrando o '0' do mandante), mas não é um super-favorito que inflacione a odd de lay para patamares incontroláveis.
2. **Janela de Odd de Lay Controlada:** `Odd_CS_0x1_Lay` entre **`10.0` e `16.0`**.
   * *Mecanismo:* Corta as odds baixas perigosas (<10.0, onde 0-1 ocorre com frequência) e corta as odds altas (>16.0, onde a liability destrói o portfólio no 1º red). Liability máxima travada em 15 unidades.
3. **Filtro Anti-Under:** `Odd_Under25_FT_Back >= 1.75` (ou `Odd_Over25 <= 2.25`).
   * *Mecanismo:* Elimina confrontos com expectativa de placar magro (0-0, 0-1, 1-0).
4. **Blacklist de Ligas de Placar 0-1:** Exclusão de ligas com incidência anômala de 0-1 em 2026:
   * `["ITALY 3", "SCOTLAND CUP", "SLOVAKIA 1", "URUGUAY 1", "UKRAINE 1", "SPAIN 1", "ROMANIA 1", "BOLIVIA 1", "ARGENTINA 1"]`.

---

## 📊 RESULTADOS APRESENTADOS PELO ANTIGRAVITY (PARA VOCÊ AUDITAR)

* **Ano de 2026 (Base FRESH3, N=558):**  
  * Greens: 528 | Reds: 30 | **Win Rate Real: 94,62%** vs **Break-Even: 93,60% (+1,02 pp)**
  * **PnL Líquido:** **+6,08 unidades** (comissão 5% deduzida) | **ROI sobre Liability: +1,09%**
  * Odd Mediana: 15.0
* **Gap Recente Testado (19/08 a 20/09/2026, N=29 liquidados):**  
  * Greens: 28 | Reds: 1 | **Win Rate: 96,55%** vs **Break-Even: 93,60% (+2,95 pp)**
  * **PnL Líquido: +0,92 unidades | ROI: +3,17%**
* **Histórico Multiano Reivindicado:**  
  * 2024: N=343, WR 93,29%, ROI −0,40%, PnL −1,37u
  * 2025: N=454, WR 94,05%, ROI +0,48%, PnL +2,16u
  * 2026: N=558, WR 94,62%, ROI +1,09%, PnL +6,08u
  * Total 3 Anos: N=1.355, PnL +6,87u

---

## ⚖️ ROTEIRO DE AUDITORIA EXIGIDO AO CLAUDE

Por favor, execute e reporte com precisão matemática os seguintes 5 blocos:

### Bloco 1: Auditoria de Reprodução e Integridade dos Dados
* Verifique a base `Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH3.csv` e os feeds em `scratch/feed_arquivo/`.
* Confirme se o P&L foi calculado sobre a **liability real** ($\text{odd} - 1$) ou sobre stake nominal (Lei 4 e Hall of Shame: "Miragem de Denominador em Lay").
* Confirme se os placares utilizados na liquidação batem 100% com o oficial da Betfair (sem leak de gols FT antes do KO).

### Bloco 2: Teste de Robustez à Especificação & Thresholds Vizinhos (Lei 13)
* O maior perigo no ARKAD é o *Garden of Forking Paths* (overfitting por mineração de cortes).
* Teste **thresholds vizinhos** para provar se o edge é real ou ruído estatístico:
  * Faixas de Odd Lay vizinhas: `[9.0, 15.0]`, `[10.0, 16.0]`, `[11.0, 17.0]`, `[10.0, 18.0]`.
  * Faixas de Odd Home vizinhas: `[1.50, 2.10]`, `[1.55, 2.15]`, `[1.60, 2.20]`.
  * O edge sobrevive ou inverte de sinal em cortes adjacentes?

### Bloco 3: Auditoria da Blacklist de Ligas (Lei 6 e Hall of Shame)
* O método usa uma blacklist de 9 ligas (`ITALY 3`, `ARGENTINA 1`, etc.).
* Calcule o resultado de 2026 **COM e SEM a Blacklist**:
  * O método continua com ROI > 0 sem a blacklist?
  * Se o lucro depende exclusivamente da remoção dessas 9 ligas, isso é overfitting de cauda (Lei 6: "Filtro retrospectivo de ligas inverte sinal no ano seguinte")?

### Bloco 4: Auditoria dos Circuit Breakers (Stop Red & Stop Green)
* Simule a operação diária cronológica em 2026:
  * Sem Stop vs Stop Red 1 red/dia vs Stop Red 2 reds/dia.
  * Qual o Max Drawdown em cada política?
  * Há risco de ruína ou perda de volume excessiva ao aplicar Stop Red de 1 red/dia?

### Bloco 5: Teste Estatístico (Bootstrap & Veredito Final)
* Execute um Bootstrap de 1.000 iterações por mês para o ano de 2026.
* Reporte o **Intervalo de Confiança de 95% do ROI sobre Liability**:
  * O piso do IC95% é estritamente positivo ou cruza o zero?
  * Qual o valor de $P(\text{ROI} \le 0)$?

---

## 📋 FORMATO DE SAÍDA ESPERADO

Apresente um resumo executivo com a tabela comparativa:

| Cenário / Teste | Amostra (N) | WR Real (%) | BE Exigido (%) | Margem (pp) | P&L (u) | ROI Liability (%) | IC95% Bootstrap | Veredito |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Lay 0x1 Sniper (Regra Proposta)** | 558 | 94,62% | 93,60% | +1,02 pp | +6,08u | +1,09% | [...] | ... |
| **Sem Blacklist de Ligas** | ... | ... | ... | ... | ... | ... | [...] | ... |
| **Threshold Vizinho (Lay 9-15)** | ... | ... | ... | ... | ... | ... | [...] | ... |
| **Threshold Vizinho (Lay 11-17)** | ... | ... | ... | ... | ... | ... | [...] | ... |
| **Stop Red 2 reds/dia** | ... | ... | ... | ... | ... | ... | [...] | ... |

**Veredito Final Obrigatório (escolha uma das 3 opções):**
1. **APROVADO PARA PRODUÇÃO:** Edge robusto, insensível a thresholds vizinhos, sobrevive sem blacklist de ligas e IC95% estritamente positivo.
2. **WATCHLIST STAKE-ZERO (QUARENTENA SET/OUT):** Promissor em 2026, mas margem fina (+1,0pp) e sensível; deve ser observado na Página 03 do Streamlit em stake-zero até acumular $N \ge 400$ apostas.
3. **REPROVADO / ARQUIVADO:** Miragem de especificação / overfitting de ligas.
