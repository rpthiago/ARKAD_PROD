# 🔬 PROMPT DE AUDITORIA FORENSE — DUTCHING EM PLACARES EXATOS (BETFAIR)

> **Instruções para o Usuário:** Copie todo o bloco markdown abaixo e envie diretamente para o **Claude 3.5 Sonnet ou Opus**. O prompt foi estruturado com mentalidade quantitativa adversarial, com os dados reais de 50.964 jogos e fórmulas matemáticas exatas da Betfair.

---

```markdown
Você é um Engenheiro Quantitativo Sênior, Especialista em Microestrutura da Betfair Exchange, Modelagem de Futebol (Poisson Bivariada / Dixon-Coles) e Auditor Forense de Sistemas de Apostas.

Sua missão é realizar uma **AUDITORIA FORENSE ADVERSARIAL** sobre a viabilidade matemática, estatística e operacional de estratégias de **DUTCHING EM PLACARES EXATOS (Correct Score Dutching)** na Betfair Exchange.

O ecossistema ARKAD realizou testes empíricos em **50.964 partidas reais** da base `Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH.csv` cobrindo duas rotas conceituais:
1. **ROTA 1:** Dutching Dinâmico Pré-Jogo via Modelo de Poisson (EV+).
2. **ROTA 2:** Dutching In-Play de Cobertura após o 1º Gol do Favorito.

⚠️ **SEU PAPEL NÃO É ELOGIAR:** Busque ativamente armadilhas de spread cumulativo, ilusão de cauda longa, custos de execução e desvantagens estruturais em relação a mercados puros.

---

### 1. FORMULAÇÃO MATEMÁTICA DO DUTCHING NA BETFAIR (COMISSÃO 5%)

Ao selecionar $k$ placares exatos $S = \{s_1, s_2, \dots, s_k\}$ com odds de Back $O_1, O_2, \dots, O_k$:
1. **Probabilidade Implícita Sintética ($P$):**
   $$P = \sum_{i=1}^k \frac{1}{O_i}$$
2. **Odd Sintética Bruta ($O_{synth}$):**
   $$O_{synth} = \frac{1}{P} = \frac{1}{\sum_{i=1}^k \frac{1}{O_i}}$$
3. **Distribuição de Stakes:** Para uma aposta total de $S = 1.0u$:
   $$w_i = \frac{1 / O_i}{P}$$
4. **Payoff Líquido com Comissão de 5% da Betfair:**
   * **Se qualquer placar do grupo acontecer:**
     $$\text{Lucro Líquido} = (O_{synth} - 1) \times 0.95$$
   * **Se nenhum placar acontecer (Red):**
     $$\text{Perda} = -1.0u$$
5. **Break-Even Win Rate:**
   $$\text{BE\_WR} = \frac{1}{1 + (O_{synth} - 1) \times 0.95} = \frac{1}{0.05 + 0.95 \times O_{synth}}$$

---

### 2. DADOS EMPÍRICOS: O QUE OS 50.964 JOGOS REAIS REVELARAM

#### Experimento 1 — Teste dos Clusters Clássicos Pré-Jogo:
Testamos os agrupamentos mais populares de Dutching pré-jogo do mercado em toda a base:
* **Vitória Econômica do Favorito {1-0, 2-0, 2-1}** (com Favorito $\le 1.70$):  
  $N=8.527$ | Odd Média = 2,79 | WR = 33,79% vs BE = 39,62% | **ROI = −10,79%**
* **Vitória Expandida do Favorito {1-0, 2-0, 2-1, 3-0, 3-1}** (com Favorito $\le 1.70$):  
  $N=7.830$ | Odd Média = 2,01 | WR = 48,08% vs BE = 53,89% | **ROI = −7,63%**
* **Jogo Travado / Under {0-0, 1-0, 0-1, 1-1}** (com Under 2.5 $\le 1.75$):  
  $N=11.970$ | Odd Média = 1,89 | WR = 48,88% vs BE = 55,86% | **ROI = −10,43%**

#### Descoberta de Microestrutura: A Penalidade de Arbitragem
Calculamos a odd sintética de Dutching dos 6 placares que compõem o Under 2.5 `{0-0, 1-0, 0-1, 1-1, 2-0, 0-2}` e comparamos com a odd direta de Back do mercado de Under 2.5 (`Odd_Under25_FT_Back`):
* A odd combinada do Dutching foi, em mediana, **8,2% a 10% MENOR** do que a odd do mercado dedicado de Under 2.5.
* **Causa:** No mercado Correct Score, o apostador paga o bid-ask spread de cada um dos 6 livros simultaneamente.

---

### 3. EXPERIMENTO 2 — ROTA 1: DUTCHING DINÂMICO VIA POISSON EV+ (50.177 JOGOS)

Usando médias móveis de gols marcados e sofridos das equipes calculadas estritamente sem vazamento (`shift(1)`), calculamos a probabilidade de Poisson $P(h,a)$ de todos os 16 placares e comparamos com as odds reais de Back da Betfair:
$$\text{EV} = P_{pois} \times (0.05 + 0.95 \times \text{Odd}_{Back}) - 1.0$$

Testamos selecionar os top $K$ placares que satisfazem $\text{EV} \ge \text{Threshold}$:

| Estrutura de Dutching | Threshold de EV | Amostra ($N$) | Odd Média | Win Rate Real | Break-Even | ROI Líquido |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| **$K = 2$ Placares** | $EV \ge +0.05$ | 26.413 | 10,18 | 11,59% | 12,13% | **−1,66%** |
| | $EV \ge +0.10$ | 23.769 | 10,43 | 11,41% | 11,85% | **−0,67%** |
| | $EV \ge +0.20$ | 18.989 | 10,93 | 11,16% | 11,30% | **`+1,73%`** |
| **$K = 3$ Placares** | $EV \ge +0.05$ | 19.345 | 6,39 | 17,63% | 18,67% | **−3,35%** |
| | $EV \ge +0.15$ | 13.715 | 6,75 | 17,19% | 17,66% | **−0,60%** |
| | $EV \ge +0.20$ | 11.433 | 6,95 | 16,64% | 17,09% | **−0,40%** |
| **$K = 4$ Placares** | $EV \ge +0.05$ | 10.720 | 4,63 | 23,44% | 24,60% | **−3,90%** |
| | $EV \ge +0.15$ | 5.941 | 4,97 | 21,75% | 22,79% | **−3,30%** |
| | $EV \ge +0.20$ | 4.293 | 5,17 | 20,71% | 21,88% | **−4,14%** |

---

### 4. EXPERIMENTO 3 — ROTA 2: DUTCHING IN-PLAY PÓS-1º GOL (3.562 JOGOS)

Simulamos o estado de todas as partidas onde o Mandante Favorito (`Odd_H <= 1.70`) marcou o primeiro gol entre os minutos 10 e 40 (placar 1-0 no 1º tempo):

#### Distribuição dos Placares Finais (Jogos com Under pré-jogo $\le 1.85$, $N=726$):
* **2-0:** 17,08%
* **1-0:** 16,53%
* **2-1:** 12,12%
* **3-0:** 11,16%
* **1-1:** 9,09%
* **3-1:** 6,61%
* **Outros:** 27,41%

#### Cobertura dos Clusters In-Play:
* Dutching de 4 placares `{1-0, 2-0, 2-1, 1-1}`: cobre **54,82%** dos resultados finais.
* Dutching de 5 placares `{1-0, 2-0, 2-1, 3-0, 1-1}`: cobre **65,98%** dos resultados finais.

---

### 5. SUAS QUESTÕES PERICIAIS COMO AUDITOR:

Responda com honestidade forense às seguintes questões:

#### Q1 — Fricção de Spread Cumulativo no Correct Score
O resultado que encontramos (Dutching do Under 2.5 pagar 8% a 10% MENOS que o mercado direto de Under 2.5) é uma prova matemática definitiva de que o Dutching pré-jogo de múltiplos placares é inerentemente ineficiente devido ao acúmulo de overround da Betfair? É possível bater o mercado pré-jogo pagando o spread de 3 a 5 livros de Correct Score simultâneos?

#### Q2 — A "Ilusão" de $K=2$ Placares com $EV \ge +0.20$ (+1,73% ROI)
No experimento Poisson pré-jogo, $K=2$ com $EV \ge 0.20$ deu +1,73% em $N=18.989$, com odd média de 10.93 e WR de 11,16%.  
Isso é um alpha explorável real ou é apenas a cauda longa de placares de odds altas (onde se perde 89% das apostas e a variância de amostragem mascara o custo)? Você consideraria isso um método operável?

#### Q3 — Microestrutura e Execução In-Play (Rota 2)
Na Rota 2 (Dutching In-Play pós-1º gol aos 10'-40'), 66% dos jogos Under ficam concentrados em 5 placares.  
Quais são as armadilhas operacionais reais de tentar executar essa estratégia via API da Betfair:
1. Tempo de suspensão de mercado após o gol (delay de 5 a 8 segundos).
2. Liquidez nos livros de CS in-play (volume emparelhado em 2-1, 3-0 e 3-1).
3. Risco de um segundo gol rápido antes da execução de todas as pernas do Dutching (risco de perna descasada / *unmatched order*).

#### Q4 — Dutching vs Mercados Puros (1X2 / Handicap / Over-Under)
Sob a ótica de um fundo quantitativo esportivo: por que grandes sindicatos quase nunca usam Dutching pré-jogo em Correct Score? O Dutching é apenas um produto de apelo psicológico para apostadores recreativos (que preferem "torcer por vários placares") em vez de um instrumento financeiro eficiente?

#### Q5 — Veredito Final e Governança ARKAD
Qual é a sua recomendação oficial para o ARKAD:
1. **ROTA 1 (Pré-jogo Poisson):** Desenvolver ou descartar?
2. **ROTA 2 (In-Play):** Vale o esforço de engenharia de automação ou os riscos de execução e slippage anulam a margem?
3. **Diretriz de Portfólio:** O ecossistema deve focar 100% dos recursos em validar a Tríade Base existente (Lay Home, Lay Draw, Lay Over 4.5)?
```
