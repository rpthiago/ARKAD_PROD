# 🔬 PROMPT DE AUDITORIA FORENSE QUANTITATIVA — CONSTANTE K_EDGE E DINÂMICA DO DRAW

> **Instruções para o Usuário:** Copie todo o bloco markdown abaixo e envie diretamente para o **Claude 3.5 Sonnet ou Opus**. O prompt foi estruturado com os dados reais da Betfair, matemática exata de Lay, e perguntas adversariais implacáveis para testar a causalidade econômica, o risco de overfitting e a viabilidade prática.

---

```markdown
Você é um Engenheiro Quantitativo Sênior, Especialista em Modelagem Estocástica de Futebol, Microestrutura de Mercados Esportivos (Betfair Exchange) e Auditor Forense de Algoritmos de Apostas.

Sua missão é realizar uma **AUDITORIA FORENSE ADVERSARIAL** nas novas descobertas empíricas do ecossistema **ARKAD**, que envolvem:
1. Um **Scan Exaustivo de 140 Cenários** de constantes multifatoriais de odds pré-jogo.
2. A descoberta de uma nova constante composta: **$K_{edge} = \frac{D_{ratio}}{\min(\text{Odd}_H, \text{Odd}_A)}$**.
3. A descoberta de um padrão in-play severo aos **60 minutos** com dados reais de carimbo de tempo de gol (`Goals_Min`).

⚠️ **SEU PAPEL NÃO É ELOGIAR:** Busque ativamente falhas, viés de mineração (*data mining bias / p-hacking*), jardim dos caminhos que se bifurcam (*garden of forking paths*), iliquidez e riscos de execução.

---

### 1. REGRAS E CONTEXTO MATEMÁTICO DO SISTEMA (GOVERNANÇA ARKAD)

O ARKAD opera sob diretrizes estritas documentadas no `GEMINI.md`:
* **Comissão Betfair:** 5% estrita sobre os lucros brutos.
* **Matemática do Lay:** 
  $$\text{EV} = p \times (1 - 0.05) - (1 - p) \times (\text{odd} - 1)$$
  Onde $p = P(\text{o Lay vencer})$. P&L: Green $= +0.95u$, Red $= -(\text{odd} - 1)u$.
* **Break-Even Win Rate:** 
  $$\text{BE\_WR} = \frac{\text{odd} - 1}{\text{odd} - 0.05}$$
  A Win Rate observada é julgada estritamente contra o $\text{BE\_WR}$, nunca contra 50%.
* **ROI sobre Liability (Capital em Risco):**
  $$\text{ROI}_{liability} = \frac{\sum \text{P&L}}{\sum (\text{odd} - 1)}$$
* **Base de Dados Auditada:** `Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH.csv` com **50.964 jogos reais** entre 2024 e 2026, com odds executáveis de Lay e Back de todos os mercados, sem estimativas e sem imputação de features ausentes (NaN = SKIP).

---

### 2. O EXPERIMENTO 1: SCAN EXAUSTIVO DE 140 CENÁRIOS PRÉ-JOGO

Seguindo a hipótese de que combinações de odds públicas poderiam gerar alpha, foram construídas 5 constantes multifatoriais combinando Match Odds (1X2), Over/Under 2.5, Over 3.5 e BTTS:
* $C_1 = \frac{\text{Odd}_D \times \text{Odd}_{Over25}}{\text{Odd}_{Fav}^2}$
* $C_2 = \frac{\text{Odd}_D}{\text{Odd}_{Over25} \times \text{Odd}_{BTTS\_Yes}}$
* $C_3 = \frac{\text{Odd}_H}{\text{Odd}_A} \times \frac{\text{Odd}_{Under25}}{\text{Odd}_{Over25}}$
* $C_4 = \frac{D_{ratio}}{\text{Odd}_{Fav}}$
* $C_5 = \frac{\text{Odd}_{Over35}}{\text{Odd}_{Over25}}$

Cada constante foi dividida em decis e quantis (Top 10%, Bottom 10%, Top 20%, Bottom 20%) e testada contra 7 mercados executáveis de Lay (Lay Draw, Lay Home, Lay Away, Lay Over 2.5, Lay Under 2.5, Lay BTTS Sim, Lay BTTS Não), totalizando **140 cenários independentes** com filtro de liquidez (spread $\le 1.25$ e $N \ge 300$).

#### Resultado do Scan de 140 Cenários:
* **138 de 140 cenários deram ROI NEGATIVO.**
* Piores cenários:
  * Lay BTTS Sim no Bottom 10% de $C_5$: $N=2.582$ | WR = 38,26% vs BE = 43,23% | **ROI = −10,79%**
  * Lay Over 2.5 no Top 10% de $C_2$: $N=4.315$ | WR = 30,48% vs BE = 33,69% | **ROI = −9,49%**
  * Lay BTTS Sim no Bottom 20% de $C_5$: $N=5.225$ | WR = 39,75% vs BE = 43,76% | **ROI = −9,03%**
* Apenas 2 cenários pontuais ficaram ligeiramente acima de zero (+0,47% e +0,82%), ambos no Lay Away com favoritos em casa (capturando o mesmo efeito de favorito do 1X2).

---

### 3. O EXPERIMENTO 2: A CONSTANTE $K_{edge}$ E A PROGRESSÃO MONOTÔNICA

Ao isolar a dinâmica da **Odd do Draw**, identificou-se que a odd do empate na Betfair carrega um sinal forte sobre o ritmo do jogo. Foi formulado o índice $D_{ratio}$:
$$D_{ratio} = \frac{\text{Odd}_D}{\text{Mediana}_D(\text{faixa de favorito})}$$

E em seguida, formulou-se a constante composta $K_{edge}$:
$$K_{edge} = \frac{D_{ratio}}{\min(\text{Odd}_H, \text{Odd}_A)}$$

Ao dividir todos os 50.964 jogos em 5 faixas ordenadas de $K_{edge}$ no mercado de **Lay Draw** (com liquidez e odds de 3.0 a 12.0), observou-se uma **progressão monotônica estrita**:

| Faixa de $K_{edge}$ | Amostra ($N$) | Win Rate Real | Break-Even WR | Saldo vs BE | ROI s/ Liability |
|---|:---:|:---:|:---:|:---:|:---:|
| **[0.2 a 0.4]** (Jogo Equilibrado / Empate Puxado) | 7.050 | 68,27% | 70,73% | −2,46% | **−3,44%** |
| **[0.4 a 0.6]** | 25.939 | 72,99% | 74,80% | −1,81% | **−2,43%** |
| **[0.6 a 0.8]** | 7.714 | 78,74% | 80,37% | −1,63% | **−2,13%** |
| **[0.8 a 1.0]** | 1.808 | 83,52% | 84,93% | −1,41% | **−1,72%** |
| **$\ge 1.0$** (Super Fav + Empate Rejeitado) | **551** | **92,01%** | **89,56%** | **+2,45%** | **`+2,41%`** |

#### Teste de Estabilidade Temporal Ano a Ano de $K_{edge} \ge 1.0$:
* **Ano 2024:** $N=236$ | WR = 92,37% | BE = 89,28% | **ROI = +2,97%**
* **Ano 2025:** $N=311$ | WR = 89,07% | BE = 89,30% | **ROI = −0,39%** (Break-even estrito)
* **Ano 2026:** $N=205$ | WR = 91,71% | BE = 89,22% | **ROI = +2,60%**
* **Total Consolidado:** $N=752$ | WR = 90,82% | BE = 89,27% | **ROI = +1,72%**

---

### 4. O EXPERIMENTO 3: SIMULAÇÃO IN-PLAY (MINUTO 60 E 75)

Utilizando os arrays de tempo exato de cada gol (`Goals_Min_H` e `Goals_Min_A`), simulou-se o estado das 50.964 partidas aos 60 e 75 minutos:

#### A. Todos os Jogos Empatados aos 60 Minutos ($N = 17.708$ partidas):
* **Com $D_{ratio} \le 0.92$ (Mercado puxou o Draw no pré-jogo):**  
  Fica Empate FT: **49,81%** | Sai Gol pós-60': 50,19% | Média de gols pós-60': 0,87.
* **Com $D_{ratio} \ge 1.10$ (Mercado empurrou o Draw no pré-jogo):**  
  Fica Empate FT: **38,51%** | **Sai Gol pós-60': 61,49%** | Média de gols pós-60': 1,23 (+41%).

#### B. Favorito ($\le 1.50$) Empatando aos 60 Minutos:
* **Favorito $\le 1.50$ com $D_{ratio} \le 0.90$ ($N=775$):**  
  Fica Empate FT: **45,55%** | Sai Gol pós-60': 54,45%.
* **Favorito $\le 1.50$ com $D_{ratio} \ge 1.12$ ($N=689$):**  
  Fica Empate FT: **32,66%** | **Sai Gol pós-60': 67,34%** (+12,89% de probabilidade de gol tardio).

---

### 5. SUAS QUESTÕES PERICIAIS COMO AUDITOR:

Responda com honestidade forense brutal a cada um dos pontos:

#### Q1 — Causalidade vs Garimpo Estatístico (Data Mining / P-Hacking)
Depois de rodar 140 cenários e ver 138 falharem, achar $K_{edge} \ge 1.0$ com ROI de +2,41% em $N=551$ é um achado genuíno com causalidade econômica real ou é a vitória inevitável do acaso (*selection bias*) após múltiplos testes? A progressão monotônica perfeita (−3,44% $\rightarrow$ −2,43% $\rightarrow$ −2,13% $\rightarrow$ −1,72% $\rightarrow$ +2,41%) reduz ou não o risco de ser mero ruído?

#### Q2 — Mecanismo Econômico de $K_{edge}$
Por que $K_{edge} = \frac{D_{ratio}}{\min(\text{Odd}_H, \text{Odd}_A)}$ funcionaria? Existe uma razão comportamental/estrutural para a Betfair errar na precificação quando um super favorito tem a odd de empate cotada acima da mediana de seus pares? Ou isso é apenas uma redundância disfarçada da nossa regra já existente de Lay Draw em Super Favorito?

#### Q3 — O Dilema Pré-Jogo vs In-Play
No pré-jogo, o ROI de $K_{edge} \ge 1.0$ é de +1,72% a +2,41% (uma margem relativamente fina sujeita a variações de spread da Betfair).  
Por outro lado, no In-Play aos 60 minutos, a taxa de gols quando o favorito está empatando salta para **67,34%**.  
Onde reside o verdadeiro valor estatístico dessa descoberta: como um **filtro pré-jogo** ou como um **catalisador operacional In-Play**?

#### Q4 — Riscos Operacionais In-Play
Se decidirmos explorar a quebra de empate aos 60 minutos em favoritos com $D_{ratio} \ge 1.12$: quais são as armadilhas reais de execução na Betfair (delay de suspensão de mercado nos gols, spread bid-ask aos 60 minutos, volume disponível e slippage)?

#### Q5 — Veredito e Governança
Qual é a sua recomendação oficial para o ARKAD:
1. Incorporar $K_{edge} \ge 1.0$ no método oficial de Lay Draw pré-jogo da Tríade?
2. Manter a Tríade intacta e colocar $K_{edge}$ estritamente em observação no **Forward Oculto em Stake-Zero**?
3. Focar no desenvolvimento de um Radar In-Play aos 60 minutos?
4. Ou descartar tudo como overfitting?
```
