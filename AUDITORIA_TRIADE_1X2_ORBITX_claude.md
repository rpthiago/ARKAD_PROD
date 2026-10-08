# AUDITORIA ARKAD: Migração Estratégica para 1X2 (Lay Draw, Lay Home, Lay Away) — Coleta Bet365 ➔ Execução OrbitX

> **Documento de Auditoria e Coordenação:** Gemini/Antigravity ➔ Claude  
> **Data:** 07/10/2026 (Noite)  
> **Objetivo:** Submeter a exame crítico a proposta de desacoplamento operacional (coleta pré-jogo Bet365 + execução em Lay no OrbitX) e auditar os números empíricos da Tríade de 1X2 em 117.771 jogos.

---

## 1. Contexto e Motivação Operacional

1. **Bloqueio Nacional da Betfair BR (MP 1.394/2026):**
   - O domínio `betfair.bet.br` redireciona via HTTP 302 para `brasilsembets.gov.br`.
   - A conta caiu em status regulatório `ACTIONS_REQUIRED`, bloqueando a autenticação da API.
   - O coletor da VPS e liquidadores foram preventivamente pausados para preservar a conta e o histórico de 3,57 GB.
2. **Abandono Estratégico de Correct Score (0x3, 2x2):**
   - Os métodos de CS tornaram-se inviáveis operacionalmente: dependiam 100% da API da Betfair, sofriam com spreads mediano de 1,19x a 1,43x, livros rasos de liquidez e alto risco de cauda (conforme alertado no Hall of Shame do `GEMINI.md`).
3. **A Nova Proposta do Thiago:**
   - Focar estritamente no mercado mais líquido e padronizado do planeta: **Match Odds (1X2)**.
   - **Desacoplamento Arquitetural:**
     - **Coleta / Radar:** Usar odds pré-jogo de 1X2 da Bet365 (base local + feeds públicos alternativos como *The Odds API* ou *football-data.co.uk*).
     - **Execução:** Entradas em **LAY** no **OrbitX** (white-label da Betfair Exchange operado via corretoras internacionais como AsianConnect/BetInAsia, sem restrições territoriais do Brasil e com **comissão de 3,0%**).

---

## 2. Estudo Empírico Preliminar (Base Bet365 Oficial)

Rodamos a simulação rigorosa diretamente na base de produção [Bases_de_Dados_API_FutPythonTrader_Bet365.csv](Bases_de_Dados_API_FutPythonTrader_Bet365.csv) (246 MB, 248.219 jogos no total), filtrando os **117.771 jogos reais** disputados entre **Janeiro de 2024 e Setembro de 2026**.

**Premissas do Modelo OrbitX:**
- Spread médio de Lay sobre a odd Bet365: **1,075x** (+7,5% acima da odd de fechamento da casa).
- Comissão da corretora: **3,0%** (taxa padrão OrbitX sobre o lucro líquido do Green).
- P&L por unidade de liability em risco:
  - Green: `+ (1.0 - 0.03) / (Odd_Lay - 1.0)`
  - Red: `- 1.0`
- Break-Even WR: `(Odd_Lay - 1.0) / (Odd_Lay - 0.03)`.

### 2.1 Resumo Estatístico dos 3 Métodos de 1X2

| Métrica | 1. Lay Draw (Super Fav) | 2. Lay Home (Fav Visitante) | 3. Lay Away (Fav Mandante) |
|---|:---:|:---:|:---:|
| **Gatilho Pré-Jogo Bet365** | $\min(\text{Odd H}, \text{Odd A}) \le 1.40$ | $\text{Odd A} \le 1.65$ | $\text{Odd H} \le 1.40$ |
| **Regra Especial de Copas** | Ligas: H ou A $\le 1.40$ \| Copas: só H $\le 1.40$ | Aberto (Ligas e Copas) | Aberto (Ligas e Copas) |
| **Banda de Odd Lay OrbitX** | $4.50 \le \text{Odd Lay} \le 10.0$ | $2.00 \le \text{Odd Lay} \le 10.0$ | $5.00 \le \text{Odd Lay} \le 25.0$ |
| **Amostra Analisada ($N$)** | **11.517 apostas** | **6.154 apostas** | **11.040 apostas** |
| **Win Rate Real (WR)** | **84,90%** | **85,70%** | **91,65%** |
| **Break-Even Necessário** | 82,81% | 84,05% | 89,67% |
| **Edge Real ($\Delta$ WR)** | **`+2,09 pp`** | **`+1,66 pp`** | **`+1,98 pp`** |
| **Odd Lay Mediana** | 5,38 (Liab: 4,38u) | 5,91 (Liab: 4,91u) | 9,14 (Liab: 8,14u) |
| **ROI / Capital em Risco** | **`+2,54%`** 🟢 | **`+1,99%`** 🟢 | **`+2,20%`** 🟢 |
| **Lucro Acumulado** | **`+1.375,6 u`** | **`+613,8 u`** | **`+2.350,1 u`** |
| **Max Drawdown Histórico** | **−115,7 u** | **−131,9 u** | **−74,0 u** |

---

### 2.2 Consistência Temporal Ano a Ano (2024, 2025, 2026)

#### 📌 Método 1: Lay Draw no Super Favorito
* **2024:** $N = 4.356$ | WR: 85,45% | **ROI/liab: `+3,00%`** | PnL: **+639,8u**
* **2025:** $N = 4.263$ | WR: 84,05% | **ROI/liab: `+1,52%`** | PnL: **+311,4u**
* **2026:** $N = 2.898$ | WR: 85,33% | **ROI/liab: `+3,33%`** | PnL: **+424,4u**
* *Diagnóstico:* 100% positivo e estável em todos os anos.

#### 📌 Método 2: Lay Home no Favorito Visitante
* **2024:** $N = 2.199$ | WR: 85,45% | **ROI/liab: `+1,50%`** | PnL: **+155,2u**
* **2025:** $N = 2.289$ | WR: 85,89% | **ROI/liab: `+2,19%`** | PnL: **+240,7u**
* **2026:** $N = 1.666$ | WR: 85,77% | **ROI/liab: `+2,38%`** | PnL: **+217,9u**
* *Diagnóstico:* Curva estritamente crescente ano a ano.

#### 📌 Método 3: Lay Away no Favorito Mandante (com Teto $\le 25.0$)
* **2024:** $N = 4.171$ | WR: 92,06% | **ROI/liab: `+2,45%`** | PnL: **+1.029,9u**
* **2025:** $N = 4.117$ | WR: 91,55% | **ROI/liab: `+2,07%`** | PnL: **+825,6u**
* **2026:** $N = 2.752$ | WR: 91,17% | **ROI/liab: `+2,01%`** | PnL: **+494,5u**
* *Diagnóstico:* Consistência regular em torno de +2,0% a +2,4%, com menor drawdown relativo (−74u).

---

### 2.3 Comparativo Crucial: Back Dupla Chance na Bet365 vs Lay OrbitX

Avaliamos também o que aconteceria se o apostador tentasse executar em **Back na Dupla Chance da Bet365** (colunas `Odd_DC_12`, `Odd_DC_X2`, `Odd_DC_1X`):

* **Dupla Chance 12 (Bet365):** $N = 14.717$ | WR: 84,83% | Odd Mediana: 1.14 | **ROI: `−2,35%` 🔴**
* **Dupla Chance X2 (Bet365):** $N = 7.607$ | WR: 87,50% | Odd Mediana: 1.11 | **ROI: `−3,14%` 🔴**
* **Dupla Chance 1X (Bet365):** $N = 11.373$ | WR: 91,85% | Odd Mediana: 1.06 | **ROI: `−2,36%` 🔴**

**Conclusão empírica:** O overround da casa esportiva tradicional (~6% a 8%) consome o edge. O lucro só existe ao atuar no lado do vendedor (LAY na Exchange), onde a comissão é cobrada apenas sobre os ganhos líquidos.

---

### 2.4 Teste de Estresse ao Spread OrbitX

Simulação variando o spread da odd de Lay em relação à Bet365:

| Spread Simulado | ROI Lay Draw | ROI Lay Home | Situação |
|:---:|:---:|:---:|:---:|
| **1,050x** (Grandes ligas europeias) | **`+3,05%`** | **`+2,46%`** | 🟢 Ampla margem |
| **1,075x** (Spread mediano de mercado) | **`+2,54%`** | **`+1,99%`** | 🟢 Padrão |
| **1,100x** (Ligas médias / mercados secundários)| **`+2,05%`** | **`+1,55%`** | 🟢 Saudável |
| **1,120x** (Pior cenário de liquidez) | **`+1,68%`** | **`+1,21%`** | 🟢 Resiste positivo |

---

## 3. Questões Críticas Submetidas à Auditoria do Claude

Pedimos seu escrutínio rigoroso sobre 4 pontos fundamentais:

1. **Validação do Desacoplamento:**
   Filtrar partidas pela closing line/pré-jogo da Bet365 para posterior execução manual/semi-automática no OrbitX é causalmente limpo ou introduz distorção de timing/odds?
2. **Auditoria do Teto do Lay Away ($\le 25.0$):**
   No `GEMINI.md`, o Lay Away havia sido arquivado como "+0,50% ROI ≈ break-even estrito" no scan amplo. Neste estudo, ao cortar as odds acima de 25.0 (eliminando o risco de zebra absurda de odd 40–100 pagar liability gigante), o ROI subiu para **+2,20% com WR de 91,65%**. Isso é uma contenção estrutural de risco de cauda válida ou constitui overfitting de corte ad-hoc?
3. **Liquidez do OrbitX na Cauda Longa:**
   A Bet365 lista centenas de ligas menores que podem não ter correspondência de liquidez no OrbitX/Betfair Exchange. Qual filtro de governança de ligas devemos manter para garantir que o usuário não receba alertas de jogos com livro vazio no OrbitX?
4. **Veredito do Portfólio & Rotina:**
   Você aprova o foco operacional na **Tríade de 1X2 (Lay Draw + Lay Home + Lay Away)** no OrbitX, com alertas organizados nos 3 blocos diários do Telegram (**06:00**, **10:30** e **16:30 BRT**)? Quais travas adicionais de mesa ou gestão de banca você recomenda?
