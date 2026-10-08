# PROMPT DE AUDITORIA: Backtest do Fluxo Semi-Automático (Agosto a Setembro/2026) — Bet365 ➔ OrbitX

> **Para:** Claude  
> **De:** Thiago & Gemini/Antigravity  
> **Arquivo de Execução:** [backtest_fluxo_agosto_setembro.py](backtest_fluxo_agosto_setembro.py)  
> **Data:** 08/10/2026  
> **Objetivo:** Submeter a exame forense o teste empírico do fluxo semi-automático proposto pelo Thiago, avaliado estritamente na janela recente (01/08/2026 a 24/09/2026) com dados casados Bet365 + Betfair com odd de Lay real.

---

## 1. Contexto e Motivação do Novo Teste

Na auditoria anterior ([AUDITORIA_FLUXO_SEMIAUTO_ORBITX_claude.md](AUDITORIA_FLUXO_SEMIAUTO_ORBITX_claude.md)), você avaliou o período de **16/03/2024 a 31/07/2026 (50.248 jogos)** e demonstrou com precisão que travas cegas em odds antigas da Betfair apresentavam expectativa matemática negativa ($-1,14\%$ a $-1,76\%$), apontando com razão que o spread na faixa alta era de $1,20\times$ a $1,35\times$.

No entanto, o Thiago levantou uma hipótese operacional crucial:
1. **Mudança de Metodologia na API em 2026:** Medimos na base que em 2024 e 2025 as odds da Betfair eram gravadas com dias de antecedência (spread mediano $1,073\times$, odd Lay mediana do empate $8,40$). Em 2026, a API passou a coletar muito mais perto do KO (spread caiu para $1,044\times$, odd Lay mediana do empate caiu para $7,00$).
2. **Janela Operacional Real (De Agosto para Cá):** A partir de Agosto de 2026 iniciou-se a nova temporada europeia e o coletor KO-10 próprio entrou em produção. O Thiago pediu: *"Vamos rodar o backtest pegando exatamente de Agosto para cá: o radar na Bet365 acha o jogo, nós vamos na Betfair ver a odd de Lay real; se bater no teto o jogo entra, e liquidamos pelo placar oficial."*

Rodamos esse pipeline exato no script [backtest_fluxo_agosto_setembro.py](backtest_fluxo_agosto_setembro.py).

---

## 2. Metodologia do Backtest ([backtest_fluxo_agosto_setembro.py](backtest_fluxo_agosto_setembro.py))

- **Período:** 01/08/2026 a 24/09/2026 (55 dias).
- **Base de Detecção (Radar):** [Bases_de_Dados_API_FutPythonTrader_Bet365.csv](Bases_de_Dados_API_FutPythonTrader_Bet365.csv) ($N = 8.652$ jogos no período).
- **Base de Execução (OrbitX / Lay Real):** [metodos_aprovados/.cache_base_betfair.csv](metodos_aprovados/.cache_base_betfair.csv) ($N = 4.180$ jogos no período).
- **Cruzamento:** Chave exata `(Data, Home_canon, Away_canon)`. Total de **4.180 jogos casados com sucesso**.
- **Comissão OrbitX:** $3,0\%$ no lucro líquido do Green. P&L por unidade de liability: Green $= +0,97 / (\text{Odd}_{Lay} - 1)$; Red $= -1,0$.
- **Comissão Betfair:** $5,0\%$ no lucro líquido.
- **Break-Even:** $(\text{Odd}_{Lay} - 1) / (\text{Odd}_{Lay} - \text{comissão})$.

---

## 3. Resultados Empíricos Obtidos

### Método 1: LAY DRAW (Radar B365 Fav $\le 1,40$ ➔ Filtro Lay OrbitX)
*Regra de Copas aplicada: em Copas/Mata-Mata só entra se Mandante $\le 1,40$; se Fav for visitante, bloqueia.*

| Teto de Lay OrbitX | N | Greens | Reds | Win Rate Real | Break-Even (c=3%) | Edge ($\Delta WR$) | Lucro OrbitX (c=3%) | ROI / Liability |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Lay $\le 6,50$** | 356 | 285 | 71 | 80,06% | 80,37% | $-0,31\text{ pp}$ | $-3,27\text{u}$ | $-0,43\%$ |
| **Lay $\le 7,00$** | **426** | **352** | **74** | **82,63%** | **81,24%** | **`+1,39 pp`** | **`+44,52 u`** 🟢 | **`+1,57%`** |
| **Lay $\le 7,50$** | 456 | 377 | 79 | 82,68% | 81,60% | **`+1,08 pp`** | **`+37,17 u`** 🟢 | **`+1,21%`** |
| **Lay $\le 8,00$** | 488 | 403 | 85 | 82,58% | 81,98% | **`+0,60 pp`** | **`+21,79 u`** 🟢 | **`+0,66%`** |

---

### Método 2: LAY HOME (Radar B365 Visitante Fav $\le 1,65$ ➔ Filtro Lay OrbitX)
*Green quando o jogo termina em Empate ou Vitória do Visitante (X2).*

| Teto de Lay OrbitX | N | Greens | Reds | Win Rate Real | Break-Even (c=3%) | Edge ($\Delta WR$) | Lucro OrbitX (c=3%) | ROI / Liability |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Lay $\le 6,00$** | 185 | 128 | 57 | 69,19% | 68,61% | $+0,58\text{ pp}$ | $-3,41\text{u}$ | $+2,02\%$ |
| **Lay $\le 7,00$** | 233 | 170 | 63 | 72,96% | 72,04% | $+0,93\text{ pp}$ | $+3,13\text{u}$ | $+2,16\%$ |
| **Lay $\le 8,00$** | **271** | **207** | **64** | **76,38%** | **74,14%** | **`+2,24 pp`** | **`+32,82 u`** 🟢 | **`+3,51%`** |
| **Lay $\le 10,00$** | 300 | 233 | 67 | 77,67% | 75,61% | **`+2,06 pp`** | **`+34,84 u`** 🟢 | **`+3,20%`** |

---

### Método 3: LAY AWAY (Radar B365 Mandante Fav $\le 1,40$ ➔ Filtro Lay OrbitX)
*Green quando o jogo termina em Vitória Mandante ou Empate (1X).*

| Teto de Lay OrbitX | N | Greens | Reds | Win Rate Real | Break-Even (c=3%) | Edge ($\Delta WR$) | Lucro OrbitX (c=3%) | ROI / Liability |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Lay $\le 15,00$** | 370 | 312 | 58 | 84,32% | 85,64% | $-1,32\text{ pp}$ | **$-34,42\text{u}$** 🔴 | $-1,93\%$ |
| **Lay $\le 25,00$** | 448 | 387 | 61 | 86,38% | 87,25% | $-0,86\text{ pp}$ | **$-10,67\text{u}$** 🔴 | $-1,36\%$ |

*Diagnóstico imediato:* O Lay Away foi reprovado e descartado do portfólio. As zebras visitantes surpreenderam no início de temporada e o método operou abaixo do break-even.

---

### Consolidado do Portfólio Ativo (Lay Draw $\le 7,00$ + Lay Home $\le 8,00$):
* **Apostas Totais:** $N = 697$ jogos.
* **Lucro Líquido Acumulado:** **`+77,34 u`** no OrbitX (comissão 3%).
* **Lucro na Betfair (c=5%):** `+66,16 u`.
* **Ambos com Win Rate acima do Break-Even.**

---

## 4. Questões Específicas para Auditoria do Claude

Pedimos seu exame rigoroso sobre os seguintes pontos:

1. **Integridade do Código e Casamento:** O script [backtest_fluxo_agosto_setembro.py](backtest_fluxo_agosto_setembro.py) possui algum leak, look-ahead bias ou defeito no cálculo de liability e break-even?
2. **A Hipótese de Mudança Metodológica da API:** Você confirma que a melhora de performance em 2026/Agosto advém da redução do spread gravado na API ($1,073\times \rightarrow 1,044\times$, odds de Lay mais fiéis à realidade da exchange), ou enxerga puramente variância amostral do início da temporada europeia?
3. **Robustez dos Tetos ($\le 7,00$ no Draw e $\le 8,00$ no Home):** No período analisado ($N=426$ e $N=271$), o WR superou o Break-Even em todas as bandas de 7,00 a 10,00. O teto de 7,00/8,00 é um corte estatisticamente sustentável ou corre risco de regressão à média?
4. **Veredito Operacional para OrbitX com Micro-Stake:** Diante de $+77,34\text{u}$ no período recente na odd de Lay real casada, você aprova colocar essa dupla em operação no OrbitX com **micro-stake (risco de \$2 a \$5 por entrada)** com monitoramento diário de $\Delta WR$ e CLV, ou ainda recomenda veto absoluto?
