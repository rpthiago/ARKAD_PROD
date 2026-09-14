# Relatório — varredura de ranking nos 43 mercados de lay e a descoberta dos regimes de odd em Correct Score

> Cole abaixo da linha no Gemini. Data: 2026-09-13. Reproduzível: `varredura_ranking_lay_todos_mercados.py`
> (grade commitada no git ANTES de rodar, commit `2e2df6c`). Tabela completa (168 células):
> `varredura_over/varredura_ranking_lay_2026-09-13.csv`. Correção formal: `CORRECAO_REGIME_ODD_CS_para_gemini.md`.

---

## 1. A pergunta

"Dos 50+ métodos reprovados, o ranking por menor odd de lay (Todos / TOP 3 / TOP 2 / TOP 1) resgata algum?"

A maioria dos reprovados é modelo de ML, in-play ou handicap — não tem odd de lay por jogo para ordenar. O que o
ranking pode afetar são os métodos de **lay pré-jogo**. Em vez de escolher a dedo, a varredura cobriu **todos os
43 mercados de lay da base** × 4 cortes, com correção por múltiplos testes sobre o total.

## 2. Método

- **Base:** Betfair apicomunidade, 52.963 jogos com placar, 2024-03-16 → 2026-09-05, odd de **lay** (`Odd_*_Lay`).
- **Célula** = (mercado, corte). Qualificado = jogo com a odd de lay dentro de uma faixa fixa por família, **sem
  nenhum outro filtro**: Match Odds e Dupla Chance 1,5-10 · Over/Under e BTTS 1,3-6 · CS exato 5-40 · Any Other 10-80.
  Ranking = posição por menor odd de lay entre os qualificados do dia.
- **P&L:** liability 1u, comissão 5%. RED = o evento do runner aconteceu (placar FT/HT da base).
- **Estatística:** bootstrap bloco-dia 10.000 (≥6 dias), p unilateral (ROI ≤ 0; ROI negativo entra com p=1),
  **BH-FDR q=0,05 sobre M = 168** (células com N ≥ 100). Decisão: PASSA (BH e piso do IC95 > 0) / REPROVA (teto
  do IC95 < 0) / INCONCLUSIVO.
- A grade (faixas, cortes, N mínimo) foi commitada antes de qualquer resultado e não foi alterada.

## 3. Resultado da grade

```
168 células | PASSA 5 | REPROVA 96 | INCONCLUSIVO 67
```

Células que passam o BH:

| mercado | corte | N | reds | red% | WR | BE | edge | odd | ROI | IC95 | p |
|---|---|---|---|---|---|---|---|---|---|---|---|
| CS 0x3 | Todos | 11.821 | 397 | 3,4% | 96,6% | 95,6% | +1,04pp | 25,0 | +1,12% | [+0,7; +1,6] | 0,0000 |
| CS 3x3 | Todos | 1.955 | 28 | 1,4% | 98,6% | 95,9% | +2,63pp | 28,0 | +2,79% | [+2,2; +3,3] | 0,0000 |
| CS 3x3 | TOP 3 | 504 | 10 | 2,0% | 98,0% | 94,8% | +3,24pp | 21,0 | +3,53% | [+2,1; +4,8] | 0,0000 |
| CS 3x3 | TOP 2 | 387 | 7 | 1,8% | 98,2% | 94,8% | +3,44pp | 24,0 | +3,76% | [+2,2; +5,1] | 0,0000 |
| CS 3x3 | TOP 1 | 245 | 4 | 1,6% | 98,4% | 94,9% | +3,51pp | 32,0 | +3,88% | [+2,0; +5,4] | 0,0002 |

**Efeito do ranking por mercado** (ROI Todos → TOP 1):

| mercado | Todos | TOP 3 | TOP 1 | ranking (TOP1 − Todos) |
|---|---|---|---|---|
| CS 2x2 | −0,87% | +0,10% | +0,30% | +1,2pp |
| CS 3x3 | +2,79% | +3,53% | +3,88% | +1,1pp |
| CS 3x1 | −0,61% | −0,47% | +0,46% | +1,1pp |
| CS 1x3 | +0,38% | +1,20% | +0,89% | +0,5pp |
| CS 0x3 | +1,12% | +0,94% | +0,77% | −0,4pp |
| CS 0x0 | −1,40% | −2,35% | −3,95% | −2,6pp |
| CS 2x0 | −0,09% | −0,94% | −3,02% | −2,9pp |
| Match Odds H | −3,99% | −7,25% | −6,10% | −2,1pp |
| Match Odds A | −3,42% | −3,66% | −7,82% | −4,4pp |
| Match Odds D | −2,89% | −3,67% | −5,85% | −3,0pp |
| Dupla Chance X2 | −5,68% | −9,79% | −17,23% | −11,6pp |
| BTTS Yes | −7,53% | −8,06% | −12,44% | −4,9pp |
| Under 1.5 FT | −3,68% | −3,96% | −7,31% | −3,6pp |
| Under 2.5 FT | −4,90% | −3,38% | −8,20% | −3,3pp |
| Over 1.5 FT | −10,07% | −11,39% | −18,78% | −8,7pp |

- Em **todos os mercados líquidos** (Match Odds, Dupla Chance, Over/Under, BTTS) o ranking **piora** o resultado,
  entre −2 e −12pp: "menor odd de lay" nesses mercados é "favorito mais forte", e o que se paga é o overround.
  Nenhum dos métodos reprovados nesses mercados (Lay Home, Lay Away, Lay Draw, Lay Under 1.5, Lay BTTS) é
  resgatado por corte algum.
- Em **Correct Score** o ranking ajuda em 2x2, 3x3, 3x1, 1x3 (+0,5 a +1,2pp) e piora em 0x0, 2x0, 0x1, 1x0, 3x0
  (−1,4 a −2,9pp).
- A única célula "nova" com sinal foi o **Lay 3x3**. A verificação dela ano a ano é o que levou à seção 4.

## 4. O que a verificação do 3x3 revelou: três regimes de odd de lay na base

Ano a ano, o 3x3 tem 46 apostas em 2024, 51 em 2025 e **1.858 em 2026**. O 0x3 cru (faixa 5-40, sem filtro) é
**−0,91% em 2024, −0,24% em 2025 e +2,98% em 2026** — a taxa de 0-3 dentro do subconjunto qualificado cai de 5,0%
para 1,8%. Testado se o placar de 2026 estaria truncado: **não** — gols por jogo 2,68 / 2,70 / 2,73, P(0-3) na
população 2,24% / 2,15% / 2,24%, e a base bate 100% com a b365 em todos os anos. O que muda é a **odd gravada**:

| meses | lay 0x3 mediana | back 0x3 mediana | spread do CS | spread do Match Odds | % de jogos com lay 0x3 ≤ 40 |
|---|---|---|---|---|---|
| 2024-03 → 2025-12 | 85-280 | 5-18 | **500-4.000%** | 3-6% | 13-24% |
| **2026-01 → 2026-04** | **38-42** | 17-23 | **20-38%** | 2-3% | **50-54%** |
| 2026-05 → 2026-06 | 110-130 | 14-16 | 450-620% | 3-4% | 20% |
| 2026-07 → 2026-09 | 72-90 | 25-28 | 60-70% | 2-3% | 23-28% |

Em 2024-25 o lado de lay do CS na base é um **livro vazio** (odd 100-1000 = ninguém oferecendo). Um jogo que
"qualifica" com lay ≤ 35 nesse regime é um jogo em que alguém já oferecia lay horas antes do KO — uma população
selecionada. Em jan-abr/2026 o snapshot é de **livro cheio** (spread 20-38%, metade dos jogos qualifica). A
cobertura das colunas é ~99% em todos os anos; o que muda é o valor, não a presença.

**Efeito nos métodos** (funções oficiais dos módulos, liability 1u, comissão 5%):

| método | A · 2024-25 (vazio) | **B · jan-abr/26 (cheio)** | C · mai-jun/26 (vazio) | D · jul-set/26 (interm. = forward) |
|---|---|---|---|---|
| Lay 0x3 TOP 3 | N=1.025 · 40 reds · **−0,28%** | N=338 · **0 reds** · **+5,64%** | N=71 · 1 red · +2,30% | N=99 · 3 reds · **+0,57%** |
| Lay 0x3 amplo (sem ranking) | N=1.666 · 58 reds · **−0,06%** | N=1.861 · 15 reds · **+3,47%** | N=105 · 2 reds · +1,59% | N=168 · 4 reds · **+1,07%** |
| Lay 2x2 TOP 3 | N=1.277 · 77 reds · **−0,78%** | N=337 · 12 reds · **+5,96%** | N=108 · 4 reds · +1,75% | N=154 · 6 reds · **+1,55%** |
| Lay 3x3 (faixa 5-40) | N=97 · 3 reds · −0,55% | N=1.760 · 23 reds · **+3,10%** | N=38 · 1 red · +0,04% | N=60 · 1 red · +1,02% |
| Lay 0x2 Zebra | N=18 · 0 reds | N=122 · 2 reds · +2,71% | — | — |
| Lay 2x0 Zebra | N=32 · 2 reds · +1,24% | N=133 · 2 reds · +5,64% | N=1 | — |

**Toda a força estatística reportada em Correct Score nos relatórios de 12-13/09 vem do regime B (4 meses).**
Fora dele, os métodos são ≈ 0 (regime A) ou +0,6 a +1,6% com N pequeno (regime D). O regime B é também o mais
próximo de uma odd executável (livro cheio) — não está "errado" — mas não é multi-ano, e o forward (regime D,
feed ≈ odd no KO) é o único dado que mede a mesma coisa que se aposta.

## 5. O que fica retificado nos relatórios anteriores

| afirmação anterior | status |
|---|---|
| "Lay 0x3 / 2x2 sobrevivem em 30 meses" (auditoria TOP 3, item 6) | **retirada** — os 30 meses somam regimes incomparáveis |
| "A regra ampla do 0x3 é o resultado mais forte da auditoria (+1,76%, P=0,000)" | **condicionada** ao regime B; em D é +1,07% com 168 apostas |
| "2026 é in-sample" (auditoria TOP 3) | **incompleta** — além de in-sample, 2026 é outro regime de odd |
| Comparação kept vs discarded (2x2: TOP 3 seleciona; 0x3: TOP 3 descarta a fatia melhor) | **mantida** — é dentro do mesmo regime |
| Mecanismo "ranking atrai o desastre nos Any Other e no 0x3; seleciona alpha no 2x2" | **mantido** — é por posição, dentro do regime |
| Classificações (🟡 2x2 e 0x3; 🔴 AO Away/Home, 0x1, 1x0; 🟡 0x0 quant., 0x2, 2x0) | **inalteradas** — o gatilho sempre foi o forward, que está no regime D com N < 100 |
| "0x2 e 2x0 sem multi-ano" | **reforçada** — e vale também para 3x3 (1.760 de 1.955 em B) |

Os três relatórios anteriores receberam um aviso no topo apontando para `CORRECAO_REGIME_ODD_CS_para_gemini.md`.

## 6. O que NÃO é afetado

- Match Odds, Over/Under, BTTS, Dupla Chance: spread estável (3-6%) em todos os meses. As reprovações desses
  mercados não dependem de regime.
- A liquidação oficial na VPS e o feed diário: independem da base histórica. O ledger do forward (01/08+) usa odd
  do feed que bate com a odd do coletor no KO (mediana Δ = 0,00 no 2x2; +0,50 no 0x3).

## 7. Resposta à pergunta original

Dos 50+ reprovados, **o ranking por menor odd não resgata nenhum**. Nos mercados líquidos ele piora; em Correct
Score o "resgate" aparente é artefato do regime jan-abr/2026. O que sobrevive é o mecanismo (CS: odd baixa =
mercado superestima o placar em 2x2; = motivo real em 0x3 e Any Other), cujo tamanho só pode ser medido no
forward — onde tudo está entre +0,6% e +1,6% com N < 100.

## 8. Perguntas abertas para o Gemini

1. O que mudou na captura da apicomunidade em jan/2026 e reverteu em mai/2026 (hora do snapshot? fonte?). Sem
   isso, todo backtest de CS na base precisa ser reportado por regime.
2. O Lay 0x0 XGB pré-registrado (+1,17% em 30 meses, IC [+0,29; +1,98]) usa `Odd_CS_0x0_Lay`, que também muda de
   regime (mediana 17-22 em A, 14,5-17,5 em B). Quanto desse número é regime B? A decomposição ainda não foi feita.
3. Existe fonte de odd de lay de CS pré-KO consistente para 2024-25? O coletor da VPS só existe desde ago/2026.
   Sem ela, o histórico utilizável de CS se resume a jan-set/2026, e o forward é o que resta.
