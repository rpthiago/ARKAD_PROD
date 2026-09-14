# AUDITORIA INDEPENDENTE — 7 mercados remanescentes de Correct Score × ranking diário por menor odd

> ⚠️ **CORREÇÃO 13/09 (ler antes):** a base tem três regimes de odd de lay em Correct Score e os ROIs
> agregados aqui misturam regimes incomparáveis. Toda a força estatística de CS vem de jan-abr/2026.
> Ver `CORRECAO_REGIME_ODD_CS_para_gemini.md`.


> Executada em 2026-09-13. Reproduzível: `auditar_ranking_cs.py` (base) e o bloco operacional em
> `varredura_over/`. Tabelas: `varredura_over/auditoria_ranking_cs_2026-09-13.csv` (+ `_apostas.csv`, uma linha por aposta).

## Dados e método

- **Base:** `metodos_aprovados/.cache_base_betfair.csv` = a mesma base Betfair apicomunidade citada como FRESH3
  (52.963 jogos com placar, 2024-03-16 → 2026-09-05; 14.132 em 2026). Odd de **lay** real (`Odd_CS_*_Lay`).
- **Regras:** exatamente as especificadas. Ranking = posição por menor odd de lay entre os qualificados do dia
  (`rank(method="first")`). Todos / TOP 3 / TOP 2 / TOP 1.
- **Liquidação:** placar FT da base. "Any Other Away Win" = visitante vence com ≥4 gols; "Any Other Home Win" =
  mandante vence com ≥4 gols.
- **P&L:** liability 1u, comissão 5%. GREEN 0,95/(odd−1), RED −1. BE = (odd−1)/(odd−0,05).
- **Bootstrap:** bloco-dia, 10.000, mínimo 6 dias. IC95 do ROI e P(ROI ≤ 0).
- **Coletor (VPS):** 1.848.407 capturas pré-KO dos 7 runners, 5.023 jogos, 17/08 → 14/09.

**Os números de 2026 do Antigravity reproduzem** (AO Away TOP 2: red 7,11% / −2,64% vs −2,66%; 0x0 TOP 1: +2,17%,
N=157; 2x0 Todos +5,64%; etc.). O que esta auditoria acrescenta é o multi-ano, o mecanismo por posição e o coletor.

**Aviso de cobertura:** as colunas `Odd_CS_0x2_Lay` e `Odd_CS_2x0_Lay` quase não existem antes de 2026 — dos 140/166
qualificados na base inteira, 122/134 são de 2026. Para 0x2 e 2x0 **não há multi-ano**; "base completa" ≈ 2026.

---

## 1. Tabela consolidada

`red%` = taxa do placar; `edge` = WR − BE em pp; `odd` = odd de lay média; IC95 e P(≤0) do ROI sobre liability.

### Lay Goleada Visitante — Any Other Away Win (U2.5 ≤2,10; lay 15-60)

| período | corte | N | G | R | red% | WR | BE | edge | odd | ROI | IC95 | P(≤0) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| completo | Todos | 11.813 | 11.375 | 438 | 3,71% | 96,3% | 97,0% | −0,71 | 36,9 | **−0,74%** | [−1,1; −0,4] | 1,000 |
| completo | TOP 3 | 2.300 | 2.184 | 116 | 5,04% | 95,0% | 95,7% | −0,70 | 24,9 | −0,73% | [−1,7; +0,2] | 0,941 |
| completo | TOP 2 | 1.605 | 1.518 | 87 | 5,42% | 94,6% | 95,4% | −0,85 | 23,5 | −0,89% | [−2,1; +0,2] | 0,938 |
| completo | TOP 1 | 837 | 792 | 45 | 5,38% | 94,6% | 95,1% | −0,46 | 21,4 | −0,49% | [−2,2; +1,1] | 0,718 |
| 2026 | Todos | 3.361 | 3.232 | 129 | 3,84% | 96,2% | 96,9% | −0,77 | 36,0 | −0,81% | [−1,4; −0,2] | 0,994 |
| 2026 | TOP 3 | 648 | 611 | 37 | 5,71% | 94,3% | 95,6% | −1,31 | 24,7 | −1,39% | [−3,3; +0,4] | 0,933 |
| 2026 | TOP 2 | 450 | 418 | 32 | **7,11%** | 92,9% | 95,4% | −2,49 | 23,3 | **−2,64%** | [−5,3; −0,2] | 0,983 |
| 2026 | TOP 1 | 237 | 221 | 16 | 6,75% | 93,2% | 95,1% | −1,85 | 21,7 | −1,96% | [−5,5; +1,2] | 0,879 |

Mecanismo por posição (completo): rank 1 odd lay 21,4 · **visitante 2,22** · mandante 3,87 · **P(goleada) 5,38%** →
rank 5+ odd 40,4 · visitante 2,90 · mandante 2,87 · P 3,31%. A odd de lay cai porque o visitante é o favorito; a
goleada fica **mais** provável (5,4% vs 3,3%) enquanto o BE só cai de 97,4% para 95,1%. **Hipótese confirmada e
estrutural em 30 meses.**

### Lay Goleada Mandante — Any Other Home Win (U2.5 ≤2,10; lay 15-60)

| período | corte | N | G | R | red% | WR | BE | edge | odd | ROI | IC95 | P(≤0) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| completo | Todos | 16.821 | 16.097 | 724 | 4,30% | 95,7% | 96,5% | −0,84 | 32,3 | **−0,86%** | [−1,2; −0,5] | 1,000 |
| completo | TOP 3 | 2.471 | 2.326 | 145 | 5,87% | 94,1% | 94,9% | −0,80 | 20,5 | −0,84% | [−1,9; +0,1] | 0,955 |
| completo | TOP 2 | 1.704 | 1.607 | 97 | 5,69% | 94,3% | 94,7% | −0,43 | 19,5 | −0,47% | [−1,7; +0,7] | 0,780 |
| completo | TOP 1 | 877 | 827 | 50 | 5,70% | 94,3% | 94,5% | −0,18 | 18,2 | −0,21% | [−1,9; +1,4] | 0,586 |
| 2026 | Todos | 4.644 | 4.458 | 186 | 4,01% | 96,0% | 96,4% | −0,42 | 30,9 | −0,43% | [−1,1; +0,2] | 0,915 |
| 2026 | TOP 3 | 689 | 655 | 34 | 4,93% | 95,1% | 94,9% | +0,18 | 20,4 | +0,19% | [−1,5; +1,8] | 0,403 |
| 2026 | TOP 2 | 472 | 453 | 19 | 4,03% | 96,0% | 94,7% | +1,28 | 19,4 | **+1,35%** | [−0,6; +3,1] | 0,075 |
| 2026 | TOP 1 | 240 | 228 | 12 | 5,00% | 95,0% | 94,4% | +0,60 | 17,8 | +0,62% | [−2,5; +3,3] | 0,317 |

O +1,3% do TOP 2 em 2026 **não sobrevive ao multi-ano** (completo TOP 2: −0,47%, IC [−1,7; +0,7]). Mesma mecânica
do lado visitante: rank 1 tem mandante 1,98 (favorito) e P(goleada) 5,7% vs 4,0% no rank 5+.

### Lay 0x1 Super Fav Mandante (H ≤1,90; lay 5-15)

| período | corte | N | G | R | red% | WR | BE | edge | odd | ROI | IC95 | P(≤0) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| completo | Todos | 447 | 411 | 36 | 8,05% | 91,9% | 93,3% | −1,31 | 14,2 | −1,42% | [−4,1; +1,1] | 0,858 |
| completo | TOP 3 | 433 | 398 | 35 | 8,08% | 91,9% | 93,3% | −1,33 | 14,2 | −1,44% | [−4,2; +1,1] | 0,857 |
| completo | TOP 2 | 404 | 372 | 32 | 7,92% | 92,1% | 93,2% | −1,15 | 14,2 | −1,25% | [−4,1; +1,4] | 0,808 |
| completo | TOP 1 | 297 | 272 | 25 | 8,42% | 91,6% | 93,2% | −1,57 | 14,0 | −1,70% | [−5,3; +1,5] | 0,828 |
| 2026 | Todos | 148 | 137 | 11 | 7,43% | 92,6% | 93,3% | −0,77 | 14,4 | −0,81% | [−5,5; +3,4] | 0,620 |
| 2026 | TOP 2 | 134 | 125 | 9 | 6,72% | 93,3% | 93,3% | −0,03 | 14,3 | −0,02% | [−4,8; +4,1] | 0,482 |

Nenhum corte, nenhum período positivo. A odd de lay concentra em 14 (BE 93,3%, exige red < 6,7%) e o 0x1 sai em
7,4-8,4%. Consistente com a auditoria do Lay 0x1 RF v2 (−2,05% na odd real, N=1.531).

### Lay 1x0 Super Fav Visitante (A ≤1,90; lay 5-15)

| período | corte | N | G | R | red% | WR | BE | edge | odd | ROI | IC95 | P(≤0) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| completo | Todos | 145 | 134 | 11 | 7,59% | 92,4% | 92,6% | −0,18 | 13,4 | −0,14% | [−5,9; +4,6] | 0,500 |
| completo | TOP 1 | 110 | 103 | 7 | 6,36% | 93,6% | 92,5% | +1,14 | 13,3 | +1,27% | [−4,0; +5,9] | 0,270 |
| 2026 | Todos | 39 | 36 | 3 | 7,69% | 92,3% | 93,1% | −0,81 | 14,1 | −0,82% | [−13,9; +7,6] | 0,510 |

145 jogos em 30 meses (110 dias com algum jogo). Volume ~1 por semana. Não há sinal em nenhum corte.

### Lay 0x0 Quantitativo (H ≤1,50 ou A ≤1,40; lay 10-20)

| período | corte | N | G | R | red% | WR | BE | edge | odd | ROI | IC95 | P(≤0) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| completo | Todos | 1.262 | 1.192 | 70 | 5,55% | 94,5% | 94,0% | +0,42 | 16,4 | +0,45% | [−0,9; +1,7] | 0,249 |
| completo | TOP 3 | 1.022 | 961 | 61 | 5,97% | 94,0% | 93,9% | +0,17 | 16,0 | +0,19% | [−1,4; +1,6] | 0,409 |
| completo | TOP 2 | 859 | 807 | 52 | 6,05% | 93,9% | 93,8% | +0,19 | 15,8 | +0,21% | [−1,6; +1,8] | 0,408 |
| completo | TOP 1 | 554 | 524 | 30 | 5,42% | 94,6% | 93,5% | +1,04 | 15,3 | +1,11% | [−1,0; +3,0] | 0,149 |
| 2026 | Todos | 367 | 347 | 20 | 5,45% | 94,6% | 94,1% | +0,48 | 16,5 | +0,53% | [−2,1; +2,9] | 0,329 |
| 2026 | TOP 1 | 157 | 150 | 7 | 4,46% | 95,5% | 93,5% | +1,99 | 15,3 | **+2,17%** | [−1,8; +5,4] | 0,105 |

Todos os pontos positivos, nenhum IC descola de zero. O TOP 1 comprime para 554 apostas em 30 meses (0,6/dia). Por
posição não há monotonia (rank 2 −1,36pp, rank 4 +3,37pp) — é ruído de N pequeno, não estrutura.
**Comparação obrigatória:** o Lay 0x0 XGB já pré-registrado (liga_rate <0,08, mkt_prob <0,10, lay 10-20) dá, na
mesma base, N=1.947, +1,17%, IC95 [+0,29; +1,98], p=0,0047 — mesmo mercado, filtro melhor fundamentado, IC descolado.

### Lay 0x2 Zebra (H ≤1,45; lay 5-25) — **sem multi-ano: 122 de 140 são 2026**

| período | corte | N | G | R | red% | WR | BE | edge | odd | ROI | IC95 | P(≤0) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| completo | Todos | 140 | 138 | 2 | 1,43% | 98,6% | 95,5% | +3,09 | 21,9 | +3,26% | [+0,9; +4,9] | 0,005 |
| completo | TOP 3 | 120 | 119 | 1 | 0,83% | 99,2% | 95,4% | +3,77 | 21,6 | +3,98% | [+2,0; +5,1] | 0,001 |
| completo | TOP 1 | 77 | 76 | 1 | 1,30% | 98,7% | 95,2% | +3,54 | 20,9 | +3,76% | [+0,8; +5,5] | 0,016 |
| 2026 | Todos | 122 | 120 | 2 | 1,64% | 98,4% | 95,8% | +2,59 | 22,6 | +2,71% | [+0,1; +4,5] | 0,022 |

### Lay 2x0 Zebra (A ≤1,45; lay 5-25) — **sem multi-ano: 134 de 166 são 2026**

| período | corte | N | G | R | red% | WR | BE | edge | odd | ROI | IC95 | P(≤0) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| completo | Todos | 166 | 162 | 4 | 2,41% | 97,6% | 93,1% | +4,45 | 14,8 | **+4,79%** | [+2,0; +6,9] | 0,001 |
| completo | TOP 3 | 144 | 140 | 4 | 2,78% | 97,2% | 93,0% | +4,25 | 14,4 | +4,58% | [+1,5; +7,0] | 0,004 |
| completo | TOP 1 | 88 | 85 | 3 | 3,41% | 96,6% | 92,7% | +3,91 | 13,9 | +4,20% | [−0,5; +7,8] | 0,031 |
| 2026 | Todos | 134 | 132 | 2 | 1,49% | 98,5% | 93,2% | +5,26 | 14,8 | +5,64% | [+3,0; +7,4] | 0,000 |

**Armadilha de cauda declarada:** 0x2 tem 2 reds em 140; 2x0 tem 4 em 166. Na odd 22 o BE exige red < 4,5%; na
odd 14,8, < 6,9%. Um red a mais no 0x2 leva o ROI de +3,3% para ~+2,5%; três a mais, para ~+1,0%. O IC do bootstrap
**não captura** isso porque quase não há reds para reamostrar — é o "100% até o primeiro red" do Hall of Shame,
na versão "98,6% até o terceiro". O ranking não ajuda: Todos ≥ TOP em ambos.

---

## 2. Diagnóstico causal — quando o ranking por menor odd seleciona alpha e quando atrai o desastre

O ranking por menor odd de lay funciona **só quando a odd baixa reflete o mercado superestimando o placar**, e
falha quando a odd baixa reflete um **motivo real** para o placar sair.

| mercado | o que puxa a odd de lay para baixo | efeito no P(placar) | efeito no ranking |
|---|---|---|---|
| 2x2 (auditoria anterior) | jogo "de 2-2": under leve, equilíbrio | P(2-2) quase constante (3-6%) enquanto o BE cai 5pp | **seleciona alpha** (rank 1 +2,2pp) |
| 0x3 (auditoria anterior) | visitante relativamente forte (2,78 vs 3,80) | P(0x3) **dobra** (1,3% → 3,6%) enquanto o BE cai só 0,6pp | **atrai o desastre** (rank 1 é a pior posição) — confirmado hoje no meu dado |
| Any Other Away | visitante favorito (2,22) | P(goleada) 3,3% → 5,4% (+64%) vs BE −2,3pp | **atrai o desastre**, estrutural em 30 meses |
| Any Other Home | mandante favorito (1,98) | P 4,0% → 5,7% (+43%) vs BE −2,4pp | idem; o +1,3% de 2026 no TOP 2 é ruído |
| 0x1 / 1x0 | super favorito | odd de lay já está no teto da faixa (14); nada a ordenar | irrelevante — N por dia ≈ 1 |
| 0x0 quantitativo | favorito mais forte (rank 1 mand 2,00) | sem monotonia por posição | ruído |
| 0x2 / 2x0 | — | sem multi-ano | Todos ≥ TOP |

A regra geral, em uma linha: **odd baixa = "o mercado acha mais provável"; ordenar por ela só é alpha quando o
mercado está errado sobre isso — e o teste é a taxa real do placar por faixa de odd, não o ROI do TOP N.**

---

## 3. Coletor VPS — liquidez, spread e deriva até o KO

Runner na faixa do método, capturas pré-KO (17/08 → 14/09). `spread` = (lay−back)/back. `size` = R$ disponível no
melhor lay. `saiu da faixa` = jogos com o runner na faixa ≥3h antes cuja odd no KO ficou fora (não filtrado pelos
outros critérios do método — é deriva do runner, não do sinal).

| runner (faixa) | janela | N | spread p50 | p75 | p90 | size p50 | p25 | ≥R$100 | deriva \|Δ\| | saiu da faixa |
|---|---|---|---|---|---|---|---|---|---|---|
| **AO Away 15-60** | KO | 6.136 | 23,8% | 52,8% | **262%** | R$149 | R$97 | 73% | 6,0 | **26%** |
| | 3h antes | 52.751 | 37,5% | 89,7% | 532% | R$141 | R$98 | 74% | | |
| **AO Home 15-60** | KO | 6.426 | 23,8% | 50,0% | **300%** | R$146 | R$96 | 73% | 5,0 | **28%** |
| | 3h antes | 55.717 | 35,0% | 89,7% | 530% | R$142 | R$100 | 75% | | |
| 0-1 5-15 | KO | 7.290 | 13,9% | 20,0% | 31,6% | R$263 | R$144 | 90% | 1,0 | 17% |
| 1-0 5-15 | KO | 9.912 | 13,0% | 20,0% | 32,7% | R$261 | R$148 | 89% | 0,7 | 12% |
| 0-0 10-20 | KO | 7.236 | 15,0% | 22,4% | 37,5% | R$177 | R$112 | 81% | 1,5 | 23% |
| 0-2 5-25 | KO | 6.895 | 17,2% | 25,0% | 43,8% | R$185 | R$111 | 80% | 1,5 | 15% |
| 2-0 5-25 | KO | 10.617 | 16,0% | 23,5% | 38,9% | R$200 | R$119 | 82% | 1,0 | 10% |

- Os **Any Other** são os runners mais finos e mais voláteis do CS: spread p90 de 262-300% no KO (e >500% de manhã),
  deriva mediana de 5-6 pontos de odd, **26-28% dos runners saem da faixa até o apito**. Mesmo que o EV fosse
  positivo, o sinal da manhã não seria o sinal do KO.
- 0-1 / 1-0 / 0-0 / 0-2 / 2-0 têm liquidez e spread compatíveis com liability de R$100-500 (stake R$5-30).

---

## 4. Vereditos

| método | veredito | motivo (dado) |
|---|---|---|
| **Lay Goleada Visitante (AO Away)** | 🔴 **REPROVADO — EV negativo** | Todos −0,74% com IC95 [−1,1; −0,4], P(≤0)=1,000 em 11.813 apostas / 30 meses. Ranking piora (red 3,7% → 5,4%; TOP 2 2026 −2,64% IC [−5,3; −0,2]). Runner mais volátil do CS (26% sai da faixa; spread p90 262%) |
| **Lay Goleada Mandante (AO Home)** | 🔴 **REPROVADO — EV negativo** | Todos −0,86% IC [−1,2; −0,5], P=1,000 em 16.821. O +1,35% do TOP 2 em 2026 (P=0,075) não sobrevive ao multi-ano (−0,47%). Mesmo mecanismo: o ranking escolhe o favorito que goleia |
| **Lay 0x1 Super Fav Mandante** | 🔴 **REPROVADO — EV negativo** | −1,42% (Todos) a −1,70% (TOP 1) no completo; nenhum corte/período positivo; N=447. Concorda com o RF v2 (−2,05%). Não há sub-faixa: a odd concentra em 14 |
| **Lay 1x0 Super Fav Visitante** | 🔴 **REPROVADO — inexplorável por volume** | 145 jogos em 30 meses (~1/semana); nenhum corte com sinal (Todos −0,14%, IC [−5,9; +4,6]). O TOP 1 +1,27% é N=110 com IC de 10pp |
| **Lay 0x0 Quantitativo** | 🟡 **Watchlist — mas REDUNDANTE** | Todos +0,45% [−0,9; +1,7]; TOP 1 +1,11% [−1,0; +3,0] P=0,149 e comprime a 0,6/dia. O Lay 0x0 XGB pré-registrado no mesmo mercado dá +1,17% com IC [+0,29; +1,98] em N=1.947. Não abrir forward paralelo: é uma versão mais fraca de algo que já está em validação |
| **Lay 0x2 Zebra** | 🟡 **Watchlist / forward stake-zero** | +3,26% IC [+0,9; +4,9] mas N=140, **2 reds**, sem multi-ano (122 são 2026). Risco de cauda não capturado pelo bootstrap. Liquidez OK (R$185, 80% ≥R$100). Ranking não ajuda — usar Todos |
| **Lay 2x0 Zebra** | 🟡 **Watchlist / forward stake-zero** | +4,79% IC [+2,0; +6,9] P=0,001, mas N=166, **4 reds**, sem multi-ano. Consistente com a memória ("micro-edge real, travado por liquidez") — no coletor a liquidez no KO é R$200 / 82% ≥R$100, suficiente para R$100-300 de liability. Ranking não ajuda — usar Todos |

Nenhum dos 7 é 🟢. Os dois 🟡 só podem ser julgados com **forward** — a base não tem 2024-2025 para eles, então o
que existe é 8 meses de amostra pequena com ~3 reds. O gatilho é o mesmo do bloco de gestão: piso do IC95 > 0 com
N ≥ 100 **e** pelo menos 8-10 reds observados (para o IC significar algo).

---

## 5. Sobre as respostas do Antigravity às perguntas abertas de ontem

- **Mecanismo do rank 1 no 0x3 — confirmado independentemente.** No meu dado: rank 1 visitante 2,78 / mandante 3,80 /
  P(0x3) 3,57%; rank 5+ visitante 3,82 / mandante 2,48 / P 1,32%. A explicação (odd baixa = visitante relativamente
  forte = mais goleada) fecha, e é a mesma mecânica que reprova os Any Other.
- **Grade de sensibilidade do 0x3** — recebida, não re-executada aqui. Coerente com o que medi (regra ampla +1,76%,
  N=3.800). Reforça a recomendação de pré-registrar a regra ampla do 0x3 como forward paralelo ao TOP 3.
