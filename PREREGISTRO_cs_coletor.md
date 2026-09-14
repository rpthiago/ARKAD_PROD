# Pré-registro — Correct Score na odd de lay do NOSSO coletor, perto do KO (grade congelada)

**Congelado em:** 2026-09-13, antes de qualquer resultado.
**Motivo:** a base histórica da apicomunidade tem o livro de lay de CS vazio em 2024-25 e cheio só em jan-abr/2026
(`CORRECAO_REGIME_ODD_CS_para_gemini.md`). Todo backtest de CS feito nela mistura regimes. Este estudo usa a única
odd de CS que nós mesmos medimos: a do coletor da VPS, **na captura mais próxima do KO** (−5 a +15 min), para
todos os 19 runners, desde 16/08/2026, com liquidação **oficial** da Betfair (`placares_ft.csv`) e, só onde ela
ainda não existe (jogos anteriores a 12/09), placar exato das bases externas.

## A grade (não muda depois de congelada)

Uma célula = (runner, favoritismo, corte).

| dimensão | valores |
|---|---|
| **runner** (19) | 0-0 · 0-1 · 0-2 · 0-3 · 1-0 · 1-1 · 1-2 · 1-3 · 2-0 · 2-1 · 2-2 · 2-3 · 3-0 · 3-1 · 3-2 · 3-3 · Any Other Home · Any Other Away · Any Other Draw |
| **faixa de odd de lay** (fixa por família) | placar exato **5-40** · Any Other **10-80** |
| **favoritismo pré-jogo** (menor back do Match Odds na captura mais próxima do KO) | `<=1.40` · `1.40-1.80` · `>1.80` |
| **corte** | Todos · TOP 3 · TOP 1 (por menor odd de lay no dia, dentro do runner) |

19 × 3 × 3 = **171 células**. Só entram na tabela células com **N ≥ 50**. Nenhum outro filtro (liga, Under, etc.).

**Por que a dimensão favoritismo e não Under 2.5:** a auditoria de 13/09 mostrou que o ranking por menor odd seleciona
alpha quando a odd baixa reflete o mercado superestimando o placar (2x2) e atrai o desastre quando reflete um
favorito forte (0x3, Any Other). Favoritismo é a variável que separa os dois casos. O Under 2.5 fica gravado por
linha para leitura, mas **não** é célula.

## Como cada aposta é avaliada

- **Odd:** lay do runner na captura mais próxima do KO (mtk em [−5, +15]). Sem captura nessa janela, o jogo não entra.
- **Resultado:** placar FT oficial (`placares_ft.csv`, runner WINNER do CS). Onde não houver (jogos antes de 12/09),
  base Betfair/b365 com placar exato, match exato → fuzzy ≥0,80. Sem placar → fora. A coluna `fonte` fica gravada.
- **RED** = o runner venceu (placar exato igual; Any Other Home = mandante venceu com ≥4 gols; Away idem; Draw = empate ≥4-4).
- **P&L:** liability 1u, comissão 5%: GREEN 0,95/(odd−1), RED −1. Break-even (odd−1)/(odd−0,05).
- **Liquidez gravada por linha** (lay_size no KO), para o relatório — não filtra.

## Estatística

- Por célula: N, reds, red%, WR, BE, gap, ROI, IC95 por **bootstrap de bloco por dia** (10.000, ≥6 dias),
  p unilateral (ROI ≤ 0); ROI negativo entra com p=1.
- **BH-FDR q=0,05 sobre M = número de células com N ≥ 50**, todas de uma vez.
- Decisão de 3 vias por célula: PASSA (BH e piso IC95 > 0) · REPROVA (teto IC95 < 0) · INCONCLUSIVO.
- **Guarda de cauda:** célula com **menos de 5 reds** não pode PASSA, mesmo com BH — vira "INCONCLUSIVO (reds<5)".
  (O "100% até o primeiro red" já enganou este projeto duas vezes.)

## Snapshots

- **Snapshot 1 (hoje):** coletor 16/08 → 13/09, ~4.500 jogos. Esperado: quase tudo inconclusivo por N.
- **Snapshot 2: 2026-10-12**, junto com a varredura Over e o julgamento da H-extra. Mesma grade.
- **Snapshot 3: 2026-11-12.** Só então uma célula que PASSA nos 3 snapshots com N crescente vira candidata a forward.
- Snapshots datados em `varredura_over/cs_coletor_<data>.csv`.

## O que NÃO fazer

- Não mudar faixas, cortes, dimensão ou N mínimo depois de ver resultado.
- Não somar células. Não "ler" o 12/10 antes do 12/10.
- Não usar a base histórica da apicomunidade para "reforçar" uma célula — foi exatamente o erro que este estudo corrige.

---

## Snapshot 1 — 2026-09-13 (coletor 16/08 → 13/09)

4.913 jogos com CS no KO; 2.168 com placar (314 oficial, 1.854 bases); **26.594 apostas, 2.159 jogos, 27 dias**.
**54 células com N ≥ 50 · BH sobre M=54 · 0 PASSA, 4 REPROVA, 50 INCONCLUSIVO.** Melhor p = 0,061 (0-0 · fav ≤1,40 ·
Todos: N=112, 2 reds, +2,12%, IC [−0,7; +4,1]). Por runner sem favoritismo (Todos), a taxa real de cada placar fica
entre −1,7pp e +0,1pp do break-even: 0-3 +0,11pp · 3-2 +0,08pp · 2-2 −0,62pp · 1-1 −1,62pp · 0-0 −1,58pp ·
Any Other Home −0,10pp · Any Other Away −0,73pp. Tabela: `varredura_over/cs_coletor_2026-09-13.csv`.
Reprovadas: 1-1 (fav >1,80, Todos, N=1.506, −2,32%, IC [−4,4; −0,5]); 0-2 (fav 1,40-1,80, N=145, −5,87%).
Próximo snapshot: 2026-10-12.
