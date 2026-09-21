# AUDITORIA INDEPENDENTE — Varredura dos 43 mercados e novos métodos 2026 (Antigravity) — Claude, 2026-09-21

Base única (Lei 7): `Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH3.csv` (52.116 jogos, 16/03/2024 → 20/08/2026) + feeds diários
`scratch/feed_arquivo/*.parquet` de 21/08 → 21/09 (3.851 jogos, 3.219 com placar oficial da VPS → base → b365). O coletor NÃO foi usado.
Convenção: liability 1u nos lays (GREEN 0,95/(odd−1), RED −1, BE (odd−1)/(odd−0,05)); backs com stake 1u (WIN 0,95(odd−1), BE 1/(1+0,95(odd−1))).
Bootstrap bloco-dia 2.000×. Períodos: 2024 · 2025 · 26 jan-abr · 26 mai-jul · 26 ago-set (FRESH3) · feed 21/08-21/09.

## 0. O achado que governa tudo: "2026" na base da API é, na prática, jan–abr/26, e jan–abr/26 é um regime de DADO, não de mercado

| medida | 2024 | 2025 | **26 jan-abr** | 26 mai-jul | 26 ago-set |
|---|---|---|---|---|---|
| jogos na base | 15.917 | 23.195 | **8.393** | 3.431 | 1.180 |
| jogos das 5 ligas grandes | 1.170 | 1.666 | 686 | 179 | **6** |
| jogos com visitante ≤ 1,40 | 680 | 1.272 | 170 | 76 | **14** |
| spread lay/back CS 3x3 (mediana) | 24,5 | 26,1 | **2,2** | 7,2 | 3,8 |
| spread lay/back CS 2x0 | 1,36 | 1,46 | **1,11** | 1,19 | 1,17 |
| spread lay/back CS 0x2 | 1,69 | 2,82 | **1,16** | 1,26 | 1,24 |
| back CS 1x1 > 25 (impossível: evento de ~12%) | 0,1% | 0,2% | **8,6%** | 0,3% | 0,1% |
| lay 3x3 ≤ 50 disponível | 2% | 5% | **29%** | 5% | 5% |
| back 0x2 (vis ≤ 1,40), mediana | 1,2* | 1,0* | **19,0** | 7,3 | 7,1 |

\* 2024-25: coluna de back do 0x2 majoritariamente vazia/zerada nesses jogos (5–24% zeros).

Spreads de Match Odds e Over/Under quase não mudam entre períodos (1,03–1,07); os de Correct Score colapsam em jan–abr e as colunas de back
carregam preços impossíveis. Isso é a forma como a API gravou o livro de CS naquele período (memória `base-betfair-regime-odd-lay-cs`), não o mercado.
Consequência: qualquer célula de CS cujo lucro mora em jan–abr/26 mede a gravação, não uma ineficiência. E a base de 2026 depois de maio encolhe
para 1/7 do volume — "2026 no verde" significa "jan–abr no verde".

## Bloco 1 — Os "7 verdes" da base ampla, por período

| mercado (sem filtro) | 2024 | 2025 | 26 jan-abr | 26 mai-jul | 26 ago-set | feed 21/08-21/09 | 3 anos |
|---|---|---|---|---|---|---|---|
| Lay CS 0x3 | −0,99% | −0,96% | **+0,76%** | −0,74% | −0,57% | −0,44% [−0,8, −0,1] | −0,66% |
| Lay CS 2x0 | −2,13% | −2,42% | **+3,49%** | −1,28% | −2,01% | −1,28% [−2,2, −0,3] | −1,27% |
| Lay CS 3x3 | −0,49% | −0,54% | **+0,53%** | −0,42% | −0,59% | −0,46% [−0,9, −0,1] | −0,34% |
| Lay CS 1x3 | −1,04% | −1,12% | **+0,51%** | −1,07% | −1,73% | −0,96% [−1,5, −0,4] | −0,84% |
| Back CS 1x1 | −25,4% | −26,8% | **+58,2%** | −15,3% | −30,2% | −9,3% [−17, −1] | −11,7% |

Todos os cinco: positivos só em jan–abr/26; negativos em 2024, 2025, mai–jul, ago–set e no feed (o único dado realmente fora da amostra), com teto
do IC abaixo de zero no feed. O "Back CS 1x1 +30,7% em 2026" vem de 8,6% de jogos com back > 25 num evento de 12% — preços que ninguém casaria
(a apresentação já era inconsistente: WR 11,67% < BE 12,76% não dá ROI positivo; dá porque a média é puxada por odds de 100–160).

**Resposta à pergunta 1:** não. Em base ampla cega, o alpha pré-jogo não apareceu na cauda de Correct Score — apareceu na *gravação* da cauda de
Correct Score em jan–abr/26. Fora desse período a cauda de CS perde como todo o resto.

## Bloco 2 — Back 0x2 super favorito visitante (vis ≤ 1,40, back 0x2 em 6–20)

| período | N | acertos | WR | BE | P&L | ROI | IC95 |
|---|---|---|---|---|---|---|---|
| 2024 | 210 | 25 | 11,9% | 12,5% | +14,9u | +7,1% | [−35, +46] |
| 2025 | 282 | 33 | 11,7% | 12,7% | −35,0u | **−12,4%** | [−42, +22] |
| 26 jan-abr | 40 | 12 | 30,0% | 9,8% | +77,8u | +194,6% | [+68, +336] |
| 26 mai-jul | 54 | 9 | 16,7% | 12,7% | +16,1u | +29,8% | [−64, +138] |
| 26 ago-set | 12 | 4 | 33,3% | 13,5% | +18,6u | +155,0% | [−31, +367] |
| **feed 21/08-21/09** | **49** | **3** | **6,1%** | 12,5% | **−18,9u** | **−38,6%** | [−100, +17] |
| 2026 FRESH3 | 106 | 25 | 23,6% | 11,7% | +112,5u | +106,2% | [+28, +187] |
| 3 anos FRESH3 | 598 | 83 | 13,9% | 12,4% | +92,4u | +15,5% | [−9, +42] P=0,11 |

- Respostas 2.1: os 5 maiores greens de 2026 somam 63u dos 94u; são jogos como Istra × Din. Zagreb (back 17,5 / **lay 29**), Paralimni × AEL
  (back 12 / **lay 130**), Orbit College × Sundowns (back 11 / **lay 1000**): a coluna de back registra um preço com o lado do lay vazio. A probabilidade
  justa de 0-2 para um visitante de 1,30 é ~12–15% (odd 7–8); ninguém casa um back a 12–17 nesse evento. WR 23,6% em N=106 tem P binomial 0,005
  contra 13,9% — mas contra a taxa de 2024-25 (11,8%) e depois de escolher a melhor entre dezenas de células, isso é o que a mineração produz.
  **Amostra pequena + preço não executável, não viés estrutural.** A taxa de 0-2 no universo vis ≤ 1,40 foi 7,6% (2024), 7,2% (2025), 9,7% no feed.
- 2.2 vizinhos: todos "positivos" em 2026 pelo mesmo motivo (jan–abr), e o 3 anos cai para +0,4% em vis ≤ 1,50 (N=1.020). Não é robustez, é o mesmo artefato.
- 2.3 feed 21/08→21/09 (odd da API na manhã, placar oficial): 49 apostas, 3 acertos, −38,6%.
- 2.4 **Veredito: REPROVAR.** A watchlist já foi feita pelo feed — 49 jogos, 5 semanas, −18,9u. O espelho (lay 0x2 no mesmo filtro) dá −6,3% em 2026:
  os dois lados perdem, que é a assinatura de overround, não de edge.

## Bloco 3 — Lay 3x3 geral (lay 15–50)

| período | N | WR | BE | ROI | IC95 |
|---|---|---|---|---|---|
| 2024 | 234 | 97,4% | 97,9% | −0,45% | [−2,8, +1,3] |
| 2025 | 266 | 97,4% | 97,9% | −0,51% | [−2,4, +1,1] |
| **26 jan-abr** | **2.186** | 98,6% | 96,8% | **+1,86%** | [+1,4, +2,3] |
| 26 mai-jul | 167 | 97,6% | 97,7% | −0,13% | [−1,9, +1,7] |
| 26 ago-set | 60 | 96,7% | 97,8% | −1,12% | [−6,6, +2,3] |
| **feed 21/08-21/09** | **151** | 96,0% | 97,8% | **−1,79%** | [−5,0, +1,1] |

- 3.1: os 2.413 de 2026 são 2.186 de jan–abr. Lay de 3x3 ≤ 50 existe em 29% dos jogos em jan–abr e em 2–5% em todos os outros períodos, incluindo o feed
  atual. A taxa real de 3-3 é estável (1,0–1,4% de todos os jogos). É cobertura/gravação do livro, não regime de mercado.
- 3.2: na odd mediana 34–46 a liability é 33–45u por 1u de stake; um 3-3 apaga 35–47 greens; em 2026 houve semana com 4 reds. Para uma margem que fora de
  jan–abr é negativa em todas as janelas. Não compensa.
- 3.3 **Veredito: DESCARTE.**

## Bloco 4 — Os métodos da Página 03, por período

| método | 2024 | 2025 | 26 jan-abr | 26 mai-jul | 26 ago-set | feed 21/08-21/09 | 3 anos |
|---|---|---|---|---|---|---|---|
| Lay 1x1 fav ≤ 1,50, lay 6–12 | −1,7% (427) | −2,0% (582) | **+2,9% (680)** | −1,6% (100) | +1,6% (44) | **−1,2% (105)** | 0,00% (1.833) |
| Lay 0x0 fav ≤ 1,40, lay 7–18 | +1,3% (114) | −1,1% (170) | +2,8% (82) | +6,6% (19) | +6,0% (10) | +6,5% (16, 0 red) | +1,0% (395) [−1,5, +3,3] |
| Lay 2x0 zebra mandante | +7,4% (17) | −3,1% (18) | **+7,6% (379)** | N=1 | **N=0** | **N=0** | +7,2% (415) |
| Lay 0x2 zebra visitante | +6,5% (13) | +7,1% (8) | **+3,3% (632)** | **N=0** | **N=0** | **N=0** | +3,4% (653) |
| Lay 0x3 zebra visitante | +2,7% (6) | N=4 | **+4,6% (209)** | **N=0** | **N=0** | **N=0** | +4,5% (219) |
| Lay 0x1 Sniper | −0,4% (343) | +0,5% (454) | — | — | — | −1,1% (135) | +0,5% (1.355) |

- A **suíte de Zebras (2x0, 0x2, 0x3) não gera um único sinal desde maio de 2026** — nem na base, nem no feed. As faixas de lay (6–25 com favorito ≤ 1,70)
  só existiam no regime jan–abr, quando o lay de CS foi gravado comprimido (mediana do lay 2x0 em 8,8 contra 17–21 nos outros períodos). "Monitorar em
  stake-zero" um método com N=0 há cinco meses não é observação; é um placar congelado de jan–abr.
- **Lay 1x1**: 3 anos exatamente zero; positivo só em jan–abr; o "+2,23% no feed recente" não reproduz — o feed 21/08→21/09 dá −1,2% em 105.
- **Lay 0x0 super fav**: o único com sinal fora de jan–abr (16 jogos no feed, 0 red; 3 anos +1,0%, IC cruza zero). É um subconjunto do Lay 0x0 já em produção
  (regra XGB congelada, `lay0x0-xgb-regra-congelada-forward`); não precisa de método paralelo — precisa de N.
- **Resposta à pergunta 4:** sob a teoria de assimetria, o que tem mecanismo é o Lay 0x0 (viés recreativo em placar de super favorito, medido em ~1 pp
  de edge real) — e já está no portfólio. As Zebras têm mecanismo plausível mas o dado que as sustenta é o artefato de jan–abr, e elas não produzem
  sinal para observar. O portfólio de observação para set/out, honestamente, é: Lay 0x0 super fav (dentro do 0x0 XGB) e nada mais desta lista.

## 3. A tese "2026 tem peso grande porque o mercado está mudando"

O que a base da API mostra sobre 2026:
1. Depois de maio ela tem 1/7 do volume e 6 jogos de ligas grandes em ago–set. "2026" na base = jan–abr.
2. Jan–abr é um regime de gravação de Correct Score (spreads colapsados, backs impossíveis, livro de 3x3 seis vezes mais "disponível"). Tudo que
   ficou verde em 2026 nos 43 mercados mora aí e vira vermelho em mai–jul, ago–set e no feed.
3. Mudança de mercado real e mensurável em 2026 existe, mas é outra: a onda de gols em favorito médio (Over 2.5, ago/26, `PREREGISTRO_lay_under25_fav_medio.md`)
   — que apareceu em jul–ago, não em jan–abr, e já perdeu força no feed de setembro.
4. Portanto o peso de 2026 deve ir para o **feed diário** (odd da API na manhã + placar oficial), que é 2026 de verdade, fora da amostra e na base certa
   — não para jan–abr/26 da FRESH3. Regra prática: nenhuma célula de CS pode ser julgada sem a coluna "mai–jul / ago–set / feed" ao lado da coluna 2026.

## 4. Governança da Página 03 — recomendações
1. Todo método listado exibe, obrigatoriamente, N e ROI nos últimos 60 dias do feed. Método com N=0 em 60 dias sai da página (hoje: 2x0, 0x2, 0x3 zebras).
2. Toda tabela de backtest de CS na base da API traz a quebra 2024 · 2025 · jan–abr · mai–jul · ago–set. Sem a quebra, o número de 2026 não é apresentável.
3. Filtro de validade de preço nas colunas de CS da API antes de qualquer varredura: descartar back de CS com lay ≥ 3× back (livro vazio) e lay ≥ 500.
4. Watchlist só entra com pré-registro (regra, N-alvo, critério de saída) e só depois de passar em mai–jul + ago–set + feed com IC que não seja negativo.
5. Back 0x2 super fav: REPROVADO. Lay 3x3: DESCARTE. Lay 1x1 e Sniper: já estão no feed a −1,2% e −1,1%; mantêm-se em stake-zero até N=400 só porque o
   custo é zero — expectativa declarada: reprovam. Zebras: retirar da página até que existam sinais.
