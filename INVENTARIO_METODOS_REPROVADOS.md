# Inventário — todos os métodos reprovados do ARKAD, por que cada um foi reprovado, e o que o ranking por menor odd faz com ele

> Consolidado em 2026-09-13 a partir de: `GEMINI.md` (seções 1 e 6), `worklog.md`, memória
> `metodos-mortos-auditados-2026-08`, `pages_arquivadas/`, logs de paper trading e as auditorias de 09-13/09.
> Convenção: odd de **lay** real da Betfair, liability 1u, comissão 5%, salvo indicação.
> Coluna **ranking**: resultado da varredura de 13/09 (`varredura_ranking_lay_todos_mercados.py`, ROI Todos → TOP 1
> na base Betfair, 43 mercados × 4 cortes, BH sobre 168 células). "n/a" = o método não tem odd de lay por jogo para
> ordenar (modelo, in-play, handicap, trade), então o ranking não se aplica.

**Legenda do motivo:** `EV−` = perde na odd real · `vig` = overround do mercado consome tudo · `overfit` = garimpo /
garden of forking paths · `miragem` = o número positivo vinha de erro de medição (odd de back, coletor, fabricação,
convenção) · `fluxo` = inviável por volume · `in-play eficiente` = o mercado ao vivo já precifica · `leak` = vazamento.

---

## A. Correct Score — lay pré-jogo (18)

| # | método | regra | número que reprovou | motivo | ranking (13/09) |
|---|---|---|---|---|---|
| 1 | Lay 0x0 genérico | lay 0-0 sem filtro forte | paper de ago/2026 **−R$1.979** na odd real (era +4.100 na odd b365) | miragem (odd de back) → EV− | Todos −1,40% → TOP 1 **−3,95%**: piora |
| 2 | Lay 0x0 Quantitativo | fav ≤1,50 casa / ≤1,40 fora, lay 10-20 | +0,45% IC [−0,9; +1,7]; TOP 1 +1,11% IC [−1,0; +3,0] | não descola; redundante com o 0x0 XGB (em validação) | TOP 1 melhora mas não descola |
| 3 | Lay 0x1 Super Fav Mandante | H ≤1,90, lay 5-15 | **−1,42%** (Todos) a −1,70% (TOP 1), N=447; FRESH −2,17% | EV− (0x1 sai em 8% vs BE 93,3%) | piora −1,75pp |
| 4 | Lay 1x0 Super Fav Visitante | A ≤1,90, lay 5-15 | N=145 em 30 meses; −0,14%; ago/25+ **−4,47%** | fluxo + EV− | piora −1,42pp |
| 5 | Lay 0x1 RF v2 (hold) | modelo RF, hold | **−2,05%** na odd real (N=1.531); o log gravava odd de **back** numa coluna `_Lay` (fator 1,39x) | miragem → EV− | n/a (modelo) |
| 6 | Lay 1x0 RF v2 | modelo RF | "mês bom, sem edge"; forward **−8%** | EV− | n/a |
| 7 | Lay 1x1 | lay 1-1 | N=412, WR 87,9% vs BE 87,5%, ROI 0,1%; colapso mai-jul (−141%, −116%, −72%) | EV− / cauda | Todos −2,52% → TOP 1 −0,77%: melhora, segue negativo |
| 8 | Lay 2x2 (genérico) | lay 2-2 sem filtro | "agosto +5,3k foi 1 mês"; liability 16,8u com margem +1,14% | cauda / EV≈0 | Todos −0,87% → TOP 1 +0,30%: melhora, ≈0 |
| 9 | Lay 0x3 (genérico, 2025) | lay 0-3 | **−R$22k**; "77,5% WR vs 96,7% BE" | EV− (medido em regime de livro vazio) | ver regime |
| 10 | Lay 0x2 Zebra | H ≤1,45, lay 5-25 | N=140 (122 em 2026), **2 reds**; +3,26% IC [+0,9; +4,9] | micro-edge sem histórico; cauda | Todos ≥ TOP: ranking não ajuda |
| 11 | Lay 2x0 Zebra | A ≤1,45, lay 5-25 | N=166 (134 em 2026), **4 reds**; +4,79% | idem | Todos ≥ TOP |
| 12 | Lay 3x3 | lay 3-3, faixa 5-40 | +2,79% N=1.955 — **1.760 em jan-abr/2026**; 2024-25 −0,55% | regime de odd (ver seção F) | passa BH nos 4 cortes, só em regime B |
| 13 | Lay Goleada Visitante (AO Away) | U2.5 ≤2,10, lay 15-60 | **−0,74%**, IC [−1,1; −0,4], N=11.813, P(≤0)=1,000 | EV− | piora: red 3,7% → 5,4% |
| 14 | Lay Goleada Mandante (AO Home) | U2.5 ≤2,10, lay 15-60 | **−0,86%**, IC [−1,2; −0,5], N=16.821 | EV− | TOP 2 2026 +1,35% não sobrevive (completo −0,47%) |
| 15 | Dutching de CS (cesta) | dutch {0-0,1-0,0-1,1-1,2-0,0-2} | odd sintética **−10,2%** vs Under 2.5 direto em 97% de 51.079 jogos | vig estrutural (k livros rasos) | n/a |
| 16 | Lay 0x1 In-Play (Rota C) | 0-0 aos 55-75', lay 0-1 em 2,00-5,50 | 0x1 sai em **26,3%** na faixa vs 15,4% geral (placar exato); gap −5,75pp, ROI −7,3% | seleção adversa; in-play eficiente | n/a |
| 17 | Lay 0x1 In-Play com cashout no HT | cashout 0-0 no HT | cashout real **−0,499u** (assumido +0,19u); ROI −11% | miragem (odd de saída fabricada) | n/a |
| 18 | Trade Lay 0x1 (sair min 60/75) | entrada pré, saída in-play | EV **−2,38/jogo** na faixa 12-16, IC90 [−3,10; −1,71], P(≤0)=100%; hold −2,3 a −3,6/jogo | in-play eficiente (odd de saída ≈ justa) | n/a |

**Em validação forward (não reprovados, mas listados porque já foram declarados mortos e reabertos em 02/09):**
Lay 2x2 TOP 3 · Lay 0x3 TOP 3 · Lay Draw (Fav ≤1,40) · Lay Home/DC X2 (FavVis ≤1,65) · Lay Over 4.5 (Under Pesado) ·
Lay 0x0 XGB (pré-registrado 02/08). Os três de CS dependem do regime de odd (seção F).

## B. Match Odds / Dupla Chance / Handicap — pré-jogo (16)

| # | método | regra | número que reprovou | motivo | ranking (13/09) |
|---|---|---|---|---|---|
| 19 | Lay Home (hold) | lay mandante | **−3,00%** na `Odd_H_Lay`; OOS ago/25+ **−2,60%**, IC exclui zero, p=1,0 | EV− (cenários "+4%" eram sem spread) | Todos −3,99% → TOP 1 **−6,10%**: piora |
| 20 | Lay Home v2 (ML) | RF/XGB | trainer **leaky** (`.last()` broadcast); leak-free +6,9% **reprova FDR** | leak → não significativo | n/a |
| 21 | Lay Home filtro refinado | odd A ∈ [1,54; 1,65] (18 "holders" em 100 features) | rendeu **menos** que a base no forward e inverteu sinal (−4,6% H1 / +17,4% H2) | overfit | n/a |
| 22 | Lay Home Falso Fav (K ≥3,80) | índice cross-market | N=337, WR 41,0% vs BE 42,4%, **−5,01%** | eficiência (produto de odds públicas) | n/a |
| 23 | Lay Away Win v2 | lay visitante | "ativado por YTD otimista"; re-auditado morto (10/08) | miragem (YTD) → EV− | Todos −3,42% → TOP 1 **−7,82%**: piora |
| 24 | Lay Away / DC 1X no Super Fav Mandante | H ≤1,45, lay A 2-15 | 1/100 holders; FRESH **+0,50%** ≈ break-even, IC [−1,8; +2,8] | EV≈0 | 1X: Todos −4,74% → TOP 1 −5,88% |
| 25 | Lay Draw genérico | lay empate sem filtro | +2%, **reprova FDR** | não significativo | Todos −2,89% → TOP 1 **−5,85%**: piora |
| 26 | Lay Draw filtro refinado | Odd_Over35 ≥2,54 | rendeu menos que a base pura | overfit | n/a |
| 27 | Lay Draw Alta Pressão (K ≤2,20) | índice cross-market | N=1.723, WR 84,9% vs BE 87,1%, **−2,09%** | eficiência: o filtro derruba o empate mas a odd cai junto | n/a |
| 28 | Back Home / Back Away (modelo médias móveis) | ML em stats públicas vs fechamento | log-loss 1,04 vs 0,97 do mercado; Back Home **−6,16%**, Back Away **−7,01%**, N=34.280 OOS | winner's curse vs closing line | n/a |
| 29 | DNB / AH 0.0 Mandante (XGB) | EV ≥5% | N=333, +1,06%; com EV ≥3% vira −0,66%; IC [−6,0; +7,4] | não robusto ao próprio limiar | n/a |
| 30 | AH +1,5 Zebra Mandante | handicap | odd assumida 2,45 → "+82%"; odd **real 1,30**: BE 81,5% vs WR 75,2% = **−4,2%** | miragem (odd inventada) | n/a |
| 31 | EH +2 / +3 "Saldo Menor" (todas as variantes) | handicap europeu em jogo de baixo xG | single, triplas, quádrupla, "sweet-spot", "Golden": todas mortas; geradores **fabricavam** odd (`Odd_H_FT×1,05+0,15`, neutralizado 10/09) | miragem (fabricação) → EV− | n/a |
| 32 | K_edge = D_ratio / Odd_Fav | constante fatiada após 138/140 negativos | bootstrap mensal IC [−10,4; +20,7], P(≤0)=0,24; 2025 (maior N) **−0,47%** | overfit (cauda de 1%) | n/a |
| 33 | Scan de 140 cenários multifatoriais de odds | combinações de odds públicas | **138/140 negativos**; Lay BTTS até −10,8%, Lay Over 2.5 até −9,5% | null result / eficiência | n/a |
| 34 | Lay Home / Lay Draw do modelo 1X2 | ML | Lay Home −5,22%, Lay Draw −3,06% N=34.280 | idem 28 | n/a |

## C. Over/Under, BTTS, HT — pré-jogo (14)

| # | método | regra | número que reprovou | motivo | ranking (13/09) |
|---|---|---|---|---|---|
| 35 | Lay Under 1.5 FT (XGBoost) | modelo, lay 2,50-4,50 | **−3,87%** N=23.424 (2024 −5,47 / 2025 −4,04 / 2026 −2,47); o modelo gera **zero** sinais (p máx 0,693 < 0,745 exigido); 8 sinais gravados não reproduzíveis (Lei nº 2) | EV− em todos os anos + violação de reprodutibilidade | Under15_FT: −3,68% → TOP 1 **−7,31%** |
| 36 | Lay Under 2.5 / Under 3.5 pré-jogo | lay em jogo over | "O/U pré-jogo hiper-eficiente" | vig | Under25 −4,90% → **−8,20%**; Under35 −8,58% → +1,18% (N=545, IC [−13; +16]) |
| 37 | Lay Over 1.5 v2 | lay over | paper log; ranking: **−10,07% → −18,78%** | vig | piora −8,7pp |
| 38 | Over 1.5 FT ML (ensemble XGB+LGBM+RF) | back over | AUC 0,615 = AUC do mercado (0,608): **circular**; acurácia 73,4% = base rate | não é informação nova | n/a |
| 39 | Over 2.5 back | back over | vig | vig | Over25 lay −7,16% → −8,96% |
| 40 | Back Under (Idea1) | back under in-play | **−3,17%** N=1.164, liquidação 100% oficial | EV− | n/a |
| 41 | Espelho Back Over / Back Under | os dois lados do mesmo cohort | Back Under **−4,64%**, Back Over **−4,63%** (N=371): os dois pagam overround 2,36% + comissão | vig | n/a |
| 42 | Under-limite v2 (in-play min 75-85) | back Under logo acima do placar | **−3,11%** N=637 (corrigido 12/09: 4 falsos GREEN do coletor); os "+18 a +26%" eram falso green sistemático do coletor | miragem (coletor) → EV− | n/a |
| 43 | Late Goal (back Over limite, min 82-86, diff==1) | in-play, stake-zero | N=152, WR 42,8% vs BE 50,1% = **−7,3pp**, ROI −16,7%; **0 sinais na coorte** (163/163 observação); 6 na faixa de odd, 5 liquidados 1G/4R | EV− + fluxo; estado (diff==1) vinha do CS de menor lay (~13% errado) | n/a |
| 44 | Back BTTS Yes / BTTS Não | back | overround **8,3%**; Back Não **−10,1%** N=2.580, 0/8 meses | vig | Lay BTTS Yes −7,53% → **−12,44%** |
| 45 | Lay BTTS Yes v2 / Lay BTTS FS | lay | "ativados por YTD otimista"; re-auditados mortos (10/08) | miragem (YTD) | piora |
| 46 | Under 1.5 HT + scan de 57 mercados | filtro competitivo | só 3 mercados positivos (todos Under HT); Under 1.5 HT teve edge e **morreu em 2023** | edge extinto | Under15_HT −5,89% → −1,85% (melhora, negativo) |
| 47 | Over 0.5 HT / Over 1.5 HT | back HT | overround HT **7,8%** | vig | Over15_HT −4,91% → −0,19% (melhora, ≈0) |
| 48 | Escanteios O/U (footystats) | back/lay cantos | mercado **calibrado** (implícita ≈ real) | eficiência | n/a |

## D. In-play e trade (10)

| # | método | regra | número que reprovou | motivo | ranking |
|---|---|---|---|---|---|
| 49 | Radar HT (back fav 0-0 no intervalo) | pressão no 1º tempo | filtro de pressão: **+12,24pp → −2,33pp (p=0,618)** com dado limpo (stats zeradas eram filtro de cobertura); **3 de 360** na banda de odd suposta | miragem (6ª Lei) + fluxo | n/a |
| 50 | Favorito Dominante in-play | fav ≤1,65 pré, empatando/atrás, xG≥1,0 e ≥2,5x, SoT≥3, tbox≥15 | bug `odd_a=0` corrigido 11/09; **0 sinais** em 2 dias; estado vinha do CS de menor lay | fluxo (não reprovado formalmente) | n/a |
| 51 | Back Favorito após levar gol da zebra | fav ≤1,50 atrás, mean-reversion | N=80, WR 46,2%, **−4,1%** | in-play eficiente | n/a |
| 52 | Lay o Empate tarde (min 75+) | "crowd enche o empate" | N=848, WR 55,8%, **−11,8%** (o empate tardio tende a ficar) | hipótese falsa | n/a |
| 53 | Lay Draw In-Play 60' em favorito empatando | D_ratio ≥1,12 | 78% de gol pós-60' mas equalizadores derrubam WR para 66,7% vs BE 70,4% (@3,2): **−4,9%** N=69 | P(evento) ≠ edge | n/a |
| 54 | BTTS Yes in-play após zebra marcar | back BTTS | odd de entrada ~2,7 já precifica o gol de volta | in-play eficiente | n/a |
| 55 | Overshoot do 1X2 pós-gol (trade) | back no pico / lay depois | conceder: N=1.631, reverte 11,9%, **−12,4%**; scorer: N=1.635, +0,76% ≈ 0 (não paga spread duplo) | sem overshoot; 1X2 líquido reage na hora | n/a |
| 56 | Filtro de janela de xG (sum_xg12) como veto no 0x0 | veto por xG | pico de 2 pontos que **inverte** OOS | overfit | n/a |
| 57 | M4 xG_open veto | veto | usa odd de **back** para lay; não significativo OOS | miragem + n.s. | n/a |
| 58 | "Arame Liso" | — | inflado **24x** pelo denominador stake-vs-liability; só funciona em K=5 | miragem (convenção) + overfit | n/a |

## E. Lotes e modelos com a API FotMob (3)

| # | item | número | motivo |
|---|---|---|---|
| 59 | Lote H1-H9 (hipóteses com xG/SoT ao vivo) | **nenhuma** sobrevive ao BH (M=9); H1 "PASSA p=0,0016" em dado parcial virou p=0,74 com dado completo | null result; lição: não rodar antes do pull terminar |
| 60 | Painel H10 (152 features) | piora a previsão nos **4 alvos** | as features que a odd não vê não existem onde o mercado é raso |
| 61 | xG-window filter (benchmark Gemini) | pico de 2 pontos, inverte OOS | overfit |

## F. O que o ranking fez com tudo isso (resumo)

Dos 61 itens, **26 têm odd de lay por jogo na base** e entraram na varredura de 43 mercados × 4 cortes:

- **Mercados líquidos (Match Odds, Dupla Chance, Over/Under, BTTS): o ranking piora em todos**, de −1 a −12pp.
  "Menor odd de lay" nesses mercados = favorito mais forte = overround maior. Lay Home, Lay Away, Lay Draw,
  Lay Under 1.5/2.5, Lay BTTS: nenhum é resgatado por corte algum.
- **Correct Score: o ranking ajuda em 2x2, 3x3, 3x1, 1x3 (+0,5 a +1,2pp) e piora em 0x0, 0x1, 1x0, 2x0, 3x0
  (−1,4 a −2,9pp).** Mecanismo: ajuda quando a odd baixa reflete o mercado superestimando o placar; piora quando
  reflete um motivo real (favorito forte → goleada / 0x3).
- **As 5 células que passaram o BH (0x3 Todos, 3x3 nos 4 cortes) dependem do regime de odd da base**: a base
  Betfair tem livro de lay **vazio** em CS de 2024 a dez/2025 (spread 500-4.000%), **cheio** em jan-abr/2026
  (20-38%) e intermediário depois. Por regime: 0x3 TOP 3 −0,28% / **+5,64%** / +0,57%; 2x2 TOP 3 −0,78% /
  **+5,96%** / +1,55%; 3x3 −0,55% / **+3,10%** / +1,02%. Toda a força estatística de CS vem de 4 meses.
  Detalhe em `CORRECAO_REGIME_ODD_CS_para_gemini.md`.

**Resposta:** dos reprovados, o ranking por menor odd **não resgata nenhum**. O que ele revela é um mecanismo em
Correct Score, cujo tamanho só o forward (odd do feed ≈ odd no KO, liquidação oficial) consegue medir — e lá tudo
está entre +0,6% e +1,6% com N < 100.

## G. Os cinco motivos que se repetem (contagem)

| motivo | itens | exemplos |
|---|---|---|
| **EV negativo na odd real** | 22 | Lay Home −3,0%, Lay Under 1.5 −3,9%, AO Away −0,74% em 11.813 |
| **miragem de medição** | 14 | odd de back numa coluna `_Lay` (0x1), coletor perdendo gol tardio (under-limite), odd de handicap inventada, stake vs liability (Arame Liso), stats zeradas (Radar HT) |
| **overround / vig** | 9 | BTTS 8,3%, HT 7,8%, espelho Back Over/Under −4,6% dos dois lados |
| **overfit / garimpo** | 8 | filtros refinados, K_edge, 140 cenários, xG-window |
| **in-play eficiente / fluxo** | 8 | overshoot, empate tardio, Rota C, Radar HT (3/360), Late Goal (0/163) |

O padrão que mais custou tempo não foi EV negativo — foi **miragem de medição**: 14 métodos pareceram positivos por
um erro de régua antes de serem medidos direito. Odd de back onde devia ser lay, placar do coletor onde devia ser
oficial, convenção de stake onde devia ser liability, `0` onde devia ser NaN.
