# AUDITORIA INDEPENDENTE — "Lay 0x1 Sniper" (Antigravity) — Claude, 2026-09-21

Bases: `Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH3.csv` (52.116 jogos, 16/03/2024 → 20/08/2026), `metodos_aprovados/.cache_base_betfair.csv`
(→ 05/09/2026), `scratch/feed_arquivo/*.parquet` (34 feeds, 19/08 → 21/09), backup do coletor da VPS (odd real de KO−10, 16/08 → 20/09).
Convenção em tudo: lay com liability 1u; GREEN = 0,95/(odd−1); RED = −1; BE = (odd−1)/(odd−0,05). Bootstrap bloco-dia 1.000× (também por mês e por jogo).
Scripts: `varredura_over/audit_sniper_2026_fresh3.csv`, `varredura_over/audit_sniper_coletor_ko10.csv`.

## Resumo executivo

| Cenário / Teste | N | WR real | BE | Margem | P&L (u) | ROI liab. | IC95 bootstrap | Veredito |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Sniper (regra proposta), FRESH3 2026** | 558 | 94,62% | 93,60% | +1,02 pp | +6,08 | +1,09% | [−0,8, +2,9] · P(≤0)=0,14 | IC cruza zero |
| Sniper 2024–2026 (3 anos) | 1.355 | 94,10% | 93,62% | +0,47 pp | +6,87 | +0,51% | [−0,9, +1,7] · P=0,22 | IC cruza zero |
| **Sem blacklist de ligas** (2026) | 608 | 94,57% | 93,60% | +0,97 pp | +6,31 | +1,04% | [−0,9, +2,8] | blacklist irrelevante |
| Só as 9 ligas "excluídas" (2026) | 50 | 94,00% | 93,56% | +0,44 pp | +0,23 | +0,46% | [−7,5, +6,9] | não são fonte de red |
| Blacklist construída em 2025, aplicada em 2026 (OOS) | 472 | 94,07% | 93,61% | +0,46 pp | +2,32 | +0,49% | [−1,6, +2,3] | filtro de liga não transfere |
| **Threshold vizinho Lay 9–15** | 318 | 93,71% | 93,34% | +0,37 pp | +1,29 | +0,41% | [−2,6, +2,9] | encolhe |
| **Threshold vizinho Lay 11–17** | 812 | 94,46% | 93,83% | +0,63 pp | +5,48 | +0,67% | [−1,1, +2,3] | encolhe; 2024-25: **−0,89%** |
| Threshold vizinho Lay 10–18 | 1.042 | 94,43% | 94,00% | +0,43 pp | +4,79 | +0,46% | [−1,0, +1,7] | encolhe; 2024-25: −0,63% |
| Threshold vizinho Lay 8–20 | 1.469 | 94,15% | 94,31% | −0,16 pp | −2,39 | −0,16% | [−1,5, +1,2] | **inverte** |
| Stop Red 2 reds/dia (2026) | 551 | — | — | — | +5,61 | +1,02% | maxDD 4,06u | irrelevante (2 dias com ≥2 reds) |
| Stop Red 1 red/dia (2026) | 516 | — | — | — | +4,28 | +0,83% | maxDD **4,92u** | piora P&L e DD |
| **Odd real KO−10 (coletor), 16/08→20/09** | 161 | 89,44% | 93,62% | **−4,18 pp** | **−7,17** | **−4,46%** | [−11,1, +0,7] · P(≤0)=0,95 | reprova na odd executável |
| idem, liquidez ≥ 200 no lay | 93 | 87,10% | 93,65% | −6,55 pp | −6,49 | −6,98% | [−14,6, **−0,3**] · P=0,98 | teto do IC < 0 |
| Feed da API (odd da manhã), 19/08→21/09 | 135 | 92,59% | 93,6% | −1,0 pp | −1,43 | −1,06% | — | o "28/1" não reproduz |

**Veredito final: 3 — REPROVADO / ARQUIVADO.**

## Bloco 1 — Reprodução e integridade
- Reproduz exatamente: 2024 N=343 −1,37u; 2025 N=454 +2,16u; 2026 N=558, 30 reds, +6,08u, ROI +1,09%, odd mediana 15,0. O P&L está sobre a
  liability real (sobre stake nominal daria +83,6u — o Antigravity não cometeu esse erro). Comissão 5% deduzida.
- Placares: gols FT da base (`Goals_H_FT/A_FT`); a regra só usa colunas de odd pré-jogo — sem leak na construção. Para o coletor usei o oficial da
  Betfair → base → b365 (cadeia do relatório das 06:00).
- A odd da base é a de **fechamento**, não a executável. Lay/back do 0x1 na regra: 1,11 na base, 1,16 no coletor. Este é o ponto que decide a auditoria (abaixo).
- **A faixa "10–16" é na prática 12–16**: com casa 1,55–2,15 e U25 ≥ 1,75 não existe lay de 0x1 abaixo de 12 (a regra com faixa 12–16 dá os mesmos 558 jogos).
  43% da amostra está em 15,0–16,0.
- **Regime do livro de CS** (memória `base-betfair-regime-odd-lay-cs`): no universo casa 1,55–2,15 & U25 ≥ 1,75, a fração de jogos com lay 0x1 dentro de 10–16
  é 10,6% (2024), 10,0% (2025), **21,4% (jan–abr/26)**, 15,7% (mai–ago/26). A regra seleciona jogos pela existência de livro no lay, o que muda de regime — não é
  uma característica do jogo. A taxa real de 0-1 no universo caiu de 5,9% (2024) para 4,5% (2026); o "edge" acompanha essa deriva, não um filtro.
- 2026 mês a mês: jan −0,6% · fev +4,6% · mar −0,8% · abr +3,9% · mai −1,2% · jun −8,3% · jul +2,3% · ago +0,7%. Dois meses (fev, abr) carregam +8,6u de +6,1u.

## Bloco 2 — Thresholds vizinhos (Lei 13)
- Odd de lay: 9–15 → +0,41% · 10–16 → +1,09% · 11–17 → +0,67% · 10–18 → +0,46% · 8–20 → **−0,16%**. O edge encolhe monotonicamente ao alargar e inverte em 8–20.
  Nos anos fora da especificação (2024–25) os vizinhos 11–17 e 10–18 são **negativos** (−0,89%, −0,63%).
- Odd da casa: 1,50–2,10 +1,68% · 1,55–2,15 +1,09% · 1,60–2,20 +1,15% · 1,40–2,30 +1,14% — pouco sensível (o filtro de casa não faz nada).
- Anti-Under: U25 ≥ 1,60 +1,20% · ≥ 1,75 +1,09% · ≥ 1,90 +1,20% · sem filtro +0,96% — o filtro não faz nada.
- Leitura: os únicos cortes que "importam" são os da odd de lay, e é exatamente aí que o vizinho inverte. Especificação, não edge.

## Bloco 3 — Blacklist (Lei 6)
- 2026 COM blacklist +6,08u (558) vs SEM +6,31u (608). A blacklist **reduz** o lucro; as 9 ligas tiveram 3 reds em 50 jogos (+0,23u) em 2026 e +2,2% em 2024–25.
  `ARGENTINA 1`, `SPAIN 1`, `UKRAINE 1` etc. não aparecem entre as 12 piores ligas de 2026 (as piores são SWEDEN 2, CONFERENCE, PORTUGAL 1, ENGLAND 4).
- Não é a blacklist que fabrica o lucro — mas ela é post-hoc e inútil, e o teste OOS mostra que filtro de liga não transfere: as 9 piores de 2025 aplicadas
  em 2026 dão +0,49% (contra +1,04% sem filtro); as 9 piores de 2024 aplicadas em 2025 dão +1,28% (contra +0,48%). Sinal aleatório.

## Bloco 4 — Circuit breakers
- Sem stop: +6,08u, maxDD 4,06u. Stop 1 red/dia: +4,28u, maxDD **4,92u**, 42 apostas puladas. Stop 2 reds/dia: +5,61u, maxDD 4,06u, 7 puladas.
- Reds não agrupam no dia (2 dias com ≥2 reds em 28 dias com red). O stop de 1 red piora tudo; o de 2 não muda nada. Com liability 1u e banca de 20u
  o DD de 4–5u é 20–25% da banca para ganhar +1%/aposta: relação risco/retorno inaceitável mesmo se o edge fosse real.

## Bloco 5 — Bootstrap e odd executável
- 2026, 1.000×: bloco-dia [−0,8, +2,9] P(≤0)=0,14 · bloco-mês [−1,0, +3,0] P=0,18 · por jogo [−0,8, +3,2] P=0,13. Piso nunca positivo. 3 anos: [−0,9, +1,7], P=0,22.
- Base até 05/09 (cache): 21/08→05/09 N=65, 4 reds, +0,26%.
- **Coletor, odd real do lay 0-1 na primeira captura 4–16 min antes do KO, 16/08→20/09**: N=161 (230 na regra; 69 sem placar ainda — jogos de 16–20/09 e ligas
  fora das bases), **17 reds, WR 89,4% vs BE 93,6%, ROI −4,46%, IC [−11,1, +0,7]**. Com liquidez ≥ 100: −5,8%, IC [−13,1, −0,2]. Com ≥ 200: −7,0%, IC [−14,6, −0,3].
  Vizinhos na odd real: 9–15 −5,0% · 11–17 −2,2% · 10–18 −2,6% · 8–20 −2,6%. Nenhum positivo.
- Feed da API (odd da manhã, os mesmos 34 dias, mesma regra, placar oficial): 162 sinais, 135 liquidados, 10 reds, −1,06%. Não consegui reproduzir o "N=29, 28/1,
  +3,17%" do gap: com o feed completo e a cadeia oficial de placares a amostra é 4,7× maior e negativa. Se os 29 são um subconjunto, o critério de seleção
  precisa ser declarado.
- O "gap" do Antigravity, na odd que existe na tela no KO, é −3,9% (N=159).

## Conclusão
1. Na odd de fechamento da base o método é ruído com sinal positivo: +1 pp de margem sobre um BE de 93,6%, IC cruza zero em todas as janelas, edge encolhe e
   inverte nos vizinhos, dois meses carregam o ano, a faixa de odd é um artefato de regime do livro de CS.
2. Na odd executável (coletor, KO−10, 161 jogos reais) o método **perde 4,5%** e, com a liquidez que uma aposta de verdade exige, o **teto do IC é negativo**.
3. Blacklist e stops não mudam nada (a blacklist reduz o lucro; o stop de 1 red aumenta o drawdown).
4. Isso é o mesmo Lay 0x1 já arquivado (`lay0x1-reprovado-todas-as-rotas`) com cortes novos. Não vai para watchlist: a observação que a watchlist faria já
   está feita — 161 jogos na odd real, 5 semanas — e reprovou. Registrar como morto (Lei 8) e não re-testar.
