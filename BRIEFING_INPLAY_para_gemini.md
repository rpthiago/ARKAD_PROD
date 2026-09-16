# Briefing para o Gemini — criar um método in-play / trade no ARKAD (dados, sem opinião)

Atualizado em 2026-09-15. Tudo abaixo é medido; nada é recomendação. Regras: `GEMINI.md`. Template obrigatório:
`PREREGISTRO_TEMPLATE_inplay.md`. Exemplos de pré-registro já feitos: `PREREGISTRO_lay2x2_apos_gol.md`,
`PREREGISTRO_late_goal_v2.md`, `PREREGISTRO_varredura_over_inplay.md`.

## 1. O que existe de dado

**Coletor Betfair na VPS (`betfair_live_odds.csv`, desde 16/08/2026, ~2,1 GB)**
- Colunas: `capture_ts, market_type, competition, home, away, ko, min_to_ko, runner, selection_id, back, back_size,
  lay, lay_size, ltp, matched`. Minuto de jogo ≈ `−min_to_ko − 15`.
- 14,27 milhões de linhas em 29 dias. Por mercado: CORRECT_SCORE 8,47 M (19 runners) · MATCH_ODDS 1,34 M ·
  OVER_UNDER_35 0,84 M · OVER_UNDER_25 0,80 M · BOTH_TEAMS_TO_SCORE 0,79 M · OVER_UNDER_15 0,74 M · OVER_UNDER_05 0,66 M ·
  FIRST_HALF_GOALS_05 0,62 M. Back e lay com tamanho, pré-jogo (de ~−5 h) e in-play.
- Granularidade in-play: passagens de ~60 s (o Late Goal e o alerta-under rodam com poll de 60 s sobre o coletor).
  Não há dado abaixo de 1 s; suspensões de mercado após gol aparecem como buracos na série.
- Cobertura: ~4.900 jogos com Correct Score no KO em 27 dias (16/08→13/09); ligas pequenas incluídas
  (a apicomunidade cobre ~40% desses jogos).
- Estado do jogo (gols já saídos) **só** pelas linhas O/U batidas (`gols_por_ou` em `varredura_over_inplay.py`):
  Over L ≤ 1,02 ou mercado da linha sumido e não voltando. Nunca pelo Correct Score de menor lay (é o placar final
  mais provável; bate com o real em 27,5% aos 10–25 min).

**Liquidação oficial (`placares_ft.csv`, desde 12/09/2026)**
- `liquidar_betfair_oficial.py` consulta `list_market_book` dos mercados CLOSED e grava o runner WINNER de CS
  (placar exato), Match Odds, O/U 0.5–3.5, BTTS, HT 0.5. 583 jogos liquidados em 4 dias; 97% dos sinais do ledger
  de 13/09 em diante liquidados por ela. Placares "Any Other" (≥4 gols de um lado) não têm placar exato.
- Antes de 12/09: bases apicomunidade (Betfair até 05/09, b365) com casamento fuzzy ≥ 0,80.

**Instrumentos in-play já rodando (stake-zero)**
- `xg_ht_log.csv`: xG/chutes/escanteios por jogo, min 50 e HT, via FotMob/Opta — 460 jogos (10–15/09), só ligas
  com cobertura Opta.
- `inplay_min80_log.csv`: xG acumulado, chutes no alvo, toques na área aos 78–83' — 145 jogos.
- `late_goal_log.csv`: 547 capturas (min 82–86, diferença de 1 gol) com odd back/lay, liquidez e spread do Over.

## 2. O que já foi testado in-play, na odd real do coletor (todos com placar oficial ou exato)

| método | regra | N | resultado |
|---|---|---|---|
| Under-limite (v1/est2/est3) | Back Under X.5 com X gols, minutos finais | 371+ | −3,1% (re-liquidado com placar exato; o coletor grosso dava +26% por falso green) |
| Espelho Over-limite | Back Over na mesma situação | 371 | −4,63% (Back Under no mesmo cohort −4,64%) |
| Idea1 | Back Under 2.5 / 3.5 com 1 gol, min 8–30 | 1.254 | −3,00% (U2.5 −4,2%; U3.5 −1,85%) |
| Under com folga | Back Under (g+1).5 / (g+2).5, 5 janelas × 3 favoritismos | 9.250 / 5.820 | −4,5% [−8,6; −0,8] / −0,93% [−4,2; +2,2]; 27 células, 0 PASSA |
| Late Goal v1 | Back Over, min 82–86, diff 1, odd 3,00–5,50 | 0 na faixa (547 capturas) | inviável; odd real mediana 2,10 |
| Late Goal v2 | idem, faixa medida 1,50–2,60 | 265 (1º olhar) | −9,7% [−23,4; +0,1]; Over 3.5 −17,6% [−33,6; −6,5]. Julgamento KO ≥ 16/09 |
| Lay Draw 60' | favorito empatando aos 60' | — | −4,9% (odd cai a ~3,2; BE 70,4% > WR 66,7%) |
| Lay 0x1 55–75' em 0-0 | odd 2,00–5,50 | 610 | −9,0% (28,7% terminam 0-1 na faixa vs 18,4% fora) |
| Radar HT | back favorito 0-0 no intervalo + pressão | 0 sinais em 21 dias | inviável por fluxo |
| Lay 2x2 após 1º gol | 4 janelas × 3 fav, odd 5–30 | 1.647 (1º olhar) | +0,04pp agregado; 12 células inconclusivas; julgamento 12/10 |
| Over 0.5–3.5 × estado × janela × fav (grade) | 60 células + H-extra | — | **lacrada até 12/10** (não lida) |
| CS no coletor perto do KO | 19 runners × fav × corte | 26.594 | 0 PASSA / 4 REPROVA / 50 INCONCLUSIVO; snapshot 2 em 12/10 |

Pré-jogo, para referência: 43 mercados × lay/back × favoritismo × contexto de gols × lado × corte, só 2026
(5.995 células): 0 PASSA fora de Correct Score; as de CS vivem do regime jan–abr da base (`base-betfair-regime`).

## 3. O que um método/trade novo precisa trazer (do template)

1. **Mecanismo** em uma frase falsificável: por que o mercado in-play erraria *nesse* instante. "P(evento) alta" e
   "odd baixa" não são mecanismo (ambos já custaram método).
2. **Regra congelada**: mercado/runner, lado, estado do jogo (pelas linhas O/U), janela de minuto, favoritismo,
   **faixa de odd medida no coletor antes de fixar** (Late Goal v1 supôs 3,00–5,50; o real era 2,10).
3. Se for **trade** (entrada + saída): a saída também tem que ser uma odd do coletor no instante da regra de saída
   (nunca estimada); P&L = as duas pernas com comissão. Já medido: sair no 0-0 HT num Lay 0x1 custa −0,499u, não
   +0,19u (Hall of Shame, "odd de saída fabricada").
4. **Critério de 3 vias** com N mínimo, IC95 bloco-dia, reds ≥ 5, BH quando houver mais de uma célula.
5. **Julgamento só em KO ≥ data do commit** do pré-registro; o passado do coletor serve de primeiro olhar declarado.

## 4. Perguntas em aberto (dados que existem e ainda não foram lidos contra a odd real)

- xG/chutes aos 50' e no HT (`xg_ht_log.csv`) vs odd de Match Odds / O/U no mesmo instante: N=460, ligas grandes.
- Pressão aos 78–83' (`inplay_min80_log.csv`) vs gol tardio: N=145.
- Caminho da odd após gol (suspensão → reabertura): o coletor tem a série a 60 s; nunca foi medido se a reabertura
  difere sistematicamente do preço 5 min depois.
- Trades entrada/saída sobre o caminho de odd (o coletor permite calcular P&L real de qualquer par entrada/saída
  desde 16/08); nenhum trade foi testado além do 0x1.
