# Suíte Trader In-Play (commit 12e5502) — correções necessárias antes do deploy na VPS

Revisão de 2026-09-15 (Claude), a pedido do Thiago. Deploy **não** feito. Os 4 métodos e o critério de 3 vias do
`PREREGISTRO_SUITE_TRADER_INPLAY.md` ficam como estão; o que precisa mudar é a **camada de dados** (entrada, estado,
saída, liquidação). Referências: `GEMINI.md` (Leis 1, 2, 3) e `PREREGISTRO_TEMPLATE_inplay.md` §5.

## O que está errado (com a linha)

| # | onde | o que acontece | por que invalida |
|---|---|---|---|
| 1 | `inplay_telemetry_engine.py:49-52` — `live_sc = latest.loc[latest['lay'].idxmin()]['runner']` | placar "atual" = runner de menor lay do Correct Score | é o placar FINAL mais provável, não o atual (acerta 27,5% aos 10–25'). Template §5 proíbe. Minuto também errado: `abs(min_to_ko)` em vez de `−min_to_ko − 15` |
| 2 | `trader_inplay_engine.py:282` (e 358, 4xx) — `match.get("Odd_D_Lay")` do `get_daily_dataframe` | odd de entrada = snapshot pré-jogo da apicomunidade | Lei 1/2: a entrada de um trade aos 20' precisa da odd in-play daquele minuto; o tracker não lê `betfair_live_odds.csv` |
| 3 | `tracker_trader_inplay.py:196-201` — `liquidar_sinais` | grava só `placar_saida` e `status=LIQUIDADO`; `odd_saida`, `pnl_liquido`, `roi_pct` ficam 0 | sem odd de saída real não existe P&L de trade; nada para IC95/BH |
| 4 | `tracker_trader_inplay.py:85-110` — `_placares_coletor_cache.csv` / `InPlayTelemetryEngine` | esse cache é do dashboard local; na VPS não existe → `mapa_live` vazio → todo jogo "PREPARAR" → 0 sinais (`--once` local: 0) | o daemon rodaria sem gravar nada |

Também: o pré-registro contém condições que nenhum dado nosso mede — "posse de bola / dominância" (M2), "sem faltas
perigosas ou escanteios sucessivos" (M3), "favorito não demonstrar reação" (M1 stop) — e o código as ignora, então
regra escrita ≠ regra executada (Lei 2). E o M4 duplica o Late Goal v2 (`PREREGISTRO_late_goal_v2.md`, já rodando
na VPS com faixa MEDIDA 1,50–2,60; a suíte fixa 1,80–2,50 sem medir).

## O que a versão corrigida precisa fazer (critério de aceite)

1. **Fonte única na VPS:** `/home/ubuntu/betfair-collector/betfair_live_odds.csv` (colunas `capture_ts, market_type,
   competition, home, away, ko, min_to_ko, runner, selection_id, back, back_size, lay, lay_size, ltp, matched`).
   Minuto = `−min_to_ko − 15`. Favorito pré-jogo = menor back de MATCH_ODDS na captura mais próxima de −5 min.
2. **Estado do jogo pelas linhas O/U batidas** (`gols_por_ou` em `varredura_over_inplay.py`: Over L ≤ 1,02 ou mercado
   sumido e não voltando). Nunca pelo CS. Se o estado for indefinido na captura → não entra (Lei 3: sem default).
3. **Entrada:** odd e liquidez do runner na **primeira captura elegível da janela** (regra do pré-registro). Gravar
   `capture_ts` da entrada. Sem captura elegível → sem sinal.
4. **Saída:** a regra de saída (gol do favorito, 1-1, N minutos, minuto-limite, gol contra) é detectada nas capturas
   seguintes; a odd de saída é a **primeira captura após o evento** com o runner presente (back para fechar lay, lay
   para fechar back). Gravar `capture_ts` da saída. Se o mercado não reabrir antes do minuto-limite → aplicar a
   regra de stop com a odd da captura em que ela dispara. Nunca odd estimada.
5. **P&L:** as duas pernas com comissão 5% (fórmulas do §1 do pré-registro), em `pnl_liquido` e `roi_pct`;
   `stake = 0.0`, `tipo_registro = OBSERVACAO_STAKE_ZERO`. Placar FT para o M4 pelo `placares_ft.csv` (oficial).
6. **Condições não mensuráveis** removidas do pré-registro (com nota "removido em 15/09: sem dado") ou substituídas
   por algo que o coletor mede. **M4:** unificar com o Late Goal v2 (mesma faixa medida) ou declarar por que difere.
7. **Faixas de odd medidas antes de congelar:** rodar a regra de entrada sobre o coletor de 16/08→13/09 e reportar
   p5/p25/mediana/p75/p95 da odd no instante, como em `PREREGISTRO_late_goal_v2.md` §1. Se a faixa escrita cobrir
   < 20% das capturas, a faixa é o dado medido, não a suposta.
8. **Primeiro olhar declarado:** o mesmo código rodado sobre 16/08→13/09 (é o que valida que a camada de dados
   funciona: N, WR, ROI por método). Julgamento só em KO ≥ data do commit corrigido; snapshot 12/10.

Com isso, o deploy (git pull, `--once`, `trader-inplay.service` com loop 60 s, cron `--settle` :40, verificação de
`stake: 0.0` no log e entrada no worklog) é feito na hora.
