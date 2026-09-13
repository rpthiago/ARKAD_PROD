# Checklist — varredura de tudo que foi feito em 09-12/09/2026

> Objetivo: conferir se **nada passou** — nem aprovação indevida, nem reprovação indevida.
> Regra da varredura: cada item tem (a) o que foi afirmado, (b) o dado, (c) a **fonte de placar**
> usada, (d) o que derrubaria. Fonte de placar é o ponto fraco da semana: o coletor da VPS erra 27%
> (sempre para menos gols); bases Betfair/b365 têm placar exato; liquidação oficial só existe desde
> 12/09 20:56. Onde a fonte foi o coletor, o número precisa ser refeito.

## A. REPROVAÇÕES — refazer o número onde a fonte era o coletor

| # | método | afirmado | N | fonte de placar | ação na varredura |
|---|---|---|---|---|---|
| A1 | Under-limite v2 | −3,11%, reprovado | 445 | **coletor** (viés A FAVOR do Under) | re-liquidar contra bases Betfair+b365 no subconjunto que casar; esperado piorar |
| A2 | Idea1 Back Under | −3,17%, sem edge | 1.164 | **verificar** (`idea1_log.csv` na VPS) | idem A1 |
| A3 | Lay 0x1 In-Play (Rota C) | seleção adversa −7,78pp | 610 | **coletor** (viés a favor, declarado) | idem; conferir a taxa de 0x1 na faixa 2,00-5,50 com placar exato |
| A4 | Lay 0x1 RF v2 (hold) | −2,05% na odd de lay real | 1.531 | b365 (exato) + odd da base Betfair (fuzzy) | conferir que o fuzzy da ODD não casou jogo errado (amostra de 20); WR sem/com cobertura já bateu (90,5 vs 91,2) |
| A5 | Late Goal | −7,3pp, 152 liq, 0 sinais na coorte | 152 | status oficial do runner | sólido; só reconferir que `ft_status=BF:*` é WINNER/LOSER, não inferência |
| A6 | Radar HT | fluxo ~0 | 3/360 | n/a | nada a refazer |
| A7 | Lay 0x0 "APROVA" de 07/09 | retirado: IC de 1 bloco + convenção stake | 46 | b365/Betfair | conferir a emenda: N=1476 vem de desvio 0,2294 / média 0,0117 — recalcular |
| A8 | Over 0.5 como atalho de N | retirado: variância igual, edge 61% | 1.703 | b365 | recalcular desvio 0,2265 vs 0,2255 |
| A9 | 5 falsos GREEN na mestre (29-30/08) | placar manual ≠ 2 bases | 5 | Betfair + b365 exatos, **confirmado pelo Thiago** | nada a refazer; a mestre foi mantida manual por decisão (opção A) |

## B. APROVAÇÕES / NÚMEROS POSITIVOS — conferir a fonte e o denominador

| # | item | afirmado | fonte de placar | ação na varredura |
|---|---|---|---|---|
| B1 | Lay 0x0 backtest | +1,10pp de WR, IC95 liab [+0,30;+1,98], p=0,0055, 30 blocos | b365 (exato) + odd base Betfair | recalcular do `lay_0x0_real_oos_bets_xgb.csv`; conferir que `odd_lay` = `Odd_CS_0x0_Lay` da base (1884/1885 bateu) |
| B2 | Forward 5 métodos | +6,90u, 431 liq, 391G/40R | oficial (11) + bases (fuzzy ≥0,80) | 149 SEM_PLACAR fora: conferir que a exclusão não seleciona por resultado (comparar WR das ligas com/sem cobertura onde houver os dois) |
| B3 | Lay Draw na mestre verificada | +5,18u / 166 | bases | só se a opção B for adotada um dia |
| B4 | Varredura Over (grade) | 0 passa / 9 reprova / 89 inconclusivo | bases (fuzzy) + oficial | reconferir 3 células REPROVA à mão (odd de back real na janela, placar) |
| B5 | Liquidador oficial | 11 jogos liquidados em 12/09 | **é a fonte** | **quando as bases chegarem a 12/09 (~17/09): cruzar `placares_ft.csv` com Betfair+b365 — tem que bater 100%**. Se não bater, tudo que usa a oficial reabre |

## C. CÓDIGO ALTERADO NA SEMANA — o que cada um mudou e como conferir

| arquivo | mudança | conferir |
|---|---|---|
| `analisar_0x0_xgb.py` | guard nk≥6 no bootstrap; 2 convenções lado a lado; forward lê o ledger | rodar; o recorte da simulação tem que dizer "IC INDEFINIDO", o forward N=6 |
| `forward_0x0/liquidar_picks_0x0.py` | ledger append-only dos picks enviados | md5 estável em 2 execuções; Hradec 0-0 RED via planilha manual |
| `relatorio_forward_5metodos.py` | 5 regras copiadas da página 01; placar oficial→Betfair→b365 (fuzzy ≥0,80) | comparar 10 sinais aleatórios com a página 01 no mesmo dia (mesma odd, mesmo jogo) |
| `conferir_placares_manuais.py` | SÓ AVISA | grep: não pode ter `to_excel`/`to_csv` em arquivo do usuário |
| `liquidar_betfair_oficial.py` (VPS) | placar pelo runner WINNER | B5 |
| `tracker_favorito_dominante_inplay.py` (VPS) | `<=` no odd_a; funil por ciclo | `odd_a>0` no cache; journal tem `[funil ...]` |
| `xg_ht_logger.py` (VPS) | API_FORA vs None; quarentena 7d | journal mostra `[quarentena]` datada, não permanente |
| `coletor_inplay_min80.py` (VPS) | placar por `get-matches-by-date`; teto 260 | liquidação "FT: x-y" no journal |
| `estrategia_lay_0x1_inplay.py` | NaN pré-jogo → SKIP | 6 casos de borda no worklog de 10/09 |
| `_gerar_excel_*saldo_menor.py` | fabricação de odd → np.nan | grep `1.05 + 0.15` = 0 |
| `automacao_diaria_aprovados.py` | **revertido** ao original em 12/09 | `git diff d052d16 -- automacao_diaria_aprovados.py` vazio contra o `.bak` |
| mestre + diárias 03-09/09 | **revertidas** ao manual em 12/09 | soma PnL_u da mestre = +12,889 |

## D. ERROS MEUS NA SEMANA — para a varredura saber onde desconfiar mais

1. "Sem false green" (10/09) → 5 falsos GREEN (11/09). A checagem confiou no placar da própria base.
2. Over 0.5 "variância muito menor, N uma fração" → variância igual, N 2,7x maior.
3. Placares de 29-30/08 "sem fonte / coletor" → eram manuais do Thiago.
4. Zerei 60 resultados manuais para PENDENTE por suposição → restaurados.
5. "A expectativa é negativa" antes de rodar a varredura Over → rodou, agregado negativo, mas uma família positiva que eu não teria olhado.
6. Sessão anterior: "APROVA" do 0x0 (IC de 1 bloco), "H1 PASSA" em dado parcial.

**Padrão:** afirmar antes de medir, e confiar no dado interno sem fonte externa. A varredura deve
priorizar exatamente os itens onde a fonte de placar era interna (A1, A2, A3) e o item que valida a
fonte nova (B5).

## Ordem sugerida

1. **B5** (quando as bases alcançarem 12/09) — valida a fonte que tudo passa a usar.
2. **A1, A2, A3** — re-liquidar os reprovados do coletor contra bases exatas.
3. **B1, B2** — reconferir os dois positivos que sustentam decisões.
4. **C** — grep/rodar cada script listado.
