# PRÉ-REGISTRO — Lay Under 2.5 FT em jogo de favorito médio ("onda de gols 2026")

Registrado em 2026-09-21 06:xx BRT, ANTES de qualquer medição no coletor (odd real de KO) e antes do primeiro sinal no forward.

## Origem (declarada como primeiro olhar, não como julgamento)
Grade congelada de `PREREGISTRO_varredura_metodos_2026.md` re-rodada em 21/09 perguntando "quem está positivo em ago-set/2026
e melhor do que antes" (`varredura_over/momento_nao_aprovados_2026-09-21.csv`, 1.366 células com ROI recente > 0):
- Lay Under 2.5 FT, fav 1.40-1.80: N=633 ROI +10,0% IC95 [+4,6, +15,4], antes (jan-jul/26) −2,5% em 2.731; passa BH-FDR no IC e na permutação.
- Não é calendário: ago-set/2024 −6,5%, ago-set/2025 −10,3%. Semanas 31-35/2026 todas positivas; jul/26 +8,7%; set até 05/09 +1,0% (N=56).
- Todas as outras células "quentes" não-CS são o mesmo tema (Over 2.5 back, Under 3.5 lay, Draw lay fav fora 1.40-1.80, CS 1x1 lay).
- 63 células com piso IC>0 contra 34 esperadas por acaso: o excesso é este tema só. Seis semanas contra dois anos de vermelho.

## Regra (congelada)
- Mercado: Over/Under 2.5 Goals FT, Betfair Exchange. Aposta: LAY "Under 2.5 Goals" (equivalente: back Over 2.5). Liability 1u.
- Favorito (menor back do Match Odds na mesma captura): 1.40 < fav <= 1.80.
- Odd de lay do Under 2.5: 1.30 <= odd <= 6.00. Liquidez no lay >= 200 (lay_size).
- Exclusões: femininos (W), reservas (Res), U1x/U2x, Women/Reserves/Youth. Sem filtro de liga.
- Momento de entrada no forward: KO−10 (primeira captura com 4 <= min_to_ko <= 16), como o ledger KO. Sinal listado também nas 06:00 (odd da API) com tag; a odd que conta é a do KO−10.
- Liquidação: gols FT >= 3 → GREEN = 0,95/(odd−1); senão RED = −1. Placar: fonte oficial da VPS → base → b365 (mesma cadeia do relatório).

## Critérios (declarados antes)
- Janela de julgamento: sinais com KO >= 2026-09-22 (o que vier do backup do coletor 16/08→20/09 é RETROSPECTIVO, item 2 abaixo, e não entra no N).
- APROVA (vira dinheiro): N >= 150 e piso do IC95 (bootstrap bloco-dia, >= 6 dias) > 0, com >= 20 reds.
- REPROVA: teto do IC95 < 0, ou ROI < −5% com N >= 80.
- Saída da onda (se aprovado e em dinheiro): ROI dos últimos 60 < −2% OU drawdown >= 4u desde o pico → stake zero, forward continua.
- Sem emenda de faixa/filtro durante a janela. Qualquer mudança = novo pré-registro.

## Item 2 — medição retrospectiva no coletor (a rodar agora, resultado anotado abaixo)
Backup local `backup_coletor/*.gz` (16/08→20/09). Mesma regra na odd de KO−10 e na primeira captura da manhã. Serve para responder
"a odd executável sustenta o que a odd de fechamento da base mostrou?" — não para aprovar.

### Resultado (rodado 2026-09-21, `varredura_over/under25_fav_medio_coletor_ko10.csv`)
Coletor, primeira captura com 4-16 min para o KO, 16/08 -> 20/09: 1.365 jogos na regra, 783 com placar (582 sem placar: jogos de 16-20/09
ainda sem liquidacao oficial + competicoes fora das bases).

| recorte | N | Over 2.5 | BE | ROI | IC95 |
|---|---|---|---|---|---|
| regra completa, odd real KO-10 | 783 | 56,7% | 58,8% | **-3,24%** | [-9,5, +3,0] |
| com liq >= 200 | 559 | 54,9% | 58,5% | **-5,87%** | [-12,4, +1,0] |
| 16-31/08 | 210 | 60,5% | 58,6% | +3,99% | [-3,5, +11,1] |
| 01-15/09 | 386 | 55,2% | 58,9% | -6,61% | [-17,3, +4,4] |
| 16-20/09 | 187 | 55,6% | 58,7% | -4,38% | (4 dias) |
| mesma regra na BASE (fechamento), 16/08 -> 05/09 | 382 | 63,9% | 58,3% | +10,15% | [+3,4, +17,3] |

Por que a base mostrava +10% e o coletor mostra -3%:
- Nao e a odd: lay do Under 2.5 no KO-10 mediana 2,45 vs fechamento 2,42; favorito 1,61 vs 1,63; 178 de 196 jogos caem na regra pelas duas odds.
- E o universo + o tempo: nos jogos do coletor que ESTAO na base (ligas apicomunidade), ate 05/09: +5,0% (N=196, IC [-6,6, +15,5]);
  nos jogos FORA da base: -8,2% (N=126). Depois do fim da base (06/09 -> 20/09): -5,4% (N=461); com liq>=200: -8,9% (N=327).
- Semana a semana no coletor: 33 -1,3% | 34 +8,8% | 35 -7,8% | 36 +6,4% | 37 -11,4% (N=305).

Conclusao do item 2: a odd executavel NAO sustenta o sinal fora das ligas da base, e em setembro o sinal ja virou. Nao entra dinheiro.
O forward em stake zero fica autorizado pela regra acima (julgamento KO >= 22/09) apenas se o Thiago quiser observar; a expectativa
declarada agora e REPROVA.
