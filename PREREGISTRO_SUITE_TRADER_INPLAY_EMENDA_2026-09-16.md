# Emenda ao PREREGISTRO_SUITE_TRADER_INPLAY.md — 2026-09-16 (camada de dados e regras mensuráveis)

**Por quê:** a implementação de 15/09 (commit 12e5502) lia placar pelo Correct Score de menor lay, odd de entrada
do feed pré-jogo e não calculava saída (`CORRECOES_SUITE_TRADER_para_antigravity.md`). Thiago escolheu a opção A:
reescrever a camada de dados sobre o coletor. Os mecanismos e o critério de 3 vias do pré-registro original ficam.
**Congelada antes de ler qualquer resultado** (o primeiro olhar abaixo foi rodado depois deste texto).

## Fonte e fatos (iguais no histórico e ao vivo — `trader_inplay_core.py`)

- Fonte única: `betfair_live_odds.csv` do coletor (VPS). Minuto = −min_to_ko − 15. Passe = linhas do mesmo jogo
  a < 90 s de distância.
- Favorito pré-jogo: menor back de MATCH_ODDS na captura mais próxima do KO (min_to_ko ≥ −5).
- Gols já saídos: linhas Over batidas (Over L ≤ 1,02) ou sumidas depois de vistas com o jogo ainda capturado;
  monotônico. Indefinido → sem entrada.
- Quem marcou: **corrigido 16/09 antes de qualquer julgamento** — medido no coletor que a Betfair NÃO remove os
  placares impossíveis do Correct Score (ex.: "1 - 0" segue com preço 36/80 num 0-1 real). O lado vem da **direção da
  odd de back do mandante no Match Odds** entre a última captura antes do gol e a primeira depois com preço:
  caiu ≥ 10% ⇒ mandante marcou; subiu ≥ 10% ⇒ visitante; senão indeterminado (espera a próxima captura).
- Odd de entrada: **primeira captura elegível da janela**; fora da faixa ou sem liquidez ⇒ `FORA_DA_FAIXA`, e o
  método não tenta de novo nesse jogo (1 trade por jogo por método).
- Odd de saída: **primeira captura com preço depois do evento de saída** (mercado suspenso ⇒ espera). Se o jogo
  acaba sem captura com preço ⇒ `SEM_ODD_SAIDA`, fora da conta (nunca estimada).
- P&L por 1u de risco, comissão 5% sobre lucro: lay fechado em back `S_in·(1 − O_in/O_out)`, `S_in = 1/(O_in−1)`;
  back fechado em lay `O_in/O_out − 1`.

## Regras congeladas (o que o código executa)

| | M1 LTD Trader | M2 Swing Fav em desvantagem | M3 Scalping Under 2.5 |
|---|---|---|---|
| estado | 0 gols | 1 gol, marcado pelo visitante | ≤ 1 gol |
| janela | 15–25' | 20–45' | 55–62' |
| favorito | mandante ≤ 1,45 | mandante ≤ 1,35 | qualquer |
| entrada | lay The Draw, **3,50–9,50** (medida: p5 3,60 · mediana 4,70 · p95 9,34; a suposta 3,00–4,20 cobria 26%), lay_size ≥ 200 | back mandante, **1,50–6,00** (medida: p5 1,58 · mediana 2,16 · p75 2,94; a suposta 2,10–3,20 cobria 32%), back_size ≥ 200 | back Under 2.5, **1,05–2,30** (medida: p5 1,08 · mediana 1,35 · p95 2,20), back_size ≥ 200 |
| saída green | gol do mandante → back Draw na 1ª captura com preço | 1-1 → lay mandante na 1ª captura | lay Under ≤ entrada − 3 ticks |
| saída stop | 68' ainda 0-0 → back Draw; gol do visitante → fecha na 1ª captura ≥ 5 min depois (ou antes se sair outro gol) | 70' ainda 0-1 → lay mandante; 0-2 → lay mandante na 1ª captura | gol → lay Under na 1ª captura; 8 min sem alvo → lay Under |

**Removido por não ser mensurável com nosso dado** (Lei 3): "dominância / posse" (M2), "sem faltas perigosas ou
escanteios" (M3), "favorito não demonstrar reação" (M1, substituído por 5 min fixos). **M3 janela A (Under 1.5 HT
33–38')**: o coletor não grava FIRST_HALF_GOALS_15 → não existe. **M4 Late Goal Trader** = `PREREGISTRO_late_goal_v2.md`,
já em serviço na VPS com faixa medida (1,50–2,60); não é duplicado aqui.

## Critério (do pré-registro original)

APROVA: N ≥ 200 trades fechados, piso do IC95 (bloco-dia, ≥ 6 dias) > 0, ≥ 10 reds, sobrevive ao BH sobre os 3
métodos. REPROVA: teto < 0, ou ROI < −10% com N ≥ 80. INCONCLUSIVO: resto.
**Julgamento só em trades com KO ≥ 2026-09-16.** Snapshot 12/10 com as outras grades. O histórico 16/08→13/09 é
primeiro olhar declarado (serve para medir as faixas e provar que a camada de dados fecha trades).

---

## Primeiro olhar — 2026-09-16 (coletor 16/08 → 16/09 09:00, 5.205 jogos; declarado, NÃO julga)

Mesmo código do serviço (`trader_inplay_core.py`), odd real do coletor, saída na primeira captura com preço após o evento.

| método | elegíveis | fechados | verdes | ROI (u por u de risco) | IC95 bloco-dia | dias |
|---|---|---|---|---|---|---|
| M1 LTD Trader | 167 | 93 | 59,1% | **−1,02%** | [−5,2; +3,0] | 26 |
| M2 Swing Fav em desvantagem | 19 | 12 | 41,7% | **−22,7%** | [−39,6; −3,5] | 9 |
| M3 Scalping Under 2.5 | 2.279 | 1.873 | 77,6% | **−2,54%** | [−3,5; −1,5] | 29 |

Por saída — M1: gol do favorito 49× (+0,113 cada; odd 4,80 → 9,40), gol da zebra 13× (−0,157), 68' em 0-0 24× (−0,209),
2 gols sem lado 7×. M2: 1-1 4× (+0,29), 0-2 3× (**−0,77**, odd 2,50 → 14), 70' em 0-1 4× (−0,42). M3: alvo de 3 ticks
1.358× (+0,063), gol 351× (−0,333), 8 min sem alvo 164× (−0,099).
Fluxo: M1 ≈ 3,5 fechados/dia; M2 ≈ 0,4/dia (N=200 levaria ~15 meses); M3 ≈ 65/dia.
Leitura registrada: M3 já tem teto do IC < 0 com N=1.873 no olhar (perde o spread: o alvo de 3 ticks paga +6% e o gol
custa −33%); M2 tem teto < 0 com N=12. Nada disso é julgamento — o julgamento é só sobre KO ≥ 16/09, em 12/10.
Tabela: `varredura_over/trader_inplay_olhar_2026-09-16.csv`.
