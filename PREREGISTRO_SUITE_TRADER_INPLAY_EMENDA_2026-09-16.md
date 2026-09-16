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
- Quem marcou: runners de Correct Score que a Betfair **remove** quando impossíveis (1 gol: "0 - 1" ausente e
  "1 - 0" presente ⇒ mandante; 2 gols: "1 - 1" única presente entre {2-0, 0-2, 1-1} ⇒ 1-1).
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
| entrada | lay The Draw, **3,00–4,20**, lay_size ≥ 200 | back mandante, **2,10–3,20**, back_size ≥ 200 | back Under 2.5, back_size ≥ 200 (faixa: a medida abaixo) |
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
