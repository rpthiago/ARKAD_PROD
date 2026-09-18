# Stop Red / Stop Green diário — auditoria e implementação (18/09/2026)

Resposta ao pedido "Circuit Breaker" de 18/09. Só dados e o que foi implementado; decisões de gestão ficam com o Thiago.

## 1. Base usada na auditoria

Jogos reais da página 02 ("Resultados dos Métodos em Validação Forward"), 01/08→18/09/2026: **648 apostas, 51 reds
(7,9%)**. Métodos: Draw 236 (27 reds), 2x2 153 (6), 0x3 120 (4), Home 114 (14), Over 4.5 25 (0). Simulação composta
a partir de R$ 2.000, liability = fração da banca, comissão 5%, stop bloqueando só KOs posteriores à liquidação (+115 min).
O benchmark reproduz o número do pedido: **sem stop, 5%/15% → R$ 4.935 (+147%), maxDD 46,6%**.

## 2. Resultados

| cenário | banca final | maxDD | jogos feitos |
|---|---|---|---|
| sem stop, 5/15% | R$ 4.935 (+147%) | 46,6% | 648 |
| stop −10%, 5/15% | R$ 5.381 (+169%) | 39,2% | 615 (pulou 33) |
| stop no 1º red, 5/15% | R$ 4.168 (+108%) | 39,2% | 545 (pulou 103) |
| sem stop, 5/10% | R$ 4.266 (+113%) | 35,0% | 648 |
| stop −10%, 5/10% | R$ 4.594 (+130%) | 29,4% | 642 (pulou 6) |
| sem stop, 5/7,5% | R$ 3.916 (+96%) | 28,8% | 648 |
| sem stop, 5/5% | R$ 3.568 (+78%) | 26,2% | 648 |
| teto de exposição aberta 20%, 5/15% | R$ 3.033 (+52%) | 28,4% | 283 (pulou 365) |
| teto 20%, 5/5% | R$ 2.800 (+40%) | 26,1% | 396 (pulou 252) |

O +212% do stop −10% citado no pedido não foi reproduzido (aqui +169%); a diferença provável está no tratamento dos
jogos simultâneos.

**Testes de acaso**
- Reds agrupam no dia? Taxa de red depois de um red no mesmo dia 9,4% (N=278) vs 6,8% sem red antes (N=370):
  +2,6pp, **P = 0,11** (3.000 permutações).
- Ganho do stop −10% sob o nulo (resultados embaralhados entre os jogos, 300×): mediana −R$ 55, IC90 [−554; +638];
  **P(ganho ≥ os +R$ 446 observados) = 0,09**. Redução de maxDD sob o nulo: mediana +1,2pp (observada +7,4pp).
- Embaralhar a ordem dos **dias** não muda nada: juros compostos são um produto de fatores diários; o stop diário só
  age dentro do dia.
- maxDD típico por sizing, com a mesma taxa de red (150 embaralhamentos): **5% → 26,5% (p95 38,6%) · 10% → 47,2%
  (p95 62,2%) · 15% → 67,9% (p95 83,3%)**. Reds seguidos para perder 30% / 50% da banca: 5%: 7/14 · 7,5%: 5/9 ·
  10%: 4/7 · 15%: 3/5.
- Kelly para lay (fração da banca em responsabilidade) f* = p − q·(odd−1). 2x2 a 18 com WR 96% (6 reds em 149):
  f* = +0,28; no limite inferior do IC95 da WR (91,5%): f* = −0,53. A amostra não determina o sinal do Kelly.
- Teto de exposição aberta é incompatível com 15%: dois jogos simultâneos já são 30% da banca.

## 3. Respostas às 4 perguntas

1. **EV × cauda:** o stop diário não altera o EV por aposta e não há agrupamento de reds no dia acima do acaso (P=0,11).
   O ganho observado (+R$ 446) fica no percentil 91 do nulo — sinal fraco. O 46,6% de maxDD a 15% foi um caminho
   favorável: o típico a 15% é 68%.
2. **Sizing × stop:** a causa do drawdown é o sizing de 15% em odds 18–25 com edge não determinado. 5/7,5% derruba o
   maxDD de 46,6% para 28,8% sem depender do caminho; o stop tira 7 pontos cortando 33 jogos específicos.
3. **Simultâneos:** stop sequencial não enxerga o sábado 16:00; teto de exposição só funciona com liabilities ≤ 5%.
4. **Regra objetiva:** liability por método pela evidência do método (piso do IC95 > 0 → até 5%; IC cruzando zero →
   ≤ 2% ou zero), teto de exposição 20–30% só com sizing ≤ 5%, stop de **banca** (pausa a −30% do pico) como freio
   estrutural; stop diário como opcional.

## 4. O que foi implementado (a pedido, com os parâmetros do pedido)

`stop_diario.py` + `stop_diario.bat`, tarefa Windows **`ARKAD_Stop_Diario`** a cada 5 min (06:10–23:55).

- **Fonte (requisito):** `metodos_aprovados/Sinais_Metodos_Aprovados_YYYY-MM-DD.xlsx` — a grade da página 01. Não usa
  `forward_ko_ledger.csv` nem `forward_5metodos_ledger.csv`.
- **Liquidação:** placar oficial da Betfair (`placares_ft.csv` da VPS via scp, hora UTC→BRT, jogos (W)/(Res)/U21
  excluídos do casamento, fuzzy ≥0,80/0,60), categoria "Any Other" quando não há placar exato, e a coluna `Placar` da
  planilha como último recurso. Um jogo só conta depois de KO + 115 min.
- **Sizing:** 5% Draw / Home / 0x0 XGBoost; 10% 2x2 / 0x3 / Over 4.5, sobre a banca do dia.
- **Banca do dia:** R$ 2.000 compostos com os jogos reais da página 02 até o dia anterior, no mesmo sizing (18/09:
  R$ 3.997); ou valor fixo em `banca_stop.json` (`banca_fixa`). A origem da banca vai no rodapé de cada alerta.
- **Gatilhos:** STOP RED se P&L liquidado do dia ≤ −10% da banca **ou** ≥ 2 reds; STOP GREEN se ≥ +10%. Cada alerta
  dispara **uma vez por dia** (estado em `stop_diario_estado.json`); a lista de "não fazer / cancelar" contém só os
  jogos com KO posterior à liquidação que disparou.
- **Mensagens:** formato do pedido (HTML no Telegram).
- **Teste 18/09:** 12 de 13 jogos liquidados pelo oficial, 12G/0R, +R$ 317,88 (+8,0%), sem gatilho.

## 5. Pontos em aberto (decisão do Thiago)

- O gatilho "2 reds" é mais agressivo que "−10%" (um red de 10% + um de 5% já fecha o dia em −15%); nos 648 jogos
  reais, 12 de 49 dias tiveram ≥ 2 reds.
- A banca composta usa a página 02 inteira (inclui 0x3/2x2 trazidos do ledger); alternativa: `banca_fixa`.
- Scripts da auditoria: `%TEMP%\stop_real.py` / `stop_real2.py` (reproduzíveis sobre o loader da página 02).
