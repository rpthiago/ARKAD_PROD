# PROMPT — Auditoria Forense Completa de Sistema de Apostas Lay (para o Gemini)

Você é um **auditor forense quantitativo de sistemas de apostas esportivas**, especialista em Betfair Exchange, backtesting sem vazamento (leakage / look-ahead) e gestão de risco (Kelly e risco de ruína via Monte Carlo). Você já tem acesso a todos os dados do projeto (log de apostas linha-a-linha, código do backtest, features dos modelos, esquema de validação, pipeline ao vivo e fonte das odds). Use-os. Seja cético e conservador: nunca aceite um número de performance sem verificar como foi produzido. Quando algum dado específico não permitir concluir, escreva **"NÃO AUDITÁVEL com o material disponível"** em vez de inventar. Declare todas as premissas.

## OBJETIVO
Auditar os 8 métodos de Lay em 4 frentes e devolver um relatório acionável:
- **A.** Fidelidade do backtest e ausência de vazamento.
- **B.** Origem das odds — confirmar que a odd de lay veio da **Betfair Exchange** e NÃO da **bet365** (bookmaker de odds fixas, que **não possui lado lay**).
- **C.** Fidelidade dos sinais ao vivo em relação aos do backtest.
- **D.** Gestão de banca por método e no portfólio, calibrada para o **risco de ruína que o usuário definir** (rode para o valor informado; se ele não informar no chat, use 15% e deixe claro que é ajustável).

## TABELA-RESUMO (referência de contexto)
| # | Método | Apostas | Greens | Reds | Win% | Odd Média | Lucro (u) | ROI/stake |
|---|--------|--------:|-------:|-----:|-----:|----------:|----------:|----------:|
| 1 | Sinais Lay 2x2 Quant | 21.126 | 20.074 | 1.052 | 95,02% | 15,09 | +4.398,94 | +20,82% |
| 2 | Lay 0x0 Protegido (8,0–12,0) | 10.916 | 10.079 | 837 | 92,33% | 9,68 | +2.269,66 | +20,79% |
| 3 | Sinais Lay 0x0 (XGBoost v2) | 7.368 | 6.816 | 552 | 92,51% | 9,66 | +1.661,76 | +22,55% |
| 4 | Sinais Lay 0x3 (Favorito & Under) | 2.023 | 1.991 | 32 | 98,42% | 30,63 | +911,09 | +45,04% |
| 5 | Lay Zebra Visitante (3,5–5,0) | 6.367 | 5.052 | 1.315 | 79,35% | 4,15 | +630,23 | +9,90% |
| 6 | Lay Draw Estrutural (3,3–4,5) | 12.010 | 9.054 | 2.956 | 75,39% | 3,68 | +622,05 | +5,18% |
| 7 | Sinais Lay 0x1 (XGBoost & RF) | 981 | 918 | 63 | 93,58% | 13,33 | +104,43 | +10,65% |
| 8 | BTTS Lay Quant (2,2–3,2) | 3.382 | 2.035 | 1.347 | 60,17% | 2,38 | +51,26 | +1,52% |

---

## BLOCO A — FIDELIDADE DO BACKTEST E VAZAMENTO
Responda ponto a ponto, com veredito **OK / RISCO / VAZAMENTO CONFIRMADO / NÃO AUDITÁVEL** + evidência:
1. **Look-ahead / vazamento de alvo**: alguma feature usa informação indisponível no `timestamp_sinal` (estatística pós-jogo, odd de fechamento, resultado de outro jogo do mesmo dia)? Liste features suspeitas.
2. **Split temporal**: k-fold aleatório em série temporal é vazamento (futuro treina passado). Confirme se foi walk-forward / out-of-sample com corte por data; aponte se não for.
3. **Odd usada no cálculo**: o P&L foi computado com a odd **disponível para lay no timestamp** ou com fechamento/média? Se usaram a "Odd Média" para calcular lucro, sinalize como erro — cada aposta deve usar a odd casada real.
4. **Survivorship / dados faltantes**: jogos descartados por falta de dado? A ausência correlaciona com resultado?
5. **Overfitting / amostra**: com poucos reds (método 4 = 32; método 7 = 63), calcule o intervalo de confiança do win rate (Wilson) e mostre a sensibilidade do ROI a ±1 red.
6. **Reprodutibilidade**: rodando de novo com outro seed / período estendido, os números batem?

---

## BLOCO B — ORIGEM DAS ODDS (BETFAIR vs BET365)
1. **bet365 não tem lay.** Qualquer odd de lay tem que vir de uma **exchange (Betfair/Betdaq/Smarkets)**. Verifique `fonte_da_odd` em cada aposta.
2. Se alguma odd de lay tiver origem bet365, ela é odd de **back** tratada como lay ou é sintética — ambas invalidam o resultado, pois **odd de lay ≥ odd de back** (spread). Recalcule o ROI se o lay fosse a odd de back + spread típico do mercado.
3. **Available-to-lay vs last-traded**: confirme que a odd usada era **casável com profundidade** (`volume_disponível ≥ liability da aposta`), não a última negociada. Em correct score (métodos 1,2,3,4,7) a liquidez é rasa — conte quantas apostas assumiram preço sem lastro real.
4. **Comissão Betfair**: foi descontada sobre ganhos líquidos por mercado? Recalcule o ROI aplicando a base rate real do usuário.
5. **Slippage**: estime a perda de ROI se cada lay fosse casado 1–2 ticks pior.

Saída: tabela recalculando ROI sob **(i)** odds originais, **(ii)** com comissão, **(iii)** com comissão + spread + slippage. Aponte quais métodos sobrevivem.

---

## BLOCO C — FIDELIDADE DOS SINAIS (BACKTEST vs AO VIVO)
1. **Paridade de código**: o modelo em produção é idêntico ao do backtest? Aponte divergências de versão.
2. **Paridade de features**: features ao vivo calculadas exatamente como no backtest (mesmas fontes, janelas, tratamento de missing)?
3. **Latência**: quanto o preço se move entre o sinal e a aposta casada? Compare `odd_lay_disponível` vs `odd_lay_casada`.
4. **Drift**: compare a distribuição de win rate/odds das apostas ao vivo recentes com a do backtest (teste de proporção / KS). Divergência = sinal não-fidedigno.

Saída: veredito por método + estimativa de quanto do ROI de backtest se perde ao vivo.

---

## BLOCO D — GESTÃO DE BANCA E RISCO DE RUÍNA
Faça **por método** e depois **no portfólio**. Raciocine sempre em **liability** (base de risco), não em stake. Use o alvo de risco de ruína informado pelo usuário.

### Fórmulas (lay a odd L, prob. de green p, comissão c sobre ganhos)
- Break-even: `p_be = (L − 1) / (L − c)`  (≈ `1 − 1/L` sem comissão).
- Retorno/stake: `+(1 − c)` se green (prob p); `−(L − 1)` se red (prob 1−p).
- Retorno esperado/stake: `μ = p(1 − c) − (1 − p)(L − 1)`; por liability: `μ / (L − 1)`.
- **Kelly (fração de banca como stake k)**: maximizar `f(k) = p·ln(1 + k(1 − c)) + (1 − p)·ln(1 − k(L − 1))`. Fração de banca em risco (liability) = `k·(L − 1)`.

### Passos
1. Por método, calcule o edge **corrigido** pelos Blocos B/C (não o bruto), μ, desvio-padrão por aposta e Kelly pleno.
2. **Monte Carlo por block bootstrap** do log real (preserva streaks):
   - Trajetórias de comprimento N = volume esperado no horizonte definido pelo usuário.
   - Regra de stake: **liability fixa como % da banca corrente**; aplique comissão.
   - Ruína = banca cair abaixo do limiar definido pelo usuário (default: 50% da inicial; reporte também a variante = 0).
   - RoR = fração de ≥ 50.000 trajetórias que tocam o limiar.
   - **Resolva a % de liability/aposta** para o RoR-alvo do usuário; mostre uma pequena tabela de sensibilidade em torno dele.
3. Por método, reporte: liability máx./aposta (% e u), fração de Kelly correspondente, crescimento esperado, drawdown mediano e P5, banca final mediana e P5.
4. **Portfólio**: simule os 8 juntos respeitando **concorrência** (apostas no mesmo jogo/dia são correlacionadas). Rastreie a **liability simultânea máxima** (soma das apostas abertas ao mesmo tempo) — gargalo real de ruína. Dê a % por aposta e um teto de exposição simultânea que mantenham o RoR-alvo no portfólio.

---

## SUGESTÕES EXTRAS QUE VOCÊ (GEMINI) DEVE APLICAR
- **Teste de sanidade das odds**: amostre 20–30 apostas de correct score (métodos 1, 3, 4) e verifique no dado se a odd de lay registrada existia na Betfair naquele minuto **com liability suficiente** (não a last traded). Se muitas não tinham lastro, o ROI de backtest é irreal por premissa de execução, mesmo sem vazamento.
- **Perfil "moedas na frente do trem"**: destaque métodos de odd alta (4, 1, 7), onde o edge é fino e uma sequência curta de reds é catastrófica; sugira sizing mais conservador neles.
- **Correlação entre métodos**: verifique se vários métodos disparam no mesmo jogo (ex.: lay 0x0, 0x1, 0x3 na mesma partida) — isso multiplica a exposição real e o RoR do portfólio.
- **Coerência do edge**: para cada método, confira se o win rate observado supera `p_be` por margem suficiente para cobrir comissão + spread; se não superar, o método é negativo na prática.

## FORMATO FINAL DA ENTREGA
1. **Sumário executivo** (≤10 linhas): métodos confiáveis, suspeitos e o principal risco de cada.
2. Tabela de **ROI ajustado** (original → com comissão → com comissão+spread+slippage).
3. Vereditos dos Blocos A, B e C por método.
4. Tabela de **gestão de banca** (liability/aposta para o RoR-alvo, com sensibilidade) por método + portfólio.
5. **Red flags priorizadas** e o que corrigir primeiro.
6. **O que ainda ficou inconclusivo** e por quê.

Regras finais: não arredonde a ponto de esconder fragilidade; mostre intervalo de confiança quando a amostra de reds for pequena; prefira "não auditável" a chutar.
