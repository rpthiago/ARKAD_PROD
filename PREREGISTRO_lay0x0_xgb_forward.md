# Pré-registro — Lay 0x0 (XGBoost) para teste FORWARD

**Data do congelamento:** 2026-08-02
**Início da medição forward:** 2026-08-03 (jogos a partir desta data, que NÃO entraram em nenhuma análise retro)

Este documento congela uma regra **antes** de vê-la funcionar no futuro. Depois desta data,
a regra NÃO pode ser re-ajustada olhando o forward — qualquer mudança invalida o teste.

---

## A REGRA (congelada)

- **Método:** Lay 0x0 (lay no placar 0-0)
- **Modelo:** XGBoost (n_estimators=250, max_depth=4, lr=0.05, subsample=0.8, colsample=0.8,
  reg_lambda=1.0) + calibração isotônica as-of, retreino mensal walk-forward
- **Features:** builder de produção do 0x0 (`treinar_lay_0x0_rf_v2.build_features`)
- **Base de treino:** Bet365 full (238k jogos)
- **Filtros de entrada (TODOS obrigatórios):**
  - `liga_0x0_rate < 0.08`  (liga não-travada; corta cauda defensiva — CAUSAL, monotônico)
  - `mkt_prob_0x0 < 0.10`   (mantido em 0.10; o 0.09 era ponto ótimo NÃO-monotônico = overfitting,
    descartado após revisão externa Gemini 2026-08-02)
  - `10 <= odd_lay <= 20`   (TETO revisado de 99 -> 20 em 2026-08-03; ver secao "TETO DE ODD" abaixo.
    A cauda odd>20 é risco de cauda que dilui/mata o edge; 10-20 é o corte mais robusto e coincide
    com o universo da página ao vivo)
  - `ev > 0.02`  (EV do lay na odd LAY REAL da Betfair, comissão 5%)
- **Odd usada:** odd de LAY REAL executável na Betfair (não a de back)
- **Sizing:** 0.25 Kelly (fração de liability)

## EVIDÊNCIA RETRO (o que motivou — e por que é otimista)

Janela OOS ago/2025→jul/2026 (walk-forward, retreino mensal, bootstrap 20k por mês, comissão 5%):

| Regra | n | ROI | IC95 | p | FDR |
|---|---|---|---|---|---|
| baseline (liga<.12, mkt<.10) | 2646 | +17,0% | [+5,9%,+28,6%] | 0,0022 | reprova |
| **congelada (liga<.08 & mkt<.10)** | **2421** | **+19,2%** | **[+7,8%,+30,9%]** | **0,0010** | **PASSA (raspando)** |

- **Decadência conhecida:** H1 +25,7% → H2 +10,8%. Expectativa forward ancorada no **H2 (~+11%)**,
  NÃO no ROI cheio. Com slippage realista (1-2 ticks), esperar ~+4 a +7%.
- XGBoost bate RandomForest v2 no crivo (RF: IC inclui zero, p=0,11). Se rodar 0x0, é XGBoost.

## VALIDAÇÃO DE ROBUSTEZ (revisão externa Gemini + testes 2026-08-02)

- **Y-randomization (leak test): PASSOU.** Com alvo de treino embaralhado (3 shuffles), o nº de
  apostas desaba (88–169 vs 2646) e o ROI vira ruído (+34%, −0,7%, −21,7%). Sem leak — pipeline honesto.
- **Sensibilidade de hiperparâmetros: ROBUSTO.** lr∈{0,04;0,05;0,06}, depth∈{3;4} → ROI +19% a +26%,
  todos passam FDR. Sem overfit estrutural. A config base é a mais conservadora.
- **Bootstrap: ROBUSTO.** IC exclui zero nos 3 esquemas (month-block uniforme, ponderado por n, e
  por jogo). O ponderado (fix Gemini) até melhora (p=0,0000).
- **FDR penalizado pelos sweeps: FRONTEIRA.** Com bootstrap uniforme e penalidade pelos 13 sweeps
  (liga 7 + mkt 6), p=0,0010 fica ACIMA do limiar (0,00086) → não passa limpo. Com bootstrap
  ponderado, passaria. Honestamente: na fronteira, não é goleada.
- **Slippage: SENSÍVEL.** Cada tick pior tira ~3pp de ROI. +1 tick: +16% (IC exclui zero, mas perde
  FDR). +3 ticks: morre. Executar com ordem limite; slippage é o maior risco prático.

## TETO DE ODD — revisão 2026-08-03 (10-99 -> 10-20)

Motivo: nos dados AO VIVO de julho, a cauda odd>20 foi desastre (-199%, 11 reds em odds 21-42).
Re-rodei o crivo com sweep de teto (walk-forward, base Betfair CHEIA ate 31/07, regra liga<.08 &
mkt<.10, janela 2025-08+). Confirmado por 2 análises independentes (minha + subagente Fable 5, bit
a bit iguais).

| Teto | n | ROI | IC95 | p | H1->H2 |
|---|---|---|---|---|---|
| 10-16 | 619 | +6,9% | [-13,6%,+24,0%] | 0,25 | +21,5 -> **-14,3** |
| **10-20** | **1333** | **+21,7%** | **[+7,6%,+35,0%]** | **0,0021** | **+32,4 -> +6,8** |
| 10-25 | 1819 | +12,9% | [-6,6%,+28,3%] | 0,086 | +26,3 -> -5,0 |
| 10-30 | 2082 | +15,2% | [-1,6%,+28,4%] | 0,036 | +26,3 -> +1,2 |
| 10-99 | 2503 | +13,2% | [-4,7%,+27,1%] | 0,064 | +25,6 -> -1,8 |

**10-20 vence disparado:** maior ROI, ÚNICO com IC excluindo zero, melhor p, ÚNICO com H2 positivo.
Alargar (25/30/99) dilui e joga o H2 a zero/negativo (a cauda machuca); encurtar (10-16) corta demais
(H2 negativo). Slippage: 10-20 mantém IC>0 até +2 ticks (+16,8%); o 10-99 morre no +1 tick.

**Ressalvas:** ainda reprova o FDR rígido (p=0,0021 > 0,00111); decadência H1->H2 persiste; o
Y-random do 10-20 NÃO veio 100% limpo (ROI positivo em amostra colapsada) — provável artefato do
filtro de EV selecionar odds baixas com taxa-base de green > break-even, não leak de feature, mas
sinaliza que parte do "edge" do 10-20 pode ser seleção de taxa-base, não skill do modelo.
Nota: números aqui usam a base cheia (mais recente/negativa) — diferem do +19,2% do snapshot congelado.

## CRITÉRIO DE SUCESSO FORWARD (pré-registrado 2026-08-02)

> ⚠️ **SUPERADO EM 2026-09-10 — ver a EMENDA no fim do documento.** O texto abaixo fica
> preservado como registro histórico. Ele tinha dois defeitos estatísticos (N sem poder e
> convenção de P&L não declarada) que produziram um `APROVA` falso em 07/09/2026.
> **Os filtros da regra NÃO mudaram** — a emenda mexe só na régua de decisão.

Medir a partir de 2026-08-03, com odd LAY REAL Betfair (coletor VPS) e resultado real:

- **APROVA** se, com amostra >= 300 apostas liquidadas (ou >= 4 meses), o **limite inferior do
  IC95 (bootstrap por mês) for > +2%** (endurecido após revisão externa: N=150 com ~10 reds tem
  variância grande demais — exigir só ROI>0 é frouxo). Não exijo passar o FDR de novo no forward.
- **REPROVA** se ROI <= 0 ou o piso do IC95 <= +2%. Aí a regra morre (ou volta pra bancada, sem
  contaminar este pré-registro).
- **Sanity mensal:** registrar ROI por mês; 2 meses seguidos negativos = alerta de decaimento.

## O QUE NÃO FAZER

- Não re-otimizar os limiares (0.08 / 0.09) olhando o forward.
- Não trocar de modelo/features no meio.
- Não contar apostas retro (< 2026-08-03) no numerador do forward.
- Não caçar novas fatias (odd, liga específica, dia) — já testado, é garimpo.

## PENDÊNCIA OPERACIONAL

Para medir o forward, o log de sinais ao vivo precisa gravar, por aposta:
`liga_0x0_rate`, `mkt_prob_0x0`, odd lay real de entrada, e o resultado. Hoje o
`sinais_lay0x0_gestao_*.xlsx` não grava as duas taxas — augmentar o logger antes de contar o forward.


---

# EMENDA 2026-09-10 — a régua de decisão (os filtros da regra continuam intocados)

**Motivo da emenda:** em 07/09/2026 o `analisar_0x0_xgb.py` imprimiu
`forward N=46 ROI=+34.3% IC95=[+34.3%,+34.3%] p=0.0000 -> APROVA`. Esse APROVA é **inválido**, por
dois defeitos do critério original — nenhum deles tem a ver com o desempenho do método.

**Declaração de conflito:** esta emenda foi escrita **depois** de ver o forward. Por isso: (a) ela
não toca em nenhum filtro de entrada; (b) ela torna a aprovação **mais difícil**, não mais fácil
(N de 300 → 1.476, e o piso deixa de ser comparado contra +2% num número inflado); (c) o gatilho
para reescrever foi um bug de código, não um resultado ruim. Se algum dia a emenda for usada para
justificar um resultado, este parágrafo é a prova de que ela endureceu o teste.

## Defeito 1 — o IC de largura zero (bug de código, já corrigido)

`boot_month()` faz bootstrap **de bloco por mês**. O forward tinha um único mês (ago/2026): com
`nk=1` toda reamostragem devolve o mesmo pool, a variância é zero e o IC95 colapsa no ponto. O piso
virou igual à estimativa e passou mecanicamente pelo `> +2%`; o `p=0.0000` é igualmente vazio.
Com um bloco só, o IC não é largo — é **indefinido**.

**Corrigido** em `analisar_0x0_xgb.py`: `MIN_BLOCOS = 6`. Abaixo disso a função devolve `NaN` e o
relatório imprime `IC INDEFINIDO (nk=N bloco < 6 minimo)` e **se recusa a decidir**. Um intervalo
de largura zero também é recusado explicitamente.

## Defeito 2 — a convenção de P&L não estava declarada

A coluna `pnl` do CSV está em **STAKE=1u** (green `+0,95`, red `−(odd−1)`). Mas o sizing
pré-registrado é *0.25 Kelly sobre liability*. Na odd ~16 as duas escalas diferem ~16×:

| janela | STAKE=1u | LIABILITY=1u |
|---|---|---|
| OOS completo (N=1947) | +17,33% | **+1,17%** |
| OOS ago/2025+ (N=1129) | +21,01% | **+1,29%** |
| forward (N=46) | +34,35% | **+1,19%** |

Todos os ROIs das tabelas de evidência retro deste documento (+19,2%, +21,7%, …) são **STAKE=1u**.
Isso não os torna errados, mas o limiar `+2%` é ambíguo entre as duas réguas — e em liability ele
seria **inatingível por construção**, porque o edge medido é de +1,17%.

**A medida livre de convenção é o WR contra o break-even:**

```
OOS completo      N=1947  WR 95,12%  BE 94,02%  gap +1,10pp
OOS ago/2025+     N=1129  WR 95,31%  BE 94,09%  gap +1,21pp
  H1              N= 565  WR 95,58%  BE 94,01%  gap +1,57pp
  H2              N= 564  WR 95,04%  BE 94,18%  gap +0,86pp   (IC liab cruza zero, p=0,20)
forward           N=  46  WR 95,65%  BE 94,47%  gap +1,18pp
```

**O edge do método é ~1,1 ponto percentual de WR.** É real no backtest (liability
IC95 `[+0,30%,+1,98%]`, p=0,0055, 30 blocos mensais) — mas é fino, e todo número de "ROI 20-34%"
é o mesmo 1,1pp visto pela lente do stake.

## Defeito 3 — o N nunca teve poder para decidir

Por aposta, em liability: média `+0,0117 u`, desvio `0,2294 u` → **ruído/sinal = 20×**
(a razão é idêntica em stake: é invariante de escala). Daí o N necessário:

| N | IC95 do ROI (liability) | decide? |
|---|---|---|
| 150 (a cláusula "≥ 4 meses") | [−2,50%, +4,84%] | não |
| **300 (o critério original)** | **[−1,43%, +3,77%]** | **não** |
| 1.000 | [−0,25%, +2,59%] | não |
| **1.476** | **[+0,00%, +2,34%]** | **sim (mínimo)** |
| 3.012 | [+0,35%, +1,99%] | sim, com 80% de poder |

O `N >= 300` estava subdimensionado ~5×, e a cláusula alternativa `>= 4 meses` equivale a ~150
apostas no ritmo real (1,24/dia) — ou seja, a cláusula **mais frouxa** era a que disparava primeiro.

## CRITÉRIO NOVO (vigente a partir de 2026-09-10)

Convenção declarada: **LIABILITY = 1 unidade, comissão 5%**. Razão: o capital em risco de um lay é
a liability, é a régua do sizing (0.25 Kelly de liability) e é a convenção única em que a base
mestre foi reunificada em 10/09.

Decisão de **três vias** — a ausência de aprovação não é reprovação:

- **APROVA** se `N >= 1476` **E** `blocos >= 6` **E** piso do IC95 (bootstrap de bloco por mês) `> 0`.
- **REPROVA** se o **teto** do IC95 `< 0` — isto é, perda demonstrada, não ausência de ganho.
  Vale em qualquer N: se o teto está abaixo de zero, o método é ruim com evidência.
- **INCONCLUSIVO** em todo o resto (inclui: N insuficiente, blocos < 6, IC cruzando zero).
  Continua coletando com stake mínimo. **Inconclusivo não é permissão para escalar.**

Removida a cláusula `ou >= 4 meses`. Removido o limiar absoluto `+2%` (era função da convenção).

**Salvaguarda operacional** (não é critério de aprovação, é gestão de risco): se o drawdown
acumulado em liability passar de `-15 u`, pausar e reauditar antes de continuar. E o alerta mensal
do original continua: 2 meses seguidos negativos = revisar, sem decidir.

**Prazo realista:** a 1,24 aposta/dia, `N=1476` chega em **~39 meses**; `N=3012` em ~80 meses. Esse
é o custo de validar um edge de 1,1pp em odd ~16. Duas saídas legítimas para encurtar — ambas
precisam de pré-registro **novo**, não desta emenda: (a) aumentar o fluxo (mais ligas dentro do
mesmo mecanismo causal); (b) medir o mesmo evento num mercado de odd menor, onde a variância por
aposta é muito menor (o Over 0.5 é o candidato natural — ver `lay0x0-vs-over05-mesmo-evento`).

## PENDÊNCIA DE DADOS — RESOLVIDA em 2026-09-10

**O que estava errado (pior do que "atraso de base"):** o forward era contado pela
**re-simulação** (`gerar_oos_real_0x0_xgb.py`), não pelos picks enviados. Três defeitos:

1. **Janelas de treino diferentes.** O gerador ao vivo treina com `Date < dia` (até ontem); a
   simulação treina com `_month < mes` (até o fim do mês anterior). Para um jogo de 15/08 o modelo
   ao vivo tem 2 semanas extras de dados → `p` diferente → **conjunto de picks diferente**.
   Medido: dos **5** picks enviados em 15/08, a simulação contém **3**. Faltam Reading x Luton
   (`p` ao vivo 0,949) e Blackpool x Wycombe (`ev` ao vivo 0,039, no fio) — e **ambos eram GREEN**.
2. **A simulação é regenerada toda semana.** Um forward pré-registrado tem que ser **imutável**:
   o que foi apostado não muda depois. Regenerar é a porta aberta para o resultado mudar sozinho.
3. **Os picks ao vivo nunca eram liquidados.** Os `picks_0x0_<data>.csv` não tinham nenhuma coluna
   de resultado. Ninguém escrevia o placar de volta.

**Consequência:** o `N=46` do relatório de 07/09 **nunca foi ao vivo**. O registro ao vivo real é
de **6 picks** (5 em 15/08 + 1 em 09/09) — o gerador diário só entrou no ar em 08/09.

**O conserto (`forward_0x0/liquidar_picks_0x0.py`):** os `picks_0x0_<data>.csv` passam a ser a
única fonte de verdade do que foi enviado, e alimentam `ledger_forward_0x0.csv`, append-only:
pick novo entra como PENDENTE com a odd **exatamente como foi enviada** (Lei nº 1 — nunca
re-buscar depois); quando o placar aparece, liquida **uma vez** e a linha fica intocável.
Placar: base b365 (`Goals_*_FT`) com prioridade, `placares_manuais.xlsx` só onde a b365 ainda não
chegou (publica com ~5 dias de atraso), e a coluna `fonte_placar` registra qual foi. Idempotente.

Rodando: diário no `gerar_picks_dia.bat` (incorpora e liquida) e semanal no `rodar_forward_0x0.bat`
(antes do analisador). O `analisar_0x0_xgb.py` passou a ler o **ledger** na seção do forward, e o
recorte recente da simulação está rotulado como *"NÃO é o forward, é referência"*.
O `registrar_hist.py` também passou a ler o ledger, e grava o ROI nas duas convenções com o nome
explícito na coluna (`fwd_ROI_liab` / `fwd_ROI_stake`).

**Estado do forward ao vivo em 10/09/2026:** N=6, 5G/1R, WR 83,33% vs break-even 94,19%
(gap −10,86pp), ROI liability −11,51%. **Sem significado estatístico em N=6** — o critério devolve
INCONCLUSIVO por blocos insuficientes (2 de 6), e é assim que deve ser.

**O que ainda falta:** o `sinais_lay0x0_gestao_*.xlsx` (log ao vivo antigo, em
`ARKAD_PROD/paper_traning_lay0x0/`) não grava `liga_0x0_rate` nem `mkt_prob_0x0` — não é usado pelo
ledger e portanto não bloqueia mais o forward, mas continua inutilizável para reconstruir o
histórico anterior a 15/08.
