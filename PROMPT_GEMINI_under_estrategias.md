# PROMPT PARA O GEMINI — Estratégias para o "Under-no-limite" (método JÁ RODANDO ao vivo)

> Cole tudo abaixo da linha no Gemini.

---

Você é um **Engenheiro Quantitativo Sênior** especialista em **microestrutura da Betfair Exchange**, modelagem in-play de futebol (Poisson/Dixon-Coles com hazard de gol dependente do tempo) e **auditoria forense de estratégias de aposta**.

Abaixo estão a especificação de um método que já roda ao vivo, os dados reais que ele produziu e as regras de validação do projeto. **Não há hipótese pré-formada da minha parte, e não quero que você confirme nada:** analise os dados e chegue às suas próprias conclusões.

## 1. O método (está NO AR, 24/7, com stake ZERO)

**"Under-no-limite v2" — back Under in-play no fim do jogo.**

- **Gatilho:** jogo ao vivo entre o **minuto 75 e 85**, placar com **2 gols** (entra Back **Under 2.5**) ou **3 gols** (entra Back **Under 3.5**) — sempre a linha **imediatamente acima do placar atual**.
- **Filtro de preço:** odd back do Under entre **1,40 e 2,20**. Liquidez ≥ R$ 300.
- **Gestão:** **hold até o fim** (sem cash-out, sem trade). Comissão 4,5%.
- **Racional original (não validado):** o método foi construído sobre a suposição de um viés comportamental in-play — que o público recreativo teme o gol tardio e por isso venderia o Under barato demais nos minutos finais. Trate isso como uma alegação a ser examinada, não como premissa.

**Infra (tudo já existe e roda sozinho):**
- Coletor Betfair 24/7 em VPS (São Paulo), com amostragem fina (45s) na janela min 80-105.
- Alerta ao vivo → Telegram, dispara o gatilho com link direto do mercado.
- **Liquidação OFICIAL da Betfair:** cada sinal grava `market_id` + `selection_id`, e um job consulta o `list_market_book` (funciona mesmo com o mercado CLOSED) para pegar o status oficial do runner: **WINNER/LOSER**.
- Log: `under_alertas_log.csv`, colunas: `data, ko, jogo, home, away, minuto, linha, estado, odd, liq, primeiro_visto, status, resultado, ft_total (BF:WINNER|BF:LOSER), pnl, market_id, selection_id`.

## 2. Os dados (cohort liquidado oficialmente pela Betfair, dado limpo desde 02/09/2026)

**N = 403 · WR = 51,6% · ROI = −4,64% por aposta · PnL = −18,71 unidades**

| Corte | N | WR | ROI |
|---|---|---|---|
| **estado 2** (Back Under 2.5) | 206 | 55,8% | **+1,85%** |
| **estado 3** (Back Under 3.5) | 165 | 47,9% | **−9,96%** |
| estado 0 (Under 0.5 no 0-0) | 13 | 38,5% | −27,2% |
| estado 1 (Under 1.5) | 19 | 47,4% | −13,5% |

- **Por faixa de odd:** 1,40-1,59 → −30,7% (N=28) · 1,60-1,79 → +0,5% (N=94) · 1,80-1,99 → −5,6% (N=127) · 2,00-2,20 → −2,3% (N=154).
- **Por minuto de entrada:** 75-79 → −23,1% (N=8) · 80-84 → −5,4% (N=351) · 85-89 → +4,4% (N=44).
- **Por dia:** 03/09 +2,1% (N=57) · 04/09 +18,0% (N=55) · 05/09 −11,3% (N=133) · 06/09 −10,4% (N=114) · 07/09 −0,1% (N=36).

Os **estados 0 e 1 foram desligados** do alerta (não geram mais sinal). O método em operação é **est2/est3**.

⚠️ **Um número deste projeto que NÃO deve ser usado:** um tracker paralelo, que liquidava reconstruindo o placar pelo Correct Score do coletor, reporta **+17,6% (N=1.015)**. Esse número é inválido por erro de medição: a amostragem grossa perde o gol do min 88-95 (a Betfair fecha o mercado antes da próxima captura), gerando *false green* sistemático que sempre infla e nunca desinfla. **Use apenas o cohort oficial (−4,64%).**

## 3. Regras de validação do projeto (restrições, não conclusões)

1. **Protocolo honesto obrigatório:** odd real da exchange (nunca odd de bookmaker), **out-of-sample forward**, **bootstrap IC95**, **correção FDR** contra o número de hipóteses já testadas, **break-even WR** explícito. Um método só é aprovado se o **IC95 excluir zero** e for positivo.
2. **CLV (Closing Line Value)** é a métrica de referência do projeto para decidir se algo é edge.
3. **Amostra pequena:** N=403 fatiado por estado × odd × minuto × liga tem alto risco de falso positivo. Qualquer recorte que você proponha precisa vir acompanhado do mecanismo que o justifica e do teste que o falsearia.
4. **Já testado e reprovado neste ecossistema — não re-propor:** constantes de odds (3.722 combos, 0 sobrevivem OOS); K_edge / índices cross-market; dutching de Correct Score (−10,2%); overshoot in-play pós-gol; cash-out / saída antecipada "a favor do tempo" com +5% (−10%); lay draw no min 60 (−4,9%); pressão cruzada; filtros refinados por scan (flipam no forward). Saída antes do fim já foi testada neste método e piorou o resultado.
5. **Alavancagem/ciclo/martingale** só se aplica sobre método já aprovado pelo item 1.

## 4. O que eu quero de você

**(A) Diagnóstico.** O que os números acima permitem afirmar sobre a existência ou não de edge? Calcule o **break-even WR** por faixa de odd, decomponha o gap entre o WR observado e o WR necessário (comissão, spread/slippage do back in-play, qualquer resto), e diga o que sobra depois dos custos. Avalie, com a evidência empírica que você conhece sobre distribuição de gols por minuto (incluindo acréscimos), se a odd praticada na janela 75-90 está bem ou mal calibrada.

**(B) Diferenças internas.** Explique o que, na sua leitura, gera a diferença entre est2 e est3, e o padrão observado por faixa de odd e por minuto de entrada. Diga em cada caso se você considera o padrão mecanismo ou ruído amostral, e mostre a conta que sustenta essa distinção.

**(C) Hipóteses.** Proponha as hipóteses que **você** considera as mais promissoras a partir desta análise — cada uma com o mecanismo econômico/comportamental declarado antes do número, e com os dados necessários para testá-la. Se a sua conclusão for que não há hipótese promissora, diga isso em vez de preencher a lista.

**(D) Pré-registro.** Para a hipótese que você julgar mais forte, escreva um pré-registro pronto para congelar: regra exata (gatilho, filtros, linha, gestão), **N mínimo**, critério de parada, teste estatístico (bootstrap + FDR contra quantas hipóteses) e **qual resultado mata a hipótese**. Precisa ser executável com o log de 17 colunas acima + o coletor Betfair.

**(E) Veredito.** Diga o que fazer com o método: continuar acumulando como está, restringir a um subconjunto (qual, e por quê), ou encerrar. Escolha uma e justifique com os números.

**Formato:** direto, com contas explícitas. Sem hedge motivacional; nenhuma afirmação de resultado sem intervalo de confiança. Se faltar dado para responder algo, diga exatamente qual coluna ou campo eu preciso passar a coletar.
