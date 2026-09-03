# ARKAD — Resumo da sessão 02/09/2026 (para o Gemini)

Contexto: seguindo o protocolo honesto (odd LAY real Betfair, walk-forward OOS, bootstrap+FDR).
Base de trabalho: `Bases_de_Dados_API_FutPythonTrader_Betfair_FRESH*.csv` (odd lay + Goals_FT reais).

## 1. Scan exaustivo de filtros (600 testes)
Testei ~100 features por método (TODAS as colunas `_Back` de todos os mercados: 1X2, O/U, BTTS,
CS, DC, HT + 6 taxas de liga + favorito), split treino Jan-Abr / teste Mai-Ago, holder =
ROI>1,5% NAS DUAS metades E N≥40. Acaso esperado ≈ 2-3 holders/método.
- **Lay Home: 18/100 holders** (muito acima do acaso) — parecia edge real; tema = favorito
  visitante moderado (Odd_A_Back∈[1,54;1,65]) + jogos de baixa expectativa de gol.
- **Lay Over 4.5: base +2,2%/+4,6%** confirma; 2-3 filtros marginais.
- **Lay Draw: 4/100** (~acaso), alguns mecanicamente plausíveis.
- **Lay Away 1/100, Lay 0x1 0/100, Lay Under 0.5 0/100 = MORTOS.**

## 2. Forward real do gap 21/08→02/09 (odd lay real + placar real)
A base histórica da API para em 20/08 (atraso ~2 semanas), então capturei os sinais dos 3
métodos vivos pelo feed diário Betfair (odd lay real) e o placar veio conferido MANUALMENTE
pelo usuário (jogo a jogo). É out-of-sample puro (o backtest nunca viu esses jogos).

| Método | N | WR | ROI/liab |
|---|---|---|---|
| Lay Draw base | 113 | 90,3% | **+5,2%** |
| Lay Draw filtrado | 46 | 87,0% | +4,0% |
| Lay Home base | 63 | 93,7% | **+7,7%** |
| Lay Home filtrado | 32 | 90,6% | +6,4% |
| Lay Over 4.5 | 11 | 100% | +5,6% |

## 3. Bootstrap por bloco-DIA (13 blocos, 20k) + IC95
(Bootstrap por SEMANA seria inútil: só 3 semanas ISO. Usei bloco-dia.)

| Método | ROI | IC95 | P(ROI≤0) | H1 / H2 |
|---|---|---|---|---|
| Draw base | +5,2% | [−1,8%; +11,8%] | 0,069 | +7,9 / +1,8 |
| Home base | +7,7% | [−0,7%; +13,6%] | 0,033 | +2,1 / +15,5 |
| Over 4.5 | +5,6% | [+5,3%; +6,0%] | 0,000 | +5,4 / +5,9 |
| Draw filtrado | +4,0% | [−9,3%; +14,8%] | 0,237 | +9,7 / −2,2 |
| Home filtrado | +6,4% | [−7,6%; +17,1%] | 0,171 | −4,6 / +17,4 |

## 4. Conclusões honestas
1. **Nenhum método cruzou o portão** (IC95 excluir zero + FDR + estabilidade). Provisório.
2. **Draw base** é o mais sólido: positivo nas 2 metades, p=0,069 com só 13 dias.
3. **Home base** tem ROI maior (+7,7%) mas instável (quase todo o lucro na 2ª metade).
4. **⚠️ Over 4.5 IC [+5,3;+6,0] é ARTEFATO**: N=11, zero red → variância zero → IC falso-apertado.
   NÃO é significância. Over 4.5 continua inconclusivo.
5. **⚠️ O filtro refinado NÃO se sustentou fora da amostra**: Draw e Home FILTRADOS renderam
   MENOS que o base e trocam de sinal entre H1/H2. Ou seja, **os 18 holders do Lay Home foram
   em boa parte sorte de amostra (garden-of-forking-paths)**. Ficamos com os métodos BASE.
6. **Por que o pipeline diário tinha poucos Home/Over antes de 31/08**: os métodos Lay Home,
   Lay Away e Lay Over 4.5 só foram integrados à automação em **31/08** (commits 2fc0e40 /
   cf631df / c73e55b). Antes disso o gerador só varria Draw/Under 0.5/0x1. Não era falta de
   jogo — o pipeline nem calculava esses mercados.

## 5. Regras CONGELADAS do forward (pré-registradas, início 02/09)
- **Lay Draw**: `min(Odd_H_Back,Odd_A_Back)≤1,40` E `Odd_D_Lay∈[4,5;10]`. Green = não-empate.
  (Filtro refinado testado e reprovado no forward: `Odd_Over35_FT_Back≥2,54`.)
- **Lay Home**: `Odd_A_Back≤1,65` E `Odd_H_Lay∈[2;10]`. Green = visitante não perde (GA≥GH).
  (Filtro refinado testado e reprovado no forward: `Odd_A_Back∈[1,54;1,65]`.)
- **Lay Over 4.5**: `Odd_Under25_FT_Back≤1,50` E `Odd_Over45_FT_Lay∈[4;20]`. Green = total≤4.
- Unidade P&L LAY (comissão 5%): green +0,95 / red −(odd−1); ROI = Σpnl/Σ(odd−1).

## 6. Infra montada hoje (OCULTA — não toca no Portfólio Oficial)
- Pasta `forward_oculto/` que NENHUMA página do Streamlit importa (não aparece no dashboard).
- `forward_capturar.py` (captura diária via feed Betfair), `forward_liquidar.py` (liquida pelo
  placar final quando a base preenche — autoritativo p/ Draw/Home/Over, sem bug de CS).
- Tarefa agendada `ARKAD_Forward_Oculto` roda todo dia 10:30.
- Log-mestre `forward_oculto_log.csv` (189 sinais 21/08-02/09, 2 pendentes).

## 7. Under-limite in-play (trilha SEPARADA — status atual)
Método ao vivo (lay Under na linha logo acima do placar, min ~75-85). É outro edge (viés
recreativo AO VIVO), não confundir com os 3 pré-jogo acima.
1. **🚨 Os +18-26% ROI que apareciam eram BUG, não edge.** A liquidação pelo coletor
   (reconstrução de CS por menor-lay, amostragem grossa) **perde o gol tardio**: a Betfair
   fecha o mercado ~min 95-100 e o gol do min 88-95 escapa entre capturas → marca **RED como
   GREEN**. Sempre infla, nunca desinfla (~+25pp fantasma). Auditoria manual: **27% false-green
   no est3, 13% no est2.** Re-liquidando na base real, o under caiu de +7% (coletor) → **−18%**.
2. **✅ Solução DEFINITIVA — feed oficial da Betfair.** `list_market_book(market_ids=[...])`
   responde o **status oficial do runner (WINNER/LOSER) MESMO com o mercado CLOSED**. Autoritativo,
   cobre TODAS as ligas, ZERO false green, sem reconstruir placar (verificado: Atlético 3 gols →
   Under 2.5 LOSER; Defence Force 2 gols → Under 2.5 WINNER). Implementado em `settle_betfair.py`
   (cron a cada 2h): o alerta loga `market_id`+`selection_id`, o feed liquida oficial. O
   `compilar_under_dia.py` PAROU de liquidar por CS — só reporta o log já liquidado pela Betfair.
3. **Edge real = break-even/negativo, NÃO +26%. Não estamos apostando real.**
4. **Alerta ao vivo 24/7** na VPS Oracle SP → Telegram (zero clique, com link direto do mercado),
   gatilho min 75-85, score-aware (dispara a menor linha Under ainda viva).
5. **Pré-registro congelado:** método = est2/est3; est0/est1 = só observação; barra N≥400 ou 5
   fins de semana; bootstrap+FDR; stake-zero até confirmar.
6. Limite de 23h/dia de Jogo Responsável da conta resolvido via cron (roda 20,5h/dia, dentro
   dos 3 tetos). NUNCA burlar — rodar DENTRO do limite.

## 8. Próximos passos / perguntas ao Gemini
- Re-rodar bootstrap quando fechar **~5 fins de semana** (N Draw ~250-300) → aí semana vira bloco.
- Em ~2 semanas a API preenche 21/08→02/09 → **dupla-checar o placar manual contra o oficial**.
- Gemini: concorda em descartar o filtro refinado do Home como sorte de amostra? Algum viés na
  captura pelo feed diário (odd lay do feed vs odd executável real) que devamos auditar?
