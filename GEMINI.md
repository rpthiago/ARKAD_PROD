# ARKAD — Regras de Engenharia e Validação de Métodos
> **LEIA ISTO ANTES DE:** montar método · auditar método · rodar no Streamlit · fazer backtest.
> Este documento é a fonte única de verdade. Toda regra aqui nasceu de um erro real que já
> custou tempo e dinheiro. Se contrariar seu instinto, o instinto está errado — siga o doc.

---

## 0. AS 5 LEIS INEGOCIÁVEIS

1. **A odd é a EXECUTÁVEL na Betfair (lay real), NUNCA a de back / b365.**
   Para LAY, casar o jogo com a coluna `Odd_*_Lay` real. A odd de back é sempre otimista
   (gap em Correct Score = +40% a +70%). Backtest, EV, filtro de entrada e paper trail —
   **TODOS na odd de lay real**. Validar um lay na odd de back é a "miragem" que matou todos
   os edges de CS.

2. **Backtest e live TÊM que ser o MESMO objeto.**
   Mesma base, mesmas colunas, mesma fórmula de feature, mesma odd, mesmo tratamento de NaN.
   Qualquer divergência invalida o backtest — o número que você reporta deixa de descrever o
   que o robô faz ao vivo.

3. **NUNCA fabricar valor de feature.**
   Sem dado suficiente → **SKIP** (igual ao `dropna` do treino). Proibido: `0.0`, `0.35`,
   `0.26`, `draw_rate_mean`, "média neutra", qualquer default. Valor inventado gera **sinal
   falso** e infla probabilidade artificialmente (foi assim que apareceram "95%-100%").

4. **Matemática do LAY (comissão 5%):**
   - EV: `ev = p*(1 - 0.05) - (1 - p)*(odd - 1)` — onde `p = P(o lay GANHAR)`. **Nunca** `p*odd-1`.
   - P&L por stake: GREEN `+0.95` · RED `-(odd - 1)`.
   - **Break-even WR** `= (odd - 1) / (odd - 0.05)`. É contra ISSO que se julga a WR, não contra 50%.
     (Ex.: lay 0-3 na odd 30 → break-even 96,7%. Ganhar 77,5% = perder muito.)

5. **Sinal ≠ edge.** ROI bonito — e principalmente ROI de **walk-forward** — não é edge enquanto
   não confirmar no **paper trading forward** (seção 2). Um mês bom é variância. Um placar raro
   que "sempre deu green" é a amostra pequena ainda não ter pego o red.

---

## 1. HALL OF SHAME — bugs reais, NÃO repita

| Erro | O certo |
|---|---|
| Validar lay na odd de **back** (~13) em vez da real (~17) | Odd de lay real (`Odd_*_Lay`) em tudo |
| Confiar no **walk-forward** em base estática (0x0: +11% no walk-forward, **negativo** no real) | **Paper trading forward** na odd real é a autoridade; backtest só descarta |
| **p-value** de WR vs 50% ("EV+ significante") | Comparar WR com **break-even WR** + **bootstrap por mês** |
| "31/31 = 100% = alpha" | Placar raro: 100% é o esperado até o 1º red. Sem odd+break-even não diz nada |
| Live carregando base **sem features ricas** (`Resultados_2026_Full.csv`) → 12 features viram `0.0` | Carregar a base do trainer (`Bases_de_Dados_API_FutPythonTrader_Bet365.csv`) e **validar colunas** |
| Cutoff **hardcoded** (`Date < "2026-08-01"`) | Cutoff **dinâmico**: `Date < data_do_1o_jogo_do_dia` |
| Feature não computada no live (ex.: `h2h_draw_rate` sempre NaN→0.0) | **Computar** igual ao trainer, ou re-treinar sem ela |
| Trocar a **semântica** da feature (juntar casa+fora num rolling só) | `H_h_*` = forma **só em casa**; `A_a_*` = **só fora**. Dois views separados (`Home`/`Away`) |
| Fallback fabricado (h2h→média, liga→0.26, time novo→0.35/0.25/0.28) | Sem dado → **SKIP** |
| `fillna(0.0)` no live vs `dropna` no treino | **SKIP** se qualquer feature do modelo for NaN |
| Staleness: `shift(1)` + `.last()` exclui o último jogo | `_decay_roll_grouped_unshifted` (inclui o último) casa com o `shift(1)` do trainer |
| Filtro permissivo: liga/rate NaN **passa** | NaN → `SKIP` (`LIGA_FORA_UNIVERSO`) |
| Capturar coluna **HT** achando que é FT | Sempre `Odd_*_FT_*`; no resolver, `'ht' not in col.lower()` |
| Consertar a estratégia **gerada**, mas o `_gen_strategy` do trainer reverte no próximo treino | Colocar o fix **no template do `_gen_strategy`** (no `treinar_*.py`) |
| "100% corrigido" / "reflete 74,89%" sem prova | **Medir**. Nada de overselling |
| **Stake-zero de mentira** (`stake: 100.0` em log de observação simulando financeiro real) | Observação usa **`stake: 0.0`** e flag explícita `OBSERVACAO_STAKE_ZERO`. P&L é puramente de papel |
| **Re-injetar métodos zumbis** (manter Lay 0x3, Lay 2x2, Lay Draw mortos no gerador de sinais) | **Expurgar 100%** dos métodos reprovados de todo gerador de sinais. Só roda o que está aprovado |
| **Backtest fantasiado de Forward** (ler base com gols FT preenchidos e liquidar no mesmo loop) | **Desacoplamento estrito:** (1) Pré-jogo gera `PENDENTE` sem ler gols; (2) Settlement pós-jogo separado |
| **Miragem de Correct Score (CS)** (Lay 1x1: Jan–Abr parecia verde, mas colapsou em Mai–Jul para ROI 0,1%) | **Expurgar classe CS inteira:** cauda gorda destrói o edge quando reds agrupam. Auditar meses recentes |
| **Modelos Lineares sem Scaler** (LR crua com features 0–50 vs 0–1 gerando "edge" falso) | **`StandardScaler` obrigatório** em LR/SVM. Se o edge evapora com normalização, era ruído de condicionamento |
| **Miragem de Denominador em Lay** (Reportar ROI de Lay sobre 1u nominal escondendo liability 3u) | **Reportar ROI sobre Capital em Risco** ($\text{liability} = \text{odd}-1$) e yield juntos |
| **Esticar Janela OOS Ad-Hoc** (Adicionar jogos além da base congelada para inflar número) | **Respeitar o Cutoff Pré-Registrado** da base congelada. Novos dados = Paper Forward |
| **Odd de Saída / Cashout Fabricada** (Assumir +0,19u de lucro no cashout 0x0 HT em Lay 0x1) | **Medir SEMPRE no coletor:** 0x0 HT no Lay 0x1 dá **−0,499u** de perda real. Nunca inventar P&L de saída |
| **Spread Arbitrário de Lay CS** (Chutar spread ×1,5 ou ×1,12 sem medir) | **Medir a mediana real:** spread mediano no coletor Betfair é **1,19x** (p75 = 1,43) |
| **Bug do Gol Tardio no Coletor** (Coletor perder gol nos acréscimos e gravar 0-1 em jogo que foi 0-2/0-3) | **Auditoria Web Obrigatória:** Todo RED registrado deve ser validado no pós-jogo (`min_to_ko <= -100`) antes de contar |
| **Liquidação por coletor grosso = FALSE GREEN sistemático em métodos UNDER** (amostragem 5 min perde o gol do min 88-95 antes de a Betfair fechar o mercado → marca RED como GREEN; SEMPRE infla, nunca desinfla). Inflou o under-limite de −18% (real) p/ +26% (coletor); auditoria manual achou 9 false greens em 42/dia | **Nunca liquidar UNDER só pelo coletor grosso.** Amostrar FINO no FT (`passar_ft`, 45s, min 80-105) p/ pegar o gol antes do fechamento + liquidar pela ÚLTIMA janela de captura (não min-lay global). Base externa não resolve (cobre ~40% dos jogos; ligas pequenas de fora). Todo ROI de método in-play vindo de coletor é suspeito até o placar final estar capturado. **ESCOPO (re-liquidado 01/09): o bug é ESPECÍFICO de métodos UNDER (dependem do TOTAL de gols). Métodos de CS-EXATO (Lay 0x1/1x0) foram re-liquidados na base e deram 0 divergências vs coletor → NÃO afetados. Só o under-limite estava corrompido; o resto do portfólio (base-settled) segue sólido.** ✅ **SOLUÇÃO DEFINITIVA (02/09): status oficial da Betfair.** Amostragem fina NÃO bastou (auditoria achou 27% false green no est3). A Betfair responde `list_market_book` por **market_id direto mesmo com o mercado CLOSED**, devolvendo o **status do runner: WINNER/LOSER** (autoritativo, cobre todas as ligas, zero false green, sem reconstruir placar). Logar market_id+selection_id do sinal e liquidar pelo status — nunca por reconstrução de placar do coletor |
| **Combinar Métodos Prematuramente** (Juntar Lay 1x0 com Lay 0x1 antes do 1x0 provar edge real) | **Tratamento e Portfólio Separados:** 1x0 é o elo fraco (−8% forward) e deve rodar isolado |
| **Odd Estimada / Inventada em Handicap** (Assumir odd 2,45 em AH +1.5 Zebra gerando "+82% ROI" falso) | **NUNCA estimar odd.** Usar coluna real (`AH_*`, `EH_*`, `Odd_*`). AH +1.5 Zebra mandante na odd real 1,30 dá **−4,2%** de prejuízo |
| **Risco de Cauda Excessivo em Lay de Odd Alta** (Lay 2x2 com liability 16,8 e margem navalha +1,14%) | **Evitar Lay de Odd Alta com Margem Fina.** 1 red apaga 17 greens, tornando o risco de ruína inaceitável |
| **Vig-Cegueira em BTTS e HT** (BTTS com 8,3% e HT com 7,8% de overround da casa) | **NUNCA apostar em BTTS ou HT genérico.** A vig alta e spreads largos consom qualquer micro-edge |
| **Miragem do O/U Pré-Jogo** (Assumir edge em Lay Under 2.5 / 3.5 em jogos over) | **O/U Pré-Jogo é hiper-eficiente.** O edge real de Over/Under vive estritamente no **In-Play (Minuto 75–85)** |
| **Micro-Edges Travados por Liquidez** (0x2, 2x0, 0x3 Zebra com N<150 no ano e odd alta) | **Expurgar do operacional ativo.** Micro-edges com N minúsculo e CS raso não comportam volume real |
| **Filtrar 0-0/Under0.5 só por `liga_rate`** (achar que "evitar liga defensiva" é o edge) | **O FAVORITISMO é o filtro dominante.** Com favorito forte, `liga_rate` vira redundante (medido: fav<1,50 dá +1,72% ignorando a liga). Favoritismo não anula liga só quando NÃO há favorito claro |
| **Assumir simetria casa/fora sem medir o EVENTO** (aplicar mesmo threshold aos dois lados) | **Medir cada lado. 0-0 é ASSIMÉTRICO** (mando ajuda o favorito de casa a marcar → visitante exige odd mais estrita: casa ≤1,50 / fora ≤1,40). **Empate é SIMÉTRICO** (venue-neutral → mesmo ≤1,40 nos dois). O tipo do evento decide |
| **Tratar API FutPythonTrader e coletor como fontes equivalentes** (mesma odd, mesma cobertura) | **Diferem.** Odd difere (snapshot da API vs perto-do-KO do coletor → FLIPAM sinais no limite ~1,40; ex.: Dinamo Zagreb 1,35 coletor vs 1,45 API) e cobertura difere (coletor pega ligas menores). **Coletor perto-do-KO é canônico; API é fallback conservador** |
| **Miragem de Spread Fixo Bet365 vs Base Betfair Real** (Assumir spread fixo 1,03x/1,05x sobre Bet365 gerando "+2% a +4% ROI" falso) | **Na base Betfair FRESH com Lay real, os spreads reais são maiores** (1X2 = 1,065x-1,091x; Under 0.5 = 1,128x; Over 4.5 = 1,194x). O edge cai pra break-even/negativo e TODOS os IC95% cruzam o zero. **Nenhum método pré-jogo está confirmado por backtest.** Só o forward ao vivo decide |
| **Overfitting por Mineração de Features (Garden of Forking Paths)** (Scan de 100 features achou 18 "holders" no Lay Home com odd [1.54, 1.65]) | **Rejeitar filtros refinados.** No forward real do gap 21/08-02/09, o filtro refinado rendeu MENOS que o base e inverteu sinal entre metades (−4,6% H1 vs +17,4% H2). **Manter estritamente as regras BASE amplas** |

---

## 2. VALIDAÇÃO HONESTA = PAPER TRADING FORWARD (NÃO walk-forward)

> **Por que NÃO walk-forward em base estática:** ele ENGANA. O Lay 0x0 deu **+11% no
> walk-forward e NEGATIVO no real** — porque a base histórica tem odd de **back** (não
> executável) e o build de features vaza via `.last()`. Vários métodos "aprovados" no
> walk-forward deram diferente no paper. **A palavra final é SEMPRE o paper trading.**

**Padrão de aprovação — PAPER TRADING FORWARD:** sinais gerados AO VIVO, odd de **lay real**
capturada na hora (coletor/API Betfair), resultado validado pelo **placar real**. É a execução
de verdade — não tem miragem de odd nem leak de base.

**Rigor que CONTINUA obrigatório (paper com N pequeno também engana):**
1. **PRÉ-REGISTRO:** congele a regra ANTES de acumular. Ajustar filtro no dado já visto = se
   enganar (foi o erro do 0x0).
2. **N mínimo:** ≥ **800 apostas** OU ≥ **5 fins de semana**. **Um mês/semana bom é variância**
   (o Under 0-0 deu +32% numa semana e −6% na seguinte).
3. **Odd = a executável** (o paper já garante isso por construção).
4. **Bootstrap por semana/jogo** → IC95 do ROI; **FDR** contra tudo já testado.
5. **Break-even WR** (Lei 4) — nunca "WR vs 50%".
6. **Estável no tempo E entre estados/placares** (não pode depender de 1 semana ou 1 placar).
7. **Mecanismo plausível** (por que o mercado erraria ali?).
8. Aprova só se TUDO acima. Passa quase tudo → **watchlist stake-zero** (segue observando).
   Falha → registra **morto** e não re-testa.
9. **Ciclo de Vida Forward Desacoplado:** O gerador diário SÓ gera sinais com status `PENDENTE` antes da bola rolar sem inspecionar gols FT. A liquidação ocorre em rotina separada pós-jogo. Proibido rodar loop em dados já finalizados chamando de "forward".
10. **Stake-Zero Genuíno:** Métodos em observação gravam `stake: 0.0` e flag `tipo_registro: 'OBSERVACAO_STAKE_ZERO'`. Jamais gravar `stake: 100.0` em watchlist.
11. **ROI de LAY sobre Capital em Risco:** Métricas de Lay devem sempre explicitar o ROI sobre a *liability real* ($\text{odd}-1$) junto ao yield por aposta, para que o risco de ruína e a assimetria fiquem transparentes.
12. **Janela OOS Estritamente Congelada:** Nunca estender a janela de Out-of-Sample além do cutoff pré-registrado da base. Dúvidas sobre dados posteriores são resolvidas exclusivamente no Paper Forward ao vivo.
13. **Robustez à Especificação & Thresholds Vizinhos:** Testar com/sem scaler, thresholds vizinhos (ex.: EV 3% vs 5%) e seeds. Se o edge evapora ou inverte o sinal em threshold vizinho, trata-se de ruído estatístico, não de edge explorável.
14. **Sinais 100% via API Betfair Cloud:** A API `get_daily_dataframe(source="betfair")` fornece as odds reais de Lay de CS (`Odd_CS_0x1_Lay`, `Odd_CS_1x0_Lay`) e Back (`Odd_H_Back`, `Odd_A_Back`, `Odd_D_Back`) de hora em hora. Não depender de coletor local para gerar sinais no Streamlit.

**Backtest histórico:** serve SÓ pra **descarte rápido** de ideia obviamente ruim e pra estimar
volume — e mesmo assim **na odd de lay real, nunca de back**. **NUNCA aprova nada.** Quem aprova
é o paper forward.

---

## 3. MONTAR / ATUALIZAR UM MÉTODO

1. Copie o template canônico (`treinar_lay_0x0_rf_v2.py`). Ajuste: **target** (o evento em que
   o LAY ganha), faixa de odd (`ODD_MIN/MAX`), coluna de odd, `EV_MIN`.
2. **Features leak-free:** rolling com `shift(1)`, alinhado por **índice as-of por jogo**
   (`reindex(df.index)`) — **NÃO** `.last()` (pega o fim da base = leak/otimista).
3. **Split por mando** quando o modelo usa `H_h_*`/`A_a_*`: view `Home` (gols/won em casa) e
   view `Away` (gols/won fora), rollings **separados**.
4. Todo rolling só produz valor com **janela completa** (senão NaN → cai no `dropna`).
5. Taxas de liga/H2H: `groupby(chave)[flag].shift(1).rolling(N, min_periods=M).mean()`. H2H com
   **chave de par ORDENADA** (`tuple(sorted([casa, fora]))`).
6. **Kelly:** CS (odds altas 6-16) → `1.0`; 1X2/OU (odds baixas 1.4-2.8) → `0.35`.
7. **SÓ integre o que confirmar no PAPER FORWARD (seção 2).** O critério "ROI>0 em 3/4 meses" de
   walk-forward é FRACO (roda em odd de back, sem bootstrap/FDR) — foi como ~45 miragens
   passaram. Backtest histórico só descarta ideia ruim; nunca aprova.

---

## 4. AUDITAR UM MÉTODO — checklist backtest ↔ live

**Verifique SEMPRE contra o código e os dados reais. Não teorize.** Reporte **break-even WR +
bootstrap**, nunca "p-value vs 50%". Confirme item a item:

- [ ] **Base:** o live carrega a MESMA base do trainer, com as colunas ricas presentes (não zeradas)?
- [ ] **Feature:** mesma fórmula (mando, janela, decay, `shift`)? Semântica igual (não juntou casa+fora)?
- [ ] **Odd:** lay real nos DOIS (backtest e live)? Ou o backtest está na odd de back (inflado)?
- [ ] **NaN:** SKIP nos dois (não `0.0`/fallback)?
- [ ] **Cutoff:** dinâmico por data (não hardcoded)?
- [ ] **Target:** polaridade certa? (`predict_proba[:,1]` = P(o lay ganhar), não o contrário)
- [ ] **Filtros:** faixa de odd, EV, liga, favorito — idênticos backtest↔live?
- [ ] **Nomes:** `_canon` (NFKD) nos dois lados? (o log põe sufixo de país; o coletor não)
- [ ] **Look-ahead:** features só de jogos passados? Gols FT só no **settlement**, nunca no gate?
- [ ] **Fabricação:** nenhum valor inventado (h2h, liga, time novo)?
- [ ] **FT vs HT:** todas as colunas são `_FT_`?
- [ ] **Regeneração:** o fix está no `_gen_strategy` do trainer (senão o próximo treino reverte)?

---

## 5. RODAR NO STREAMLIT (deploy)

1. **Histórico:** use um **loader dedicado** que carrega a base do trainer e **valida as colunas
   ricas** (ex.: `hist_rf_loader.load_hist_rf`), abortando/avisando se faltarem — **não** o
   `_hist_df` genérico (ele pode carregar uma base sem features).
2. **Odd real:** aplicar `aplicar_odds_lay` (troca a odd de back pela odd de lay real da Betfair).
   Sinal sem odd Betfair no dia → **SKIP** (não usar a de back).
3. **Paper trail HONESTO:** logar a odd **executável (pós-`aplicar_odds_lay`)** num registro
   central (`paper_log_real` → `paper_trading_real.csv`) — **não** a odd interna da estratégia
   (que grava a b365 inflada).
4. **Sem duplicar:** a geração roda **no clique do botão** (não a cada rerun). Cargas pesadas em
   `@st.cache_data`.
5. **Batch (`rodar_ao_vivo`) também aplica `aplicar_odds_lay`** antes de dimensionar — senão o
   portfolio diário sai na odd b365.
6. Não use `st.X() if cond else st.Y()` (renderiza objeto); trate `idxmax` de grupo all-NaN com
   `fillna(-inf)` (pandas 3.x no Cloud).

---

## 6. VERDADES ATUAIS (estado real dos métodos — na odd executável)

- **MORTOS / miragem (na odd de lay real):** Lay 0x0 (paper de agosto: **−R$1.979**), Lay 1x0
  (teve mês bom na odd real, mas **sem edge confirmado** — não escalar), Lay 2x0 / 0x2
  (falso-positivo de CS raso), Lay 2x2 (agosto +5,3k foi **1 mês**; instável, cauda gorda com liability 16,8u e margem de apenas +1,14% não paga risco de ruína; **DESCARTADO**),
  Lay 0x3 (**−R$22k**; 77,5% WR vs 96,7% break-even), Lay 0x1 In-Play (cashout 0x0 no HT gera **−0,499u** de perda real em vez do presumido +0,19u, colapsando ROI para **−11%** em N=17k e N=473 ticks reais no coletor; **DESCARTADO**), Lay 1x1 (N=412, WR 87,9% vs BE 87,5%,
  ROI 0,1%, colapso Mai-Jul: Mai −141,5%, Jun −115,9%, Jul −71,6%), Lay Draw (+2%, reprova FDR),
- **MORTOS / miragem (na odd de lay real e scan exaustivo):** Lay 0x0 (paper de agosto: **−R$1.979**), Lay 1x0
  (teve mês bom na odd real, mas **sem edge confirmado** — não escalar), Lay 2x0 / 0x2
  (falso-positivo de CS raso), Lay 2x2 (agosto +5,3k foi **1 mês**; instável, cauda gorda com liability 16,8u e margem de apenas +1,14% não paga risco de ruína; **DESCARTADO**),
  Lay 0x3 (**−R$22k**; 77,5% WR vs 96,7% break-even), Lay 0x1 In-Play (cashout 0x0 no HT gera **−0,499u** de perda real em vez do presumido +0,19u, colapsando ROI para **−11%** em N=17k e N=473 ticks reais no coletor; **DESCARTADO**), Lay 1x1 (N=412, WR 87,9% vs BE 87,5%,
  ROI 0,1%, colapso Mai-Jul: Mai −141,5%, Jun −115,9%, Jul −71,6%),
  Lay 0x1 Super Fav Mandante (0/100 holders no scan de 600 testes; na base Betfair FRESH com Lay real 1,12x dá **−2,17% ROI**, IC95 [−8,0%, +2,9%]; **REPROVADO / ARQUIVADO**),
  Lay Under 0.5 FT em Super Fav (0/100 holders no scan; na base Betfair FRESH com Lay real 1,13x dá **−0,76% ROI**, IC95 [−7,4%, +4,8%]; **REPROVADO / ARQUIVADO**),
  Lay Away / DC 1X no Super Fav Mandante (1/100 holders no scan; na base Betfair FRESH com Lay real 1,09x dá apenas **+0,50% ROI** ≈ break-even estrito, IC95 [−1,8%, +2,8%] cruza zero; **REPROVADO / ARQUIVADO**),
  Lay Under 1.5 FT XGBoost (walk-forward OOS de 24 meses negativo em **−0,50%**, 2025 = −7,2%, instável H1 −63u / H2 +21u; **REPROVADO / ARQUIVADO**),
  AH +1.5 Zebra Mandante (miragem de odd estimada 2,45; na odd real da base 1,30 tem break-even 81,5% vs WR 75,2% = **−4,2%** de prejuízo; **MIRAGEM DESCARTADA**),
  DNB / AH 0.0 Mandante (XGBoost EV>=5%: N=333, ROI +1,06%, mas com EV>=3% inverte para -0,66% e IC95 [-6,0%, +7,4%] cruza zero; mercado 1X2 hiper-eficiente),
  Back BTTS Yes / BTTS Não (overround de 8,3% consome edge; Back Não = −10,1% em 2.580 jogos, 0/8 meses; ruído de escala), Over 2.5, Saldo Menor, EH+3 múltiplas,
  escanteios, Over 1.5 ML, HT scans.
- **A Tríade Sobrevivente em Observação (FORWARD OCULTO STAKE-ZERO — 02/09/2026):**
  - **1. Lay Draw Base em Super Favorito (`min(Odd_H_Back, Odd_A_Back) <= 1.40`, `Odd_D_Lay 4.5 a 10.0`):** 👑 **O MAIS SÓLIDO DO PORTFÓLIO**:
    No forward real do gap 21/08 a 02/09 (N=113 jogos reais com odd lay executável Betfair): WR **90,3%**, ROI/liability **`+5,2%`**, estável em ambas as metades (H1 **+7,9%** / H2 **+1,8%**), P(ROI≤0) = 0,069. Regra BASE pura congelada. O filtro refinado (`Odd_Over35 >= 2.54`) rendeu menos e foi descartado por overfitting.
  - **2. Lay Home Base no Favorito Visitante (`Odd_A_Back <= 1.65`, `Odd_H_Lay 2.0 a 10.0`):** 👑 **MAIOR ROI / VOLATILIDADE MODERADA**:
    No forward real do gap 21/08 a 02/09 (N=63 jogos reais com odd lay executável Betfair): WR **93,7%**, ROI/liability **`+7,7%`**, P(ROI≤0) = 0,033, porém concentrado em H2 (H1 +2,1% / H2 +15,5%). Regra BASE pura congelada. O filtro refinado pós-scan (`Odd_A ∈ [1.54, 1.65]`) rendeu menos (+6,4%) e inverteu o sinal (−4,6% H1 / +17,4% H2) — **rejeitado como overfitting / garden of forking paths**.
  - **3. Lay Over 4.5 FT em Jogos Under (`Odd_Under25_Back <= 1.50`, `4.0 <= Odd_Over45_Lay <= 20.0`):** ⚠️ **SOBREVIVENTE DE CAUDA (OBSERVAÇÃO ESTRITA)**:
    Na base Betfair FRESH com Lay real: N=130, WR 96,9% vs BE 94,6%, ROI/liability **+2,91%** (único que segurou margem positiva). No gap 21/08 a 02/09: N=11, WR 100%, ROI +5,6%, mas com IC falsamente apertado devido a N minúsculo e zero reds. Segue em observação estrita pelo volume intrinsecamente raro (~0,4 jogo/dia).
  - **4. Under-Limite In-Play (Minutos 75–85):** 🛑 **QUARENTENA TÉCNICA STAKE-ZERO TOTAL**:
    Os supostos +18% a +26% eram artefato de bug de amostragem grossa (gol nos acréscimos perdido antes do fechamento do mercado gerando 27% de false-green no est3). Ao re-liquidar pelo placar real, caiu para **−18% de prejuízo**.
    **SOLUÇÃO OFICIAL IMPLANTADA:** Liquidação 100% oficial pela Betfair API via `market_id` (status WINNER/LOSER oficial). O robô na VPS Oracle SP opera em **stake: 0.0 absoluto** até acumular N ≥ 400 jogos auditados oficialmente.
- **Diretriz de Infraestrutura e Governança Oficial:**
  - O pipeline de observação corre desacoplado na pasta `forward_oculto/` (tarefa `ARKAD_Forward_Oculto` todo dia 10:30; log central `forward_oculto_log.csv`).
  - **Critério de Saída da Quarentena:** N ≥ 300 a 400 apostas por método OU 5 fins de semana com dados limpos e liquidação oficial. Bootstrap por bloco-semana com IC95% estritamente excluindo zero e FDR aprovado. Até lá: **ZERO CAPITAL REAL**.
- O endpoint `fetch_betfair_daily` e `get_daily_dataframe(source="betfair")` **já entregam as odds reais de Lay e Back da Betfair Exchange** (validado 1,00x vs API direta da Betfair).
- **Fixar código faz o live parar de mentir vs o backtest — NÃO cria edge.** Um método sem edge
  continua sem edge depois de bem estruturado. Backtest retroativo não é dinheiro real; autoridade é o paper forward executável ao vivo.

---

*Mantido por: auditoria ARKAD (Claude / Antigravity), ago/2026. Atualize a seção 6 quando um método mudar de
status. As regras 0-5 são estáveis — só mude com evidência.*
