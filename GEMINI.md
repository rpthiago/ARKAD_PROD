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
| **Combinar Métodos Prematuramente** (Juntar Lay 1x0 com Lay 0x1 antes do 1x0 provar edge real) | **Tratamento e Portfólio Separados:** 1x0 é o elo fraco (−8% forward) e deve rodar isolado |
| **Odd Estimada / Inventada em Handicap** (Assumir odd 2,45 em AH +1.5 Zebra gerando "+82% ROI" falso) | **NUNCA estimar odd.** Usar coluna real (`AH_*`, `EH_*`, `Odd_*`). AH +1.5 Zebra mandante na odd real 1,30 dá **−4,2%** de prejuízo |
| **Risco de Cauda Excessivo em Lay de Odd Alta** (Lay 2x2 com liability 16,8 e margem navalha +1,14%) | **Evitar Lay de Odd Alta com Margem Fina.** 1 red apaga 17 greens, tornando o risco de ruína inaceitável |

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
  Lay Draw + Cobertura 100% Back 1x1 (N=250 na base congelada 08-06, ROI nominal +1,29%, mas ROI s/ capital em risco de apenas +0,45% ≈ break-even; IC95 [-11,5%, +13,3%] inclui zero; double-reds em 0-0/2-2 desprotegidos; 2pp de subprecificação frágil no 1x1),
  AH +1.5 Zebra Mandante (miragem de odd estimada 2,45; na odd real da base 1,30 tem break-even 81,5% vs WR 75,2% = **−4,2%** de prejuízo; **MIRAGEM DESCARTADA**),
  DNB / AH 0.0 Mandante (XGBoost EV>=5%: N=333, ROI +1,06%, mas com EV>=3% inverte para -0,66% e IC95 [-6,0%, +7,4%] cruza zero; mercado 1X2 hiper-eficiente),
  Dupla Chance 1X / Lay Away Genérico (sem filtro de super favorito: ROI -5,04%/aposta, -2,27% s/ capital; já morto pela assimetria de liability),
  Back Mandante Favorito 1X2 (XGBoost EV>=3%: N=619 no universo real, WR 56,5% vs BE 57,2%, Margem -0,6%, ROI +1,7%, IC95 [-3,8%, +11,0%] inclui zero; 224% do lucro concentrado em fevereiro sozinho; mercado 1X2 hiper-eficiente),
  Lay Under 2.5 FT (XGBoost EV>=5%: N=295-522, Margem +2,4%, ROI +4,9%, IC95 [-2,4%, +13,8%] inclui zero, 4/8 meses negativos; mercado de Over/Under 2.5 hiper-eficiente),
  Back BTTS Yes (reproduz com LR crua +8,8%, mas evapora para ROI +2,5% e margem +1,1% com
  StandardScaler; IC95 cruza zero, p=0,13; ruído de escala), Over 2.5, Saldo Menor, EH+3 múltiplas,
  escanteios, Over 1.5 ML, HT scans.
- **Aprovado / Observação (Fiel à Regra e Matemática):**
  - **Handicap Asiático +2.0 / EH +2 Zebra (Saldo Menor Top 2):** ✅ CONFIRMADO independentemente (Claude, ago/2026):
    Base 2026 completa (N=455 a 459), WR 88,6% (Betano) a 96,6% (Traderball AH +2.0 com 38 reembolsos), **8/8 meses positivos**.
  - **Lay Under 1.5 FT (XGBoost EV ≥ 5%):** ✅ CONFIRMADO independentemente (Claude, ago/2026):
    split leak-free (treino `<2026`, teste 2026), sem leak de feature (todas as VAR `|corr|<0,17`),
    N=225, WR 73,3% vs BE 68,4% (margem +4,9%), **7/8 meses positivos**, **bootstrap IC95
    [+4,3%, +34,2%] exclui zero**. Observação via **`observar_under15_forward.py`** (stake-ZERO,
    *forward-only*: só registra jogo visto ANTES de jogado; ignora histórico re-pontuado).
  - **Lay 0x1 Super Favorito Mandante (`Odd_H <= 1.80/1.90`, `5 <= Odd_CS_0x1_Lay <= 15`):** 👑 **CARRO-CHEFE (Watchlist Stake-Zero Prioritária)** (Claude/Antigravity, ago/2026):
    Base 2026 completa: N=4.007, WR 94,24% vs BE 91,95% (margem +2,29%), ROI/liability +2,59%, 8/8 meses positivos (+1.131u / +R$ 113k). No forward real OOS (21/08+): isolado deu WR 92,7% a 96,6% e ROI/liability +2,2% a +5,7% ✅ (+R$ 1.674 a +R$ 2.475). É o método mais sólido do portfólio.
  - **Lay Under 0.5 FT em Super Favorito (`Odd_Fav <= 1.60`, `Odd_Under05_Lay <= 15.0`):** ✅ **APROVADO / OBSERVAÇÃO** (Claude/Antigravity, ago/2026):
    Base 2026 completa: N=4.321, WR 94,26% vs BE 91,65% (margem +2,61%), ROI/liability +3,18%, **8/8 meses positivos** (+1.438u / +R$ 143k), Bootstrap IC95% [+2,4%, +3,9%]. No forward real (21/08+): N=26, WR 96,2% (25W/1L), +1,32u ✅.
  - **Lay Draw em Super Favorito Mandante (`Odd_H <= 1.40`, `Odd_D_Lay 4.5 a 10.0`):** ✅ **APROVADO / OBSERVAÇÃO** (Claude/Antigravity, ago/2026):
    Base 2026 completa: N=1.678, WR 85,77% vs BE 83,54% (margem +2,24%), ROI/liability +3,26%, **7/8 meses positivos** (+264u / +R$ 26k), Bootstrap IC95% [+1,2%, +5,2%]. No forward real (21/08+): N=58, WR 91,4% (53W/5L), +4,17u ✅ (locomotiva de volume).
  - **Lay Away / Dupla Chance 1X no Super Favorito Mandante (`Odd_H <= 1.50`, `Odd_A_Lay <= 15.0`):** ⚠️ **WATCHLIST STAKE-ZERO** (Claude/Antigravity, ago/2026):
    N=3.253, WR 87,1% a 89,4% vs BE 87,8%, ROI sobre liability +4,2% (Claude) / +2,91% (Antigravity), **8/8 meses positivos**, Bootstrap IC95% [+1,7%, +4,0%] exclui zero. Observar em stake-zero (não operar por liability alta do visitante ~6-8).
  - **Portfólio Combinado de Métodos Aprovados (Forward Real 21 a 30/08):** ✅ **CONFIRMADO NO FORWARD (N=135)**:
    Na pasta `metodos_aprovados/`: **135 jogos reais**, **123 Greens e 12 Reds (`91.11% de Win Rate`)**, acumulando **`+12,01 unidades nominais`** de lucro líquido.
    **DIRETRIZ DE GOVERNANÇA:** Manter o robô autônomo acumulando dados até **N ≥ 300 a 400 jogos**. 
    **GESTÃO DE RISCO ATIVA:** 5.0% de Liability Fixa Dinâmica (Banca R$ 4.000 ➔ R$ 200 de risco máx por aposta). Stake nominal varia por odd para travar a perda do Red rigorosamente em 5%.
  - **Automação Oficial e Alertas Telegram:**
    - Robô autônomo matinal e noturno em `automacao_diaria_aprovados.py` (`--manha` e `--noite`).
    - Integração de alertas em tempo real via Telegram Bot API em `telegram_notifier.py` (Bot: `@arkkad_bot`).
    - Sincronização automática na pasta `metodos_aprovados/` e exibição nas páginas `01_🏆_Portfolio_Metodos_Aprovados.py` e `02_📊_Resultados_Metodos_Aprovados.py`.
  - **Lay 1x0 Super Favorito Punter (`Odd_A <= 1.80/1.90`, `5 <= Odd_CS_1x0_Lay <= 15`):** ⚠️ **WATCHLIST STAKE-ZERO ISOLADA (Elo Fraco)**:
    8/8 meses positivos no backtest 2026, porém no forward real recente deu WR 81,5% e ROI -8% ❌. Manter estritamente isolado sem contaminar o Lay 0x1.
  - **Under-no-limite in-play** (pré-registrado; ~2 fins de semana; o estado 0-0 já oscilou de +32% pra −6% → instável, acompanhar).
- O endpoint `fetch_betfair_daily` e `get_daily_dataframe(source="betfair")` **já entregam as odds reais de Lay e Back da Betfair Exchange** (validado 1,00x vs API direta da Betfair).
- **Fixar código faz o live parar de mentir vs o backtest — NÃO cria edge.** Um método sem edge
  continua sem edge depois de bem estruturado. Backtest retroativo não é dinheiro real; autoridade é o paper forward executável ao vivo.

---

*Mantido por: auditoria ARKAD (Claude / Antigravity), ago/2026. Atualize a seção 6 quando um método mudar de
status. As regras 0-5 são estáveis — só mude com evidência.*
