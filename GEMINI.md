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
  (falso-positivo de CS raso), Lay 2x2 (agosto +5,3k foi **1 mês**; instável, sem confirmação),
  Lay 0x3 (**−R$22k**; 77,5% WR vs 96,7% break-even), Lay Draw (+2%, reprova FDR), Over 2.5,
  Saldo Menor, EH+3 múltiplas, escanteios, Over 1.5 ML, HT scans.
- **Observação / candidato (stake-zero):** 
  - **Lay Under 1.5 FT (XGBoost EV ≥ 5%):** ✅ CONFIRMADO independentemente (Claude, ago/2026):
    split leak-free (treino `<2026`, teste 2026), sem leak de feature (todas as VAR `|corr|<0,17`),
    N=225, WR 73,3% vs BE 68,4% (margem +4,9%), **7/8 meses positivos**, **bootstrap IC95
    [+4,3%, +34,2%] exclui zero**. Observação via **`observar_under15_forward.py`** (stake-ZERO,
    *forward-only*: só registra jogo visto ANTES de jogado; ignora histórico re-pontuado).
    Falta p/ produção: FDR formal + confirmação forward real (single-split, não walk-forward).
    ⚠️ **NÃO usar `gerar_sinais_forward_diario.py`** (DEPRECADO/arquivado em `_arquivo_backtest_gemini/`):
    violava a própria regra de stake-zero (`stake:100`), empacotava as miragens já mortas
    (0x3/2x2/BTTS/Lay Draw universo) e re-pontuava a base histórica chamando de "forward".
  - **Under-no-limite in-play** (pré-registrado; ~2 fins de semana; o estado 0-0 já oscilou de +32% pra −6% → instável, acompanhar).
- O endpoint `fetch_betfair_daily` **já entrega a odd de lay real** (validado 1,00x vs API direta
  da Betfair). A inflação estava só na **base b365 histórica / paper logs**.
- **Fixar código faz o live parar de mentir vs o backtest — NÃO cria edge.** Um método sem edge
  continua sem edge depois de bem escrito.

---

*Mantido por: auditoria ARKAD (Claude / Antigravity), ago/2026. Atualize a seção 6 quando um método mudar de
status. As regras 0-5 são estáveis — só mude com evidência.*
