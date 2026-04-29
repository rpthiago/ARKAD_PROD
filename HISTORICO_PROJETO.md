# Histórico do Projeto ARKAD_PROD

> Última atualização: 27/04/2026 (Grande Reforma de Performance)  
> Objetivo: registro completo de tudo que foi construído, decidido e testado — serve de ponto de partida para qualquer sessão futura.

---

## 1. Visão Geral

Sistema de **sinais de apostas lay (mercado Resultado Correto / Correct Score)** na Betfair, composto por:

| Camada | Arquivo | Função |
|---|---|---|
| Dashboard | `main.py` | Interface Streamlit para operador ver sinais do dia |
| API local | `servidor_arkad.py` | FastAPI porta 8080 — endpoint `/arkad/sinais` |
| Ingestão | `ingestao_tempo_real.py` | Busca dados Bet365/Betfair via FutPython |
| Motor | `engine_ciclo_producao.py` | Simulação/backtesting com stake progressivo |
| Backtesting WF | `walk_forward_backtest.py` | Validação out-of-sample walk-forward |
| Filtro operacional | `config_rodos_master.json` | **Blacklist** de combos Liga+Método+Odd tóxicos |
| Config produção | `config_prod_v1.json` | Parâmetros ativos em produção |

---

## 2. Histórico de Marcos

### 2026-04-28 — Restauração do Padrão Ouro (97% WR) e Fix de Odds Reais

#### Sintoma observado
- Sinais de ligas como ISRAEL 1 aparecendo como EXECUTED com Odds de 15.5 e 32.0.
- Divergência entre o Win Rate do backtest automático (93%) e o manual (97%).

#### Causas raiz mapeadas
1. **Ponto Cego de Odd**: O scanner filtrava pela `Odd_Base` (sinal), mas ignorava se a `Odd real` da Betfair disparava acima do limite de 11.5 durante a execução.
2. **Whitelist Dessincronizada**: A `config_prod_v1.json` não continha as 19 ligas core do consolidado manual de sucesso, distorcendo os resultados.
3. **Base de Backtest Incompleta**: O script de backtest ignorava o arquivo `apostas_reais_consolidado.xlsx`.

#### Correções e Melhorias Aplicadas
1. **Blindagem de Odd Real**:
   - Modificado `main.py` para validar a Odd da Betfair contra o teto de 11.5 antes de marcar como EXECUTED.
   - Sincronizado `engine_ciclo_producao.py` com a mesma regra de teto absoluto na Odd de execução.
2. **Sincronização de Whitelist**:
   - Restaurada a Whitelist Ouro com as 19 ligas do consolidado (Espanha 2, Itália 1, Alemanha 1, Inglaterra 1, Israel 1, etc.).
3. **Backtest Fiel**:
   - Reconfigurado `_backtest_abril_2026.py` para carregar a base consolidada de 92 jogos e aplicar a whitelist/blacklist de produção.
   - **Resultado Validado**: Win Rate de **96.0% (Híbrido)** e **100% (Somente 0x1)** no mês de Abril.

#### Commits de referência
- `eb4e32e` — feat: restauração do Padrão Ouro (97% WR). Whitelist sincronizada e trava de Odd Real.

---


### 2026-04-24 — Incidente Streamlit Cloud (fallback persistente) e estabilização

#### Sintoma observado

- Painel em fallback local com mensagens longas e diagnóstico misturando causas diferentes.
- Status frequente: `Fallback Local Ativo`.
- Erro final identificado no cloud: `401/403` nos endpoints FutPython (não era mais erro de conectividade local).

#### Causas raiz mapeadas

- Detecção de cloud previamente agressiva, forçando fallback antes de tentar fluxo live.
- Tentativa de endpoint local (`127.0.0.1`) no Streamlit Cloud, gerando ruído de diagnóstico.
- Token/permissão FutPython retornando `401/403` no ambiente cloud.

#### Correções aplicadas

- Restauração do fluxo live normal sem bloqueio automático por cloud.
- Bloqueio de tentativa de localhost quando em runtime Streamlit Cloud.
- Fallback de autenticação FutPython com múltiplos formatos (header/query) para reduzir 401/403.
- Limpeza fina do painel:
  - `Fonte de dados` agora exibe resumo curto.
  - Detalhes completos ficam em expander técnico.
  - Mensagem específica para autenticação negada (`401/403`).

#### Playbook rápido (para não repetir demora)

1. Verificar o texto de `Fonte de dados`:
  - Se contiver `401/403`: foco em token/permissão, não em rede.
  - Se contiver `timed out`: foco em rede/timeout.
2. No Streamlit Cloud, validar Secrets:
  - `FUTPYTHON_TOKEN` sem espaços e com valor atualizado.
3. Reboot no app após alterar Secrets.
4. Confirmar sucesso quando aparecer:
  - `Ingestao em tempo real ativa (...)`.
5. Se cair em fallback:
  - Abrir expander `Detalhes tecnicos da fonte de dados` e usar a causa direta para correção.

#### Commits de referência desta estabilização

- `8968839` — restaura ingestão live sem bloqueio cloud.
- `4f12ba3` — não tenta localhost no Streamlit Cloud.
- `55c1054` — fallback de autenticação FutPython (redução de 401/403).
- `0ecc1f1` — tratamento de 401/403 no painel sem mensagem de localhost.


### 2026-04-15 — Migração e reestruturação (ARKAD_PROD)

- Projeto migrado de `DASHBOARD_ARKAD-1` para repositório dedicado `ARKAD_PROD`.
- Arquivos de produção antes em `utilitarios/producao/` passaram a ficar na **raiz** do repo.
- `recalculo_sem_combos_usuario.csv` (base histórica sem combos do usuário) movido para raiz como fallback.

**Mudanças em `main.py`:**
- Adicionado fallback duplo quando a API local cai:
  1. Planilhas operacionais do dia (`Apostas_*.xlsx` na raiz).
  2. CSV histórico `recalculo_sem_combos_usuario.csv`.
- Fluxo principal: tenta ingestão live → tenta endpoint local → fallback local.

**Mudanças em `servidor_arkad.py`:**
- Endpoint `/arkad/sinais` passou a tentar ingestão em tempo real (FutPython) **antes** de aplicar o filtro Rodo.
- Se a ingestão falha, cai para CSV local como contingência.

**Criação de `ingestao_tempo_real.py`:**
- Módulo novo com provedores configuráveis por endpoint.
- Integra Bet365 + Betfair via FutPython (`api.futpythontrader.com`).
- Opção `odds_api` disponível mas desativada.
- Normaliza colunas para o pipeline existente (expansão CS em 2 linhas: 0x1 e 1x0).

---

### 2026-04-17 — Estudo Comparativo de 5 Métodos de Stake

Dataset usado: `pacote_reproducao_todos_jogos_20250801_20260414.csv`  
Período: 20/08/2025 a 14/04/2026 | Entradas: 357 | Win rate observado: **97,76%**

#### Métodos testados (todos com o mesmo rodo master)

| Método | Config | Rampa | Slippage | Liquidez |
|---|---|---|---|---|
| Juros_Rampa | `config_backtest_exec.json` | ✅ | ❌ | ❌ |
| Progressiva_Fixa500 | `config_backtest_resp_fixa_500.json` | ❌ | ❌ | ❌ |
| Metodo3_Hibrido | `config_backtest_metodo3_hibrido.json` | ✅ | ❌ | ❌ |
| Metodo3_B_Defensivo | `config_backtest_metodo3b_defensivo.json` | ✅ | ❌ | ❌ |
| Metodo3_C_Agressivo | `config_backtest_metodo3c_agressivo.json` | ✅ | ❌ | ❌ |

#### Resultados

| Método | Lucro (R$) | DD abs (R$) | DD% | Score Lucro/DD |
|---|---|---|---|---|
| Progressiva_Fixa500 | 17.529 | 747 | 4,26% | **23,46** |
| Metodo3_B_Defensivo | 28.251 | 1.232 | 4,36% | 22,93 |
| Metodo3_Hibrido | 30.947 | 1.518 | 4,91% | 20,38 |
| Metodo3_C_Agressivo | 46.933 | 2.612 | 5,56% | 17,97 |
| Juros_Rampa | 50.635 | 2.861 | 5,65% | 17,70 |

#### Campeões por regra de decisão

| Regra (peso Lucro / DD / PiorLinha) | Campeão |
|---|---|
| Conservadora 35/45/20 | **Progressiva_Fixa500** |
| Equilibrada 50/30/20 | **Metodo3_C_Agressivo** ⭐ |
| Agressiva 65/20/15 | **Metodo3_C_Agressivo** ⭐ |

> **Conclusão do estudo:** Metodo3_C_Agressivo vence 2 de 3 regras e é o candidato natural para produção em perfil equilibrado/agressivo.

Arquivos de referência:
- `Arquivados_Apostas_Diarias/Relatorios/Comparativo_Juros_vs_Progressiva/DOSSIE_CONTINUIDADE_20260417.md`
- `grid_decisao_5_metodos_20260417.csv` e variantes por regra
- `manifest_estudo_20260417.json`

---

### 2026-04-23 — Sessão 2: bug crítico do filtro, comparativo JC, coluna Prio

#### 2026-04-23-A — Bug crítico: rodo_mode estava invertido

**Problema encontrado:** `config_prod_v1.json` não tinha a chave `rodo_mode`, então o código defaultava para `"whitelist"`. Em modo whitelist, o engine executa **apenas** os combos que batem nas regras. Como **todas** as 31 regras do `config_rodos_master.json` definem combos **tóxicos** (lucro_combo negativo), o sistema estava executando exclusivamente as piores combinações históricas.

**Prova numérica (dataset 527 entradas ago/2025–abr/2026):**

| Modo | Entradas | WR | Lucro |
|---|---|---|---|
| Whitelist (bug) | ~170 | 84.8% | −R$ 3.380 |
| **Blacklist (correto)** | 357 | 97.8% | **+R$ 50.635** |

**Correção:** adicionado `"rodo_mode": "blacklist"` em `runtime_data` do `config_prod_v1.json`.

**Commit:** `cb191d1`

---

#### 2026-04-23-B — Comparativo: Scanner Juros Compostos (DASHBOARD_ARKAD-1) vs Blacklist Atual

O repositório paralelo `DASHBOARD_ARKAD-1` tinha um scanner chamado "Juros Compostos" (`streamlit_scanner_juros_compostos.py` / página `27_📈_Scanner_Juros_Compostos.py`) com filtros baseados em:
- Lay 0x1: odd entre 8-11, exclui 8 ligas genéricas
- Lay 1x0: ligas `ITALY 1`, `SPAIN 1`, `SPAIN 2` apenas, odd ≤ 11

**Resultados no mesmo dataset (527 entradas):**

| Cenário | Entradas | WR | Lucro | DD | Score L/DD |
|---|---|---|---|---|---|
| Sem filtro | 527 | 93.4% | +R$ 36.212 | R$ 5.222 | 6.93 |
| Juros Compostos (JC) | 475 | 92.8% | +R$ 27.733 | R$ 6.449 | 4.30 |
| **Blacklist Atual** | **357** | **97.8%** | **+R$ 50.635** | **R$ 2.861** | **17.70** |

**Análise de overlap:**
- Aprovados por ambos: 314 entradas | WR 97.5%
- Só JC aprova: 161 entradas | WR 83.9% ← 26 perdas extras
- Só Blacklist aprova: 43 entradas | WR **100%** ← JC bloqueava greens

**Conclusão:** Blacklist é superior em todas as dimensões. O JC usa regras por liga inteira (impreciso); a Blacklist usa combos Liga+Método+Odd (cirúrgico).

Script de análise: `_comparativo_jc_vs_bl_tmp.py` (temporário, não commitado)

---

#### 2026-04-23-C — Análise de volume diário e regra de prioridade

**Contexto:** usuário relatou dificuldade em executar todos os jogos em dias de alto volume.

**Dados históricos (pós-blacklist):**
- Máximo histórico: 5 entradas/dia
- Média: 2.2 entradas/dia | Mediana: 2 entradas/dia
- Nunca houve mais de 5 entradas num único dia

**WR por faixa de odd e método:**

| Método | Faixa | WR |
|---|---|---|
| Lay 0x1 | odd 9-11 | 94% |
| Lay 0x1 | odd 7-9 | 93.4% |
| Lay 1x0 | odd < 9 | 90.7% |
| Lay 1x0 | odd 7-9 | 88.5% |

**Regra de prioridade definida:**
- P1 ⭐ → Lay 0x1 odd ≥ 9
- P2 → Lay 0x1 odd 7-9
- P3 → Lay 1x0 odd < 9
- P4 → Lay 1x0 odd ≥ 9

**Backtest cap 4/dia vs blacklist puro:**

| Métrica | Blacklist Puro | BL + Cap 4/dia |
|---|---|---|
| Entradas | 357 | 348 |
| Lucro | **+R$ 50.635** | +R$ 46.628 |
| DD | **R$ 2.861** | R$ 4.252 |
| Score | **17.70** | 10.97 |

**Conclusão do backtest:** o cap piora o resultado. As 9 entradas descartadas pelo cap foram **100% GREEN**. O filtro blacklist já é preciso o suficiente — não há entradas ruins que um cap eliminaria. A prioridade serve apenas como **guia operacional manual** em dias com múltiplos jogos simultâneos, não como filtro matemático.

Script de análise: `_backtest_prioridade_tmp.py` (temporário, não commitado)

---

#### 2026-04-23-D — Coluna Prio no dashboard (`main.py`)

**Mudança implementada em `main.py`:**
- Nova função `_calc_prio(metodo, odd)` calcula P1/P2/P3/P4 por linha
- Coluna `Prio` adicionada ao DataFrame de saída do `_apply_rodo_filter`
- Lista de jogos agora **ordenada por prioridade** (P1 no topo) em vez de por horário
- Coluna `Prio` exibida nas 3 tabelas: "ENTRADA AGORA", "Próximos Jogos", "Dia Inteiro"
- CSV de download também inclui a coluna `Prio`
- Seção histórica (data diferente de hoje) também mostra `Prio`

**Commit:** `ddc4b93`

---

## 3. Arquitetura Atual (23/04/2026)

### 3.1 Pipeline de sinais (produção)

```
[FutPython API]
  Bet365 endpoint  ──┐
  Betfair endpoint ──┤──► ingestao_tempo_real.py ──► load_live_dataframe()
                     │         (cross B365 x BF)
                     │
                     ▼
             servidor_arkad.py  (FastAPI :8080)
                     │
                     ▼ /arkad/sinais?date=YYYY-MM-DD
                     │
             main.py (Streamlit)
                     │
                 Filtro Rodo
               (whitelist mode)
                     │
              ┌──────┴──────┐
           EXECUTED        SKIP
         (mostra ao       (ignora)
          operador)
```

**Fallback** (quando a API cai):
1. `main.py` tenta `load_live_dataframe()` direto
2. Se falhar → lê `Apostas_*.xlsx` na raiz
3. Se não houver xlsx → lê `recalculo_sem_combos_usuario.csv`

### 3.2 Filtro Rodo (`config_rodos_master.json`)

- **Modo:** `blacklist` — um jogo aparece como EXECUTED se **NÃO bater** em nenhum filtro.
- **31 regras** do tipo: Liga + Método + faixa de Odd.
- Cada regra tem `lucro_combo` histórico negativo = combinação tóxica que é bloqueada.
- Exemplo de regra bloqueada: `SPAIN 1 | Lay_CS_1x0_B365 | Odd 6-8` → lucro histórico: −3.920.
- Origem: gerado a partir de `config_prod_v1.json` em 15/04/2026.
- Bug corrigido em 23/04/2026: `rodo_mode` estava ausente (defaultava para whitelist = invertido).

### 3.3 Motor de Stake (`engine_ciclo_producao.py`)

**Parâmetros de produção (`config_prod_v1.json`):**

| Parâmetro | Valor |
|---|---|
| Base inicial | R$ 500 |
| Teto | R$ 2.000 |
| Compound limit | 2x |
| Step-up target | 4x base (ao atingir, dobra a base) |
| Step-down limit | -2x base (ao atingir, reduz a base) |
| Slippage max | 3 ticks (0,3 odd) |
| Liquidez mínima | 1.000 matched |
| Circuit breaker | DD > -1,5 ou 3 reds consecutivos |

**Rampa de entrada (3 fases):**
- Fase 1: 30% da base
- Fase 2: 60% da base
- Fase 3: 100% da base

### 3.4 Ingestão em tempo real

Provedores ativos em `config_prod_v1.json`:

| Fonte | Endpoint | Token | Método padrão |
|---|---|---|---|
| Bet365 | `api.futpythontrader.com/.../bet365/{data}/` | `FUTPYTHON_TOKEN` | Lay_CS_0x1_B365 |
| Betfair | `api.futpythontrader.com/.../betfair/{data}/` | `FUTPYTHON_TOKEN` | Lay_CS_0x1_BF |

- Cross B365×BF: Back → `odd_signal`, Lay → `odd_betfair`
- CS expandido em 2 linhas: `Lay_CS_0x1` e `Lay_CS_1x0`

---

## 4. Configs Disponíveis

| Arquivo | Uso | Rampa | Slippage | Liquidez |
|---|---|---|---|---|
| `config_prod_v1.json` | **PRODUÇÃO ATIVA** | ✅ | ✅ | ✅ |
| `config_prod_v1_sem_liq.json` | Teste sem filtro liquidez | ✅ | ✅ | ❌ |
| `config_backtest_exec.json` | Backtest Juros_Rampa | ✅ | ❌ | ❌ |
| `config_backtest_resp_fixa_500.json` | Backtest Progressiva_Fixa | ❌ | ❌ | ❌ |
| `config_backtest_metodo3_hibrido.json` | Backtest M3 Híbrido | ✅ | ❌ | ❌ |
| `config_backtest_metodo3b_defensivo.json` | Backtest M3-B Defensivo | ✅ | ❌ | ❌ |
| `config_backtest_metodo3c_agressivo.json` | Backtest M3-C Agressivo | ✅ | ❌ | ❌ |
| `config_rodos_master.json` | Filtros operacionais Rodo | — | — | — |

---

## 5. Estrutura de Relatórios Arquivados

```
Arquivados_Apostas_Diarias/
└── Relatorios/
    ├── Comparativo_Automatizado/         ← KPIs e comparativos rápidos
    │   ├── kpis_latest_prod_v1.json
    │   └── comparativo_resp500_*.csv/json
    │
    ├── Comparativo_Juros_vs_Progressiva/ ← Estudo principal 17/04/2026
    │   ├── DOSSIE_CONTINUIDADE_20260417.md   ← LEIA PRIMEIRO ao retomar
    │   ├── manifest_estudo_20260417.json
    │   ├── grid_decisao_5_metodos_*.csv/json (4 variações de regra)
    │   ├── juros_rampa_hist_20260417_ops.csv + summary.json
    │   ├── progressiva_fixa_hist_20260417_ops.csv + summary.json
    │   ├── metodo3_hibrido_hist_20260417_ops.csv + summary.json
    │   ├── metodo3b_defensivo_hist_20260417_ops.csv + summary.json
    │   └── metodo3c_agressivo_hist_20260417_ops.csv + summary.json
    │
    └── WalkForward/                      ← ~40 arquivos de validação WF
        └── wf_[run_id]_walkforward_{resumo|ops|agregado|meta}.*
```

---

## 6. Comandos de Reprodução

### Rodar backtest de um método
```bash
python engine_ciclo_producao.py \
  --input <csv_de_entrada> \
  --config config_backtest_metodo3c_agressivo.json \
  --environment historico \
  --output-dir Arquivados_Apostas_Diarias/Relatorios/Comparativo_Juros_vs_Progressiva \
  --run-id metodo3c_agressivo_hist_YYYYMMDD \
  --skip-mini-report
```

### Rodar walk-forward
```bash
python walk_forward_backtest.py \
  --input <csv_de_entrada> \
  --config config_prod_v1.json \
  --train-days 90 --test-days 14 --step 14 \
  --output-dir Arquivados_Apostas_Diarias/Relatorios/WalkForward
```

### Subir servidor local
```bash
uvicorn servidor_arkad:app --host 127.0.0.1 --port 8080
```

### Subir dashboard
```bash
streamlit run main.py
```

---

## 7. Próximos Passos (pendentes)

### 7.1 Análise do filtro Rodo — status atualizado (23/04/2026)
- ✅ Bug do rodo_mode corrigido (whitelist → blacklist)
- ✅ Comparativo com Scanner Juros Compostos feito — Blacklist é superior
- **Pendente:**
  - Investigar janeiro/2026 especificamente: único mês onde blacklist ficou abaixo do sem-filtro (R$ +3.574 vs R$ +1.011 — na verdade blacklist foi melhor, mas vale stress-test)
  - Considerar remover as 11 regras Rodo com apenas 1 entrada histórica (Rodo_22 a Rodo_31) — base estatística insuficiente
  - Recalcular `lucro_combo` por regra com dataset pós-14/04/2026 quando disponível

### 7.2 Próximos passos do estudo de métodos (DOSSIE_20260417)
1. Stress test por subperíodos (mensal/trimestral) para M3-C Agressivo.
2. Sensibilidade de `step_up_target` e `compound_limit` no M3-C.
3. Simulação de cenários adversos (sequências de perda).
4. Definir regra de corte automática para troca entre M3-B ↔ M3-C.
5. Dashboard de KPIs em tempo real (SQLite).

### 7.3 Melhorias operacionais
- Remover `st.write(f"Debug API URL: ...")` do `main.py` quando for para produção definitiva.
- Considerar autenticação no endpoint `/arkad/sinais` (atualmente sem auth).

---

## 8. Dependências

```
streamlit      # Dashboard web
pandas         # DataFrames
plotly         # Gráficos
openpyxl       # Leitura de Excel (Apostas_*.xlsx)
requests       # Chamadas HTTP (FutPython API)
fastapi        # API server
uvicorn        # ASGI server
```

Ambiente: `conda activate streamlit_env`

---

## 9. Variáveis de Ambiente Necessárias

| Variável | Uso |
|---|---|
| `FUTPYTHON_TOKEN` | Token de autenticação da API FutPython (Bet365/Betfair) |

---

## 10. Convenções do Projeto

- Planilhas operacionais do dia: `Apostas_*.xlsx` na **raiz** do repo.
- Relatórios e backtests: sempre em `Arquivados_Apostas_Diarias/Relatorios/`.
- IDs de run: `{descricao}_{YYYYMMDD}` (ex: `metodo3c_agressivo_hist_20260417`).
- Colunas obrigatórias no CSV de entrada: `Data_Arquivo`, `Horario_Entrada`, `Liga`, `Jogo`, `Metodo`, `Odd_Base`, `Resultado` (0 ou 1).
