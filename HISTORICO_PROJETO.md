# Histórico do Projeto ARKAD_PROD

> Última atualização: 23/04/2026  
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
| Filtro operacional | `config_rodos_master.json` | Whitelist de combos Liga+Método+Odd permitidos |
| Config produção | `config_prod_v1.json` | Parâmetros ativos em produção |

---

## 2. Histórico de Marcos

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

- **Modo:** `whitelist` — um jogo só aparece como EXECUTED se **bater** em algum filtro.
- **20+ regras** do tipo: Liga + Método + faixa de Odd.
- Cada regra tem `lucro_combo` histórico registrado (negativo = combinação tóxica que foi excluída).
- Exemplo de regra excluída: `SPAIN 1 | Lay_CS_1x0_B365 | Odd 6-8` → lucro histórico: -3.920.
- Origem: gerado a partir de `config_prod_v1.json` em 15/04/2026.

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

### 7.1 Análise do filtro Rodo (próxima sessão)
- **Ponto de parada:** filtro precisa ser reanalisado.
- Verificar se as 20+ regras do `config_rodos_master.json` ainda fazem sentido com os dados atuais.
- Avaliar se o modo whitelist está cortando entradas boas ou deixando passar entradas ruins.
- Possíveis ações:
  - Recalcular `lucro_combo` por regra com dataset mais recente.
  - Testar impacto de remover/adicionar regras individualmente.
  - Comparar Baseline_Sem_Filtro vs Atual_Filtrada no walk-forward.

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
