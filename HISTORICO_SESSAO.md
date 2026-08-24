# Histórico da Sessão ARKAD — o que foi feito (para memória / Gemini)
> Data: 2026-08-24. Trabalho conjunto usuário + Claude. Leia junto com `ARKAD_REGRAS_METODOS.md`
> (as regras) e `GEMINI.md` (cópia). Este arquivo é o REGISTRO do que já foi decidido e construído.

---

## 1. VEREDITOS HONESTOS DOS MÉTODOS (o mais importante — não re-litigar)

Protocolo de validação: **paper trading forward + odd de LAY REAL da Betfair + bootstrap + FDR +
break-even WR**. Walk-forward em base estática ENGANA (usa odd de back inflada + vaza via `.last()`).

Na **odd executável** (a real da Betfair), o quadro é:

| Método | Veredito | Evidência |
|---|---|---|
| Lay 0x0 | **break-even/negativo** | WR ~95% vs break-even ~94% (odd real ~17) = margem ~0. O +P&L é na odd das planilhas (~13, inflada). Paper agosto na odd real ≈ negativo. |
| Lay 0x1 / 1x0 / 2x0 / 0x2 | **negativos** | WR abaixo do break-even no paper de agosto |
| Lay 2x2 | **break-even (variância)** | WR 95% ≈ BE 95%; agosto ~zero; 28 meses walk-forward −8% |
| Lay 0x3 | **FRAUDE do prompt guru** | "31/31 = 100% +R$2.945" era mentira: real 40 jogos, 9 reds, 77,5% WR, **−R$22.205**; break-even 96,7% |
| Lay Draw | **+2%, reprova FDR** | 0 sinais em ago (seletivo). Backtest do Gemini de +R$79k era **IN-SAMPLE** (modelo treinado em TODOS os dados, incl. 2026). OOS real = +3,5% na odd de back → **−2% na odd de lay** |
| Saldo Menor, Over 2.5, EH+3 múltiplas, escanteios, Over 1.5 ML, HT scans | **mortos** | negativos/circular na odd real |

**Candidato/observação IN-PLAY** (com o coletor Betfair): **Under-no-limite** (pré-registrado,
~2 fins de semana; estado 0-0 já oscilou +32% → −6% = instável, acompanhar) e **Back BTTS No**.

**CONCLUSÃO CENTRAL:** nenhum método **pré-jogo** separa claramente do break-even na odd real.
**Consertar código NÃO cria edge** — só faz o live parar de mentir vs o backtest.

---

## 2. BUGS ENCONTRADOS E CORRIGIDOS

### Lay Draw (auditoria de fidelidade backtest↔live) — CORRIGIDOS:
1. Base errada no live (`Resultados_2026_Full.csv` sem xGOT/BigChances/Possession → 12 features viravam 0.0) → passou a usar base com features ricas.
2. `h2h_draw_rate` sempre NaN→0.0 (feature do modelo nunca computada) → agora computada (par ordenado, rolling 8/min 2).
3. Cutoff de data **hardcoded** `< "2026-08-01"` → dinâmico (`< data do 1º jogo`).
4. Features FABRICADAS p/ time desconhecido (0.35/0.25/0.28) → **SKIP**.
5. `fillna(0.0)` no live vs `dropna` no treino → **SKIP** se qualquer feature NaN.
6. Filtro de liga permissivo em NaN → skip (LIGA_FORA_UNIVERSO).
7. **Odd de BACK no backtest** vs LAY no live → PENDENTE: re-treinar na odd de lay (o ROI do backtest está inflado enquanto isso).

### ⚠️ BUG QUE O GEMINI INTRODUZIU ao reescrever o lay_draw (CORRIGIR):
- As features viraram **"sem mando" (venue-agnostic)**: juntou casa+fora num rolling só e mandou o mesmo valor pros dois slots. O modelo é **por-mando** (`H_h_*` = jogos EM CASA, `A_a_*` = FORA — ver trainer, comentário "split H/A"). Isso diverge do modelo treinado. **Voltar ao split H/A separado.**
- Re-fabricou valores que dizia ter removido: `h2h → draw_rate_mean` e `liga → 0.26` (deixar NaN→SKIP).

### Base desatualizada (Cloud ≠ local) — CORRIGIDO:
- A base completa (`Bases_de_Dados_API_FutPythonTrader_Bet365.csv`, ~229 MB) está no `.gitignore` (grande demais pro GitHub) → o **Streamlit Cloud** usava a `b365_base_lean.csv`, que estava **parada em 05/07**. Resultado: Cloud gerava sinais com features de 6 semanas → **diferentes do local**.
- FIX: sincronizar a lean com a full (até 21/08). **Provado A/B**: os 6 métodos ML geram **idêntico** em Cloud e local.
- Manutenção: `baixar_base_completa.py` + `atualizar_lean_base.py` + push. Rotina semanal agendada.

---

## 3. PIPELINE DE PAPER TRADING (construído, 100% local, sem Google/download)

```
Agendador → gerar_sinais_local.py → consolidar_sinais.py → paper_consolidado.csv → Página Resultados
                (8 métodos)          (uniformiza + placar)      (schema único)         (WR vs BE, ROI)
```

- **gerar_sinais_local.py**: gera os 8 métodos (6 ML via `predict_and_evaluate_live` + 0x3/2x2 de regra) → `sinais_gerados/sinais_gerados_<data>.xlsx`.
- **consolidar_sinais.py**: uniformiza TODAS as planilhas (formatos diferentes) num `paper_consolidado.csv`; **placar em 3 fontes**: (1) base histórica Bet365 (exato+fuzzy, cobre passado), (2) **coletor Betfair na VPS** (recente, placar = runner CS de menor lay no fim), (3) `placares_manuais.xlsx` (Excel manual). Corte a partir de **2026-08-09** (início da coleta). Nomes de método limpos sempre.
- **placares_manuais.xlsx**: os pendentes são exportados pra esse Excel (cols `Gols_M`/`Gols_V` vazias); o usuário preenche à mão, salva, roda de novo. O sistema **preserva o que foi digitado** e só acrescenta novos pendentes.
- **preencher_placares.py**: preenche uma planilha avulsa (versão single).
- **Página 20** (ARKAD): lê `paper_consolidado.csv` — por método (WR, odd, **break-even %**, margem, ROI, P&L), curva acumulada, P&L/dia, download Excel, pendentes.
- **DASHBOARD** (mesmo esquema): `rodar_ao_vivo.py` (gera, com odd de lay real via `aplicar_odds_lay`, grava `paper_trading_real.csv`) + `preencher_paper_real.py` (placar) → **Página 33**.

Estado atual (paper 09-24/08, na odd das planilhas): 285/312 com placar (91%). 0x0 +9,7k, 0x3 +2,5k,
2x2 +90 (todos colados no break-even = mirage de odd); 0x1/1x0/2x0/0x2 negativos. **Lay Draw = 0 sinais** (seletivo demais no período).

---

## 4. TAREFAS AGENDADAS (Windows Task Scheduler)

| Tarefa | Quando | Faz |
|---|---|---|
| `ARKAD_base_semanal` | Segunda 08:00 | baixa base full (API) → sync lean → git push |
| `ARKAD_paper` | Diário 09:00 | gerar_sinais_local → consolidar → git push do CSV |
| `DASHBOARD_paper` | Diário 09:10 | rodar_ao_vivo → preencher_paper_real → git push do CSV |

Os `.bat` fazem `git add/commit/push` do CSV no fim → o Streamlit Cloud fica em dia sozinho.

---

## 5. ARQUIVOS CRIADOS/ALTERADOS

**ARKAD_PROD:** `gerar_sinais_local.py`, `consolidar_sinais.py`, `preencher_placares.py`,
`baixar_base_completa.py`, `atualizar_lean_base.py`, `atualizar_paper.bat`, `atualizar_base_semanal.bat`,
`pages/20_📊_Resultados_Paper.py`, `ARKAD_REGRAS_METODOS.md`, `GEMINI.md`, `HISTORICO_SESSAO.md`,
`hist_rf_loader.py`, `lay_draw_rf_v2_strategy.py` (fixes de fidelidade), `b365_base_lean.csv` (sincronizada até 21/08).

**DASHBOARD_ARKAD-1:** `preencher_paper_real.py`, `pages/33_📊_Resultados_Paper.py`, `atualizar_paper.bat`,
`ARKAD_REGRAS_METODOS.md`, `paper_log_real.py`, `rodar_ao_vivo.py` (aplica odd real + log central),
`4 estratégias CS` (removido log_paper_trade b365), skill `testar-metodo` (agora paper forward).

---

## 6. REGRAS (playbook, já no repo: ARKAD_REGRAS_METODOS.md / GEMINI.md)
- **5 leis:** (1) odd de lay REAL, nunca back; (2) backtest = live (mesmo objeto); (3) nunca fabricar feature (sem dado → SKIP); (4) matemática do lay + break-even WR; (5) sinal ≠ edge.
- **Validação = PAPER FORWARD**, não walk-forward.
- **Hall of shame** dos bugs reais. **Checklist** backtest↔live. **Deploy** Streamlit.

---

## 7. MANUTENÇÃO / PENDÊNCIAS
- Base atualiza sozinha **toda segunda** (tarefa semanal). Se atualizar a full à mão, rode `atualizar_lean_base.py` + push.
- **CORRIGIR** o bug de mando (venue-agnostic) que o Gemini pôs no `lay_draw_rf_v2_strategy.py` do live → voltar ao split H/A do trainer; e tirar os fallbacks `h2h→média` e `liga→0.26`.
- **Re-treinar Lay Draw na odd de lay** (o backtest usa back = inflado).
- Não commitar os 65 MB da lean **todo dia** (só semanal — evita inchar o repo; se crescer, migrar p/ Git LFS).
- 27 jogos ainda pendentes no `placares_manuais.xlsx` (opcional preencher).
- DASHBOARD: aplicar os 3 pendentes de lá (opcional).
- **NÃO** ressuscitar métodos mortos com prompt guru (odd de back, p-value vs 50%, backtest in-sample). Rodar sempre pelo protocolo honesto.
