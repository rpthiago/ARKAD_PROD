# SPEC — Download Betfair Historic Data (BASIC grátis) p/ backtestar Lay 0x1 min60

Objetivo: obter a **odd in-play REAL** ao longo do jogo (1 min de granularidade, tier BASIC
grátis) pra medir de verdade o Lay 0x1 saindo no minuto 60 — hoje "não-testável". A Betfair
NÃO tem mercado de escanteio, então isto cobre **placar/gol** (Lay 0x1, Lay 0x0, Over 0.5 live),
não canto.

Site: **https://historicdata.betfair.com** (logar com a conta Betfair; se for conta .br/.com.au
pode redirecionar — usar a que tiver a app key).

---

## FASE 1 — PILOTO (o mínimo pra validar Lay 0x1 min60; download menor)

No menu **Download / Data Extraction**, marcar EXATAMENTE:

| Campo | Valor |
|---|---|
| **Plan** | **Basic** (grátis — conferir £0.00 no basket) |
| **Sport / Event Type** | **Soccer** |
| **Market Types** | **Correct Score** (só isso na fase 1) |
| **From date** | **2025-08-01** |
| **To date** | **2026-07-31** |
| **Countries** | **All** (o método não é liga-específico) |
| **Competitions** | **All** |
| **File format** | arquivos de mercado (.bz2 stream) — padrão |

Passos: aplicar filtros → **Add to basket** → conferir **total £0.00** → checkout → **Download**.
Vem um arquivo grande (`.tar`/vários `.bz2`), um por mercado.

### Por que essas escolhas
- **Correct Score** contém o runner **"0 - 1"** → a odd de LAY dele minuto a minuto é o que o
  Lay 0x1 precisa. Também dá pra **inferir o placar ao vivo** (quais placares ainda são possíveis).
- **12 meses recentes** = amostra suficiente e download menor pra validar rápido o parser + método.
- **Basic (1 min)** basta: decisão é "qual a odd no minuto 60", não scalping de tick.

### Cuidados
- **Confirme £0.00** antes do checkout (Basic é grátis; se cobrar, você filtrou Advanced/Pro).
- Pode haver **limite mensal de download** no Basic → se travar, baixe **mês a mês** (ajuste From/To).
- Tamanho: Soccer Correct Score 12m = alguns GB. Tenha espaço em disco. Se for demais, começe com
  **só 2026-01 a 2026-07** (6 meses) e a gente expande.

---

## FASE 2 — COMPLETO (só se o piloto mostrar sinal)

Adicionar aos filtros:

| Campo | Valor |
|---|---|
| **Market Types** | Correct Score **+ Over/Under 0.5 Goals + Match Odds** |
| **From date** | **2023-01-01** |
| **To date** | **2026-07-31** |

- **Over/Under 0.5** → backtesta o **Over 0.5 live (min 25)** com odd real.
- **Match Odds** → ajuda a inferir estado do jogo (favoritismo/suspensões), arquivos pequenos.
- Período estendido (2023+) → ver o **regime** do edge ao longo do tempo (quando ligou/desligou) —
  o que resolve o medo de "pegar o método já caindo".

---

## O QUE EU FAÇO COM OS ARQUIVOS (parser — já deixo pronto)

O formato é o **stream histórico** (linhas JSON, mensagens MCM). O `betfairlightweight` (que já
usamos no coletor) tem parser nativo. Vou construir `parse_historic_lay0x1.py` que, por mercado
Correct Score:
1. Lê o stream, acha o runner **"0 - 1"**.
2. Marca o **kickoff** (transição para in-play / `marketDefinition.inPlay=true`).
3. Extrai a **odd de lay do "0 - 1"** em marcos: **abertura, min ~45, min 60, min 75, FT**.
4. Infere o **placar** em cada marco (pelas seleções de CS ainda ativas / preço ~colapsado).
5. Monta uma tabela por jogo: `entrada, odd_min60, placar_min60, placar_final` → aí calculo o
   P&L do **trade "entra pré-jogo, sai no min 60"** com odd de saída **REAL**, e comparo com o hold.

Saída: `lay0x1_inplay_backtest.csv` + o veredito honesto (ROI/IC/FDR do trade min60 vs hold).

---

## RESUMO DO QUE VOCÊ FAZ AGORA
1. Logar em historicdata.betfair.com.
2. Marcar os filtros da **FASE 1** (Basic · Soccer · Correct Score · 2025-08→2026-07 · All).
3. Conferir **£0.00**, baixar.
4. Me dizer a pasta onde salvou → eu rodo o parser e te trago o backtest do Lay 0x1 min60.
