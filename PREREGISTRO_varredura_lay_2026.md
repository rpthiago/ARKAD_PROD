# Pré-registro — os 43 mercados de lay × 4 cortes, SÓ com jogos de 2026 (grade congelada)

**Congelado em:** 2026-09-15, antes de rodar. **Proposta:** Thiago ("podemos tentar montar esses mesmos métodos,
apenas olhando os jogos desse ano"). Harness: `varredura_ranking_lay_todos_mercados.py --desde 2026-01-01`
(mesma grade, mesmas faixas, mesmos cortes de 13/09 — só a janela muda). Base: `.cache_base_betfair.csv`.

## 1. Mecanismo / motivo

A varredura por período (15/09, `varredura_over/gap_por_periodo_43_mercados_2026-09-15.csv`) mostrou a mediana do
gap WR−BE dos 43 mercados indo de −2,3pp (2025) para −1,6pp (2026 jan–jul) e −1,1pp (2026 ago–set), com 26 de 39
mercados melhorando. Hipótese a testar: **em 2026 algum mercado de lay cru (ou seu ranking TOP k) passa a ter
edge**. Hipótese alternativa já registrada: a melhora é regime de odd da base (spread de lay mais apertado), uniforme,
e não cria edge em nenhum mercado.

**Já morreu nessa direção:** varredura completa 2024–2026 (13/09): 5 PASSA / 96 REPROVA / 67 INCONCLUSIVO, e os 5 PASSA
eram CS do regime jan–abr/2026 (`RELATORIO_VARREDURA_43_MERCADOS_E_REGIME_para_gemini.md`).

## 2. A grade (idêntica à de 13/09)

- 43 mercados de lay (Match Odds, DC, O/U FT e HT, BTTS, CS exato, Any Other) × cortes Todos / TOP 3 / TOP 2 / TOP 1.
- Faixas fixas: MO/DC 1,5–10 · O/U e BTTS 1,3–6 · CS 5–40 · Any Other 10–80. Nenhum outro filtro.
- Célula entra com **N ≥ 100**. P&L: liability 1u, comissão 5%.
- **Janela A:** 2026-01-01 → fim da base (05/09/2026). **Janela B (reportada, não decide):** 2026-08-01 → fim.

## 3. Critério (3 vias, igual ao de 13/09)

- PASSA: sobrevive ao BH-FDR q=0,05 sobre M = nº de células com N ≥ 100 **e** piso do IC95 (bootstrap bloco-dia,
  ≥ 6 dias) > 0 **e** reds ≥ 5.
- REPROVA: teto do IC95 < 0. INCONCLUSIVO: resto.
- Uma célula que PASSA em 2026 **não vira método**: vira candidata a forward com odd real do coletor (Lei 5), com
  pré-registro próprio e julgamento em jogos com KO ≥ data desse pré-registro.

## 4. O que NÃO fazer

- Não mexer em faixa, corte ou N mínimo depois de ver a tabela. Não somar células. Não escolher a janela depois
  de ver o resultado (a janela A é a decisão; a B é leitura).
- Não usar odd de back. Não tratar "melhora do gap" como edge: edge é piso do IC95 > 0.
