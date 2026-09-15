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

---

## Resultado — 2026-09-15 (rodado depois do commit 9574346)

**Janela A (2026-01-01 → 05/09, 14.132 jogos): 165 células · BH sobre M=165 · 14 PASSA / 59 REPROVA / 92 INCONCLUSIVO.**
As 14 PASSA são todas Correct Score (0x3 Todos/TOP3/TOP2/TOP1, 2x2 TOP1/2/3, 3x3 Todos/TOP1/2/3, 2x0 Todos, 1x3
Todos/TOP3); 2 delas têm reds < 5 (3x3 TOP 1 e TOP 2) → INCONCLUSIVO pela guarda de cauda.
Aberto por sub-período (ROI): **jan–abr +2,6% a +11,0% em todas; mai–jul −3,1% a +2,8%; ago–set −2,2% a +4,4%**,
com N de 20 a 108 fora do jan–abr. Ex.: 0x3 Todos +3,98% (jan–abr, 4.237) → −0,54% (mai–jul, 743) → +0,23%
(ago–set, 600); 2x0 Todos +4,25% → −0,64% → −1,73%; 1x3 Todos +2,59% → −1,73% → −1,16%.

**Janela B (2026-08-01 → 05/09, 2.589 jogos): 72 células · 0 PASSA / 17 REPROVA / 55 INCONCLUSIVO.** Melhor p = 0,139
(CS 1x2 TOP 3, N=108, +2,80%, IC [−2,3; +7,1]).

**Leitura (registrada, não é veredito):** o "2026" que passa é o regime jan–abr de livro cheio de CS já documentado
em `CORRECAO_REGIME_ODD_CS_para_gemini.md`; nenhum mercado de Match Odds, DC, O/U, BTTS ou HT passa em 2026; nenhuma
célula passa na janela de agosto para cá. Nenhuma célula vira candidata a forward por este teste.
Tabelas: `varredura_over/varredura_ranking_lay_2026-09-15_2026.csv` e `..._2026ago.csv`.
