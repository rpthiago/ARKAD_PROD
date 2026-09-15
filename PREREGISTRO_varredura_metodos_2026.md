# Pré-registro — varredura ampla de MÉTODOS (mercado × lado × condições) só com jogos de 2026

**Congelado em:** 2026-09-15, antes de rodar. **Proposta:** Thiago ("tem mais uma infinita possibilidade de métodos
na Betfair"). A varredura anterior (`PREREGISTRO_varredura_lay_2026.md`) só olhou mercado cru × ranking. Esta cruza
cada mercado com as condições que os métodos do portfólio usam (favoritismo, contexto de gols, lado do favorito),
nos dois lados (lay e back). Harness: `varredura_metodos_2026.py`. Base: `.cache_base_betfair.csv` (odds pré-jogo
da apicomunidade, mesma da varredura anterior — regime de odd de CS jan–abr/2026 continua valendo como ressalva).

## 1. A grade (congelada)

| dimensão | valores |
|---|---|
| runner (43) | H · A · D · 1X · X2 · 12 · O/U 0.5–4.5 FT · O/U 0.5–2.5 HT · BTTS Yes/No · 16 CS exatos · Any Other H/A/D |
| lado (2) | **lay** na `Odd_*_Lay` · **back** na `Odd_*_Back` |
| faixa de odd (fixa por família, igual à varredura de 13/09) | MO/DC 1,5–10 · O/U e BTTS 1,3–6 · CS 5–40 · Any Other 10–80 (as mesmas para back) |
| favoritismo pré-jogo = min(Odd_H_Back, Odd_A_Back) | Todos · ≤1,40 · 1,40–1,80 · >1,80 |
| contexto de gols = Odd_Under25_FT_Back | Todos · ≤1,60 (jogo under) · 1,60–2,20 · >2,20 (jogo over) |
| lado do favorito | Todos · favorito em casa · favorito fora |
| corte de ranking (menor odd do runner no dia, dentro da célula) | Todos · TOP 3 |

43 × 2 × 4 × 4 × 3 × 2 = **8.256 células** possíveis. Só entram na tabela células com **N ≥ 100**. Nenhum filtro
de liga, stat, xG ou hora.

## 2. Avaliação

- **Lay:** liability 1u, comissão 5% → GREEN 0,95/(odd−1), RED −1; BE = (odd−1)/(odd−0,05).
- **Back:** stake 1u, comissão 5% → WIN 0,95·(odd−1), LOSE −1; BE = 1/(1+0,95·(odd−1)).
- RED/LOSE pelo placar FT/HT da base (mesmas funções de evento da varredura de 13/09).
- IC95 por **bootstrap de bloco por dia** (≥ 6 dias): **2.000 reamostragens na varredura**, **10.000** re-rodadas
  nas células que sobreviverem. p unilateral (ROI ≤ 0); ROI negativo entra com p = 1.
- **BH-FDR q = 0,05 sobre M = nº de células com N ≥ 100, todas de uma vez** (lay e back juntos).
- Decisão: PASSA (BH **e** piso IC95 > 0 **e** reds/loses ≥ 5) · REPROVA (teto IC95 < 0) · INCONCLUSIVO.

## 3. Janelas

- **Janela A (decide):** 2026-01-01 → fim da base (05/09/2026).
- **Janela B (só leitura):** 2026-08-01 → fim da base.
- Toda célula PASSA da janela A é aberta em jan–abr / mai–jul / ago–set, para separar regime de odd de edge.

## 4. O que NÃO fazer

- Não acrescentar dimensão, faixa ou corte depois de ver a tabela. Não somar células. Não escolher janela depois.
- Uma célula que PASSA **não vira método**: vira candidata a forward na odd real do coletor, com pré-registro
  próprio e julgamento em jogos com KO ≥ data desse pré-registro (Lei 5).
- Não usar odd de back para julgar lay nem vice-versa (Lei 1).
