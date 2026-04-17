# Relatorio Comparativo de Teste (Walk-Forward)

- Run ID: wf_20250801_20260416_exec_cfg_pos_alteracoes
- Gerado em: 2026-04-16 22:26:32
- Escopo: somente segmento de teste (sem treino)

## Sintese Executiva

- Atual_Filtrada teve lucro total de 4417.83 vs 2459.08 da Baseline (delta +1958.75).
- Win rate medio: 96.06% vs 91.88% (delta +4.19 p.p.).
- Drawdown max abs: 579.43 vs 781.47 (melhora de 202.04 na filtrada).

## Tabela Comparativa (Teste)

| Estrategia | Folds | Executadas Media | WinRate Medio (%) | Lucro Total | Drawdown Max Abs | Profit Factor Medio |
|---|---:|---:|---:|---:|---:|---:|
| Atual_Filtrada | 8 | 22.50 | 96.06 | 4417.83 | 579.43 | 376.2877 |
| Baseline_Sem_Filtro | 8 | 33.25 | 91.88 | 2459.08 | 781.47 | 1.9978 |

## Ranking de Folds por Robustez (Atual_Filtrada)

Score composto (0-100): 35% Lucro, 25% Drawdown (inverso), 20% WinRate, 15% PF (cap 10), 5% Volume.

| Rank | Fold | Inicio | Fim | Executadas | WinRate (%) | Lucro | Drawdown Abs | PF | Robustez |
|---:|---:|---|---|---:|---:|---:|---:|---:|---:|
| 1 | 8 | 2026-03-23 | 2026-04-07 | 28 | 100.00 | 1025.19 | 0.00 | 999.0000 | 100.00 |
| 2 | 2 | 2025-12-05 | 2025-12-21 | 26 | 100.00 | 904.39 | 0.00 | 999.0000 | 94.51 |
| 3 | 7 | 2026-03-04 | 2026-03-20 | 23 | 100.00 | 771.91 | 0.00 | 999.0000 | 88.16 |
| 4 | 5 | 2026-01-26 | 2026-02-15 | 26 | 96.15 | 743.26 | 176.38 | 5.2139 | 66.38 |
| 5 | 3 | 2025-12-22 | 2026-01-09 | 19 | 94.74 | 389.14 | 224.53 | 2.7331 | 41.25 |
| 6 | 4 | 2026-01-10 | 2026-01-25 | 17 | 94.12 | 348.68 | 193.68 | 2.8003 | 39.32 |
| 7 | 1 | 2025-11-21 | 2025-12-04 | 25 | 96.00 | 116.67 | 579.43 | 1.2014 | 17.35 |
| 8 | 6 | 2026-02-16 | 2026-03-03 | 16 | 87.50 | 118.59 | 255.60 | 1.3532 | 14.30 |

## Observacoes de Qualidade

- O segmento de teste da Atual_Filtrada ficou positivo em todos os folds desta execucao.
- Valores de PF=999 em folds sem perdas foram mantidos na tabela e capados no score para evitar distorcao.
- Este relatorio evita vies de treino por usar somente metricas de teste.