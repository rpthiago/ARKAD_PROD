# Dossie de Continuidade - Pesquisa de Metodos (17/04/2026)

## Objetivo
Comparar 5 metodos de stake no mesmo dataset e manter material pronto para retomada.

## Dataset e Escopo
- Dataset: pacote_reproducao_todos_jogos_20250801_20260414.csv
- Entradas por metodo: 357
- Win rate observado em todos os metodos: 97.76%

## Metodos Avaliados
- Juros_Rampa: config_backtest_exec.json
- Progressiva_Fixa500: config_backtest_resp_fixa_500.json
- Metodo3_Hibrido: config_backtest_metodo3_hibrido.json
- Metodo3_B_Defensivo: config_backtest_metodo3b_defensivo.json
- Metodo3_C_Agressivo: config_backtest_metodo3c_agressivo.json

## Resultado Base (Lucro/DD)
- Progressiva_Fixa500: Lucro=17529.29 | DD_abs=747.34 | DD%=4.26 | Lucro/DD=23.4556
- Metodo3_B_Defensivo: Lucro=28250.73 | DD_abs=1232.01 | DD%=4.36 | Lucro/DD=22.9306
- Metodo3_Hibrido: Lucro=30946.53 | DD_abs=1518.23 | DD%=4.91 | Lucro/DD=20.3833
- Metodo3_C_Agressivo: Lucro=46933.34 | DD_abs=2611.82 | DD%=5.56 | Lucro/DD=17.9696
- Juros_Rampa: Lucro=50635.31 | DD_abs=2861.34 | DD%=5.65 | Lucro/DD=17.6964

## Campeoes por Regua
- Regua 50/30/20 (Lucro/DD/PiorLinha): Metodo3_C_Agressivo (Score=0.585331)
- Regua Conservadora 35/45/20: Progressiva_Fixa500 (Score=0.650000)
- Regua Agressiva 65/20/15: Metodo3_C_Agressivo (Score=0.680296)

## Leitura Rapida
- Perfil conservador: Progressiva_Fixa500 lidera.
- Perfil equilibrado (50/30/20): Metodo3_C_Agressivo lidera.
- Perfil agressivo: Metodo3_C_Agressivo lidera; Juros_Rampa aparece forte na segunda posicao.

## Arquivos para abrir primeiro
- grid_decisao_5_metodos_20260417.csv
- grid_decisao_5_metodos_custom_50_30_20_20260417.csv
- grid_decisao_5_metodos_conservadora_35_45_20_20260417.csv
- grid_decisao_5_metodos_agressiva_65_20_15_20260417.csv
- grid_decisao_5_metodos_duas_reguas_20260417.csv
- manifest_estudo_20260417.json

## Proximos Passos Sugeridos
1. Rodar stress test por subperiodos (mensal e trimestral) para estabilidade.
2. Testar sensibilidade do Metodo3_C_Agressivo em step_up_target e compound_limit.
3. Validar impacto de perdas sequenciais com simulacao de cenarios adversos.
4. Definir regra de corte operacional para troca automatica entre M3-B e M3-C.

## Comandos de Reproducao
1. Juros_Rampa: python engine_ciclo_producao.py --input <csv> --config config_backtest_exec.json --environment historico --output-dir Arquivados_Apostas_Diarias/Relatorios/Comparativo_Juros_vs_Progressiva --run-id juros_rampa_hist_20260417 --skip-mini-report
2. Progressiva_Fixa500: python engine_ciclo_producao.py --input <csv> --config config_backtest_resp_fixa_500.json --environment historico --output-dir Arquivados_Apostas_Diarias/Relatorios/Comparativo_Juros_vs_Progressiva --run-id progressiva_fixa_hist_20260417 --skip-mini-report
3. Metodo3_Hibrido: python engine_ciclo_producao.py --input <csv> --config config_backtest_metodo3_hibrido.json --environment historico --output-dir Arquivados_Apostas_Diarias/Relatorios/Comparativo_Juros_vs_Progressiva --run-id metodo3_hibrido_hist_20260417 --skip-mini-report
4. Metodo3_B_Defensivo: python engine_ciclo_producao.py --input <csv> --config config_backtest_metodo3b_defensivo.json --environment historico --output-dir Arquivados_Apostas_Diarias/Relatorios/Comparativo_Juros_vs_Progressiva --run-id metodo3b_defensivo_hist_20260417 --skip-mini-report
5. Metodo3_C_Agressivo: python engine_ciclo_producao.py --input <csv> --config config_backtest_metodo3c_agressivo.json --environment historico --output-dir Arquivados_Apostas_Diarias/Relatorios/Comparativo_Juros_vs_Progressiva --run-id metodo3c_agressivo_hist_20260417 --skip-mini-report