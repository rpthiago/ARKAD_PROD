# ARKAD_PROD

## Benchmark travado (fidelidade ao recorte historico 97/96)

Objetivo: reproduzir exatamente o recorte de referencia do commit historico e validar se o replay bate o mesmo Win Rate.

### Rodar em 1 linha

```powershell
conda run -n streamlit_env python _benchmark_fidelidade_97.py
```

### Rodar o universo 97/96 em 1 linha

```powershell
conda run -n streamlit_env python _rodar_universo_97_96.py
```

### Passo a passo curto

1. Execute o comando unico acima na raiz do repositorio.
2. Abra o resumo em [Arquivados_Apostas_Diarias/Relatorios/Backtest_Abril_2026/Benchmark_97/benchmark97_report.json](Arquivados_Apostas_Diarias/Relatorios/Backtest_Abril_2026/Benchmark_97/benchmark97_report.json).
3. Confirme que wr_replay_igual_referencia esta como true.

### Arquivos gerados

- [Arquivados_Apostas_Diarias/Relatorios/Backtest_Abril_2026/Benchmark_97/referencia_eb4e32e_abril_cenario_B_filtros.csv](Arquivados_Apostas_Diarias/Relatorios/Backtest_Abril_2026/Benchmark_97/referencia_eb4e32e_abril_cenario_B_filtros.csv)
- [Arquivados_Apostas_Diarias/Relatorios/Backtest_Abril_2026/Benchmark_97/referencia_eb4e32e_config_backtest_exec.json](Arquivados_Apostas_Diarias/Relatorios/Backtest_Abril_2026/Benchmark_97/referencia_eb4e32e_config_backtest_exec.json)
- [Arquivados_Apostas_Diarias/Relatorios/Backtest_Abril_2026/Benchmark_97/replay_eb4e32e_cfg_commit_summary.json](Arquivados_Apostas_Diarias/Relatorios/Backtest_Abril_2026/Benchmark_97/replay_eb4e32e_cfg_commit_summary.json)
- [Arquivados_Apostas_Diarias/Relatorios/Backtest_Abril_2026/Benchmark_97/delta_atual_vs_ref_linhas_adicionadas.csv](Arquivados_Apostas_Diarias/Relatorios/Backtest_Abril_2026/Benchmark_97/delta_atual_vs_ref_linhas_adicionadas.csv)
- [Arquivados_Apostas_Diarias/Relatorios/Backtest_Abril_2026/Benchmark_97/delta_atual_vs_ref_linhas_removidas.csv](Arquivados_Apostas_Diarias/Relatorios/Backtest_Abril_2026/Benchmark_97/delta_atual_vs_ref_linhas_removidas.csv)