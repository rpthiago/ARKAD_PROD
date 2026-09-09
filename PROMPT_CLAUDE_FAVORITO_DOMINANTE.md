Olá, Claude!

Seguindo o rigor do **GEMINI.md**, antes de qualquer execução congelamos o pré-registro em `PRE_REGISTRO_FAVORITO_DOMINANTE.md` e criamos o script observacional `tracker_favorito_dominante_inplay.py` (status: `OBSERVACAO_STAKE_ZERO`).

### 1. A Tese do Método
Entrar em Back Favorito no 0x0 a odd esmagada (1.30–1.40) é um desastre matemático (break-even > 75%). No entanto, há duas anomalias comportamentais onde a odd se expande para **$\ge 1.70$ até $2.60$**:
1. **Trailing Favorite (Desvantagem):** O favorito pré-jogo toma um gol acidental no 1T (0x1 ou 1x2 entre 30' e 70'), fazendo a odd de Back disparar para 2.00–2.50.
2. **Empate Tardio (Drawn Favorite):** Jogo empatado em 0x0 ou 1x1 entre os 40' e 65', expandindo a odd para 1.75–1.95.

O gatilho só dispara se as estatísticas ao vivo da Opta comprovarem que o resultado parcial é enganoso (amassamento real):
- $\text{xG}_{\text{fav}} \ge 1.00$
- $\text{xG}_{\text{fav}} \ge 2.5 \times \text{xG}_{\text{zebra}}$
- $\text{Chutes no Alvo}_{\text{fav}} \ge 3$
- $\text{Toques na Área}_{\text{fav}} \ge 15$

---

### 2. Engenharia do Script (`tracker_favorito_dominante_inplay.py`)
Já incorporamos todos os consertos que você fez no coletor anterior:
- **Liquidação segura:** Usa a agenda do dia (`get-matches-by-date`), consultando todos os placares em 1 requisição no encerramento (sem re-consultar pendentes individualmente).
- **Anti Zero-Fill (Lei nº 6):** Se a liga não tem cobertura Opta, o jogo é sumariamente ignorado.
- **Orçamento blindado:** Intervalo de 180s e teto próprio de 200 reqs/dia com sleep.
- **Compatibilidade de ambiente:** O `load_key()` já lê direto do `alerta.env`, `.rapidapi_key` ou `~/.rapidapi_key`.
- **Log Central:** Registra entradas e liquidações em `favorito_dominante_log.csv`.

---

### 3. O que precisamos de você na VPS:

1. **Auditoria do Pré-Registro:**
   - Dê uma olhada no `PRE_REGISTRO_FAVORITO_DOMINANTE.md`. Você vê algum vício de seleção nos thresholds ($xG \ge 1.00$, razão $2.5\times$, toques na área $\ge 15$)?
   - Como estamos em stake zero, a estimativa de odd in-play (2.30 para perdendo, 1.85 para empatado) serve como baseline honesto de observação?
2. **Implantação na VPS:**
   - O arquivo `tracker_favorito_dominante_inplay.py` já está no repo. Basta sincronizá-lo para `/home/ubuntu/betfair-collector`.
   - Teste com:
     ```bash
     python3 tracker_favorito_dominante_inplay.py --once
     ```
   - Se aprovar, pode rodar via systemd (`favorito-dominante.service`) ou em background com nohup para acumular dados passivos ao lado do `xg-ht` e do `inplay-min80`.
3. **Registro:**
   - Ao subir, registre uma entrada breve no `worklog.md` com a sua leitura do pré-registro.
