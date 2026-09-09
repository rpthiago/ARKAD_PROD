Olá, Claude!

Seguindo a nossa diretriz de **testar empiricamente em vez de teorizar**, criamos o coletor in-play para o **Under Limite**: `coletor_inplay_min80.py`.

O objetivo é monitorar partidas ao vivo na janela do **minuto 78 a 83**, capturar as estatísticas profundas acumuladas (xG total, chutes no alvo, toques na área, grandes chances) e liquidar no apito final (`gol_tardio = 1` se saiu gol após os 80' [RED], ou `0` se não saiu [GREEN]). Isso nos dará a primeira base empírica com centenas de jogos para saber se a pressão aos 80' realmente prevê o gol tardio.

O script `coletor_inplay_min80.py` já está na raiz do repositório e foi adaptado para ler a chave da RapidAPI tanto de `.rapidapi_key`, `~/.rapidapi_key` quanto do `alerta.env` da VPS.

---

### Tarefas para você na VPS:

1. **Atualizar o repositório:**
   ```bash
   git pull origin main
   ```

2. **Validar a execução:**
   Rode uma checagem rápida no ambiente da VPS com o venv:
   ```bash
   python3 coletor_inplay_min80.py --once
   ```
   Verifique se a chave da RapidAPI em `alerta.env` é carregada com sucesso e se o arquivo `inplay_min80_log.csv` é inicializado.

3. **Criar o serviço systemd (`inplay-min80.service`):**
   Crie `/etc/systemd/system/inplay-min80.service` (ajuste o caminho do venv/diretório conforme o padrão que você usou no `xg-ht` e `radar-ht`):
   ```ini
   [Unit]
   Description=ARKAD Coletor In-Play Minuto 80 (Under Limite)
   After=network.target

   [Service]
   Type=simple
   User=root
   WorkingDirectory=/root/ARKAD_PROD
   ExecStart=/root/ARKAD_PROD/venv/bin/python3 -u coletor_inplay_min80.py
   Restart=always
   RestartSec=30

   [Install]
   WantedBy=multi-user.target
   ```

4. **Habilitar e Iniciar o Serviço:**
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now inplay-min80
   sudo systemctl status inplay-min80
   ```

5. **Verificar os logs iniciais:**
   Confirme se o serviço está `active (running)` via `journalctl -u inplay-min80 -n 20 --no-pager`.

Ao concluir, adicione uma entrada no topo do `worklog.md` registrando a ativação do serviço `inplay-min80` na VPS!
