#!/usr/bin/env bash
# instalar_coletor.sh — RODA NO HOST NOVO (fora do Brasil), Ubuntu 22.04+ com usuario `ubuntu`.
# Instala o coletor a partir do coletor_pacote.tar.gz gerado por empacotar_coletor.sh.
#
#   bash instalar_coletor.sh [caminho-do-pacote]
#
# Antes de rodar: confirme com `bash testar_acesso_betfair.sh` que ESTE host enxerga a Betfair.
set -eu
PACOTE="${1:-$HOME/coletor_pacote.tar.gz}"
BASE=/home/ubuntu/betfair-collector
[ -f "$PACOTE" ] || { echo "nao achei $PACOTE"; exit 1; }

echo "=== 0. este host enxerga a Betfair? ==="
if [ -f "$(dirname "$0")/testar_acesso_betfair.sh" ]; then
  bash "$(dirname "$0")/testar_acesso_betfair.sh" | sed 's/^/   /'
  echo "   (se aparecer BLOQUEADO acima, pare aqui — mudar de host nao resolveu)"
  read -r -p "   continuar mesmo assim? [s/N] " r; [ "${r:-N}" = "s" ] || exit 1
fi

echo "=== 1. dependencias do sistema ==="
sudo apt-get update -qq
sudo apt-get install -y -qq python3-venv python3-pip build-essential >/dev/null

echo "=== 2. desempacotando ==="
mkdir -p "$BASE" "$HOME/certs"
TMP=$(mktemp -d); tar -xzf "$PACOTE" -C "$TMP"
cp -rp "$TMP/betfair-collector/." "$BASE/"
[ -d "$TMP/certs" ] && cp -p "$TMP/certs/." "$HOME/certs/" && chmod 600 "$HOME"/certs/*

echo "=== 3. ajustando caminhos dos certificados no alerta.env ==="
if [ -f "$BASE/alerta.env" ]; then
  for k in BETFAIR_CERT_CRT_PATH BETFAIR_CERT_PEM_PATH; do
    atual=$(grep "^$k=" "$BASE/alerta.env" | cut -d= -f2- || true)
    [ -n "$atual" ] && novo="$HOME/certs/$(basename "$atual")" && \
      sed -i "s|^$k=.*|$k=$novo|" "$BASE/alerta.env" && echo "   $k -> $novo"
  done
  chmod 600 "$BASE/alerta.env"
fi

echo "=== 4. ambiente python ==="
python3 -m venv "$BASE/venv"
"$BASE/venv/bin/pip" install -q --upgrade pip
if [ -f "$TMP/requirements.txt" ]; then
  "$BASE/venv/bin/pip" install -q -r "$TMP/requirements.txt" || \
    "$BASE/venv/bin/pip" install -q betfairlightweight pandas requests
else
  "$BASE/venv/bin/pip" install -q betfairlightweight pandas requests
fi

echo "=== 5. servicos ==="
if [ -d "$TMP/systemd" ]; then
  sudo cp -p "$TMP/systemd/"*.service /etc/systemd/system/ 2>/dev/null || true
  sudo systemctl daemon-reload
fi

echo "=== 6. teste de login (sem subir servico) ==="
cd "$BASE"
set +e
set -a; . ./alerta.env; set +a
./venv/bin/python - <<'PY'
import sys; sys.path.insert(0, "/home/ubuntu/betfair-collector")
try:
    import coletar_betfair_direto as C
    t = C.login()
    f = t.account.get_account_funds()
    print("LOGIN OK — saldo disponivel: %.2f" % f.available_to_bet_balance)
except Exception as e:
    print("LOGIN FALHOU: %s" % str(e)[:200])
    print("Se a mensagem citar HTML/bloqueio, este host tambem esta bloqueado.")
    print("Se citar geo/conta, a Betfair BR nao aceita login deste pais — ai mudar de host nao resolve.")
PY
set -e

echo
echo "=== o que falta, se o login acima deu OK ==="
echo "  1) sudo systemctl enable --now betfair-collector"
echo "  2) crontab: copie as linhas de $TMP/crontab.txt (parada 03:30 e volta 07:00 UTC)"
echo "  3) suba os demais servicos conforme precisar:"
echo "     sudo systemctl enable --now liquidador-betfair sinais-ko"
echo "  4) traga o historico grande da VPS antiga, SEM saturar a rede:"
echo "     scp -l 24000 -i <chave> ubuntu@163.176.59.215:/home/ubuntu/betfair-collector/betfair_live_odds.csv $BASE/"
echo "  5) atualize o IP nos scripts locais do PC (relatorio das 06:00, backups, este diretorio)."
rm -rf "$TMP"
