#!/usr/bin/env bash
# instalar_proxy_vps.sh — transforma um VPS novo num proxy HTTP com senha, para o coletor usar
# no lugar do iProyal. Roda NO VPS NOVO (Ubuntu 22.04/24.04), como ubuntu ou root.
#
#   bash instalar_proxy_vps.sh <usuario> <senha> [porta]
#
# Depois disso, o BETFAIR_PROXY do coletor vira:
#   http://<usuario>:<senha>@<IP_DO_VPS>:<porta>
#
# Por que 3proxy e nao squid: binario pequeno, config de 6 linhas, e nao reescreve cabecalho
# nem insere Via/X-Forwarded-For por padrao — que e o que faz um proxy ser detectado.
set -eu

USUARIO="${1:?uso: bash instalar_proxy_vps.sh <usuario> <senha> [porta]}"
SENHA="${2:?falta a senha}"
PORTA="${3:-13128}"

echo "=== 1. dependencias ==="
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential wget >/dev/null

echo "=== 2. compilando o 3proxy ==="
cd /tmp
rm -rf 3proxy-0.9.4 3proxy.tar.gz
wget -q https://github.com/3proxy/3proxy/archive/refs/tags/0.9.4.tar.gz -O 3proxy.tar.gz
tar xzf 3proxy.tar.gz
cd 3proxy-0.9.4
make -f Makefile.Linux >/dev/null 2>&1
sudo mkdir -p /opt/3proxy/bin /etc/3proxy
sudo cp bin/3proxy /opt/3proxy/bin/

echo "=== 3. configuracao ==="
sudo tee /etc/3proxy/3proxy.cfg >/dev/null <<CFG
daemon
maxconn 200
nserver 1.1.1.1
nserver 8.8.8.8
nscache 65536
timeouts 1 5 30 60 180 1800 15 60
users ${USUARIO}:CL:${SENHA}
auth strong
allow ${USUARIO}
proxy -n -a -p${PORTA}
flush
CFG
sudo chmod 600 /etc/3proxy/3proxy.cfg

echo "=== 4. servico ==="
sudo tee /etc/systemd/system/3proxy.service >/dev/null <<UNIT
[Unit]
Description=3proxy — proxy HTTP do coletor ARKAD
After=network.target

[Service]
ExecStart=/opt/3proxy/bin/3proxy /etc/3proxy/3proxy.cfg
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
UNIT
sudo systemctl daemon-reload
sudo systemctl enable --now 3proxy

echo "=== 5. firewall ==="
if command -v ufw >/dev/null 2>&1; then
  sudo ufw allow ${PORTA}/tcp >/dev/null 2>&1 || true
fi

sleep 2
echo
echo "=== 6. teste local ==="
IP_SAIDA=$(curl -s -m 15 --proxy "http://${USUARIO}:${SENHA}@127.0.0.1:${PORTA}" https://api.ipify.org || echo "FALHOU")
echo "  IP de saida pelo proxy: ${IP_SAIDA}"
echo
echo "=== 7. a Betfair responde daqui? ==="
for U in "https://api.betfair.com/exchange/betting/rest/v1.0/listEventTypes/" \
         "https://identitysso-cert.betfair.bet.br/api/certlogin"; do
  R=$(curl -s -o /tmp/_p -w "%{http_code}" -m 25 --proxy "http://${USUARIO}:${SENHA}@127.0.0.1:${PORTA}" \
       -X POST -H "Content-Type: application/json" -d "{}" "$U" 2>/dev/null || echo "erro")
  C=$(head -c 90 /tmp/_p 2>/dev/null | tr -d '\n')
  if echo "$C" | grep -qi "just a moment\|attention required\|cloudflare"; then V="BLOQUEADO pelo Cloudflare"
  elif echo "$C" | grep -qi "brasilsembets"; then V="bloqueio da MP"
  elif echo "$C" | grep -qiE '\{|faultcode|error report'; then V="OK — servidor da Betfair respondeu"
  else V="indefinido"; fi
  printf "  %-58s HTTP %-4s %s\n" "${U#https://}" "$R" "$V"
done
echo
echo "Se as duas linhas acima disserem OK, este VPS serve de proxy."
echo "No coletor, troque o .env para:"
echo "  BETFAIR_PROXY=http://${USUARIO}:<senha>@$(curl -s -m 8 https://api.ipify.org):${PORTA}"
