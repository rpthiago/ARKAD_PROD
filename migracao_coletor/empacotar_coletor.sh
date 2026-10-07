#!/usr/bin/env bash
# empacotar_coletor.sh — RODA NA VPS ATUAL (Sao Paulo). Gera um pacote com tudo que o coletor precisa
# para subir em outro host: codigo, certificados, variaveis, units do systemd e o crontab.
# NAO inclui o betfair_live_odds.csv (3,1 GB) — esse vai separado, por scp com limite de banda.
#
#   bash empacotar_coletor.sh
#   -> /tmp/coletor_pacote.tar.gz
set -eu
BASE=/home/ubuntu/betfair-collector
SAIDA=/tmp/coletor_pacote.tar.gz
TMP=$(mktemp -d)

echo "montando pacote..."
mkdir -p "$TMP/betfair-collector" "$TMP/systemd"

# 1) codigo (scripts .py do diretorio, sem backups nem caches)
cd "$BASE"
for f in *.py; do
  case "$f" in *.bak*|*pycache*) continue;; esac
  cp -p "$f" "$TMP/betfair-collector/" 2>/dev/null || true
done

# 2) credenciais e certificados (o alerta.env aponta os caminhos dos certs)
cp -p alerta.env "$TMP/betfair-collector/" 2>/dev/null || echo "  [aviso] alerta.env nao encontrado"
# shellcheck disable=SC1091
set +u; . "$BASE/alerta.env" 2>/dev/null || true; set -u
for v in "${BETFAIR_CERT_CRT_PATH:-}" "${BETFAIR_CERT_PEM_PATH:-}"; do
  [ -n "$v" ] && [ -f "$v" ] && { mkdir -p "$TMP/certs"; cp -p "$v" "$TMP/certs/"; echo "  cert: $(basename "$v")"; }
done

# 3) estado que vale levar (mapas e caches pequenos; o historico grande vai separado)
for f in market_map.csv fotmob_map.csv fotmob_ligas.csv xg_ft_ligas.csv alerta_under_seen.txt \
         forward_ko_ledger.csv placares_ft.csv under_alertas_log.csv xg_ht_log.csv xg_ft_backfill.csv; do
  [ -f "$f" ] && cp -p "$f" "$TMP/betfair-collector/" && echo "  estado: $f"
done

# 4) units do systemd e crontab
for u in betfair-collector liquidador-betfair sinais-ko trader-inplay saida-zebra late-goal \
         inplay-min80 xg-ht favorito-dominante; do
  [ -f "/etc/systemd/system/$u.service" ] && cp -p "/etc/systemd/system/$u.service" "$TMP/systemd/"
done
crontab -l > "$TMP/crontab.txt" 2>/dev/null || echo "" > "$TMP/crontab.txt"

# 5) requisitos exatos do venv atual (para o host novo instalar o mesmo)
"$BASE/venv/bin/pip" freeze > "$TMP/requirements.txt" 2>/dev/null || echo "  [aviso] nao consegui ler o venv"

tar -czf "$SAIDA" -C "$TMP" .
rm -rf "$TMP"
echo
echo "-> $SAIDA  ($(du -h "$SAIDA" | cut -f1))"
echo "ATENCAO: o pacote contem SENHA e CERTIFICADOS. Transfira por scp e apague depois."
echo
echo "no seu PC:   scp -i <chave> ubuntu@163.176.59.215:$SAIDA ."
echo "no host novo: scp -i <chave> coletor_pacote.tar.gz ubuntu@<ip-novo>: && bash instalar_coletor.sh"
