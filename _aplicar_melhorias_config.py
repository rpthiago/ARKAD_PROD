"""
Aplica as melhorias validadas no backtest comparativo ao config_universo_97.json
Mudancas:
  1. odd_max 0x1: 11.5 -> 13.0
  2. +9 novas ligas 0x1: GERMANY 1, GERMANY 2, ITALY 2, ITALY 3, AUSTRIA 1,
                          SWITZERLAND 1, CZECH 1, FRANCE 3, EUROPA CHAMPIONS LEAGUE
  3. Cap diario 0x1 e 1x0: 4 -> 6
  4. Remove rodo id=11 (ITALY 1 | Lay_CS_1x0 | Odd 10-12) — 100% green em 2026
  5. Atualiza version e data de modificacao
"""
import json
import shutil
from pathlib import Path
from datetime import datetime

CFG_PATH = Path("C:/Users/thiag/OneDrive/Documentos/GitHub/ARKAD_PROD/config_universo_97.json")
BACKUP   = CFG_PATH.with_suffix(".json.bak_antes_melhorias")

# ── Backup ────────────────────────────────────────────────────────────────────
shutil.copy2(CFG_PATH, BACKUP)
print(f"Backup salvo: {BACKUP}")

# ── Carregar ──────────────────────────────────────────────────────────────────
cfg = json.loads(CFG_PATH.read_text(encoding="utf-8"))

# ── 1. odd_max 0x1: 11.5 -> 13.0 ─────────────────────────────────────────────
old_max = cfg["runtime_data"]["filtros_metodo"]["Lay_CS_0x1_B365"]["odd_max"]
cfg["runtime_data"]["filtros_metodo"]["Lay_CS_0x1_B365"]["odd_max"] = 13.0
print(f"[1] odd_max 0x1: {old_max} -> 13.0")

# ── 2. Novas ligas 0x1 ────────────────────────────────────────────────────────
NOVAS_LIGAS = [
    "GERMANY 1",
    "GERMANY 2",
    "ITALY 2",
    "ITALY 3",
    "AUSTRIA 1",
    "SWITZERLAND 1",
    "CZECH 1",
    "FRANCE 3",
    "EUROPA CHAMPIONS LEAGUE",
]

ligas_atuais = cfg["runtime_data"]["filtros_metodo"]["Lay_CS_0x1_B365"]["ligas_permitidas"]
ligas_set    = set(l.upper() for l in ligas_atuais)
adicionadas  = []
for liga in NOVAS_LIGAS:
    if liga.upper() not in ligas_set:
        ligas_atuais.append(liga)
        adicionadas.append(liga)

ligas_atuais.sort()
cfg["runtime_data"]["filtros_metodo"]["Lay_CS_0x1_B365"]["ligas_permitidas"] = ligas_atuais
print(f"[2] Novas ligas 0x1 adicionadas ({len(adicionadas)}): {adicionadas}")
print(f"    Total ligas 0x1: {len(ligas_atuais)}")

# ── 3. Cap diario: 4 -> 6 ─────────────────────────────────────────────────────
for metodo in ["Lay_CS_0x1_B365", "Lay_CS_1x0_B365"]:
    old_cap = cfg["runtime_data"]["filtros_metodo"][metodo].get("cap_diario", 4)
    cfg["runtime_data"]["filtros_metodo"][metodo]["cap_diario"] = 6
    print(f"[3] cap_diario {metodo}: {old_cap} -> 6")

# ── 4. Remover rodo id=11 (ITALY 1 | Lay_CS_1x0 | Odd 10-12) ─────────────────
# Remover de filtros_rodo (nivel raiz)
antes_raiz = len(cfg.get("filtros_rodo", []))
cfg["filtros_rodo"] = [r for r in cfg.get("filtros_rodo", []) if r.get("id") != 11]
depois_raiz = len(cfg.get("filtros_rodo", []))

# Remover de filters.filtros_rodo
antes_filters = len(cfg.get("filters", {}).get("filtros_rodo", []))
if "filters" in cfg and "filtros_rodo" in cfg["filters"]:
    cfg["filters"]["filtros_rodo"] = [r for r in cfg["filters"]["filtros_rodo"] if r.get("id") != 11]
depois_filters = len(cfg.get("filters", {}).get("filtros_rodo", []))

print(f"[4] Rodo id=11 removido:")
print(f"    filtros_rodo raiz: {antes_raiz} -> {depois_raiz}")
print(f"    filters.filtros_rodo: {antes_filters} -> {depois_filters}")

# ── 5. Atualizar version e metadata ──────────────────────────────────────────
old_version = cfg.get("version", "")
cfg["version"] = "v3-melhorias-2026-05-11"
cfg["ultima_atualizacao"] = datetime.now().strftime("%Y-%m-%d %H:%M")
cfg["changelog"] = cfg.get("changelog", [])
cfg["changelog"].append({
    "data":    datetime.now().strftime("%Y-%m-%d"),
    "versao":  "v3-melhorias-2026-05-11",
    "mudancas": [
        "odd_max Lay_CS_0x1_B365: 11.5 -> 13.0",
        "Novas ligas 0x1: GERMANY 1, GERMANY 2, ITALY 2, ITALY 3, AUSTRIA 1, SWITZERLAND 1, CZECH 1, FRANCE 3, EUROPA CHAMPIONS LEAGUE",
        "cap_diario 0x1 e 1x0: 4 -> 6",
        "Removido rodo id=11 (ITALY 1 | Lay_CS_1x0_B365 | Odd 10-12) — 100% green em 2026",
        "Backtest comparativo: ROI +71% -> +137% | Lucro R$791 -> R$1515 (Jan-Abr 2026)",
    ]
})
print(f"[5] Version: {old_version} -> {cfg['version']}")

# ── Salvar ────────────────────────────────────────────────────────────────────
CFG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"\nConfig salvo: {CFG_PATH}")

# ── Verificacao ───────────────────────────────────────────────────────────────
cfg_check = json.loads(CFG_PATH.read_text(encoding="utf-8"))
fm = cfg_check["runtime_data"]["filtros_metodo"]
print(f"\nVERIFICACAO:")
print(f"  odd_max 0x1:  {fm['Lay_CS_0x1_B365']['odd_max']}")
print(f"  ligas 0x1:    {len(fm['Lay_CS_0x1_B365']['ligas_permitidas'])}")
print(f"  cap 0x1:      {fm['Lay_CS_0x1_B365'].get('cap_diario')}")
print(f"  cap 1x0:      {fm['Lay_CS_1x0_B365'].get('cap_diario')}")
rodos_ids = [r['id'] for r in cfg_check.get('filtros_rodo', [])]
print(f"  rodos raiz:   {rodos_ids}")
rodos_ids2 = [r['id'] for r in cfg_check.get('filters', {}).get('filtros_rodo', [])]
print(f"  rodos filters:{rodos_ids2}")
print(f"  version:      {cfg_check['version']}")
print(f"\nConcluido! Backup em: {BACKUP}")
