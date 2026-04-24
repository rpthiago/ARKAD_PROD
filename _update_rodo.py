import json
from pathlib import Path
from datetime import datetime

p = Path("config_rodos_master.json")
cfg = json.loads(p.read_text(encoding="utf-8"))
regras = cfg["filtros_rodo"]
print("Regras antes:", len(regras))

# Remove Rodo_09 BULGARIA 1 | 0x1 | 8-10 (WR 93% — estava bloqueando entradas boas)
regras = [r for r in regras if not (
    r.get("league") == "BULGARIA 1" and
    r.get("method_equals") == "Lay_CS_0x1_B365" and
    r.get("odd_min") == 8.0 and r.get("odd_max") == 10.0
)]

# Remove Rodo_22 ITALY 1 | 0x1 | 8-10 (WR 100% — estava bloqueando entradas boas)
regras = [r for r in regras if not (
    r.get("league") == "ITALY 1" and
    r.get("method_equals") == "Lay_CS_0x1_B365" and
    r.get("odd_min") == 8.0 and r.get("odd_max") == 10.0
)]

# Adiciona TURKEY 2 (17 ent, WR 0%, PnL estimado -R$8500)
next_id = max(r["id"] for r in regras) + 1
regras.append({
    "id": next_id,
    "name": "Rodo_{:02d} TURKEY 2 | Lay_CS_0x1_B365 | Odd 8-11.5".format(next_id),
    "league": "TURKEY 2",
    "method_equals": "Lay_CS_0x1_B365",
    "odd_min": 8.0,
    "odd_max": 11.5,
    "lucro_combo": -8500.0,
    "apostas": 17
})

print("Regras depois:", len(regras))
for r in regras:
    print("  [{}] {}".format(r["id"], r["name"]))

cfg["filtros_rodo"] = regras
cfg["version"] = "rodo-master-v2"
cfg["generated_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
cfg["generated_from"] = "auditoria_rodo_2026-04-24"

p.write_text(json.dumps(cfg, ensure_ascii=False, indent=4), encoding="utf-8")
print("\nconfig_rodos_master.json salvo.")

# Valida JSON
json.loads(p.read_text(encoding="utf-8"))
print("JSON valido.")
