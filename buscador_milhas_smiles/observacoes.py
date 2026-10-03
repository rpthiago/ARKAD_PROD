"""
Módulo de Coleta e Auditoria da 'Verdade de Campo' (Ground Truth) de Milhas Smiles.
Permite registrar o que a Smiles efetivamente cobrou ao vivo (quando o usuário abre o link)
e compara com o que o modelo previu, gerando estatísticas de erro, viés e calibração.
Atende diretamente à auditoria do Claude e às 5 Leis do GEMINI.md.
"""
import os
import sys
import csv
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from buscador_milhas_smiles.main import carregar_config
from buscador_milhas_smiles.smiles_scanner import cotar_voo_smiles

CSV_PATH = Path(__file__).resolve().parent / "observacoes_smiles.csv"

FIELDNAMES = [
    "data_registro",
    "origem",
    "destino",
    "data_ida",
    "data_volta",
    "tipo_viagem",
    "tipo_precificacao",
    "cia_aerea",
    "milhas_estimadas",
    "milhas_reais",
    "erro_milhas",
    "erro_pct",
    "preco_dinheiro_gf",
    "cpm_real_derivado",
    "taxas_reais",
    "assento_disponivel",
    "observacoes",
]


def inicializar_csv_se_necessario():
    if not CSV_PATH.exists():
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()


def registrar_observacao(
    origem: str,
    destino: str,
    data_ida: str,
    milhas_reais: int,
    data_volta: Optional[str] = None,
    taxas_reais: float = 0.0,
    assento_disponivel: bool = True,
    observacoes: str = "",
) -> Dict[str, Any]:
    inicializar_csv_se_necessario()
    cfg = carregar_config()
    origem = origem.upper()
    destino = destino.upper()

    dest_cfg = cfg.get("destinos", {}).get(destino)

    cotacao = cotar_voo_smiles(
        origem=origem,
        destino=destino,
        data_ida=data_ida,
        data_volta=data_volta,
        cfg_dest=dest_cfg,
    )

    milhas_est = cotacao.milhas
    erro_abs = milhas_est - milhas_reais
    erro_pct = round((erro_abs / milhas_reais) * 100, 1) if milhas_reais > 0 else 0.0
    preco_gf = cotacao.preco_dinheiro_estimado

    cpm_real = round((preco_gf / (milhas_reais / 1000.0)), 2) if milhas_reais > 0 else 0.0

    registro = {
        "data_registro": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "origem": origem,
        "destino": destino,
        "data_ida": data_ida,
        "data_volta": data_volta or "",
        "tipo_viagem": cotacao.tipo_viagem,
        "tipo_precificacao": cotacao.tipo_precificacao,
        "cia_aerea": cotacao.cia_aerea,
        "milhas_estimadas": milhas_est,
        "milhas_reais": milhas_reais,
        "erro_milhas": erro_abs,
        "erro_pct": erro_pct,
        "preco_dinheiro_gf": preco_gf,
        "cpm_real_derivado": cpm_real,
        "taxas_reais": taxas_reais,
        "assento_disponivel": 1 if assento_disponivel else 0,
        "observacoes": observacoes,
    }

    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writerow(registro)

    return registro


def gerar_relatorio_precisao() -> None:
    if not CSV_PATH.exists():
        print("Nenhuma observação de campo registrada ainda.")
        return

    linhas: List[Dict[str, Any]] = []
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            linhas.append(row)

    if not linhas:
        print("Arquivo de observações vazio.")
        return

    print(f"\n=======================================================")
    print(f"📊 RELATÓRIO DE PRECISÃO & CALIBRAÇÃO SMILES (N={len(linhas)})")
    print(f"=======================================================")

    erros_pct_gol = []
    cpms_gol = []
    erros_pct_int = []
    cpms_int = []
    disponibilidade = []

    for l in linhas:
        erro = float(l.get("erro_pct", 0))
        cpm = float(l.get("cpm_real_derivado", 0))
        disp = int(l.get("assento_disponivel", 1))

        if l.get("tipo_precificacao") == "dinamica_gol":
            erros_pct_gol.append(erro)
            if cpm > 0:
                cpms_gol.append(cpm)
        else:
            erros_pct_int.append(erro)
            if cpm > 0:
                cpms_int.append(cpm)
        disponibilidade.append(disp)

    taxa_disp = (sum(disponibilidade) / len(disponibilidade)) * 100 if disponibilidade else 0

    print(f"Taxa de Disponibilidade Real de Assentos: {taxa_disp:.1f}%")
    if erros_pct_gol:
        mape_gol = sum(abs(e) for e in erros_pct_gol) / len(erros_pct_gol)
        vies_gol = sum(erros_pct_gol) / len(erros_pct_gol)
        cpm_medio_gol = sum(cpms_gol) / len(cpms_gol) if cpms_gol else 0.0
        print(f"\n[GOL Doméstico - N={len(erros_pct_gol)}]")
        print(f"  - Erro Médio Absoluto (MAPE): {mape_gol:.1f}%")
        print(f"  - Viés Médio (Viés + superestima, - subestima): {vies_gol:+.1f}%")
        print(f"  - R$/milheiro real derivado: R$ {cpm_medio_gol:.2f} (modelo usa R$ 18.00)")

    if erros_pct_int:
        mape_int = sum(abs(e) for e in erros_pct_int) / len(erros_pct_int)
        vies_int = sum(erros_pct_int) / len(erros_pct_int)
        cpm_medio_int = sum(cpms_int) / len(cpms_int) if cpms_int else 0.0
        print(f"\n[Internacional Parceiras - N={len(erros_pct_int)}]")
        print(f"  - Erro Médio Absoluto (MAPE): {mape_int:.1f}%")
        print(f"  - Viés Médio (Viés + superestima, - subestima): {vies_int:+.1f}%")
        print(f"  - R$/milheiro real derivado: R$ {cpm_medio_int:.2f} (modelo dinâmico usa R$ 13.50)")
    
    print("\nÚltimos 5 registros:")
    for l in linhas[-5:]:
        disp_icon = "✅" if int(l.get("assento_disponivel", 1)) == 1 else "❌"
        print(
            f"  {disp_icon} {l['data_registro'][:10]} | {l['origem']}➔{l['destino']} | "
            f"Est: {int(l['milhas_estimadas']):,} | Real: {int(l['milhas_reais']):,} | Erro: {float(l['erro_pct']):+.1f}%"
        )
    print("=======================================================\n")


def main():
    parser = argparse.ArgumentParser(description="Auditoria da Verdade de Campo Smiles")
    parser.add_argument("--origem", type=str, help="Código do aeroporto de origem (ex: CNF, GRU)")
    parser.add_argument("--destino", type=str, help="Código do aeroporto de destino (ex: JPA, MAD)")
    parser.add_argument("--data-ida", type=str, help="Data de ida YYYY-MM-DD")
    parser.add_argument("--data-volta", type=str, default=None, help="Data de volta YYYY-MM-DD")
    parser.add_argument("--milhas-reais", type=int, help="Milhas cobradas ao vivo na Smiles")
    parser.add_argument("--taxas", type=float, default=0.0, help="Taxas de embarque em R$ cobradas na Smiles")
    parser.add_argument("--sem-assento", action="store_true", help="Marcar se não havia assento disponível")
    parser.add_argument("--obs", type=str, default="", help="Observações adicionais")
    parser.add_argument("--relatorio", action="store_true", help="Gera o relatório de erro e calibração")

    args = parser.parse_args()

    if args.relatorio:
        gerar_relatorio_precisao()
        return

    if not (args.origem and args.destino and args.data_ida and args.milhas_reais is not None):
        parser.print_help()
        print("\nExemplo de uso:")
        print("  python -m buscador_milhas_smiles.observacoes --origem CNF --destino JPA --data-ida 2026-12-21 --data-volta 2026-12-28 --milhas-reais 83500 --taxas 75.0")
        return

    reg = registrar_observacao(
        origem=args.origem,
        destino=args.destino,
        data_ida=args.data_ida,
        data_volta=args.data_volta,
        milhas_reais=args.milhas_reais,
        taxas_reais=args.taxas,
        assento_disponivel=not args.sem_assento,
        observacoes=args.obs,
    )

    print(f"\n[REGISTRADO COM SUCESSO]")
    print(f"Rota: {reg['origem']} ➔ {reg['destino']} ({reg['data_ida']} a {reg['data_volta']})")
    print(f"Milhas Estimadas: {reg['milhas_estimadas']:,} | Milhas Reais: {reg['milhas_reais']:,}")
    print(f"Erro do Modelo: {reg['erro_milhas']:+,} milhas ({reg['erro_pct']:+.1f}%)")
    print(f"R$/milheiro real derivado: R$ {reg['cpm_real_derivado']:.2f}")
    print(f"Salvo em: {CSV_PATH.name}\n")


if __name__ == "__main__":
    main()
