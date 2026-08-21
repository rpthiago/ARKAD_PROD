import sys
sys.stdout.reconfigure(encoding='utf-8')
import pandas as pd
from metodo_lay2x2_strategy import validar_entrada_lay2x2

matches = [
    ("Kairat Almaty", "Anderlecht", 20.0, 1.73, "0x3"),
    ("FC Inter Turku", "FC Copenhagen", 19.0, 1.95, "0x0"),
    ("Mjallby", "Red Bull Salzburg", 17.0, 1.90, "0x1"),
    ("Vendsyssel FF", "Hillerod Fodbold", 18.5, 1.99, "1x0"),
    ("FC Nordsjaelland", "St Gallen", 20.0, 1.85, "1x0"),
    ("Egnatia Rrogozhine", "Lillestrom", 20.0, 1.92, "0x0"),
    ("Trabzonspor", "Ferencvaros", 19.0, 1.88, "0x1"),
    ("Klaksvikar Itrottarfelag", "FK Riga", 15.5, 1.80, "0x0"),
    ("Gornik Zabrze", "Monaco", 19.0, 1.96, "2x3"),
    ("KF Drita", "Inter Club Escaldes", 17.0, 2.22, "2x2"),  # RED 1
    ("Crvena Zvezda", "Plzen", 16.0, 1.85, "3x0"),
    ("OFI", "CSKA Sofia", 19.0, 1.73, "3x0"),
    ("Sion", "Ajax", 16.0, 2.15, "2x4"),
    ("Lugano", "Maccabi Tel Aviv", 19.0, 2.05, "2x1"),
    ("Hearts", "Rapid Vienna", 16.0, 2.28, "2x2"),  # RED 2
    ("Sheff Wed", "Bradford", 17.0, 2.00, "0x1"),
    ("Hajduk Split", "Rakow Czestochowa", 19.0, 1.83, "2x2"),  # RED 3
    ("Real Santander", "Barranquilla", 18.0, 1.94, "2x2"),  # RED 4
    ("Macara", "Santos", 19.0, 1.81, "0x0"),
    ("Venados FC", "Dorados", 16.5, 2.12, "4x2")
]

approved = []
for h, a, o_2x2, o_u25, score in matches:
    ok, reason = validar_entrada_lay2x2(odd_lay_2x2=o_2x2, odd_under25=o_u25)
    is_2x2 = (score == "2x2")
    status = "APROVADO" if ok else "REJEITADO"
    res = ("RED" if is_2x2 else "GREEN") if ok else "FORA DA ENTRADA"
    approved.append({
        "Jogo": f"{h} x {a}", "Odd 2x2": o_2x2, "Odd U25": o_u25,
        "Placar": score, "Status Robô": status, "Resultado Operativo": res
    })

df = pd.DataFrame(approved)
print(df.to_string(index=False), flush=True)

df_app = df[df["Status Robô"] == "APROVADO"].copy()
print("\n=== RESUMO DAS ENTRADAS EFETIVADAS HOJE COM O CÓDIGO CORRIGIDO ===", flush=True)
print(f"Total Aprovados : {len(df_app)}", flush=True)
print(f"Greens          : {(df_app['Resultado Operativo'] == 'GREEN').sum()}", flush=True)
print(f"Reds            : {(df_app['Resultado Operativo'] == 'RED').sum()}", flush=True)
