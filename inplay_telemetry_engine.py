# -*- coding: utf-8 -*-
"""
inplay_telemetry_engine.py — Motor de Telemetria e Decisão In-Play
Cruza a lista de jogos pré-selecionados pela IA com o comportamento das partidas em tempo real.
Calcula gatilhos de entrada, stop-loss no minuto 75' e confirmação de greens ao vivo.
"""

import os
import sys
import unicodedata
import re
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent

def _canon(s):
    if not isinstance(s, str) or not s: return ''
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]", "", s)

class InPlayTelemetryEngine:
    def __init__(self, cache_file=None):
        self.cache_file = cache_file or (ROOT / "_placares_coletor_cache.csv")
        self._mapa_live = {}
        self.recarregar_telemetria()

    def recarregar_telemetria(self):
        """Lê os últimos ticks capturados pela API da Betfair / coletor in-play."""
        if not self.cache_file.exists():
            return
        try:
            df_ticks = pd.read_csv(self.cache_file)
            df_ticks['min_to_ko'] = pd.to_numeric(df_ticks.get('min_to_ko'), errors='coerce')
            df_ticks['lay'] = pd.to_numeric(df_ticks.get('lay'), errors='coerce')
            
            mapa = {}
            for (ko, home, away), g in df_ticks.groupby(['ko', 'home', 'away']):
                k_date = str(ko)[:10]
                m_key = f"{k_date}_{_canon(home)}_{_canon(away)}"
                g_sorted = g.sort_values('min_to_ko')
                
                # Placar final (último tick registrado)
                f_ticks = g_sorted[g_sorted['min_to_ko'] <= -90]
                if f_ticks.empty:
                    f_ticks = g_sorted.tail(15)
                vf = f_ticks.dropna(subset=['lay'])
                final_sc = vf.loc[vf['lay'].idxmin()]['runner'] if not vf.empty else "N/A"
                
                # Placar mais recente (live)
                latest = g_sorted.tail(10).dropna(subset=['lay'])
                live_sc = latest.loc[latest['lay'].idxmin()]['runner'] if not latest.empty else "N/A"
                min_ko = g_sorted['min_to_ko'].min() if not g_sorted.empty else 0
                min_decorrido = abs(int(min_ko)) if min_ko < 0 else 0
                
                # Extrair gols
                gh, ga = -1, -1
                if live_sc != "N/A" and ' - ' in live_sc:
                    pts = live_sc.split(' - ')
                    if len(pts) == 2 and pts[0].isdigit() and pts[1].isdigit():
                        gh, ga = int(pts[0]), int(pts[1])
                        
                mapa[m_key] = {
                    'placar_final': final_sc,
                    'placar_live': live_sc,
                    'min_decorrido': min_decorrido,
                    'gh': gh,
                    'ga': ga,
                    'total_gols': gh + ga if gh >= 0 else 0,
                    'diff_gols': abs(gh - ga) if gh >= 0 else 0
                }
            self._mapa_live = mapa
        except Exception:
            pass

    def avaliar_situacao_inplay(self, data_str, home, away, metodo_pre):
        """
        Avalia o comportamento ao vivo do jogo e emite o status/gatilho operacional:
        - Para Lay Under 1.5 FT:
          * Se total_gols >= 2: GREEN CONFIRMADO!
          * Se minuto >= 75' e total_gols < 2: ALERTA DE STOP LOSS (Cashout a ~1.22 para salvar banca).
          * Se minuto < 75' e total_gols < 2: AGUARDANDO EM ANDAMENTO.
        """
        m_key = f"{data_str}_{_canon(home)}_{_canon(away)}"
        tele = self._mapa_live.get(m_key)
        
        if not tele:
            return {
                'status': 'AGUARDANDO_KICKOFF',
                'badge': '⏳ Pré-Jogo',
                'placar': '0 - 0',
                'minuto': "0'",
                'recomendacao_live': 'Aguardar início da partida.'
            }
            
        m = tele['min_decorrido']
        tot_g = tele['total_gols']
        diff = tele['diff_gols']
        placar = tele['placar_live'] if tele['placar_live'] != 'N/A' else tele['placar_final']
        
        if 'Under 1.5' in metodo_pre:
            if tot_g >= 2:
                return {
                    'status': 'GREEN_CONFIRMADO',
                    'badge': '🟢 GREEN GANHO',
                    'placar': placar,
                    'minuto': f"{m}'" if m < 90 else "FT",
                    'recomendacao_live': 'Operação ganha! Já saíram 2+ gols.'
                }
            elif m >= 75:
                return {
                    'status': 'STOP_LOSS_75',
                    'badge': '🛡️ STOP LOSS (75\')',
                    'placar': placar,
                    'minuto': f"{m}'",
                    'recomendacao_live': f'Aos {m}\' com {tot_g} gol(s): Executar Cashout / Stop Loss para limitar prejuízo a ~0.75u.'
                }
            elif m > 0:
                return {
                    'status': 'EM_ANDAMENTO',
                    'badge': '⚽ AO VIVO',
                    'placar': placar,
                    'minuto': f"{m}'",
                    'recomendacao_live': f'Jogo aos {m}\' ({placar}). Operação correndo normalmente.'
                }
        
        # Para Lay 2x2
        elif '2x2' in metodo_pre:
            gh, ga = tele.get('gh', -1), tele.get('ga', -1)
            if (gh > 2 or ga > 2) and gh >= 0 and ga >= 0:
                return {
                    'status': 'GREEN_CONFIRMADO',
                    'badge': '🟢 GREEN GANHO',
                    'placar': placar,
                    'minuto': f"{m}'" if m < 90 else "FT",
                    'recomendacao_live': f'Placar {placar}: 2x2 se tornou impossível. Operação ganha!'
                }
            elif m >= 90:
                if placar == '2 - 2':
                    return {
                        'status': 'RED',
                        'badge': '🔴 RED (2x2)',
                        'placar': placar,
                        'minuto': "FT",
                        'recomendacao_live': 'Terminou em 2x2.'
                    }
                else:
                    return {
                        'status': 'GREEN_CONFIRMADO',
                        'badge': '🟢 GREEN GANHO',
                        'placar': placar,
                        'minuto': "FT",
                        'recomendacao_live': f'Terminou em {placar}. Operação ganha!'
                    }
            elif m > 0:
                return {
                    'status': 'EM_ANDAMENTO',
                    'badge': '⚽ AO VIVO',
                    'placar': placar,
                    'minuto': f"{m}'",
                    'recomendacao_live': f'Jogo aos {m}\' ({placar}).'
                }
                
        # Para Lay 0x3
        elif '0x3' in metodo_pre:
            gh, ga = tele.get('gh', -1), tele.get('ga', -1)
            if (gh >= 1 or ga >= 4) and gh >= 0 and ga >= 0:
                return {
                    'status': 'GREEN_CONFIRMADO',
                    'badge': '🟢 GREEN GANHO',
                    'placar': placar,
                    'minuto': f"{m}'" if m < 90 else "FT",
                    'recomendacao_live': f'Placar {placar}: 0x3 se tornou impossível. Operação ganha!'
                }
            elif m >= 90:
                if placar == '0 - 3':
                    return {
                        'status': 'RED',
                        'badge': '🔴 RED (0x3)',
                        'placar': placar,
                        'minuto': "FT",
                        'recomendacao_live': 'Terminou em 0x3.'
                    }
                else:
                    return {
                        'status': 'GREEN_CONFIRMADO',
                        'badge': '🟢 GREEN GANHO',
                        'placar': placar,
                        'minuto': "FT",
                        'recomendacao_live': f'Terminou em {placar}. Operação ganha!'
                    }
            elif m > 0:
                return {
                    'status': 'EM_ANDAMENTO',
                    'badge': '⚽ AO VIVO',
                    'placar': placar,
                    'minuto': f"{m}'",
                    'recomendacao_live': f'Jogo aos {m}\' ({placar}).'
                }
                
        # Para Lay 0x1
        elif '0x1' in metodo_pre:
            gh, ga = tele.get('gh', -1), tele.get('ga', -1)
            if (gh >= 1 or ga >= 2) and gh >= 0 and ga >= 0:
                return {
                    'status': 'GREEN_CONFIRMADO',
                    'badge': '🟢 GREEN GANHO',
                    'placar': placar,
                    'minuto': f"{m}'" if m < 90 else "FT",
                    'recomendacao_live': f'Placar {placar}: 0x1 se tornou impossível. Operação ganha!'
                }
            elif gh == 0 and ga == 1 and m < 90:
                return {
                    'status': 'STOP_RED_30',
                    'badge': '🛡️ STOP RED 30%',
                    'placar': placar,
                    'minuto': f"{m}'",
                    'recomendacao_live': f'Aos {m}\' com 0x1: Executar Stop Red para travar perda em 30% da liability.'
                }
            elif m >= 45 and m <= 55 and gh == 0 and ga == 0:
                return {
                    'status': 'CASHOUT_HT_0X0',
                    'badge': '💰 CASHOUT HT (0x0)',
                    'placar': placar,
                    'minuto': f"{m}'",
                    'recomendacao_live': 'Intervalo em 0x0: Executar saída no HT com lucro de tempo (~20% da stake).'
                }
            elif m >= 90:
                if placar == '0 - 1':
                    return {
                        'status': 'RED',
                        'badge': '🔴 RED (0x1)',
                        'placar': placar,
                        'minuto': "FT",
                        'recomendacao_live': 'Terminou em 0x1.'
                    }
                else:
                    return {
                        'status': 'GREEN_CONFIRMADO',
                        'badge': '🟢 GREEN GANHO',
                        'placar': placar,
                        'minuto': "FT",
                        'recomendacao_live': f'Terminou em {placar}. Operação ganha!'
                    }
            elif m > 0:
                return {
                    'status': 'EM_ANDAMENTO',
                    'badge': '⚽ AO VIVO',
                    'placar': placar,
                    'minuto': f"{m}'",
                    'recomendacao_live': f'Jogo aos {m}\' ({placar}).'
                }
                
        return {
            'status': 'FINALIZADO' if m >= 90 else 'EM_ANDAMENTO',
            'badge': f'⚽ {placar}',
            'placar': placar,
            'minuto': f"{m}'",
            'recomendacao_live': 'Monitorando telemetria.'
        }
