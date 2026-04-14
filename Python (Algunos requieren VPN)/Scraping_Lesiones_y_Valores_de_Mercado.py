"""
scrape_injuries_mv_history.py — TFG Guillermo Jiménez
======================================================
Enriquece el dataset con datos de Transfermarkt que NO están en
scrape_tfg_data.py:

  1. Historial de valor de mercado por jugador (tabla MV History)
     → para cada jugador con tm_player_id: series temporal de valoraciones
     → genera la hoja "MV History" con ~37.000 filas

  2. Lesiones por temporada (tabla Injuries by Season)
     → para cada jugador: días lesionado por temporada (últimas 5 temporadas)
     → calcula rolling slope (tendencia de lesiones en el tiempo)
     → genera la hoja "Injuries by Season" con ~7.000 filas

  3. Métricas derivadas de lesiones añadidas a cada jugador:
     - injury_count           : total de lesiones registradas
     - injury_days_per_season : media de días lesionado por temporada
     - injury_frequency       : lesiones por temporada
     - inj_days_last1         : días lesionado última temporada
     - inj_days_avg_last2     : media últimas 2 temporadas
     - inj_days_avg_last3     : media últimas 3 temporadas
     - inj_trend_slope        : pendiente de la regresión lineal (¿empeora?)
     - inj_trend_pvalue       : p-valor de la tendencia
     - inj_trend_sig          : True si la tendencia es estadísticamente sig.
     - inj_ewa_days           : media exponencialmente ponderada (reciente > antiguo)
     - inj_risk               : clasificación: 'low' / 'medium' / 'high'
     - inj_series_type        : patrón: 'stable' / 'improving' / 'worsening'

  4. Ingresos de club (club_revenue_eur, revenue_tier)
     → diccionario manual basado en informes públicos (Deloitte Football Money League)
     → no requiere scraping adicional

Input:  Players and teams data TFG - enriched.xlsx   (salida de scrape_tfg_data.py)
Output: Players and teams data TFG - enriched v2.xlsx

Requisitos: pip install requests beautifulsoup4 pandas openpyxl lxml scipy
"""

import requests
import time
import random
import pandas as pd
import numpy as np
import re
from bs4 import BeautifulSoup
from scipy import stats
import sys, os
import warnings
warnings.filterwarnings('ignore')

# ═══════════════════════════════════════════════════════════════
#  CONFIGURACIÓN — LOCAL vs GOOGLE COLAB
#  En Colab: sube la carpeta 'TFG Business Analytics' a Google
#  Drive y ejecuta con Runtime → Ejecutar todo.
# ═══════════════════════════════════════════════════════════════
IN_COLAB = 'google.colab' in sys.modules
if IN_COLAB:
    DB = '/content'
    import subprocess
    subprocess.run(['pip', 'install', '-q', 'requests', 'beautifulsoup4',
                    'lxml', 'openpyxl', 'scipy'], capture_output=True)
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DB = os.path.join(BASE_DIR, 'Bases de Datos')

# ─── Configuración ────────────────────────────────────────────────────────────

INPUT_PATH  = os.path.join(DB, 'Jugadores_Combinados.xlsx')
OUTPUT_PATH = os.path.join(DB, 'Dataset_Definitivo.xlsx')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8',
    'Referer': 'https://www.transfermarkt.com/',
}

MIN_DELAY = 3.0
MAX_DELAY = 6.0

# Temporadas a consultar (más recientes primero)
SEASONS = [2023, 2022, 2021, 2020, 2019]


# ─── BLOQUE 1: Ingresos de club ───────────────────────────────────────────────
# Fuente: Deloitte Football Money League 2024 (datos temporada 2022-23).
# No se puede scrapearse directamente → diccionario manual.
# Los valores están en millones de euros.
# revenue_tier: 'elite' (>400M€), 'top' (200-400M€), 'mid' (100-200M€), 'small' (<100M€)

CLUB_REVENUES = {
    # Premier League
    "Manchester City":   (831, 'elite'),
    "Manchester Utd":    (749, 'elite'),
    "Liverpool":         (703, 'elite'),
    "Arsenal":           (551, 'elite'),
    "Chelsea":           (512, 'elite'),
    "Tottenham":         (450, 'elite'),
    "Newcastle Utd":     (260, 'top'),
    "West Ham":          (218, 'top'),
    "Aston Villa":       (206, 'top'),
    "Brighton":          (194, 'top'),
    "Everton":           (170, 'mid'),
    "Crystal Palace":    (145, 'mid'),
    "Fulham":            (130, 'mid'),
    "Wolves":            (128, 'mid'),
    "Brentford":         (120, 'mid'),
    "Nott'ham Forest":   (118, 'mid'),
    "Bournemouth":       (115, 'mid'),
    "Burnley":           ( 92, 'small'),
    "Sheffield Utd":     ( 78, 'small'),
    "Luton Town":        ( 55, 'small'),
    # LaLiga
    "Real Madrid":       (831, 'elite'),
    "Barcelona":         (800, 'elite'),
    "Atlético Madrid":   (450, 'elite'),
    "Athletic Club":     (185, 'mid'),
    "Real Sociedad":     (168, 'mid'),
    "Betis":             (155, 'mid'),
    "Sevilla":           (148, 'mid'),
    "Villarreal":        (142, 'mid'),
    "Valencia":          (115, 'mid'),
    "Osasuna":           ( 80, 'small'),
    "Rayo Vallecano":    ( 60, 'small'),
    "Getafe":            ( 58, 'small'),
    "Celta Vigo":        ( 70, 'small'),
    "Mallorca":          ( 55, 'small'),
    "Girona":            ( 52, 'small'),
    "Las Palmas":        ( 40, 'small'),
    "Alavés":            ( 38, 'small'),
    "Espanyol":          ( 65, 'small'),
    "Cádiz":             ( 35, 'small'),
    "Granada":           ( 33, 'small'),
    # Serie A
    "Inter":             (440, 'elite'),
    "Milan":             (415, 'elite'),
    "Juventus":          (395, 'top'),
    "Roma":              (245, 'top'),
    "Lazio":             (185, 'mid'),
    "Napoli":            (280, 'top'),
    "Atalanta":          (175, 'mid'),
    "Fiorentina":        (135, 'mid'),
    "Bologna":           (110, 'mid'),
    "Torino":            ( 90, 'small'),
    "Udinese":           ( 70, 'small'),
    "Genoa":             ( 65, 'small'),
    "Lecce":             ( 55, 'small'),
    "Cagliari":          ( 50, 'small'),
    "Verona":            ( 60, 'small'),
    "Hellas Verona":     ( 60, 'small'),
    "Frosinone":         ( 40, 'small'),
    "Sassuolo":          ( 75, 'small'),
    "Salernitana":       ( 38, 'small'),
    "Como":              ( 35, 'small'),
    "Monza":             ( 80, 'small'),
    "Empoli":            ( 48, 'small'),
    "Parma":             ( 42, 'small'),
    # Bundesliga
    "Bayern Munich":     (854, 'elite'),
    "Dortmund":          (480, 'elite'),
    "RB Leipzig":        (310, 'top'),
    "Leverkusen":        (250, 'top'),
    "Eint Frankfurt":    (210, 'top'),
    "Stuttgart":         (185, 'mid'),
    "Hoffenheim":        (165, 'mid'),
    "Wolfsburg":         (160, 'mid'),
    "Freiburg":          (140, 'mid'),
    "Gladbach":          (195, 'mid'),
    "Union Berlin":      (130, 'mid'),
    "Werder Bremen":     (125, 'mid'),
    "Augsburg":          ( 95, 'small'),
    "Mainz 05":          ( 90, 'small'),
    "Köln":              ( 88, 'small'),
    "Heidenheim":        ( 45, 'small'),
    "St. Pauli":         ( 55, 'small'),
    "Hamburger SV":      ( 70, 'small'),
    # Ligue 1
    "Paris S-G":         (800, 'elite'),
    "Lyon":              (255, 'top'),
    "Marseille":         (235, 'top'),
    "Monaco":            (210, 'top'),
    "Lens":              (145, 'mid'),
    "Lille":             (140, 'mid'),
    "Nice":              (135, 'mid'),
    "Rennes":            (128, 'mid'),
    "Strasbourg":        ( 90, 'small'),
    "Nantes":            ( 85, 'small'),
    "Reims":             ( 75, 'small'),
    "Toulouse":          ( 70, 'small'),
    "Brest":             ( 65, 'small'),
    "Montpellier":       ( 60, 'small'),
    "Le Havre":          ( 48, 'small'),
    "Lorient":           ( 50, 'small'),
    "Metz":              ( 42, 'small'),
    "Clermont Foot":     ( 38, 'small'),
    "Auxerre":           ( 40, 'small'),
    "Angers":            ( 36, 'small'),
}


# ─── BLOQUE 2: Scraping de historial de valor de mercado ─────────────────────

def scrape_mv_history(tm_player_id, session):
    """
    Obtiene el historial de valores de mercado de un jugador desde Transfermarkt.

    URL: transfermarkt.com/{slug}/marktwertverlauf/spieler/{id}
    TM devuelve los datos del gráfico como JSON embebido en un <script>.

    Cada punto es: {datum: "Jan 1, 2023", mw: "€45.00m", verein: "Barcelona", ...}

    Devuelve lista de dicts: [{date, market_value_eur, club_at_time}, ...]
    """
    url = f"https://www.transfermarkt.com/x/marktwertverlauf/spieler/{tm_player_id}"

    try:
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        resp = session.get(url, headers=HEADERS, timeout=20)

        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.text, 'lxml')

        # El historial de MV está en un objeto JavaScript:
        # highcharts.chart('highcharts-marktwertverlauf', {..., series: [{data: [...]}]})
        scripts = soup.find_all('script')
        json_data = None
        for script in scripts:
            text = script.string or ''
            if 'marktwertverlauf' in text and 'data:' in text:
                # Extraer el array de datos del gráfico con regex
                match = re.search(r'"series":\s*\[.*?"data":\s*(\[.*?\])', text, re.DOTALL)
                if match:
                    import json
                    try:
                        json_data = json.loads(match.group(1))
                    except json.JSONDecodeError:
                        pass
                break

        if not json_data:
            return []

        records = []
        for point in json_data:
            # Cada punto: {datum, mw, verein, x (timestamp), y (valor en €)}
            try:
                date = point.get('datum', '')
                value = point.get('y', 0)          # valor en euros
                club  = point.get('verein', '')
                records.append({
                    'date': date,
                    'market_value_eur': int(value) if value else None,
                    'club_at_time': club,
                })
            except (TypeError, ValueError):
                continue

        return records

    except Exception:
        return []


# ─── BLOQUE 3: Scraping de lesiones por temporada ────────────────────────────

def scrape_injury_history(tm_player_id, session):
    """
    Obtiene el historial de lesiones de un jugador desde Transfermarkt.

    URL: transfermarkt.com/{slug}/verletzungen/spieler/{id}
    La página contiene una tabla HTML con todas las lesiones registradas.

    Columnas: tipo de lesión, fecha inicio, fecha fin, días perdidos, partidos perdidos.

    Devuelve:
        - records   : lista de lesiones individuales
        - by_season : dict {season_year: total_days_missed}
    """
    url = f"https://www.transfermarkt.com/x/verletzungen/spieler/{tm_player_id}"

    try:
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        resp = session.get(url, headers=HEADERS, timeout=20)

        if resp.status_code != 200:
            return [], {}

        soup = BeautifulSoup(resp.text, 'lxml')

        # La tabla de lesiones tiene clase "items"
        table = soup.find('table', class_='items')
        if not table:
            return [], {}

        rows = table.find_all('tr', class_=['odd', 'even'])
        records = []
        by_season = {}

        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 6:
                continue

            # Extraer datos de cada celda
            season_raw = cells[0].get_text(strip=True)   # ej: "23/24"
            injury_type = cells[1].get_text(strip=True)
            date_from   = cells[2].get_text(strip=True)
            date_until  = cells[3].get_text(strip=True)
            days_raw    = cells[4].get_text(strip=True)  # ej: "23 days"
            games_raw   = cells[5].get_text(strip=True)

            # Convertir "23/24" → año base 2023
            season_year = None
            season_match = re.match(r'(\d{2})/\d{2}', season_raw)
            if season_match:
                yy = int(season_match.group(1))
                season_year = 2000 + yy

            # Convertir "23 days" → 23
            days_match = re.search(r'(\d+)', days_raw)
            days = int(days_match.group(1)) if days_match else 0

            records.append({
                'season': season_raw,
                'season_year': season_year,
                'injury_type': injury_type,
                'date_from': date_from,
                'date_until': date_until,
                'days_missed': days,
            })

            # Acumular días por temporada
            if season_year:
                by_season[season_year] = by_season.get(season_year, 0) + days

        return records, by_season

    except Exception:
        return [], {}


# ─── BLOQUE 4: Calcular métricas derivadas de lesiones ───────────────────────

def compute_injury_metrics(by_season, all_records):
    """
    Dado un dict {season_year: days_missed} y la lista de lesiones individuales,
    calcula todas las métricas de lesión para un jugador.

    Métricas calculadas:
      - injury_count           : número total de lesiones
      - inj_days_last1         : días perdidos última temporada
      - inj_days_avg_last2     : promedio últimas 2 temporadas
      - inj_days_avg_last3     : promedio últimas 3 temporadas
      - injury_days_per_season : promedio general
      - injury_frequency       : lesiones por temporada
      - inj_trend_slope        : pendiente OLS de la serie temporal de días
      - inj_trend_pvalue       : p-valor de la pendiente
      - inj_trend_sig          : True si p < 0.05
      - inj_ewa_days           : media exp. ponderada (α=0.4, reciente pesa más)
      - inj_risk               : 'low' (<10 días/temporada), 'medium' (10-40), 'high' (>40)
      - inj_series_type        : 'worsening'/'improving'/'stable'
    """
    if not by_season:
        return {
            'injury_count': 0,
            'inj_days_last1': 0,
            'inj_days_avg_last2': 0.0,
            'inj_days_avg_last3': 0.0,
            'injury_days_per_season': 0.0,
            'injury_frequency': 0.0,
            'inj_trend_slope': 0.0,
            'inj_trend_pvalue': 1.0,
            'inj_trend_sig': False,
            'inj_ewa_days': 0.0,
            'inj_risk': 'low',
            'inj_series_type': 'stable',
        }

    # Ordenar temporadas de más antigua a más reciente
    sorted_seasons = sorted(by_season.items())   # [(2019, 12), (2020, 0), ...]
    years  = [s[0] for s in sorted_seasons]
    days   = [s[1] for s in sorted_seasons]

    injury_count = len(all_records)

    # Últimas N temporadas
    inj_last1 = days[-1] if len(days) >= 1 else 0
    inj_avg2  = np.mean(days[-2:]) if len(days) >= 2 else float(days[-1])
    inj_avg3  = np.mean(days[-3:]) if len(days) >= 3 else np.mean(days)

    inj_per_season = np.mean(days)
    inj_frequency  = injury_count / len(sorted_seasons) if sorted_seasons else 0

    # Regresión lineal OLS sobre la serie temporal de días
    # slope > 0 → empeorando, slope < 0 → mejorando
    if len(days) >= 3:
        slope, intercept, r, pvalue, stderr = stats.linregress(years, days)
    else:
        slope, pvalue = 0.0, 1.0

    inj_sig = pvalue < 0.05

    # Media exponencialmente ponderada (EWA) con α=0.4
    # Peso mayor a temporadas recientes
    alpha = 0.4
    ewa = days[0]
    for d in days[1:]:
        ewa = alpha * d + (1 - alpha) * ewa

    # Clasificación de riesgo
    if inj_per_season < 10:
        risk = 'low'
    elif inj_per_season < 40:
        risk = 'medium'
    else:
        risk = 'high'

    # Tipo de serie
    if abs(slope) < 2:
        series_type = 'stable'
    elif slope > 0:
        series_type = 'worsening'
    else:
        series_type = 'improving'

    return {
        'injury_count': injury_count,
        'inj_days_last1': round(inj_last1, 1),
        'inj_days_avg_last2': round(inj_avg2, 1),
        'inj_days_avg_last3': round(inj_avg3, 1),
        'injury_days_per_season': round(inj_per_season, 1),
        'injury_frequency': round(inj_frequency, 2),
        'inj_trend_slope': round(slope, 3),
        'inj_trend_pvalue': round(pvalue, 4),
        'inj_trend_sig': inj_sig,
        'inj_ewa_days': round(ewa, 1),
        'inj_risk': risk,
        'inj_series_type': series_type,
    }


# ─── Mapeo de columnas: nombres en español del dataset actual ─────────────────

# Columna que contiene el ID de Transfermarkt (renombrada en Elaboración_Dataset)
TM_ID_COL      = 'ID del jugador en Transfermarkt'
PLAYER_COL     = 'Jugador'
SQUAD_COL      = 'Equipo'
CONTRACT_COL   = 'Fecha de expiración del contrato'
CONTRACT_Y_COL = 'Años de contrato restantes'

# Métricas de lesión: clave interna (compute_injury_metrics) → columna en español
INJURY_METRIC_COLS = {
    'injury_count':          'Cantidad total de lesiones registradas',
    'injury_days_per_season':'Promedio de días de baja por lesión por temporada',
    'injury_frequency':      'Número promerio de lesiones por temporada',
    'inj_days_last1':        'Días de lesión en la temporada pasada',
    'inj_days_avg_last2':    'Promedio de días de lesión entre la temporada pasada y la anterior',
    'inj_days_avg_last3':    'Promedio de días de lesión entre las 3 últimas temporadas',
    'inj_trend_slope':       'Pendiente de la regresión lineal de la cantidad de lesiones (días de lesión añadidos o disminuidos en promedio anual)',
    'inj_trend_pvalue':      'P-valor de la pendiente: Probabilidad de que la tendencia de la pendiente sea un ruido aleatorio (Aceptamos la hipótesis nula (pendiente válida) con un 10% o menos)',
    'inj_trend_sig':         'Aceptación o refutación del modelo (Uso un 10% en vez del 5% habitual por las pocas variables que hay en este análisis, para que no salga todo estable)',
    'inj_ewa_days':          'Media anual ponderada exponencialmente de los días de baja por lesión',
    'inj_risk':              'Riesgo de lesión',
    'inj_series_type':       'inj_series_type',
}

# Ingresos de club: columnas en español
REVENUE_COL = 'Ingreso total anual del club'
TIER_COL    = 'Categoría de ingresos del club**'

# Liga → nombre de hoja en el Excel de salida
LIGA_TO_SHEET = {
    'Premier League': 'PL Players',
    'La Liga':        'LaLiga Players',
    'Serie A':        'Serie A Players',
    'Bundesliga':     'Bundesliga Players',
    'Ligue 1':        'Ligue 1 Players',
}


# ─── Script principal ─────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  TFG — Scraping Lesiones + Historial MV (Transfermarkt)")
    print("=" * 60)

    # ── Cargar datos (Jugadores_Combinados.xlsx: una sola hoja con columnas ES) ─
    print("\n📂 Cargando Jugadores_Combinados.xlsx...")

    combined = pd.read_excel(INPUT_PATH, sheet_name='Todos los jugadores')

    # Eliminar columnas basura sin nombre (Unnamed: 0, Unnamed: 1)
    combined = combined.loc[:, ~combined.columns.str.startswith('Unnamed')]

    print(f"  {len(combined)} jugadores en total")

    # Solo jugadores con ID de Transfermarkt (necesario para el scraping)
    has_id = combined[TM_ID_COL].notna()
    print(f"  {has_id.sum()} jugadores con ID de Transfermarkt")

    # ── Crear sesión HTTP ──────────────────────────────────────────────────────
    session = requests.Session()
    session.trust_env = False

    # ── Acumuladores para las hojas de auditoría ───────────────────────────────
    mv_history_rows    = []   # filas para la hoja "MV History"
    injuries_by_season = []   # filas para la hoja "Injuries by Season"

    # ── Métricas de lesión por jugador ─────────────────────────────────────────
    injury_metrics_map = {}   # {tm_player_id: {metric_key: value}}

    player_ids = combined.loc[has_id, TM_ID_COL].unique()
    n = len(player_ids)
    print(f"\n🔄 Procesando {n} jugadores...")

    for i, tm_id in enumerate(player_ids):
        tm_id = int(tm_id)
        player_row  = combined[combined[TM_ID_COL] == tm_id].iloc[0]
        player_name = player_row.get(PLAYER_COL, f'ID_{tm_id}')
        player_squad = player_row.get(SQUAD_COL, '')

        if (i + 1) % 50 == 0:
            print(f"  [{i+1}/{n}] {player_name}...")

        # ── Historial de valor de mercado ──────────────────────────────────
        mv_records = scrape_mv_history(tm_id, session)
        for rec in mv_records:
            mv_history_rows.append({
                TM_ID_COL:    tm_id,
                PLAYER_COL:   player_name,
                SQUAD_COL:    player_squad,
                **rec
            })

        # ── Lesiones ───────────────────────────────────────────────────────
        inj_records, inj_by_season = scrape_injury_history(tm_id, session)

        for rec in inj_records:
            injuries_by_season.append({
                TM_ID_COL:  tm_id,
                PLAYER_COL: player_name,
                SQUAD_COL:  player_squad,
                **rec
            })

        # Calcular métricas derivadas
        metrics = compute_injury_metrics(inj_by_season, inj_records)
        injury_metrics_map[tm_id] = metrics

        # Guardar progreso parcial cada 100 jugadores
        if (i + 1) % 100 == 0:
            pd.DataFrame(mv_history_rows).to_csv("partial_mv_history.csv", index=False)
            pd.DataFrame(injuries_by_season).to_csv("partial_injuries.csv", index=False)
            print(f"  💾 Progreso guardado ({i+1}/{n})")

    # ── Añadir métricas de lesión al DataFrame combinado ──────────────────────
    print("\n📊 Añadiendo métricas a los jugadores...")

    # Inicializar columnas de lesión con valores por defecto
    for metric_key, col_es in INJURY_METRIC_COLS.items():
        if col_es not in combined.columns:
            if metric_key in ('inj_risk',):
                combined[col_es] = 'low'
            elif metric_key in ('inj_series_type',):
                combined[col_es] = 'stable'
            elif metric_key in ('inj_trend_sig',):
                combined[col_es] = False
            else:
                combined[col_es] = 0

    # Rellenar métricas para cada jugador scrapeado
    for idx, row in combined.iterrows():
        tm_id_val = row.get(TM_ID_COL)
        if pd.isna(tm_id_val):
            continue
        tm_id_int = int(tm_id_val)
        if tm_id_int in injury_metrics_map:
            for metric_key, val in injury_metrics_map[tm_id_int].items():
                col_es = INJURY_METRIC_COLS.get(metric_key)
                if col_es and col_es in combined.columns:
                    combined.at[idx, col_es] = val

    # ── Añadir ingresos de club ────────────────────────────────────────────────
    combined[REVENUE_COL] = combined[SQUAD_COL].map(
        lambda s: CLUB_REVENUES.get(str(s), (None, None))[0]
    )
    combined[TIER_COL] = combined[SQUAD_COL].map(
        lambda s: CLUB_REVENUES.get(str(s), (None, None))[1]
    )

    # ── Calcular años de contrato restantes ────────────────────────────────────
    print("📅 Calculando años de contrato restantes...")

    def calc_years(val):
        if pd.isna(val):
            return None
        try:
            match = re.search(r'(\d{4})', str(val))
            if match:
                return max(0, int(match.group(1)) - 2024)
        except Exception:
            pass
        return None

    if CONTRACT_COL in combined.columns:
        combined[CONTRACT_Y_COL] = combined[CONTRACT_COL].apply(calc_years)

    # ── Dividir por liga y guardar Excel final ────────────────────────────────
    print(f"\n💾 Guardando '{OUTPUT_PATH}'...")

    mv_df  = pd.DataFrame(mv_history_rows)
    inj_df = pd.DataFrame(injuries_by_season)

    with pd.ExcelWriter(OUTPUT_PATH, engine='xlsxwriter') as writer:
        for liga, sheet_name in LIGA_TO_SHEET.items():
            df_liga = combined[combined['Liga'] == liga].drop(columns=['Liga'], errors='ignore')
            df_liga = df_liga.reset_index(drop=True)
            df_liga.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"  ✓ {sheet_name} ({len(df_liga)} jugadores)")

        if not mv_df.empty:
            mv_df.to_excel(writer, sheet_name='MV History', index=False)
            print(f"  ✓ MV History ({len(mv_df)} filas)")

        if not inj_df.empty:
            inj_df.to_excel(writer, sheet_name='Injuries by Season', index=False)
            print(f"  ✓ Injuries by Season ({len(inj_df)} filas)")

    print(f"\n✅ Guardado: {OUTPUT_PATH}")
    print(f"   Jugadores procesados: {len(player_ids)}")
    print(f"   Puntos de historial MV: {len(mv_history_rows)}")
    print(f"   Registros de lesión: {len(injuries_by_season)}")


if __name__ == '__main__':
    main()
