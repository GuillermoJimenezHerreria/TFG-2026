"""
TFG Guillermo Jiménez - Scraping de datos faltantes
=====================================================
Este script obtiene los valores de mercado de Transfermarkt
para todos los jugadores del Excel del TFG.

Requisitos: pip install requests beautifulsoup4 pandas openpyxl lxml

Uso:
    python scrape_tfg_data.py

El script genera un nuevo archivo Excel con las columnas añadidas:
    - market_value_eur      (valor de mercado en €)
    - contract_until        (fecha fin de contrato)
    - tm_player_id          (ID de Transfermarkt)
"""

import requests
import time
import random
import pandas as pd
import re
import json
from bs4 import BeautifulSoup
from difflib import SequenceMatcher
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
                    'lxml', 'openpyxl'], capture_output=True)
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DB = os.path.join(BASE_DIR, 'Bases de Datos')

# ─── Configuración ───────────────────────────────────────────────────────────

EXCEL_PATH  = os.path.join(DB, 'Base de datos inicial.xlsx')
OUTPUT_PATH = os.path.join(DB, 'Jugadores_Combinados.xlsx')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xhtml+xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Referer': 'https://www.transfermarkt.com/',
    'Cache-Control': 'no-cache',
}

# Delay entre peticiones (segundos) para no saturar el servidor
MIN_DELAY = 2.0
MAX_DELAY = 4.5

# ─── Mapeo de nombres FBref → Transfermarkt (verein_id, season) ──────────────

TEAM_TM_IDS = {
    # Premier League
    "Arsenal":           ("arsenal-fc",        11,    2024),
    "Aston Villa":       ("aston-villa",        405,   2024),
    "Bournemouth":       ("afc-bournemouth",    989,   2024),
    "Brentford":         ("brentford-fc",       1148,  2024),
    "Brighton":          ("brighton-amp-hove-albion",  1237, 2024),
    "Burnley":           ("fc-burnley",         1132,  2024),
    "Chelsea":           ("fc-chelsea",         631,   2024),
    "Crystal Palace":    ("crystal-palace",     873,   2024),
    "Everton":           ("fc-everton",         29,    2024),
    "Fulham":            ("fc-fulham",          931,   2024),
    "Leeds United":      ("leeds-united",       399,   2024),
    "Liverpool":         ("fc-liverpool",       31,    2024),
    "Manchester City":   ("manchester-city",    281,   2024),
    "Manchester Utd":    ("manchester-united",  985,   2024),
    "Newcastle Utd":     ("newcastle-united",   762,   2024),
    "Nott'ham Forest":   ("nottingham-forest",  703,   2024),
    "Sunderland":        ("sunderland-afc",     289,   2024),
    "Tottenham":         ("tottenham-hotspur",  148,   2024),
    "West Ham":          ("west-ham-united",    379,   2024),
    "Wolves":            ("wolverhampton-wanderers", 543, 2024),
    # LaLiga
    "Alavés":            ("deportivo-alaves",   1108,  2024),
    "Athletic Club":     ("athletic-club",      621,   2024),
    "Atlético Madrid":   ("atletico-madrid",    13,    2024),
    "Barcelona":         ("fc-barcelona",       131,   2024),
    "Betis":             ("real-betis-balompie",150,   2024),
    "Celta Vigo":        ("rc-celta-de-vigo",   940,   2024),
    "Elche":             ("elche-cf",           1728,  2024),
    "Espanyol":          ("rcd-espanyol",       714,   2024),
    "Getafe":            ("getafe-cf",          3709,  2024),
    "Girona":            ("girona-fc",          12321, 2024),
    "Levante":           ("levante-ud",         8304,  2024),
    "Mallorca":          ("rcd-mallorca",       237,   2024),
    "Osasuna":           ("ca-osasuna",         331,   2024),
    "Oviedo":            ("real-oviedo",        862,   2024),
    "Rayo Vallecano":    ("rayo-vallecano",     367,   2024),
    "Real Madrid":       ("real-madrid",        418,   2024),
    "Real Sociedad":     ("real-sociedad",      681,   2024),
    "Sevilla":           ("sevilla-fc",         368,   2024),
    "Valencia":          ("fc-valencia",        1049,  2024),
    "Villarreal":        ("villarreal-cf",      1050,  2024),
    # Serie A
    "Atalanta":          ("atalanta-bc",        800,   2024),
    "Bologna":           ("fc-bologna",         1025,  2024),
    "Cagliari":          ("cagliari-calcio",    1390,  2024),
    "Como":              ("como-1907",          22944, 2024),
    "Cremonese":         ("us-cremonese",       3260,  2024),
    "Fiorentina":        ("acf-fiorentina",     430,   2024),
    "Genoa":             ("genoa-cfc",          252,   2024),
    "Hellas Verona":     ("hellas-verona",      276,   2024),
    "Inter":             ("inter-mailand",      46,    2024),
    "Juventus":          ("juventus-fc",        506,   2024),
    "Lazio":             ("ss-lazio",           398,   2024),
    "Lecce":             ("us-lecce",           1639,  2024),
    "Milan":             ("ac-mailand",         5,     2024),
    "Napoli":            ("ssc-napoli",         6195,  2024),
    "Parma":             ("parma-calcio-1913",  117,   2024),
    "Pisa":              ("ac-pisa-1909",       2036,  2024),
    "Roma":              ("as-rom",             12,    2024),
    "Sassuolo":          ("us-sassuolo",        6574,  2024),
    "Torino":            ("fc-turin",           416,   2024),
    "Udinese":           ("udinese-calcio",     410,   2024),
    # Bundesliga
    "Augsburg":          ("fc-augsburg",        167,   2024),
    "Bayern Munich":     ("fc-bayern-munchen",  27,    2024),
    "Dortmund":          ("borussia-dortmund",  16,    2024),
    "Eint Frankfurt":    ("eintracht-frankfurt",24,    2024),
    "Freiburg":          ("sc-freiburg",        60,    2024),
    "Gladbach":          ("borussia-monchengladbach", 18, 2024),
    "Hamburger SV":      ("hamburger-sv",       41,    2024),
    "Heidenheim":        ("1-fc-heidenheim-1846",2110, 2024),
    "Hoffenheim":        ("tsg-hoffenheim",     533,   2024),
    "Köln":              ("1-fc-koln",          3,     2024),
    "Leverkusen":        ("bayer-04-leverkusen",15,    2024),
    "Mainz 05":          ("1-fsv-mainz-05",     39,    2024),
    "RB Leipzig":        ("rb-leipzig",         23826, 2024),
    "St. Pauli":         ("fc-st-pauli",        35,    2024),
    "Stuttgart":         ("vfb-stuttgart",      79,    2024),
    "Union Berlin":      ("1-fc-union-berlin",  89,    2024),
    "Werder Bremen":     ("sv-werder-bremen",   86,    2024),
    "Wolfsburg":         ("vfl-wolfsburg",      82,    2024),
    # Ligue 1
    "Angers":            ("angers-sco",         22926, 2024),
    "Auxerre":           ("aj-auxerre",         1427,  2024),
    "Brest":             ("stade-brest-29",     3911,  2024),
    "Le Havre":          ("le-havre-ac",        1298,  2024),
    "Lens":              ("rc-lens",            826,   2024),
    "Lille":             ("losc-lille",         1082,  2024),
    "Lorient":           ("fc-lorient",         1442,  2024),
    "Lyon":              ("olympique-lyonnais", 1041,  2024),
    "Marseille":         ("olympique-marseille",244,   2024),
    "Metz":              ("fc-metz",            347,   2024),
    "Monaco":            ("as-monaco",          162,   2024),
    "Nantes":            ("fc-nantes",          995,   2024),
    "Nice":              ("ogc-nice",           417,   2024),
    "Paris FC":          ("paris-fc",           7642,  2024),
    "Paris S-G":         ("paris-saint-germain",583,   2024),
    "Rennes":            ("stade-rennais",      273,   2024),
    "Strasbourg":        ("rc-strasbourg-alsace",667,  2024),
    "Toulouse":          ("toulouse-fc",        415,   2024),
}

# ─── Funciones de scraping ────────────────────────────────────────────────────

def parse_market_value(value_str):
    """Convierte '€45m' o '€500k' a número entero en euros."""
    if not value_str or value_str in ['-', '—', '']:
        return None
    value_str = value_str.replace('€', '').replace(',', '.').strip()
    try:
        if 'm' in value_str.lower():
            return int(float(value_str.lower().replace('m', '')) * 1_000_000)
        elif 'k' in value_str.lower():
            return int(float(value_str.lower().replace('k', '')) * 1_000)
        else:
            return int(float(value_str))
    except (ValueError, TypeError):
        return None


def similar(a, b):
    """Ratio de similitud entre dos cadenas."""
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def scrape_squad(team_name, tm_slug, tm_id, season_id, session):
    """
    Scrape la página de plantilla de un equipo en Transfermarkt.
    Devuelve lista de dicts: {name, position, age, market_value_eur, contract_until, tm_player_id}
    """
    url = f"https://www.transfermarkt.com/{tm_slug}/kader/verein/{tm_id}/saison_id/{season_id}"

    try:
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        resp = session.get(url, headers=HEADERS, timeout=20)

        if resp.status_code != 200:
            print(f"  ⚠ HTTP {resp.status_code} para {team_name}")
            return []

        soup = BeautifulSoup(resp.text, 'lxml')
        rows = soup.select('table.items tbody tr.odd, table.items tbody tr.even')

        if not rows:
            # Intentar selector alternativo
            rows = soup.select('table.items tr')

        players = []
        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 6:
                continue

            # Nombre del jugador
            name_cell = row.select_one('td.hauptlink a')
            if not name_cell:
                continue
            name = name_cell.get_text(strip=True)

            # Posición
            pos_cell = row.select_one('td.posrela table tr:last-child td')
            position = pos_cell.get_text(strip=True) if pos_cell else ''

            # Valor de mercado (última celda con clase rechts)
            mv_cell = row.select_one('td.rechts.hauptlink')
            market_value_raw = mv_cell.get_text(strip=True) if mv_cell else ''
            market_value = parse_market_value(market_value_raw)

            # Contrato hasta
            contract_cells = row.find_all('td', class_='zentriert')
            contract_until = ''
            for cc in contract_cells:
                text = cc.get_text(strip=True)
                if re.match(r'\d{1,2}/\d{4}|\d{4}', text):
                    contract_until = text
                    break

            # ID del jugador (de la URL del enlace)
            player_link = name_cell.get('href', '')
            tm_player_id_match = re.search(r'/spieler/(\d+)', player_link)
            tm_player_id = int(tm_player_id_match.group(1)) if tm_player_id_match else None

            players.append({
                'tm_name': name,
                'position': position,
                'market_value_eur': market_value,
                'market_value_raw': market_value_raw,
                'contract_until': contract_until,
                'tm_player_id': tm_player_id,
            })

        return players

    except Exception as e:
        print(f"  ✗ Error scraping {team_name}: {e}")
        return []


def match_players(fbref_players, tm_players):
    """
    Empareja jugadores de FBref con los de Transfermarkt por similitud de nombre.
    Devuelve dict: {fbref_index: tm_player_dict}
    """
    matches = {}
    used_tm = set()

    for idx, fbref_name in enumerate(fbref_players):
        if pd.isna(fbref_name):
            continue

        best_score = 0
        best_tm = None

        for tm_idx, tm_player in enumerate(tm_players):
            if tm_idx in used_tm:
                continue
            score = similar(str(fbref_name), tm_player['tm_name'])
            if score > best_score:
                best_score = score
                best_tm = (tm_idx, tm_player)

        # Solo aceptar si la similitud es > 0.7
        if best_score > 0.70 and best_tm:
            matches[idx] = best_tm[1]
            used_tm.add(best_tm[0])
        elif best_score > 0.50 and best_tm:
            # Coincidencia débil - registrar con flag
            tm_data = best_tm[1].copy()
            tm_data['match_confidence'] = 'low'
            matches[idx] = tm_data
            used_tm.add(best_tm[0])

    return matches


# ─── Carga del Excel ──────────────────────────────────────────────────────────

def load_all_players():
    """Carga todos los jugadores del Excel en un DataFrame unificado."""
    xl = pd.ExcelFile(EXCEL_PATH)

    sheet_config = {
        'PL Players':         ('Premier League', 2),
        'LaLiga Players':     ('LaLiga',         5),
        'Serie A Players':    ('Serie A',         4),
        'Bundesliga Players': ('Bundesliga',      4),
        'Ligue 1 Players':    ('Ligue 1',         4),
    }

    all_dfs = []
    for sheet, (league, hrow) in sheet_config.items():
        df = pd.read_excel(xl, sheet_name=sheet, header=hrow)
        # Normalizar columnas
        if 'Player' not in df.columns:
            cols = df.columns.tolist()
            # Buscar columna que parezca nombre de jugador
            for c in cols:
                if df[c].dtype == object and df[c].dropna().str.len().mean() > 5:
                    df = df.rename(columns={c: 'Player'})
                    break

        df = df[df['Player'].notna()].copy()
        df = df[~df['Player'].astype(str).str.match(r'Player|Rk|▲|^\s*$')]
        df['League'] = league
        df['_sheet'] = sheet
        all_dfs.append(df)

    combined = pd.concat(all_dfs, ignore_index=True)
    combined['market_value_eur'] = None
    combined['market_value_raw'] = None
    combined['contract_until'] = None
    combined['tm_player_id'] = None
    combined['match_confidence'] = None

    return combined


# ─── Script principal ─────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  TFG Scraping - Transfermarkt Market Values")
    print("=" * 60)
    print()

    # Cargar datos existentes
    df = load_all_players()
    print(f"   {len(df)} jugadores cargados de {df['League'].nunique()} ligas")

    # Crear sesión
    session = requests.Session()
    session.trust_env = False  # ignorar proxy del sistema

    # Procesar equipo por equipo
    total_teams = len(TEAM_TM_IDS)
    matched_total = 0
    not_found = []

    for i, (fbref_name, (tm_slug, tm_id, season_id)) in enumerate(TEAM_TM_IDS.items()):
        print(f"\n[{i+1:02d}/{total_teams}] {fbref_name} (TM ID: {tm_id})")

        # Obtener jugadores de este equipo del Excel
        team_mask = df['Squad'].astype(str) == fbref_name
        team_players = df.loc[team_mask, 'Player'].tolist()

        if not team_players:
            print(f"  → No hay jugadores de {fbref_name} en el Excel, saltando...")
            continue

        print(f"  → {len(team_players)} jugadores en FBref para este equipo")

        # Scraping de Transfermarkt
        tm_players = scrape_squad(fbref_name, tm_slug, tm_id, season_id, session)

        if not tm_players:
            print(f"  → No se obtuvieron datos de TM")
            not_found.append(fbref_name)
            continue

        print(f"  → {len(tm_players)} jugadores encontrados en TM")

        # Emparejar jugadores
        matches = match_players(team_players, tm_players)
        print(f"  → {len(matches)}/{len(team_players)} emparejamientos")

        # Actualizar DataFrame
        team_indices = df[team_mask].index.tolist()
        for local_idx, tm_data in matches.items():
            global_idx = team_indices[local_idx]
            df.at[global_idx, 'market_value_eur'] = tm_data.get('market_value_eur')
            df.at[global_idx, 'market_value_raw'] = tm_data.get('market_value_raw')
            df.at[global_idx, 'contract_until'] = tm_data.get('contract_until')
            df.at[global_idx, 'tm_player_id'] = tm_data.get('tm_player_id')
            df.at[global_idx, 'match_confidence'] = tm_data.get('match_confidence', 'high')

        matched_total += len(matches)

        # Guardar progreso parcial cada 10 equipos
        if (i + 1) % 10 == 0:
            df.to_csv("partial_progress.csv", index=False)
            print(f"  Progreso guardado ({matched_total} jugadores con datos)")

    # ─── Resumen ──────────────────────────────────────────────────────────────

    print("\n" + "=" * 60)
    print("  RESUMEN FINAL")
    print("=" * 60)

    n_with_value = df['market_value_eur'].notna().sum()
    print(f"\n Jugadores con valor de mercado: {n_with_value}/{len(df)} "
          f"({n_with_value/len(df)*100:.1f}%)")

    if not_found:
        print(f"\n⚠ Equipos sin datos de TM ({len(not_found)}):")
        for t in not_found:
            print(f"   - {t}")

    print(f"\n Distribución de valores (€M):")
    vals = df['market_value_eur'].dropna() / 1_000_000
    print(f"   Mediana: {vals.median():.1f}M  |  Media: {vals.mean():.1f}M")
    print(f"   Min: {vals.min():.2f}M  |  Max: {vals.max():.1f}M")
    print(f"   Top 10 por liga:")

    for league in df['League'].unique():
        league_vals = df[df['League'] == league].nlargest(1, 'market_value_eur')
        if not league_vals.empty:
            row = league_vals.iloc[0]
            print(f"   {league}: {row.get('Player','')} → €{row['market_value_eur']/1e6:.1f}M")

    # ─── Guardar Excel final ───────────────────────────────────────────────────

    # Reorganizar columnas: mover las nuevas justo después de la columna Player/Squad
    cols = df.columns.tolist()
    new_cols = ['market_value_eur', 'market_value_raw', 'contract_until',
                'tm_player_id', 'match_confidence', 'League']
    other_cols = [c for c in cols if c not in new_cols + ['League', '_sheet']]

    # Insertar después de las primeras columnas de identidad
    identity_cols = ['Player', 'Nation', 'Pos', 'Squad', 'Age', 'Born']
    identity_cols = [c for c in identity_cols if c in other_cols]
    rest_cols = [c for c in other_cols if c not in identity_cols]

    final_order = identity_cols + new_cols[:5] + ['League'] + rest_cols
    final_order = [c for c in final_order if c in df.columns]

    df[final_order].to_excel(OUTPUT_PATH, index=False)
    print()


if __name__ == '__main__':
    main()
