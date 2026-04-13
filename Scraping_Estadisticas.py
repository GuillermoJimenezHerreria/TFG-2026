"""
scrape_fbref.py — TFG Guillermo Jiménez
========================================
Descarga las estadísticas de jugadores de las 5 grandes ligas europeas
desde FBref (sports-reference.com) y las guarda en un Excel estructurado.

Fuente: FBref publica tablas HTML con estadísticas por temporada.
        Cada liga tiene su propia URL de estadísticas de jugadores.

Output: Players and teams data TFG.xlsx
        → 5 hojas de jugadores (una por liga)
        → 5 hojas de equipos (una por liga)

Requisitos: pip install requests beautifulsoup4 pandas openpyxl lxml
"""

import requests
import time
import random
import pandas as pd
from bs4 import BeautifulSoup
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

# ─── Configuración ────────────────────────────────────────────────────────────

OUTPUT_PATH = os.path.join(DB, 'Base de datos inicial.xlsx')

# FBref bloquea scrapers sin User-Agent. Simulamos un navegador real.
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://fbref.com/',
}

# Delay entre peticiones: FBref limita a ~20 req/min sin bloquear
MIN_DELAY = 4.0
MAX_DELAY = 7.0

# ─── URLs de estadísticas estándar por liga (temporada 2023-24) ───────────────
# FBref organiza los datos por "competition". El parámetro ?comp_id=X
# selecciona la competición. Usamos la tabla "stats_standard" que incluye
# goles, asistencias, xG, minutos, etc.
#
# Estructura de la URL:
#   /en/comps/{comp_id}/stats/players/{comp_id}-stats
#
LEAGUE_URLS = {
    'PL Players': {
        'url': 'https://fbref.com/en/comps/9/stats/players/Premier-League-Stats',
        'league': 'Premier League',
    },
    'LaLiga Players': {
        'url': 'https://fbref.com/en/comps/12/stats/players/La-Liga-Stats',
        'league': 'LaLiga',
    },
    'Serie A Players': {
        'url': 'https://fbref.com/en/comps/11/stats/players/Serie-A-Stats',
        'league': 'Serie A',
    },
    'Bundesliga Players': {
        'url': 'https://fbref.com/en/comps/20/stats/players/Bundesliga-Stats',
        'league': 'Bundesliga',
    },
    'Ligue 1 Players': {
        'url': 'https://fbref.com/en/comps/13/stats/players/Ligue-1-Stats',
        'league': 'Ligue 1',
    },
}

# URLs para estadísticas de equipos (tabla "stats_squads_standard_for")
TEAM_URLS = {
    'PL Teams':          'https://fbref.com/en/comps/9/stats/squads/Premier-League-Stats',
    'LaLiga Teams':      'https://fbref.com/en/comps/12/stats/squads/La-Liga-Stats',
    'Serie A Teams':     'https://fbref.com/en/comps/11/stats/squads/Serie-A-Stats',
    'Bundesliga Teams':  'https://fbref.com/en/comps/20/stats/squads/Bundesliga-Stats',
    'Ligue 1 Teams':     'https://fbref.com/en/comps/13/stats/squads/Ligue-1-Stats',
}

# ─── Función principal de scraping ───────────────────────────────────────────

def scrape_fbref_table(url, table_id=None):
    """
    Descarga una página de FBref y extrae la tabla de estadísticas principal.

    FBref renderiza las tablas en HTML estático (no JavaScript), así que
    podemos parsear directamente con BeautifulSoup.

    Parámetros:
        url      : URL completa de FBref
        table_id : ID del elemento <table> a extraer (None = primera tabla)

    Devuelve: pd.DataFrame con las estadísticas, o None si hay error.
    """
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
    except requests.RequestException as e:
        print(f"  ✗ Error de conexión: {e}")
        return None

    if resp.status_code == 429:
        # FBref devuelve 429 si hacemos demasiadas peticiones seguidas.
        # Esperamos más tiempo y reintentamos una vez.
        print("  ⚠ Rate limit (429). Esperando 60 segundos...")
        time.sleep(60)
        resp = requests.get(url, headers=HEADERS, timeout=30)

    if resp.status_code != 200:
        print(f"  ✗ HTTP {resp.status_code}")
        return None

    soup = BeautifulSoup(resp.text, 'lxml')

    # FBref encierra algunas tablas en comentarios HTML para evitar scraping.
    # Hay que descomentarlas manualmente.
    from bs4 import Comment
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    for comment in comments:
        comment_soup = BeautifulSoup(comment, 'lxml')
        tables = comment_soup.find_all('table')
        if tables:
            for table in tables:
                if table_id is None or table.get('id') == table_id:
                    soup.body.append(table)

    # Buscar la tabla por ID o coger la primera disponible
    if table_id:
        table = soup.find('table', {'id': table_id})
    else:
        # FBref usa IDs como "stats_standard_9" para la Premier League
        table = soup.find('table', id=lambda x: x and 'stats_standard' in x)
        if not table:
            table = soup.find('table', class_='stats_table')

    if not table:
        print("  ✗ No se encontró la tabla de estadísticas")
        return None

    # pandas.read_html entiende HTML de tabla directamente
    df = pd.read_html(str(table), header=1)[0]

    # FBref incluye filas repetidas de cabecera ("Rk", "Player") que hay que eliminar
    if 'Rk' in df.columns:
        df = df[df['Rk'] != 'Rk']
        df = df[df['Rk'].notna()]

    # Eliminar columnas "Matches" (son enlaces en el HTML, sin valor numérico)
    df = df.drop(columns=[c for c in df.columns if 'Matches' in str(c)], errors='ignore')

    # FBref usa MultiIndex de columnas en algunas tablas; aplanar
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join([str(a), str(b)]).strip('_')
                      if b and b != a else str(a)
                      for a, b in df.columns]

    # Convertir columnas numéricas
    for col in df.columns:
        if col not in ['Player', 'Nation', 'Pos', 'Squad', 'Age', 'Born', 'Comp']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


# ─── Script principal ─────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  FBref Scraping — 5 Grandes Ligas (2023-24)")
    print("=" * 60)

    sheets = {}

    # ── Jugadores ──────────────────────────────────────────────
    for sheet_name, config in LEAGUE_URLS.items():
        print(f"\n📥 {sheet_name} ({config['league']})...")
        df = scrape_fbref_table(config['url'])

        if df is None:
            print(f"  ✗ Falló el scraping de {sheet_name}")
            continue

        # Añadir columna de liga para identificar la hoja después de merge
        df['League'] = config['league']

        # Limpiar: quitar jugadores sin nombre
        df = df[df['Player'].notna()].copy()
        df = df[~df['Player'].astype(str).str.strip().isin(['', 'Player'])]

        print(f"  ✅ {len(df)} jugadores descargados")
        sheets[sheet_name] = df

    # ── Equipos ────────────────────────────────────────────────
    for sheet_name, url in TEAM_URLS.items():
        print(f"\n📥 {sheet_name}...")
        df = scrape_fbref_table(url)

        if df is None:
            print(f"  ✗ Falló el scraping de {sheet_name}")
            continue

        print(f"  ✅ {len(df)} equipos descargados")
        sheets[sheet_name] = df

    # ── Guardar Excel ──────────────────────────────────────────
    print(f"\n💾 Guardando '{OUTPUT_PATH}'...")

    # Orden de hojas: primero jugadores, luego equipos
    sheet_order = list(LEAGUE_URLS.keys()) + list(TEAM_URLS.keys())

    with pd.ExcelWriter(OUTPUT_PATH, engine='openpyxl') as writer:
        for sheet_name in sheet_order:
            if sheet_name in sheets:
                sheets[sheet_name].to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"  ✓ Hoja '{sheet_name}' guardada")

    print(f"\n✅ Archivo guardado: {OUTPUT_PATH}")
    print(f"   {len(sheets)} hojas creadas")


if __name__ == '__main__':
    main()
