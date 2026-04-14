"""
Elaboración_Dataset_Definitivo.py
==================================
Script de limpieza y estandarización del Dataset_Definitivo.

Pasos aplicados:
  1. Estandarización de columnas: todos los tabs de jugadores pasan a tener
     los mismos nombres en español que PL Players.
  2. Estandarización de tabs de equipos: mismos nombres en español que PL Teams.
  3. Corrección de valores de mercado erróneos (cruce con Transfermarkt por ID).
  4. Eliminación de filas donde el jugador no tiene valor de mercado público.

Fuente : Dataset_Definitivo.xlsx  (output de Scraping_Lesiones_y_Valores_de_Mercado.py)
Destino: Dataset_Definitivo.xlsx  (sobreescribe con la versión limpia)
"""

import sys, os
import pandas as pd
import numpy as np

# ═══════════════════════════════════════════════════════════════
#  CONFIGURACIÓN — LOCAL vs GOOGLE COLAB
#  En Colab: sube la carpeta 'TFG Business Analytics' a Google
#  Drive y ejecuta con Runtime → Ejecutar todo.
# ═══════════════════════════════════════════════════════════════
IN_COLAB = 'google.colab' in sys.modules
if IN_COLAB:
    DB = '/content'
    import subprocess
    subprocess.run(['pip', 'install', '-q', 'openpyxl', 'xlsxwriter'],
                   capture_output=True)
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DB = os.path.join(BASE_DIR, 'Bases de Datos')

SOURCE = os.path.join(DB, 'Dataset_Definitivo.xlsx')
DEST   = os.path.join(DB, 'Dataset_Definitivo.xlsx')

# ─── 1. Mapa de renombrado de columnas: inglés → español ─────────────────────
PLAYER_RENAME = {
    'Player':            'Jugador',
    'Nation':            'Nacionalidad',
    'Pos':               'Posición',
    'Squad':             'Equipo',
    'Age':               'Edad',
    'Born':              'Año de nacimiento',
    'MP':                'Partidos jugados esta temporada',
    'Starts':            'Titularidades',
    'Min':               'Minutos jugados esta temporada',
    '90s':               'Minutos totales/90',
    'Gls':               'Goles',
    'Ast':               'Asistencias',
    'G+A':               'G+A',
    'G-PK':              'Goles que no son de penalti',
    'PK':                'Goles de penalti',
    'PKatt':             'Penaltis tirados',
    'CrdY':              'Amarillas',
    'CrdR':              'Rojas',
    'xG':                'Goles esperados',
    'npxG':              'Goles esperados sin contar penaltis',
    'xAG':               'Asistencias esperadas',
    'npxG+xAG':          'Contribuciones de gol esperadas sin penaltis',
    'PrgC':              'Conducción de balón de al menos 10 metros hacia la portería rival',
    'PrgP':              'Pases progresivos *',
    'PrgR':              'Recepciones de pases progresivos',
    'Gls.1':             'Goles/ 90 mins',
    'Ast.1':             'Asistencias/ 90mins',
    'G+A.1':             'G+A/ 90 mins',
    'G-PK.1':            'Goles sin contar penaltis/ 90 mins',
    'G+A-PK':            'G+A sin contar penaltis/ 90 mins',
    'xG.1':              'Goles esperados/ 90 mins',
    'xAG.1':             'Asistencias esperadas/ 90 mins',
    'xG+xAG':            '(Goles+Asistencias) esperados/ 90 mins',
    'npxG.1':            'Goles esperados sin contar penaltis/ 90 mins',
    'npxG+xAG.1':        '(Goles sin contar penaltis + Asistencias) esperados/ 90 mins',
    'market_value_eur':  'Valor de mercado',
    'dob_tm':            'Fecha de nacimiento',
    'tm_player_id':      'ID del jugador en Transfermarkt',
    'match_confidence':  'match_confidence',
    'contract_until':    'Fecha de expiración del contrato',
    'contract_years_remaining': 'Años de contrato restantes',
    'injury_count':      'Cantidad total de lesiones registradas',
    'injury_days_per_season': 'Promedio de días de baja por lesión por temporada',
    'injury_frequency':  'Número promerio de lesiones por temporada',
    'club_revenue_eur':  'Ingreso total anual del club',
    'revenue_tier':      'Categoría de ingresos del club**',
    'mv_history_points': '***Índice de madurez de carrera',
    'inj_days_last1':    'Días de lesión en la temporada pasada',
    'inj_days_avg_last2':'Promedio de días de lesión entre la temporada pasada y la anterior',
    'inj_days_avg_last3':'Promedio de días de lesión entre las 3 últimas temporadas',
    'inj_trend_slope':   'Pendiente de la regresión lineal de la cantidad de lesiones (días de lesión añadidos o disminuidos en promedio anual)',
    'inj_trend_pvalue':  'P-valor de la pendiente: Probabilidad de que la tendencia de la pendiente sea un ruido aleatorio (Aceptamos la hipótesis nula (pendiente válida) con un 10% o menos)',
    'inj_trend_sig':     'Aceptación o refutación del modelo (Uso un 10% en vez del 5% habitual por las pocas variables que hay en este análisis, para que no salga todo estable)',
    'inj_ewa_days':      'Media anual ponderada exponencialmente de los días de baja por lesión',
    'inj_risk':          'Riesgo de lesión',
    'inj_series_type':   'inj_series_type',
    # Columnas adicionales de otras ligas
    'Entradas ganadas (FBref)':         'Entradas ganadas ',
    'Intercepciones (FBref)':           'Intercepciones',
    'Faltas cometidas (FBref)':         'Faltas cometidas ',
    'Faltas recibidas (FBref)':         'Faltas recibidas ',
    'Centros (FBref)':                  'Centros',
    'Tarjetas amarillas (FBref)':       'Tarjetas amarillas',
    'Tarjetas rojas (FBref)':           'Tarjetas rojas ',
    'Goles (FBref)':                    'Goles ',
    'Tiros totales (FBref)':            'Tiros totales',
    'Tiros a puerta (FBref)':           'Tiros a puerta ',
    'Precisión de tiro % (FBref)':      'Precisión de tiro % ',
    'Tiros por 90 min (FBref)':         'Tiros por 90 min',
    'Penaltis marcados (FBref)':        'Penaltis marcados',
    'Penaltis tirados (FBref)':         'Penaltis tirados.1',
    'PJ Portero (FBref)':               'PJ Portero ',
    'Titularidades Portero (FBref)':    'Titularidades Portero ',
    'Goles encajados/90 (FBref)':       'Goles encajados/90 ',
    'Tiros a puerta recibidos (FBref)': 'Tiros a puerta recibidos ',
    'Paradas (FBref)':                  'Paradas ',
    '% Paradas (FBref)':                '% Paradas ',
    'Porterías a cero (FBref)':         'Porterías a cero ',
    '% Porterías a cero (FBref)':       '% Porterías a cero ',
    'Penaltis recibidos (FBref)':       'Penaltis recibidos ',
    'Penaltis encajados (FBref)':       'Penaltis encajados ',
    'Penaltis parados (FBref)':         'Penaltis parados ',
    '% Penaltis parados (FBref)':       '% Penaltis parados ',
}

# Columnas extra que traen los tabs no-PL y que se eliminan
COLS_TO_DROP_NONPL = ['Unnamed: 2', 'Rk', 'market_value_raw', 'tm_name_matched', 'Matches']

# ─── 2. Mapa de renombrado de columnas de equipos ────────────────────────────
TEAM_RENAME = {
    'Squad':            'Equipo',
    '# Pl':             'Cantidad de jugadores',
    'Age':              'Promedio de edad',
    'Poss':             'Promedio del porcentaje de posesión ',
    'Gls':              'Goles',
    'Ast':              'Asistencias',
    'G+A':              'Goles + Asistencias',
    'G-PK':             'Goles que no son de penalti',
    'PK':               'Goles de penalti',
    'PKatt':            'Penaltis tirados',
    'CrdY':             'Amarillas',
    'CrdR':             'Rojas',
    'xG':               'Goles esperados',
    'npxG':             'Goles esperados sin contar penaltis',
    'xAG':              'Asistencias esperadas',
    'npxG+xAG':         'Goles esperados sin penaltis + asistencias esperadas',
    'PrgC':             'Conducciones de balón significativas',
    'PrgP':             'Pases de avances significativos',
    # Columnas por partido (se añade sufijo _p90 a las duplicadas en el origen)
    'Gls_p90':          'Goles por partido',
    'Ast_p90':          'Asistencias por partido',
    'G+A_p90':          'Goles + Asistencias por partido',
    'G-PK_p90':         'Goles sin contar penaltis por partido',
    'G+A-PK':           'Goles sin penaltis + Asistencias por partido',
    'xG_p90':           'Goles esperados por partido',
    'xAG_p90':          'Asistencias esperadas por partido',
    'xG+xAG':           '(Goles + Asistencias) esperados por partido',
    'npxG_p90':         'Goles esperados por partido sin contar penaltis',
    'npxG+xAG_p90':     '(Goles sin contar penaltis + Asistencias) esperados por partido',
}

# ─── 3. Correcciones de valores de mercado verificadas en Transfermarkt ───────
#
# Se detectaron jugadores cuyos datos de Transfermarkt estaban cruzados con
# otro jugador de nombre similar (p.ej. Rodrigo Mendoza tenía el valor de
# Rodri del Man City: 110M€). Se corrigen aquí con los valores reales.
# Si mv=None se elimina la fila (el jugador no tiene valor público).
#
MV_FIXES = {
    'PL Players': {
        'Estêvão Willian': {'tm_id': 1056993, 'mv': 80_000_000},   # Chelsea, préstamo Palmeiras
        'Igor':            {'tm_id': None,     'mv': None},          # ID duplicado con otro jugador
        'Bradley Burrowes':{'tm_id': 1202361,  'mv':  2_000_000},
        'Joe Knight':      {'tm_id':  978668,  'mv':    100_000},
        'Rio Ngumoha':     {'tm_id': 1108466,  'mv': 20_000_000},
    },
    'LaLiga Players': {
        'Rodrigo Mendoza': {'tm_id':  961297,  'mv': 20_000_000},   # Confundido con Rodri (Man City)
        'Rubén':           {'tm_id':  705813,  'mv':  1_500_000},
        'Alexandre Alemão':{'tm_id':  560417,  'mv':  3_500_000},
        'Dawda Camara':    {'tm_id':  891923,  'mv':  1_000_000},
        'Sergio Lozano':   {'tm_id': 1384902,  'mv':     25_000},
    },
    'Serie A Players': {
        'Gabriele Piccinini': {'tm_id': 628365, 'mv': 4_000_000},
        'Massimo Pessina':    {'tm_id': 992569, 'mv': 1_000_000},
    },
    'Bundesliga Players': {
        'Wisdom Mike':   {'tm_id': 1084539, 'mv': 3_000_000},
        'Patrice Čović': {'tm_id': 1114155, 'mv': 4_000_000},
    },
    'Ligue 1 Players': {
        'Francisco Sierralta':      {'tm_id':  371436, 'mv':  1_500_000},
        'Telli Siwe':               {'tm_id': 1091353, 'mv':  1_000_000},
        'Daren Mosengo':            {'tm_id': 1119880, 'mv':     50_000},
        'Soriba Diaoune':           {'tm_id': 1184007, 'mv':    300_000},
        'Mathys De Carvalho':       {'tm_id': 1004486, 'mv':  4_000_000},
        'Ismaël Guerti':            {'tm_id':  860053, 'mv':    100_000},
        'Aladji Bamba':             {'tm_id':  975347, 'mv':  3_500_000},
        'Quentin Ndjantou Mbitcha': {'tm_id': 1053208, 'mv':  7_000_000},
        'Adama Camara':             {'tm_id':  973949, 'mv':  1_500_000},  # Diferente de Dawda Camara
        'Dayann Methalie':          {'tm_id': 1191444, 'mv': 12_000_000},
        'Raphael Le Guen':          {'tm_id': 1166473, 'mv':     50_000},
        'Stephan Zagadou':          {'tm_id': 1228317, 'mv':    100_000},
    },
}


# ─── Funciones auxiliares ─────────────────────────────────────────────────────

def process_player_tab(df, tab_name, is_pl=False):
    """Estandariza un tab de jugadores al formato de PL Players."""
    df = df.copy()
    df.dropna(how='all', inplace=True)

    if is_pl:
        # PL Players: solo renombrar la columna de nombre (col C, que aparece sin nombre)
        rename_map = {df.columns[2]: 'Jugador'}
        df.rename(columns=rename_map, inplace=True)
        # Eliminar columnas trailing sin nombre
        drop_trailing = [c for c in df.columns if str(c).startswith('Unnamed: 8')]
        df.drop(columns=drop_trailing, inplace=True)
    else:
        # Otros tabs: eliminar columnas extra que no están en PL Players
        drop = [c for c in COLS_TO_DROP_NONPL if c in df.columns]
        df.drop(columns=drop, inplace=True)

        # Calcular G+A sin contar penaltis = Goles sin penalti + Asistencias
        # (PL Players la tiene; los demás tabs no)
        if 'G+A' in df.columns and 'G-PK' in df.columns and 'Ast' in df.columns:
            ga_idx = df.columns.get_loc('G+A') + 1
            df.insert(ga_idx, 'G+A sin contar penaltis', df['G-PK'] + df['Ast'])

        # Renombrar todas las columnas al español
        df.rename(columns=PLAYER_RENAME, inplace=True)

    # Aplicar correcciones de valores de mercado
    jugador_col = 'Jugador'
    mv_col      = 'Valor de mercado'
    id_col      = 'ID del jugador en Transfermarkt'
    conf_col    = 'match_confidence'

    if tab_name in MV_FIXES and jugador_col in df.columns:
        for player, fix in MV_FIXES[tab_name].items():
            mask = df[jugador_col] == player
            if mask.any():
                if id_col in df.columns:
                    df.loc[mask, id_col] = fix['tm_id'] if fix['tm_id'] is not None else np.nan
                if mv_col in df.columns:
                    df.loc[mask, mv_col] = fix['mv'] if fix['mv'] is not None else np.nan
                if conf_col in df.columns:
                    df.loc[mask, conf_col] = np.nan
                print(f"  [{tab_name}] Corregido: {player}")

    # Eliminar filas sin valor de mercado (limpieza principal)
    if mv_col in df.columns:
        before = len(df)
        df = df[df[mv_col].notna()]
        dropped = before - len(df)
        if dropped:
            print(f"  [{tab_name}] Eliminadas {dropped} filas sin valor de mercado")

    df.reset_index(drop=True, inplace=True)
    return df


def find_header_row(df_raw):
    """Detecta la fila del encabezado buscando 'Squad'."""
    for i, row in df_raw.iterrows():
        if 'Squad' in row.values:
            return i
    return None


def process_team_tab(df_raw, tab_name):
    """Estandariza un tab de equipos al formato de PL Teams."""
    header_row = find_header_row(df_raw)
    if header_row is None:
        raise ValueError(f"No se encontró fila de encabezado en {tab_name}")

    header_vals = list(df_raw.iloc[header_row])
    data_rows   = df_raw.iloc[header_row + 1:].copy()

    # Desduplicar nombres de columna: la sección Per 90 repite los mismos nombres
    seen = {}
    deduped = []
    for h in header_vals:
        if pd.isna(h):
            deduped.append(None)
        else:
            h = str(h)
            if h in seen:
                seen[h] += 1
                deduped.append(f"{h}_p90")
            else:
                seen[h] = 1
                deduped.append(h)

    data_rows.columns = deduped

    # Mantener solo filas con nombre de equipo real
    data_rows = data_rows[
        data_rows['Squad'].notna() &
        (data_rows['Squad'].astype(str).str.strip() != '')
    ]

    # Eliminar columnas de tiempo de juego que no están en PL Teams
    for c in ['MP', 'Starts', 'Min', '90s']:
        if c in data_rows.columns:
            data_rows.drop(columns=[c], inplace=True)

    # Eliminar columnas sin nombre (índices originales Unnamed)
    data_rows.drop(columns=[c for c in data_rows.columns if c is None], inplace=True)

    # Renombrar al español
    data_rows.rename(columns=TEAM_RENAME, inplace=True)
    data_rows.reset_index(drop=True, inplace=True)
    return data_rows


# ─── Main ─────────────────────────────────────────────────────────────────────
if __name__ == '__main__':

    print("Cargando Dataset_Definitivo.xlsx...")
    raw_sheets = pd.read_excel(SOURCE, sheet_name=None, header=0)

    PLAYER_TABS = [
        'PL Players', 'LaLiga Players', 'Serie A Players',
        'Bundesliga Players', 'Ligue 1 Players'
    ]
    TEAM_TABS = [
        'LaLiga Teams', 'Serie A Teams',
        'Bundesliga Teams', 'Ligue 1 Teams'
    ]

    all_dfs = {}

    print("\n=== Tabs de jugadores ===")
    for tab in PLAYER_TABS:
        df_raw = raw_sheets[tab]
        is_pl  = (tab == 'PL Players')
        df_clean = process_player_tab(df_raw, tab, is_pl)
        print(f"  {tab}: {df_raw.shape[0]} filas → {df_clean.shape[0]} filas")
        all_dfs[tab] = df_clean

    print("\n=== Tabs de equipos ===")
    all_dfs['PL Teams'] = raw_sheets['PL Teams']
    print(f"  PL Teams: {raw_sheets['PL Teams'].shape} (sin cambios)")

    for tab in TEAM_TABS:
        df_raw   = pd.read_excel(SOURCE, sheet_name=tab, header=None)
        df_clean = process_team_tab(df_raw, tab)
        print(f"  {tab}: {df_clean.shape}")
        all_dfs[tab] = df_clean

    # Tabs adicionales: se conservan tal cual
    for tab in ['MV History', 'Injuries by Season']:
        if tab in raw_sheets:
            all_dfs[tab] = raw_sheets[tab]

    print(f"\nGuardando en {DEST}...")
    sheet_order = PLAYER_TABS + ['PL Teams'] + TEAM_TABS + ['MV History', 'Injuries by Season']

    # Se usa xlsxwriter para evitar corrupción de archivo al guardar con pandas
    with pd.ExcelWriter(DEST, engine='xlsxwriter') as writer:
        for sheet in sheet_order:
            if sheet in all_dfs:
                all_dfs[sheet].to_excel(writer, sheet_name=sheet, index=False)

    print("¡Hecho!")
    print("\nResumen de filas:")
    for tab in PLAYER_TABS:
        print(f"  {tab}: {len(all_dfs[tab])} jugadores")
