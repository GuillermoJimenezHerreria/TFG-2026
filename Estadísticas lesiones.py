"""
auditoria_lesiones_tfg.py — TFG Guillermo Jiménez
==================================================
Script de auditoría de las métricas de lesión.

PROPÓSITO:
  Reproducir y explicar, paso a paso, cómo se calcula cada una de las
  9 métricas de lesión que aparecen en la hoja "4. Métricas Finales"
  del archivo "Auditoria Lesiones TFG.xlsx", partiendo de los datos
  crudos de la hoja "1. Lesiones Brutas".

  El script permite:
    1. Auditar un jugador concreto (por nombre o TM ID) → explicación detallada.
    2. Validar todos los jugadores → compara métricas recalculadas vs Excel.
    3. Exportar un informe de validación completo.

USO:
  python auditoria_lesiones_tfg.py                      # audita James Milner (ejemplo)
  python auditoria_lesiones_tfg.py --player "Kylian Mbappé"
  python auditoria_lesiones_tfg.py --id 342229
  python auditoria_lesiones_tfg.py --validar-todos       # compara todos vs Excel

MÉTRICAS CALCULADAS (9 + clasificaciones):
  1.  N Temporadas              → número de temporadas con datos
  2.  Días última temporada     → días lesionado en la temporada más reciente
  3.  Media últimas 2 temp.     → promedio aritmético de las 2 últimas temporadas
  4.  Media últimas 3 temp.     → promedio aritmético de las 3 últimas temporadas
  5.  EWA Días                  → media exponencialmente ponderada (α=0.4)
  6.  Pendiente OLS             → tendencia lineal días ~ año (regresión OLS)
  7.  p-valor OLS               → significatividad estadística de la pendiente
  8.  Sig. (α=10%)              → booleano: p < 0.10
  9.  Riesgo                    → low / medium / high según media general
  10. Tipo de Tendencia         → stable / worsening / improving

FUENTE DE DATOS:
  Transfermarkt — endpoint scraping /verletzungen/{tm_id}
  Los datos crudos están almacenados en:
    Auditoria Lesiones TFG.xlsx → hoja "1. Lesiones Brutas"

Requisitos: pip install pandas numpy scipy openpyxl
"""

import argparse
import sys
import pandas as pd
import numpy as np
from scipy import stats

# ─── Rutas ────────────────────────────────────────────────────────────────────
import os

IN_COLAB = 'google.colab' in sys.modules
if IN_COLAB:
    DB = '/content'
    import subprocess
    subprocess.run(['pip', 'install', '-q', 'openpyxl', 'scipy'],
                   capture_output=True)
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DB = os.path.join(BASE_DIR, 'Bases de Datos')

AUDITORIA_PATH = os.path.join(DB, 'Auditoria Lesiones TFG.xlsx')

# ─── Constantes del modelo ────────────────────────────────────────────────────
ALPHA_EWA       = 0.4    # factor de decaimiento para la media exp. ponderada
UMBRAL_PVALOR   = 0.10   # umbral de significatividad estadística (10%)
UMBRAL_SLOPE    = 2.0    # días/año mínimos para no clasificar la tendencia como "stable"
RIESGO_LOW      = 10     # días/temporada: por debajo → riesgo bajo
RIESGO_MEDIUM   = 40     # días/temporada: entre 10 y 40 → riesgo medio; >40 → alto


# ══════════════════════════════════════════════════════════════════════════════
# BLOQUE 1 — Carga de datos
# ══════════════════════════════════════════════════════════════════════════════

def cargar_datos(path=AUDITORIA_PATH):
    """Lee las 4 hojas relevantes del Excel de auditoría."""
    xl = pd.ExcelFile(path)

    # Hoja 1: datos crudos (una fila por lesión)
    brutas = xl.parse("1. Lesiones Brutas", header=1)
    brutas.columns = ["tm_id", "jugador", "equipo", "temporada",
                      "tipo_lesion", "fecha_inicio", "fecha_fin",
                      "dias_baja", "fila_resumen"]

    # Hoja 2: días agregados por jugador × temporada (ya filtrados)
    serie = xl.parse("2. Serie Temporal", header=1)
    serie.columns = ["tm_id", "jugador", "equipo", "temporada",
                     "anio_temporada", "dias_baja", "pendiente_acum"]

    # Hoja 4: métricas finales (para validación)
    metricas = xl.parse("4. Métricas Finales", header=1)
    metricas.columns = [
        "tm_id", "jugador", "equipo", "n_temp",
        "dias_ult", "media_ult2", "media_ult3",
        "ewa_dias", "pendiente", "pvalor", "p_menor_010",
        "sig_alfa10", "riesgo", "tipo_tendencia"
    ]

    return brutas, serie, metricas


# ══════════════════════════════════════════════════════════════════════════════
# BLOQUE 2 — Lógica de cálculo (reprodución exacta de scrape_injuries_mv_history.py)
# ══════════════════════════════════════════════════════════════════════════════

def serie_temporal_jugador(df_serie, tm_id):
    """
    Filtra la hoja "2. Serie Temporal" por jugador y devuelve
    la serie ordenada de (año, días_baja).

    La hoja ya tiene aplicado el filtro de filas resumen:
    en la extracción original se descartaron las filas donde
    dias_baja es None (resúmenes del tipo "5 lesiones - 120 días").
    """
    datos = df_serie[df_serie["tm_id"] == tm_id].copy()
    datos = datos.dropna(subset=["anio_temporada", "dias_baja"])
    datos["anio_temporada"] = datos["anio_temporada"].astype(int)
    datos = datos.sort_values("anio_temporada")
    return list(zip(datos["anio_temporada"].tolist(),
                    datos["dias_baja"].tolist()))


def calcular_metricas(serie, verbose=False, nombre=""):
    """
    Reproduce el cálculo de compute_injury_metrics() de scrape_injuries_mv_history.py
    con cada paso documentado.

    Parámetros
    ----------
    serie   : list[(año_int, días_int)] ordenada de más antigua a más reciente
    verbose : si True, imprime el desglose completo en consola
    nombre  : nombre del jugador (para el encabezado del informe)

    Retorna dict con todas las métricas.
    """

    def sep(title=""):
        if verbose:
            print(f"\n  {'─'*55}")
            if title:
                print(f"  {title}")

    if verbose:
        print(f"\n{'═'*60}")
        print(f"  AUDITORÍA DE LESIONES — {nombre}")
        print(f"{'═'*60}")

    # ── Datos de entrada ──────────────────────────────────────────────────────
    years = [s[0] for s in serie]
    days  = [s[1] for s in serie]
    n     = len(days)

    if verbose:
        sep("DATOS DE ENTRADA (Hoja: 2. Serie Temporal)")
        print(f"  Temporadas disponibles: {n}")
        for yr, d in serie:
            print(f"    {yr}/{str(yr+1)[2:]}  →  {d} días de baja")

    # ── MÉTRICA 1: N Temporadas ───────────────────────────────────────────────
    sep("MÉTRICA 1 — N Temporadas")
    if verbose:
        print(f"  Fórmula : CONTAR filas en Serie Temporal para este jugador")
        print(f"  Resultado: N = {n}")

    # ── MÉTRICA 2: Días última temporada ─────────────────────────────────────
    dias_ult = days[-1] if n >= 1 else 0
    sep("MÉTRICA 2 — Días última temporada")
    if verbose:
        print(f"  Fórmula : Tomar el valor de la temporada más reciente")
        print(f"  Temporada más reciente: {years[-1]}/{str(years[-1]+1)[2:]}")
        print(f"  Resultado: {dias_ult} días")

    # ── MÉTRICA 3: Media últimas 2 temporadas ────────────────────────────────
    if n >= 2:
        vals2 = days[-2:]
        media2 = round(np.mean(vals2), 1)
        formula2 = f"({' + '.join(str(v) for v in vals2)}) / {len(vals2)}"
    else:
        vals2 = days
        media2 = round(float(days[-1]), 1)
        formula2 = f"{days[-1]} (solo 1 temporada disponible)"

    sep("MÉTRICA 3 — Media últimas 2 temporadas")
    if verbose:
        print(f"  Fórmula : PROMEDIO(días_t-1, días_t)")
        print(f"  Valores : {vals2}")
        print(f"  Cálculo : {formula2} = {media2}")

    # ── MÉTRICA 4: Media últimas 3 temporadas ────────────────────────────────
    if n >= 3:
        vals3 = days[-3:]
        media3 = round(np.mean(vals3), 1)
        formula3 = f"({' + '.join(str(v) for v in vals3)}) / {len(vals3)}"
    else:
        vals3 = days
        media3 = round(np.mean(days), 1)
        formula3 = f"PROMEDIO de todas las temporadas disponibles ({media3})"

    sep("MÉTRICA 4 — Media últimas 3 temporadas")
    if verbose:
        print(f"  Fórmula : PROMEDIO(días_t-2, días_t-1, días_t)")
        print(f"  Valores : {vals3}")
        print(f"  Cálculo : {formula3} = {media3}")

    # ── MÉTRICA 5: EWA (Media Exponencialmente Ponderada) ────────────────────
    #
    #  Fórmula: EWA_t = α · días_t + (1 − α) · EWA_(t-1)
    #  con α = 0.4 y EWA_0 = días de la temporada más antigua
    #
    #  Interpretación: las temporadas más recientes pesan más que las antiguas.
    #  Con α=0.4, la última temporada tiene peso 0.4, la penúltima 0.24,
    #  la anterior 0.144, etc.
    #
    ewa = float(days[0])
    ewa_steps = [(years[0], days[0], ewa)]
    for yr, d in zip(years[1:], days[1:]):
        ewa = ALPHA_EWA * d + (1 - ALPHA_EWA) * ewa
        ewa_steps.append((yr, d, round(ewa, 3)))

    ewa_final = round(ewa, 1)

    sep("MÉTRICA 5 — EWA Días (Media Exponencialmente Ponderada, α=0.4)")
    if verbose:
        print(f"  Fórmula : EWA_t = α · días_t + (1 − α) · EWA_{{t-1}}")
        print(f"  α       = {ALPHA_EWA}  →  peso reciente > peso antiguo")
        print(f"  EWA_0   = {days[0]} días  (temporada más antigua, {years[0]})")
        for yr, d, e in ewa_steps[1:]:
            prev_e = ewa_steps[ewa_steps.index((yr, d, e)) - 1][2]
            print(f"  {yr}/{str(yr+1)[2:]}  → {ALPHA_EWA}·{d} + {1-ALPHA_EWA}·{prev_e} = {e}")
        print(f"  EWA final = {ewa_final} días")

    # ── MÉTRICA 6 + 7: Regresión lineal OLS (tendencia días ~ año) ───────────
    #
    #  Ajusta una recta: días = β₀ + β₁·año
    #  usando mínimos cuadrados ordinarios (OLS).
    #
    #  β₁ (pendiente / slope):
    #    > 0 → el jugador se lesiona MÁS días cada año (empeorando)
    #    < 0 → el jugador se lesiona MENOS días cada año (mejorando)
    #    ≈ 0 → sin tendencia clara
    #
    #  p-valor de β₁:
    #    < 0.10 → la tendencia es estadísticamente significativa (α=10%)
    #
    #  Fórmulas OLS manuales (implementadas internamente por scipy.stats.linregress):
    #    β₁ = Σ[(xi − x̄)(yi − ȳ)] / Σ[(xi − x̄)²]
    #    β₀ = ȳ − β₁·x̄
    #    SSres = Σ(yi − ŷi)²
    #    MSE   = SSres / (n − 2)
    #    SE(β₁) = √(MSE / Σ(xi − x̄)²)
    #    t     = β₁ / SE(β₁)
    #    p     = 2 · P(T > |t|)  con n−2 grados de libertad
    #
    if n >= 3:
        slope, intercept, r_value, pvalue, stderr = stats.linregress(years, days)

        if verbose:
            x_arr = np.array(years)
            y_arr = np.array(days)
            x_mean = x_arr.mean()
            y_mean = y_arr.mean()
            Sxx   = np.sum((x_arr - x_mean) ** 2)
            Syy   = np.sum((y_arr - y_mean) ** 2)
            Sxy   = np.sum((x_arr - x_mean) * (y_arr - y_mean))
            SSres = np.sum((y_arr - (intercept + slope * x_arr)) ** 2)
            MSE   = SSres / (n - 2) if n > 2 else 0
            se_slope = np.sqrt(MSE / Sxx) if Sxx > 0 else 0
            t_stat = slope / se_slope if se_slope > 0 else 0

        slope  = round(slope, 3)
        pvalue = round(pvalue, 4)

        sep("MÉTRICA 6+7 — Pendiente OLS y p-valor")
        if verbose:
            print(f"  Modelo  : días_baja = β₀ + β₁·año   (regresión OLS)")
            print(f"  n       = {n} temporadas (mínimo 3 para calcular)")
            print(f"  x̄ (años)  = {round(x_mean,2)}   ȳ (días) = {round(y_mean,2)}")
            print(f"  Sxx     = Σ(xi−x̄)²  = {round(Sxx,2)}")
            print(f"  Sxy     = Σ(xi−x̄)(yi−ȳ) = {round(Sxy,2)}")
            print(f"  β₁ (slope)  = Sxy/Sxx = {round(Sxy,2)}/{round(Sxx,2)} = {slope} días/año")
            print(f"  β₀          = ȳ − β₁·x̄ = {round(y_mean,2)} − {slope}·{round(x_mean,2)} = {round(intercept,2)}")
            print(f"  SSres   = Σ(yi − ŷi)²  = {round(SSres,2)}")
            print(f"  MSE     = SSres/(n−2)   = {round(SSres,2)}/{n-2} = {round(MSE,2)}")
            print(f"  SE(β₁)  = √(MSE/Sxx)   = √({round(MSE,2)}/{round(Sxx,2)}) = {round(se_slope,4)}")
            print(f"  t       = β₁/SE(β₁)    = {slope}/{round(se_slope,4)} = {round(t_stat,3)}")
            print(f"  p-valor = 2·P(T>|{round(t_stat,3)}|) con {n-2} g.l. = {pvalue}")
    else:
        slope  = 0.0
        pvalue = 1.0
        sep("MÉTRICA 6+7 — Pendiente OLS y p-valor")
        if verbose:
            print(f"  ⚠ Solo {n} temporada(s) — mínimo 3 para OLS.")
            print(f"  Se asignan valores por defecto: slope=0.0, p-valor=1.0")

    # ── MÉTRICA 8: Significatividad estadística ───────────────────────────────
    sig = pvalue < UMBRAL_PVALOR

    sep("MÉTRICA 8 — Sig. (α=10%)")
    if verbose:
        print(f"  Criterio: p-valor < {UMBRAL_PVALOR}")
        print(f"  p-valor  = {pvalue}  →  {'SÍ significativa' if sig else 'NO significativa'}")

    # ── MÉTRICA 9: Riesgo ─────────────────────────────────────────────────────
    #
    #  Se basa en la MEDIA GENERAL de días de baja por temporada.
    #
    #  Umbrales:
    #    < 10 días/temporada  → 'low'    (lesiones leves o muy esporádicas)
    #    10 – 40 días         → 'medium' (una lesión moderada por temporada)
    #    > 40 días            → 'high'   (lesiones graves o recurrentes)
    #
    media_general = round(np.mean(days), 1)

    if media_general < RIESGO_LOW:
        riesgo = 'low'
    elif media_general < RIESGO_MEDIUM:
        riesgo = 'medium'
    else:
        riesgo = 'high'

    sep("MÉTRICA 9 — Riesgo")
    if verbose:
        formula_media = f"({' + '.join(str(d) for d in days)}) / {n}"
        print(f"  Fórmula : media general = PROMEDIO(todos los días)")
        print(f"  Cálculo : {formula_media} = {media_general} días/temporada")
        print(f"  Umbrales:")
        print(f"    < {RIESGO_LOW} días → 'low'     {'◄ TÚ' if riesgo=='low' else ''}")
        print(f"    {RIESGO_LOW}–{RIESGO_MEDIUM} días → 'medium'  {'◄ TÚ' if riesgo=='medium' else ''}")
        print(f"    > {RIESGO_MEDIUM} días → 'high'    {'◄ TÚ' if riesgo=='high' else ''}")
        print(f"  Resultado: '{riesgo}'")

    # ── MÉTRICA 10: Tipo de Tendencia ─────────────────────────────────────────
    #
    #  Se basa en el valor absoluto de la pendiente OLS:
    #    |slope| < 2  → 'stable'    (variación < 2 días/año: sin tendencia clara)
    #    slope  > 2   → 'worsening' (más días de baja cada año)
    #    slope  < -2  → 'improving' (menos días de baja cada año)
    #
    if abs(slope) < UMBRAL_SLOPE:
        tipo = 'stable'
    elif slope > 0:
        tipo = 'worsening'
    else:
        tipo = 'improving'

    sep("MÉTRICA 10 — Tipo de Tendencia")
    if verbose:
        print(f"  Fórmula : basada en la pendiente OLS (β₁ = {slope} días/año)")
        print(f"  Criterio:")
        print(f"    |β₁| < {UMBRAL_SLOPE} → 'stable'    {'◄ TÚ' if tipo=='stable' else ''}")
        print(f"    β₁  > {UMBRAL_SLOPE} → 'worsening' {'◄ TÚ' if tipo=='worsening' else ''}")
        print(f"    β₁  < -{UMBRAL_SLOPE} → 'improving' {'◄ TÚ' if tipo=='improving' else ''}")
        print(f"  Resultado: '{tipo}'")

    # ── Resumen final ─────────────────────────────────────────────────────────
    resultado = {
        "n_temp"      : n,
        "dias_ult"    : dias_ult,
        "media_ult2"  : media2,
        "media_ult3"  : media3,
        "ewa_dias"    : ewa_final,
        "pendiente"   : slope,
        "pvalor"      : pvalue,
        "p_menor_010" : "SÍ" if sig else "NO",
        "sig_alfa10"  : "SÍ" if sig else "NO",
        "riesgo"      : riesgo,
        "tipo_tend"   : tipo,
    }

    if verbose:
        sep()
        print(f"\n  {'─'*55}")
        print(f"  RESUMEN MÉTRICAS FINALES — {nombre}")
        print(f"  {'─'*55}")
        print(f"  {'N Temporadas':<30} {n}")
        print(f"  {'Días última temporada':<30} {dias_ult}")
        print(f"  {'Media últimas 2 temp.':<30} {media2}")
        print(f"  {'Media últimas 3 temp.':<30} {media3}")
        print(f"  {'EWA Días (α=0.4)':<30} {ewa_final}")
        print(f"  {'Pendiente OLS (días/año)':<30} {slope}")
        print(f"  {'p-valor OLS':<30} {pvalue}")
        print(f"  {'Significativa (α=10%)':<30} {'Sí' if sig else 'No'}")
        print(f"  {'Riesgo':<30} {riesgo}")
        print(f"  {'Tipo de Tendencia':<30} {tipo}")
        print(f"  {'─'*55}\n")

    return resultado


# ══════════════════════════════════════════════════════════════════════════════
# BLOQUE 3 — Auditoría de un jugador concreto
# ══════════════════════════════════════════════════════════════════════════════

def auditar_jugador(nombre=None, tm_id=None, path=AUDITORIA_PATH):
    brutas, serie, metricas = cargar_datos(path)

    # Buscar el jugador
    if tm_id is not None:
        fila = metricas[metricas["tm_id"] == tm_id]
        if fila.empty:
            fila = serie[serie["tm_id"] == tm_id]
            if not fila.empty:
                nombre = fila.iloc[0]["jugador"]
                tm_id  = int(fila.iloc[0]["tm_id"])
    elif nombre is not None:
        fila = metricas[metricas["jugador"].str.contains(nombre, case=False, na=False)]
        if fila.empty:
            print(f"⚠ Jugador '{nombre}' no encontrado en Métricas Finales.")
            # Buscar en Serie Temporal
            fila2 = serie[serie["jugador"].str.contains(nombre, case=False, na=False)]
            if fila2.empty:
                print("  Tampoco encontrado en Serie Temporal. Revisa el nombre.")
                return
            tm_id  = int(fila2.iloc[0]["tm_id"])
            nombre = fila2.iloc[0]["jugador"]
        else:
            tm_id  = int(fila.iloc[0]["tm_id"])
            nombre = fila.iloc[0]["jugador"]
    else:
        # Ejemplo por defecto: James Milner
        ejemplo = serie.iloc[0]
        tm_id   = int(ejemplo["tm_id"])
        nombre  = ejemplo["jugador"]
        print(f"ℹ No se especificó jugador. Usando ejemplo: {nombre} (ID {tm_id})")

    # Obtener serie temporal del jugador
    datos_serie = serie_temporal_jugador(serie, tm_id)
    if not datos_serie:
        print(f"⚠ No hay datos de serie temporal para {nombre} (ID {tm_id})")
        return

    # Calcular métricas con explicación paso a paso
    resultado = calcular_metricas(datos_serie, verbose=True, nombre=nombre)

    # Comparar con los valores almacenados en el Excel
    fila_excel = metricas[metricas["tm_id"] == tm_id]
    if not fila_excel.empty:
        r = fila_excel.iloc[0]
        print("  VALIDACIÓN vs Excel (Hoja 4. Métricas Finales):")
        print(f"  {'─'*55}")

        def check(campo, calc, excel_val, decimales=1):
            try:
                calc_r  = round(float(calc), decimales)
                excel_r = round(float(excel_val), decimales)
                ok = "✅" if calc_r == excel_r else "❌"
                print(f"  {ok} {campo:<28} Calc={calc_r:<8} Excel={excel_r}")
            except Exception:
                print(f"  ⚠  {campo:<28} Calc={calc}  Excel={excel_val} (no numérico)")

        check("N Temporadas",           resultado["n_temp"],    r["n_temp"],    0)
        check("Días última temp.",      resultado["dias_ult"],  r["dias_ult"],  0)
        check("Media últ. 2",           resultado["media_ult2"],r["media_ult2"])
        check("Media últ. 3",           resultado["media_ult3"],r["media_ult3"])
        check("EWA Días",               resultado["ewa_dias"],  r["ewa_dias"])
        check("Pendiente OLS",          resultado["pendiente"], r["pendiente"], 3)
        check("p-valor",                resultado["pvalor"],    r["pvalor"],    4)
        cat_ok = resultado["riesgo"] == str(r["riesgo"]).strip().lower()
        print(f"  {'✅' if cat_ok else '❌'} {'Riesgo':<28} Calc={resultado['riesgo']:<8} Excel={r['riesgo']}")
        tipo_ok = resultado["tipo_tend"] == str(r["tipo_tendencia"]).strip().lower()
        print(f"  {'✅' if tipo_ok else '❌'} {'Tipo Tendencia':<28} Calc={resultado['tipo_tend']:<8} Excel={r['tipo_tendencia']}")
        print(f"  {'─'*55}")


# ══════════════════════════════════════════════════════════════════════════════
# BLOQUE 4 — Validación de todos los jugadores
# ══════════════════════════════════════════════════════════════════════════════

def validar_todos(path=AUDITORIA_PATH, exportar=True):
    """
    Recalcula las métricas para todos los jugadores y compara con el Excel.
    Exporta un informe de diferencias si exportar=True.
    """
    brutas, serie, metricas = cargar_datos(path)
    ids = metricas["tm_id"].dropna().unique()

    print(f"\n🔍 Validando {len(ids)} jugadores...\n")

    errores = []
    ok_total = 0

    for tm_id in ids:
        tm_id = int(tm_id)
        datos_serie = serie_temporal_jugador(serie, tm_id)
        if not datos_serie:
            continue

        calc = calcular_metricas(datos_serie, verbose=False)
        fila = metricas[metricas["tm_id"] == tm_id].iloc[0]
        nombre = str(fila["jugador"])

        diffs = {}
        campos_num = {
            "n_temp":    ("n_temp",    0),
            "dias_ult":  ("dias_ult",  0),
            "media_ult2":("media_ult2",1),
            "media_ult3":("media_ult3",1),
            "ewa_dias":  ("ewa_dias",  1),
            "pendiente": ("pendiente", 3),
            "pvalor":    ("pvalor",    4),
        }
        for c_key, (e_key, dec) in campos_num.items():
            try:
                c_val = round(float(calc[c_key]), dec)
                e_val = round(float(fila[e_key]), dec)
                if c_val != e_val:
                    diffs[c_key] = (c_val, e_val)
            except Exception:
                pass

        if calc["riesgo"] != str(fila["riesgo"]).strip().lower():
            diffs["riesgo"] = (calc["riesgo"], fila["riesgo"])
        if calc["tipo_tend"] != str(fila["tipo_tendencia"]).strip().lower():
            diffs["tipo_tend"] = (calc["tipo_tend"], fila["tipo_tendencia"])

        if diffs:
            errores.append({"tm_id": tm_id, "jugador": nombre, "diferencias": diffs})
        else:
            ok_total += 1

    total = len(ids)
    print(f"✅ Coincidencias exactas : {ok_total}/{total}")
    print(f"❌ Discrepancias         : {len(errores)}/{total}")

    if errores:
        print("\nPrimeros 10 con discrepancias:")
        for e in errores[:10]:
            print(f"  [{e['tm_id']}] {e['jugador']}")
            for campo, (calc_v, excel_v) in e["diferencias"].items():
                print(f"      {campo}: calculado={calc_v} | excel={excel_v}")

    if exportar and errores:
        rows = []
        for e in errores:
            for campo, (c, x) in e["diferencias"].items():
                rows.append({
                    "tm_id": e["tm_id"],
                    "jugador": e["jugador"],
                    "campo": campo,
                    "calculado": c,
                    "excel": x,
                    "diferencia": round(float(c) - float(x), 4)
                        if str(c).replace(".", "").replace("-", "").isdigit() else "—"
                })
        df_err = pd.DataFrame(rows)
        out = "informe_validacion_lesiones.csv"
        df_err.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"\n📄 Informe exportado: {out}")


# ══════════════════════════════════════════════════════════════════════════════
# BLOQUE 5 — Entrada por línea de comandos
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Auditoría de métricas de lesión — TFG Guillermo Jiménez"
    )
    parser.add_argument("--player", "-p", type=str, default=None,
                        help="Nombre (parcial) del jugador a auditar")
    parser.add_argument("--id", "-i", type=int, default=None,
                        help="TM ID del jugador a auditar")
    parser.add_argument("--validar-todos", action="store_true",
                        help="Valida todos los jugadores contra el Excel")
    parser.add_argument("--excel", type=str,
                        default=AUDITORIA_PATH,
                        help="Ruta al archivo Auditoria Lesiones TFG.xlsx")
    args = parser.parse_args()

    if args.validar_todos:
        validar_todos(path=args.excel)
    else:
        auditar_jugador(nombre=args.player, tm_id=args.id, path=args.excel)
