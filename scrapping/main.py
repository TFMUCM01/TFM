# -*- coding: utf-8 -*-
"""
main.py — Scraper de titulares (Wayback directo) → Snowflake (solo días faltantes) con LOG DETALLADO

- Si la tabla no tiene datos: arranca en 2024-01-01.
- Si tiene datos: arranca en MAX(fecha) + 1 día.
- Llega hasta AYER (Europe/Madrid).
- NO usa la API de Wayback; prueba varias horas del día y valida que el snapshot sea del MISMO día.
- Upsert con MERGE en Snowflake por (fecha, titular).
- LOGS detallados por cada paso.

Requisitos:
  pip install requests beautifulsoup4 lxml pandas snowflake-connector-python
"""

import os
import sys
import time
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

import requests
import pandas as pd
from bs4 import BeautifulSoup
import snowflake.connector

# =========================
# CONFIG
# =========================

SLEEP_BETWEEN_DIAS = int(os.getenv("SLEEP_BETWEEN_DIAS", "1"))  # logs más ágiles
WAYBACK_TIMEOUT = 25
SNAPSHOT_TIMEOUT = 25
START_DEFAULT = date(2024, 1, 1)  # si no hay información, iniciar desde 2024-01-01
VERBOSE = os.getenv("VERBOSE", "1").lower() in ("1", "true", "yes")

NOTICIEROS = [
    {"nombre": "BBC",           "url": "https://www.bbc.com/news",         "fuente": "BBC",        "idioma": "en", "tabla": "BBC_TITULARES"},
    {"nombre": "ABC",           "url": "https://www.abc.es/economia/",     "fuente": "ABC",        "idioma": "es", "tabla": "ABC_TITULARES"},
    {"nombre": "EL_ECONOMISTA", "url": "https://www.eleconomista.es/economia/", "fuente": "EL ECONOMISTA", "idioma":"es","tabla":"EL_ECONOMISTA_TITULARES"},
    {"nombre": "BLOOMBERG",     "url": "https://www.bloomberg.com/europe", "fuente": "BLOOMBERG",  "idioma": "en", "tabla": "BLOOMBERG_TITULARES"},
    {"nombre": "EL_PAIS",       "url": "https://elpais.com/economia/",     "fuente": "EL PAIS",    "idioma": "es", "tabla": "EL_PAIS_TITULARES"},
    {"nombre": "THE_TIMES",     "url": "https://www.thetimes.com/",        "fuente": "THE TIMES",  "idioma": "en", "tabla": "THE_TIMES_TITULARES"},
    {"nombre": "EXPANSION",     "url": "https://www.expansion.com/",       "fuente": "EXPANSION",  "idioma": "es", "tabla": "EXPANSION_TITULARES"},
]

def _get_env_any(*names: str) -> Optional[str]:
    """Devuelve el primer valor no vacío entre múltiples variables de entorno."""
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return None

SNOWFLAKE_SCHEMA_VAL = _get_env_any("SNOWFLAKE_SCHEMA1", "SNOWFLAKE_SCHEMA")
SNOWFLAKE_SCHEMA_FROM = "SNOWFLAKE_SCHEMA1" if os.getenv("SNOWFLAKE_SCHEMA1") else "SNOWFLAKE_SCHEMA"

SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': SNOWFLAKE_SCHEMA_VAL,  # acepta SCHEMA1 o SCHEMA
    'role': os.getenv('SNOWFLAKE_ROLE') or None,
}

def _must(value: Optional[str], name_hint: str) -> str:
    if not value:
        print(f"[CONFIG] Falta variable de entorno: {name_hint}", file=sys.stderr)
        raise RuntimeError(f"Missing env var: {name_hint}")
    return value

# Validación (nota: para schema mostramos que aceptamos SCHEMA1 o SCHEMA)
SNOWFLAKE_CONFIG['user']      = _must(SNOWFLAKE_CONFIG['user'],      "SNOWFLAKE_USER")
SNOWFLAKE_CONFIG['password']  = _must(SNOWFLAKE_CONFIG['password'],  "SNOWFLAKE_PASSWORD")
SNOWFLAKE_CONFIG['account']   = _must(SNOWFLAKE_CONFIG['account'],   "SNOWFLAKE_ACCOUNT")
SNOWFLAKE_CONFIG['warehouse'] = _must(SNOWFLAKE_CONFIG['warehouse'], "SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_CONFIG['database']  = _must(SNOWFLAKE_CONFIG['database'],  "SNOWFLAKE_DATABASE")
SNOWFLAKE_CONFIG['schema']    = _must(SNOWFLAKE_CONFIG['schema'],    "SNOWFLAKE_SCHEMA1 o SNOWFLAKE_SCHEMA")

# =========================
# Utilidades
# =========================

TZ_MADRID = ZoneInfo("Europe/Madrid")

def ayer_madrid() -> date:
    return (datetime.now(TZ_MADRID) - timedelta(days=1)).date()

def log_error(mensaje: str) -> None:
    with open("scraping_log.txt", "a", errors="ignore") as f:
        f.write(f"{datetime.now()} - {mensaje}\n")

def log_info(msg: str) -> None:
    print(msg, flush=True)

# =========================
# Wayback directo (sin API)
# =========================

def _timestamp_candidates_for_day(fecha_str: str) -> List[str]:
    """Timestamps HHMMSS del MISMO día (de tarde a madrugada)."""
    return [
        "235959", "220000", "210000", "200000",
        "180000", "150000", "120000", "090000",
        "060000", "030000", "000000",
    ]

def _is_same_day_snapshot(final_url: str, fecha_str: str) -> bool:
    """Comprueba /web/YYYYMMDDhhmmss/ y que YYYYMMDD == fecha_str."""
    try:
        part = final_url.split("/web/")[1].split("/", 1)[0]
        return part[:8] == fecha_str
    except Exception:
        return False

def obtener_snapshot_url_directo(original_url: str, fecha_str: str) -> Tuple[Optional[str], List[str]]:
    """
    Prueba varias horas del día con URLs directas y valida que el snapshot quede en el MISMO día.
    Devuelve (url_valida | None, logs_detalle).
    """
    detail_logs = []
    base_url = original_url.strip("/")
    for hhmmss in _timestamp_candidates_for_day(fecha_str):
        candidate = f"https://web.archive.org/web/{fecha_str}{hhmmss}/{base_url}/"
        try:
            resp = requests.get(candidate, timeout=WAYBACK_TIMEOUT, allow_redirects=True)
            final_url = resp.url.replace("http://", "https://")
            detail_logs.append(f"  - Probar {candidate} → {resp.status_code} final={final_url}")
            if resp.status_code == 200 and _is_same_day_snapshot(final_url, fecha_str):
                detail_logs.append(f"    ✓ Aceptado (mismo día {fecha_str})")
                return final_url, detail_logs
            else:
                detail_logs.append(f"    × Rechazado (no es {fecha_str})")
        except Exception as e:
            err = f"[WB-direct] Error acceso {candidate}: {e}"
            log_error(err)
            detail_logs.append(f"    ! {err}")
    return None, detail_logs

def extraer_titulares(snapshot_url: str, fecha_str: str, fuente: Optional[str] = None) -> List[Dict]:
    """Extrae h1/h2/h3 con >3 palabras."""
    titulares: List[Dict] = []
    try:
        res = requests.get(snapshot_url, timeout=SNAPSHOT_TIMEOUT)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, 'lxml')
        encabezados = soup.find_all(['h1', 'h2', 'h3'])
        if VERBOSE:
            log_info(f"[{fuente}] {len(encabezados)} encabezados en {snapshot_url}")
        for t in encabezados:
            texto = t.get_text(strip=True)
            if texto and len(texto.split()) > 3:
                titulares.append({"fecha": fecha_str, "titular": texto, "url_archivo": snapshot_url})
    except Exception as e:
        log_error(f"[{fuente or 'GENERAL'}] Error accediendo a snapshot: {e}")
    return titulares

# =========================
# Snowflake
# =========================

def sf_connect():
    log_info(f"[SF] Conectando → user={SNOWFLAKE_CONFIG['user']} account={SNOWFLAKE_CONFIG['account']} "
             f"db={SNOWFLAKE_CONFIG['database']} schema={SNOWFLAKE_CONFIG['schema']} (schema via {SNOWFLAKE_SCHEMA_FROM})")
    return snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG['user'],
        password=SNOWFLAKE_CONFIG['password'],
        account=SNOWFLAKE_CONFIG['account'],
        warehouse=SNOWFLAKE_CONFIG['warehouse'],
        database=SNOWFLAKE_CONFIG['database'],
        schema=SNOWFLAKE_CONFIG['schema'],
        role=SNOWFLAKE_CONFIG.get('role')
    )

def count_rows(tabla: str) -> int:
    ctx = sf_connect()
    cs = ctx.cursor()
    try:
        full = f"{SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{tabla}"
        cs.execute(f"SELECT COUNT(*) FROM {full}")
        return int(cs.fetchone()[0])
    except snowflake.connector.errors.ProgrammingError:
        # tabla no existe
        return 0
    finally:
        cs.close()
        ctx.close()

def obtener_ultima_fecha_en_snowflake(tabla: str) -> date:
    """
    Devuelve:
      - MAX(fecha) + 1 día, si hay datos.
      - START_DEFAULT (2024-01-01), si no hay datos.
    """
    ctx = sf_connect()
    cs = ctx.cursor()
    try:
        full = f"{SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{tabla}"
        cs.execute(f"SELECT MAX(fecha) FROM {full}")
        r = cs.fetchone()
        if r and r[0]:
            ultima = r[0]
            log_info(f"[SF] {tabla}: MAX(fecha) = {ultima}")
            return ultima + timedelta(days=1)
        else:
            log_info(f"[SF] {tabla}: sin datos → inicio {START_DEFAULT}")
            return START_DEFAULT
    finally:
        cs.close()
        ctx.close()

def ensure_table(tabla: str) -> None:
    ctx = sf_connect()
    cs = ctx.cursor()
    try:
        full = f"{SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{tabla}"
        cs.execute(f"""
            CREATE TABLE IF NOT EXISTS {full} (
                fecha DATE,
                titular STRING,
                url_archivo STRING,
                fuente STRING,
                idioma STRING
            )
        """)
        ctx.commit()
    finally:
        cs.close()
        ctx.close()

def subir_a_snowflake_merge(df: pd.DataFrame, tabla: str) -> Tuple[int, int, int]:
    """
    MERGE por (fecha, titular) para insertar solo faltantes.
    Devuelve (rowcount_reportado, antes, despues).
    """
    if df.empty:
        log_info(f"[SF] No hay datos para subir a {tabla}.")
        return (0, count_rows(tabla), count_rows(tabla))

    df = df.copy()
    df["fecha"] = pd.to_datetime(df["fecha"], format="%Y%m%d").dt.date

    ensure_table(tabla)

    before = count_rows(tabla)

    ctx = sf_connect()
    cs = ctx.cursor()
    try:
        full = f"{SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{tabla}"
        tmp = "TMP_TITULARES"
        cs.execute(f"CREATE OR REPLACE TEMP TABLE {tmp} LIKE {full}")

        rows = df[["fecha","titular","url_archivo","fuente","idioma"]].values.tolist()
        insert_sql = f"INSERT INTO {tmp} (fecha, titular, url_archivo, fuente, idioma) VALUES (%s, %s, %s, %s, %s)"
        cs.executemany(insert_sql, rows)
        if VERBOSE:
            log_info(f"[SF] Cargados {len(rows)} registros a {tmp}")

        merge_sql = f"""
            MERGE INTO {full} t
            USING {tmp} s
              ON t.fecha = s.fecha AND t.titular = s.titular
            WHEN MATCHED THEN UPDATE SET
              t.url_archivo = s.url_archivo,
              t.fuente = s.fuente,
              t.idioma = s.idioma
            WHEN NOT MATCHED THEN
              INSERT (fecha, titular, url_archivo, fuente, idioma)
              VALUES (s.fecha, s.titular, s.url_archivo, s.fuente, s.idioma)
        """
        cs.execute(merge_sql)
        rc = cs.rowcount if hasattr(cs, "rowcount") and cs.rowcount is not None else -1
        ctx.commit()

    finally:
        cs.close()
        ctx.close()

    after = count_rows(tabla)
    return (rc, before, after)

# =========================
# MAIN
# =========================

if __name__ == "__main__":
    FECHA_FIN = ayer_madrid()
    log_info(f"=== INICIO SCRAPER — FECHA_FIN (ayer Madrid): {FECHA_FIN} ===")
    log_info(f"[DEBUG] ENTORNO Snowflake: account={SNOWFLAKE_CONFIG['account']} db={SNOWFLAKE_CONFIG['database']} schema={SNOWFLAKE_CONFIG['schema']} (via {SNOWFLAKE_SCHEMA_FROM}) warehouse={SNOWFLAKE_CONFIG['warehouse']}")

    resumen = []

    for medio in NOTICIEROS:
        nombre = medio["nombre"]
        url = medio["url"]
        fuente = medio["fuente"]
        idioma = medio["idioma"]
        tabla = medio["tabla"]

        log_info(f"\n--- Procesando: {nombre} ({fuente}) → tabla {tabla} ---")
        FECHA_INICIO = obtener_ultima_fecha_en_snowflake(tabla)

        if FECHA_INICIO > FECHA_FIN:
            log_info(f"[SKIP] Nada que actualizar: {FECHA_INICIO} > {FECHA_FIN}")
            resumen.append((fuente, 0, 0, 0, 0))
            continue

        log_info(f"[RANGO] {FECHA_INICIO} → {FECHA_FIN}")
        resultados: List[Dict] = []
        dias_intentados = dias_con_snapshot = total_titulares = 0

        fecha = FECHA_INICIO
        while fecha <= FECHA_FIN:
            fecha_str = fecha.strftime("%Y%m%d")
            dias_intentados += 1
            log_info(f"[{fuente}] Día {fecha_str} — buscando snapshot del MISMO día")
            snapshot_url, detail_logs = obtener_snapshot_url_directo(url, fecha_str)

            for line in detail_logs:
                log_info(line)

            if snapshot_url:
                dias_con_snapshot += 1
                log_info(f"[{fuente}] Snapshot aceptado: {snapshot_url}")
                try:
                    tits = extraer_titulares(snapshot_url, fecha_str, fuente=fuente)
                    for t in tits:
                        t["fuente"] = fuente
                        t["idioma"] = idioma
                    total_titulares += len(tits)
                    log_info(f"[{fuente}] Titulares {fecha_str}: {len(tits)}")
                    resultados.extend(tits)
                except Exception as e:
                    log_error(f"[{fuente}] Error en {fecha_str}: {e}")
                    log_info(f"[{fuente}] ERROR extracción en {fecha_str}: {e}")
            else:
                log_info(f"[{fuente}] Sin snapshot válido (mismo día) para {fecha_str}")

            time.sleep(SLEEP_BETWEEN_DIAS)
            fecha += timedelta(days=1)

        if resultados:
            df_nuevo = pd.DataFrame(resultados).drop_duplicates(subset=["fecha","titular"])
            log_info(f"[{fuente}] Subiendo a Snowflake: {len(df_nuevo)} filas (deduplicadas en memoria)")
            rc, before, after = subir_a_snowflake_merge(df_nuevo, tabla)
            delta = after - before
            log_info(f"[{fuente}] MERGE rowcount={rc} | antes={before} después={after} Δ={delta}")
            resumen.append((fuente, dias_intentados, dias_con_snapshot, total_titulares, delta))
        else:
            log_info(f"[{fuente}] No hubo nuevos titulares para {fuente}.")
            resumen.append((fuente, dias_intentados, dias_con_snapshot, 0, 0))

    # Resumen final
    log_info("\n=== RESUMEN ===")
    log_info("Fuente | Días intentados | Días con snapshot | Titulares extraídos | Δ filas en tabla")
    for fuente, di, ds, tot, delta in resumen:
        log_info(f"{fuente:10} | {di:14} | {ds:17} | {tot:18} | {delta:14}")

    log_info("=== FIN SCRAPER ===")
