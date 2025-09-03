# -*- coding: utf-8 -*-
"""
main.py — Scraper de titulares (Wayback directo) → Snowflake (solo días faltantes)

- Si la tabla no tiene datos: arranca en 2024-01-01.
- Si tiene datos: arranca en MAX(fecha) + 1 día.
- Llega hasta AYER (Europe/Madrid).
- NO usa la API de Wayback; prueba varias horas del día y valida que el snapshot sea del MISMO día.
- Upsert con MERGE en Snowflake por (fecha, titular) para evitar duplicados.

Requisitos:
  pip install requests beautifulsoup4 lxml pandas snowflake-connector-python
"""

import os
import sys
import time
from typing import List, Dict, Optional
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

import requests
import pandas as pd
from bs4 import BeautifulSoup
import snowflake.connector

# =========================
# CONFIG
# =========================

SLEEP_BETWEEN_DIAS = 2
WAYBACK_TIMEOUT = 30
SNAPSHOT_TIMEOUT = 30
START_DEFAULT = date(2024, 1, 1)  # <-- si no hay información, iniciar desde 01/01/2024

NOTICIEROS = [
    {"nombre": "BBC",           "url": "https://www.bbc.com/news",         "fuente": "BBC",        "idioma": "en", "tabla": "BBC_TITULARES"},
    {"nombre": "ABC",           "url": "https://www.abc.es/economia/",     "fuente": "ABC",        "idioma": "es", "tabla": "ABC_TITULARES"},
    {"nombre": "EL_ECONOMISTA", "url": "https://www.eleconomista.es/economia/", "fuente": "EL ECONOMISTA", "idioma":"es","tabla":"EL_ECONOMISTA_TITULARES"},
    {"nombre": "BLOOMBERG",     "url": "https://www.bloomberg.com/europe", "fuente": "BLOOMBERG",  "idioma": "en", "tabla": "BLOOMBERG_TITULARES"},
    {"nombre": "EL_PAIS",       "url": "https://elpais.com/economia/",     "fuente": "EL PAIS",    "idioma": "es", "tabla": "EL_PAIS_TITULARES"},
    {"nombre": "THE_TIMES",     "url": "https://www.thetimes.com/",        "fuente": "THE TIMES",  "idioma": "en", "tabla": "THE_TIMES_TITULARES"},
    {"nombre": "EXPANSION",     "url": "https://www.expansion.com/",       "fuente": "EXPANSION",  "idioma": "es", "tabla": "EXPANSION_TITULARES"},
]

SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA1'),  # <- como pediste
    'role': os.getenv('SNOWFLAKE_ROLE') or None,
}

def _must(value: Optional[str], name: str) -> str:
    if not value:
        print(f"[CONFIG] Falta variable de entorno: {name}", file=sys.stderr)
        raise RuntimeError(f"Missing env var: {name}")
    return value

for k in ['user','password','account','warehouse','database','schema']:
    SNOWFLAKE_CONFIG[k] = _must(SNOWFLAKE_CONFIG[k], f"SNOWFLAKE_{k.upper()}")

# =========================
# Utilidades
# =========================

TZ_MADRID = ZoneInfo("Europe/Madrid")

def ayer_madrid() -> date:
    return (datetime.now(TZ_MADRID) - timedelta(days=1)).date()

def log_error(mensaje: str) -> None:
    with open("scraping_log.txt", "a", errors="ignore") as f:
        f.write(f"{datetime.now()} - {mensaje}\n")

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

def obtener_snapshot_url_directo(original_url: str, fecha_str: str) -> Optional[str]:
    """
    Prueba varias horas del día con URLs directas y valida que el snapshot quede en el MISMO día.
    Devuelve None si ese día no tiene snapshot.
    """
    base_url = original_url.strip("/")
    for hhmmss in _timestamp_candidates_for_day(fecha_str):
        candidate = f"https://web.archive.org/web/{fecha_str}{hhmmss}/{base_url}/"
        try:
            resp = requests.get(candidate, timeout=WAYBACK_TIMEOUT, allow_redirects=True)
            final_url = resp.url.replace("http://", "https://")
            if resp.status_code == 200 and _is_same_day_snapshot(final_url, fecha_str):
                return final_url
        except Exception as e:
            log_error(f"[WB-direct] Error acceso {candidate}: {e}")
            continue
    return None

def extraer_titulares(snapshot_url: str, fecha_str: str, fuente: Optional[str] = None) -> List[Dict]:
    """Extrae h1/h2/h3 con >3 palabras."""
    titulares: List[Dict] = []
    try:
        res = requests.get(snapshot_url, timeout=SNAPSHOT_TIMEOUT)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, 'lxml')
        encabezados = soup.find_all(['h1', 'h2', 'h3'])
        print(f"[{fuente}] {len(encabezados)} encabezados en {snapshot_url}")
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
    return snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG['user'],
        password=SNOWFLAKE_CONFIG['password'],
        account=SNOWFLAKE_CONFIG['account'],
        warehouse=SNOWFLAKE_CONFIG['warehouse'],
        database=SNOWFLAKE_CONFIG['database'],
        schema=SNOWFLAKE_CONFIG['schema'],
        role=SNOWFLAKE_CONFIG.get('role')
    )

def obtener_ultima_fecha_en_snowflake(tabla: str) -> date:
    """
    Devuelve:
      - MAX(fecha) + 1 día, si hay datos.
      - START_DEFAULT (2024-01-01), si no hay datos.
    """
    ctx = sf_connect()
    cs = ctx.cursor()
    try:
        tabla_completa = f"{SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{tabla}"
        cs.execute(f"SELECT MAX(fecha) FROM {tabla_completa}")
        r = cs.fetchone()
        if r and r[0]:
            ultima = r[0]
            print(f"Última fecha en Snowflake para {tabla}: {ultima}")
            return ultima + timedelta(days=1)
        else:
            print(f"No hay registros en {tabla}. Iniciando desde {START_DEFAULT}.")
            return START_DEFAULT
    finally:
        cs.close()
        ctx.close()

def subir_a_snowflake_merge(df: pd.DataFrame, tabla: str) -> None:
    """
    MERGE por (fecha, titular) para insertar solo faltantes.
    Actualiza url_archivo/fuente/idioma si ya existía el titular en ese día.
    """
    if df.empty:
        print(f"No hay datos para subir a {tabla}.")
        return

    df = df.copy()
    df["fecha"] = pd.to_datetime(df["fecha"], format="%Y%m%d").dt.date

    ctx = sf_connect()
    cs = ctx.cursor()
    try:
        tabla_completa = f"{SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{tabla}"
        # Crea si no existe
        cs.execute(f"""
            CREATE TABLE IF NOT EXISTS {tabla_completa} (
                fecha DATE,
                titular STRING,
                url_archivo STRING,
                fuente STRING,
                idioma STRING
            )
        """)
        # Tabla temporal
        tmp = "TMP_TITULARES"
        cs.execute(f"CREATE OR REPLACE TEMP TABLE {tmp} LIKE {tabla_completa}")

        # Inserta a temporal
        rows = df[["fecha","titular","url_archivo","fuente","idioma"]].values.tolist()
        insert_sql = f"INSERT INTO {tmp} (fecha, titular, url_archivo, fuente, idioma) VALUES (%s, %s, %s, %s, %s)"
        cs.executemany(insert_sql, rows)

        # MERGE (upsert)
        merge_sql = f"""
            MERGE INTO {tabla_completa} t
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
        ctx.commit()
        print(f"{len(rows)} filas upsert en {tabla}.")
    finally:
        cs.close()
        ctx.close()

# =========================
# MAIN: solo días faltantes [MAX(fecha)+1 .. AYER]
# =========================

if __name__ == "__main__":
    FECHA_FIN = ayer_madrid()
    print(f"FECHA_FIN (ayer Madrid): {FECHA_FIN}")

    for medio in NOTICIEROS:
        nombre = medio["nombre"]
        url = medio["url"]
        fuente = medio["fuente"]
        idioma = medio["idioma"]
        tabla = medio["tabla"]

        print(f"\nProcesando noticiero: {nombre} ({fuente})")
        FECHA_INICIO = obtener_ultima_fecha_en_snowflake(tabla)

        if FECHA_INICIO > FECHA_FIN:
            print(f"Nada que actualizar para {fuente}: {FECHA_INICIO} > {FECHA_FIN}")
            continue

        print(f"Actualizar: {FECHA_INICIO} → {FECHA_FIN}")
        resultados: List[Dict] = []

        fecha = FECHA_INICIO
        while fecha <= FECHA_FIN:
            fecha_str = fecha.strftime("%Y%m%d")
            print(f"[{fuente}] Día {fecha_str}")

            snapshot_url = obtener_snapshot_url_directo(url, fecha_str)
            if snapshot_url:
                try:
                    tits = extraer_titulares(snapshot_url, fecha_str, fuente=fuente)
                    for t in tits:
                        t["fuente"] = fuente
                        t["idioma"] = idioma
                    if tits:
                        print(f"  + {len(tits)} titulares")
                        resultados.extend(tits)
                    else:
                        print("  · sin titulares")
                except Exception as e:
                    log_error(f"[{fuente}] Error en {fecha_str}: {e}")
            else:
                print("  · sin snapshot válido (mismo día)")

            time.sleep(SLEEP_BETWEEN_DIAS)
            fecha += timedelta(days=1)

        if resultados:
            df_nuevo = pd.DataFrame(resultados).drop_duplicates(subset=["fecha","titular"])
            subir_a_snowflake_merge(df_nuevo, tabla)
            print(f"Total subidos/actualizados para {fuente}: {len(df_nuevo)}")
        else:
            print(f"No hubo nuevos titulares para {fuente}.")
