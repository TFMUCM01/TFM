# -*- coding: utf-8 -*-
"""
main.py — Scraper de titulares (Wayback directo) → Snowflake

- Configuración embebida (NOTICIEROS y credenciales por entorno).
- Ayer en Europe/Madrid (día completo).
- NO usa la API de Wayback; construye URLs con timestamp y valida que el snapshot sea del MISMO día.
- Inserta en Snowflake con executemany (sin pyarrow/WRITE_PANDAS).

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
# CONFIG (antes en config.py)
# =========================

# Parámetros de ejecución
SLEEP_BETWEEN_DIAS = 2
RETRIES = 3
WAYBACK_TIMEOUT = 30
SNAPSHOT_TIMEOUT = 30

# Lista de noticieros
NOTICIEROS = [
    {"nombre": "BBC",           "url": "https://www.bbc.com/news",         "fuente": "BBC",        "idioma": "en", "tabla": "BBC_TITULARES"},
    {"nombre": "ABC",           "url": "https://www.abc.es/economia/",     "fuente": "ABC",        "idioma": "es", "tabla": "ABC_TITULARES"},
    {"nombre": "EL_ECONOMISTA", "url": "https://www.eleconomista.es/economia/", "fuente": "EL ECONOMISTA","idioma":"es","tabla":"EL_ECONOMISTA_TITULARES"},
    {"nombre": "BLOOMBERG",     "url": "https://www.bloomberg.com/europe", "fuente": "BLOOMBERG",  "idioma": "en", "tabla": "BLOOMBERG_TITULARES"},
    {"nombre": "EL_PAIS",       "url": "https://elpais.com/economia/",     "fuente": "EL PAIS",    "idioma": "es", "tabla": "EL_PAIS_TITULARES"},
    {"nombre": "THE_TIMES",     "url": "https://www.thetimes.com/",        "fuente": "THE TIMES",  "idioma": "en", "tabla": "THE_TIMES_TITULARES"},
    {"nombre": "EXPANSION",     "url": "https://www.expansion.com/",       "fuente": "EXPANSION",  "idioma": "es", "tabla": "EXPANSION_TITULARES"},
]

# Credenciales Snowflake desde entorno (manteniendo SNOWFLAKE_SCHEMA1)
SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA1'),  # <- tal como lo pediste
    'role': os.getenv('SNOWFLAKE_ROLE') or None,
}

def _must(value: Optional[str], name: str) -> str:
    if not value:
        print(f"[CONFIG] Falta variable de entorno: {name}", file=sys.stderr)
        raise RuntimeError(f"Missing env var: {name}")
    return value

# Validación mínima
for k in ['user','password','account','warehouse','database','schema']:
    SNOWFLAKE_CONFIG[k] = _must(SNOWFLAKE_CONFIG[k], f"SNOWFLAKE_{k.upper()}")

# =========================
# Helpers de logging y fecha
# =========================

def log_error(mensaje: str) -> None:
    with open("scraping_log.txt", "a", errors="ignore") as f:
        f.write(f"{datetime.now()} - {mensaje}\n")

TZ_MADRID = ZoneInfo("Europe/Madrid")

def ayer_madrid() -> date:
    return (datetime.now(TZ_MADRID) - timedelta(days=1)).date()

# =========================
# Scraper (antes en scraper.py)
# =========================

def _timestamp_candidates_for_day(fecha_str: str) -> List[str]:
    """
    Timestamps HHMMSS razonables del MISMO día (de tarde a madrugada).
    """
    return [
        "235959", "220000", "210000", "200000",
        "180000", "150000", "120000", "090000",
        "060000", "030000", "000000",
    ]

def _is_same_day_snapshot(final_url: str, fecha_str: str) -> bool:
    """
    Verifica que final_url contenga /web/YYYYMMDDhhmmss/ y que YYYYMMDD == fecha_str.
    """
    try:
        part = final_url.split("/web/")[1].split("/", 1)[0]
        return part[:8] == fecha_str
    except Exception:
        return False

def obtener_snapshot_url_directo(original_url: str, fecha_str: str) -> Optional[str]:
    """
    Sin usar la API de Wayback: prueba varias horas del día y valida
    que la redirección final sea del MISMO día. Devuelve None si no hay snapshot ese día.
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
    """
    Extrae h1/h2/h3 con >3 palabras como titular básico.
    """
    titulares: List[Dict] = []
    try:
        res = requests.get(snapshot_url, timeout=SNAPSHOT_TIMEOUT)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, 'lxml')

        encabezados = soup.find_all(['h1', 'h2', 'h3'])
        print(f"[{fuente}] {len(encabezados)} encabezados en {snapshot_url}")

        for t in encabezados:
            texto = t.get_text(strip=True)
            if not texto or len(texto.split()) <= 3:
                continue
            titulares.append({
                "fecha": fecha_str,
                "titular": texto,
                "url_archivo": snapshot_url
            })
    except Exception as e:
        log_error(f"[{fuente or 'GENERAL'}] Error accediendo a snapshot: {e}")
    return titulares

# =========================
# Snowflake utils (antes en snowflake_utils.py)
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

def obtener_ultima_fecha_en_snowflake(config: Dict[str,str], tabla: str) -> date:
    """
    Devuelve MAX(fecha)+1 si hay datos; si no, arranca en 2024-01-01.
    """
    ctx = sf_connect()
    cs = ctx.cursor()
    try:
        tabla_completa = f"{config['database']}.{config['schema']}.{tabla}"
        cs.execute(f"SELECT MAX(fecha) FROM {tabla_completa}")
        r = cs.fetchone()
        if r and r[0]:
            ultima = r[0]  # tipo date
            print(f"Última fecha en Snowflake para {tabla}: {ultima}")
            return ultima + timedelta(days=1)
        else:
            print(f"No se encontraron registros en {tabla}. Iniciando desde 2024-01-01.")
            return datetime.strptime("20240101", "%Y%m%d").date()
    finally:
        cs.close()
        ctx.close()

def subir_a_snowflake(df: pd.DataFrame, config: Dict[str,str], tabla: str) -> None:
    """
    Inserta filas en {tabla}. Evita duplicados en memoria (drop_duplicates).
    Si necesitas deduplicar en BD, agrega una UNIQUE KEY (fecha,titular) o usa MERGE.
    """
    if df.empty:
        print(f"No hay datos para subir a {tabla}.")
        return

    df = df.copy()
    df["fecha"] = pd.to_datetime(df["fecha"], format="%Y%m%d").dt.date

    ctx = sf_connect()
    cs = ctx.cursor()
    try:
        tabla_completa = f"{config['database']}.{config['schema']}.{tabla}"

        # Crear tabla si no existe
        cs.execute(f"""
            CREATE TABLE IF NOT EXISTS {tabla_completa} (
                fecha DATE,
                titular STRING,
                url_archivo STRING,
                fuente STRING,
                idioma STRING
            )
        """)

        rows = df[["fecha","titular","url_archivo","fuente","idioma"]].values.tolist()
        insert_sql = f"INSERT INTO {tabla_completa} (fecha, titular, url_archivo, fuente, idioma) VALUES (%s, %s, %s, %s, %s)"
        cs.executemany(insert_sql, rows)
        print(f"{len(rows)} filas insertadas en {tabla}.")
    finally:
        cs.close()
        ctx.close()

# =========================
# MAIN
# =========================

if __name__ == "__main__":
    FECHA_FIN = ayer_madrid()  # Ayer en Madrid
    print(f"FECHA_FIN (ayer Madrid): {FECHA_FIN}")

    for medio in NOTICIEROS:
        nombre = medio["nombre"]
        url = medio["url"]
        fuente = medio["fuente"]
        idioma = medio["idioma"]
        tabla = medio["tabla"]

        print(f"\nProcesando noticiero: {nombre} ({fuente})")

        FECHA_INICIO = obtener_ultima_fecha_en_snowflake(SNOWFLAKE_CONFIG, tabla)
        if FECHA_INICIO > FECHA_FIN:
            print(f"Nada que hacer para {fuente}: {FECHA_INICIO} > {FECHA_FIN}")
            continue

        print(f"Fecha de inicio: {FECHA_INICIO}")
        print(f"Fecha de fin:    {FECHA_FIN}")

        resultados: List[Dict] = []
        fecha = FECHA_INICIO
        while fecha <= FECHA_FIN:
            fecha_str = fecha.strftime("%Y%m%d")
            print(f"[{fuente}] Procesando {fecha_str}...")

            snapshot_url = obtener_snapshot_url_directo(url, fecha_str)

            if snapshot_url:
                try:
                    tits = extraer_titulares(snapshot_url, fecha_str, fuente=fuente)
                    for t in tits:
                        t["fuente"] = fuente
                        t["idioma"] = idioma
                    if tits:
                        print(f"{len(tits)} titulares encontrados.")
                        resultados.extend(tits)
                    else:
                        print("Snapshot sin titulares.")
                except Exception as e:
                    log_error(f"[{fuente}] Error en {fecha_str}: {e}")
            else:
                print(f"[{fuente}] Sin snapshot válido (mismo día) para {fecha_str}.")

            time.sleep(SLEEP_BETWEEN_DIAS)
            fecha += timedelta(days=1)

        # Subida a Snowflake
        if resultados:
            df_nuevo = pd.DataFrame(resultados)
            df_nuevo.drop_duplicates(subset=["fecha", "titular"], inplace=True)
            subir_a_snowflake(df_nuevo, SNOWFLAKE_CONFIG, tabla)
            print(f"Total titulares subidos para {fuente}: {len(df_nuevo)}")
        else:
            print(f"No se encontraron titulares nuevos para {fuente}.")
