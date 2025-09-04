# -*- coding: utf-8 -*-
"""
main.py — Scraper titulares (Wayback 12:00 id_) → Snowflake
- Solo días faltantes: MAX(fecha)+1 .. ayer (Europe/Madrid). Si no hay datos: 2024-01-01.
- Una sola hora fija: 12:00:00 (mediodía) con variante id_.
- Extracción multi-estrategia (h1/h2/h3 + selectores por medio + red genérica).
- MERGE por (fecha, titular). Logs detallados con ejemplos.

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

SLEEP_BETWEEN_DIAS = int(os.getenv("SLEEP_BETWEEN_DIAS", "1"))
WAYBACK_TIMEOUT = 25
SNAPSHOT_TIMEOUT = 25
START_DEFAULT = date(2024, 1, 1)
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

def _get_env_any(*names: str):
    for n in names:
        v = os.getenv(n)
        if v:
            return v, n
    return None, None

schema_val, schema_from = _get_env_any("SNOWFLAKE_SCHEMA1", "SNOWFLAKE_SCHEMA")

SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': schema_val,
    'role': os.getenv('SNOWFLAKE_ROLE') or None,
}

def _must(value: Optional[str], name_hint: str) -> str:
    if not value:
        print(f"[CONFIG] Falta variable de entorno: {name_hint}", file=sys.stderr)
        raise RuntimeError(f"Missing env var: {name_hint}")
    return value

# Validación
SNOWFLAKE_CONFIG['user']      = _must(SNOWFLAKE_CONFIG['user'],      "SNOWFLAKE_USER")
SNOWFLAKE_CONFIG['password']  = _must(SNOWFLAKE_CONFIG['password'],  "SNOWFLAKE_PASSWORD")
SNOWFLAKE_CONFIG['account']   = _must(SNOWFLAKE_CONFIG['account'],   "SNOWFLAKE_ACCOUNT")
SNOWFLAKE_CONFIG['warehouse'] = _must(SNOWFLAKE_CONFIG['warehouse'], "SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_CONFIG['database']  = _must(SNOWFLAKE_CONFIG['database'],  "SNOWFLAKE_DATABASE")
SNOWFLAKE_CONFIG['schema']    = _must(SNOWFLAKE_CONFIG['schema'],    "SNOWFLAKE_SCHEMA1 o SNOWFLAKE_SCHEMA")

# =========================
# Utilidades
# =========================

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
HDRS = {"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9,es-ES;q=0.8"}

TZ_MADRID = ZoneInfo("Europe/Madrid")

def ayer_madrid() -> date:
    return (datetime.now(TZ_MADRID) - timedelta(days=1)).date()

def log_error(mensaje: str) -> None:
    with open("scraping_log.txt", "a", errors="ignore") as f:
        f.write(f"{datetime.now()} - {mensaje}\n")

def log_info(msg: str) -> None:
    print(msg, flush=True)

# =========================
# Wayback a hora fija (12:00) con id_
# =========================

def _snapshot_1200_id(original_url: str, fecha_str: str) -> str:
    """Construye directamente la URL de snapshot a las 12:00 con id_ (sin redirecciones)."""
    base = original_url.strip("/")
    return f"https://web.archive.org/web/{fecha_str}120000id_/{base}/"

def obtener_snapshot_url_mediodia(original_url: str, fecha_str: str) -> Tuple[Optional[str], str, int]:
    """
    Devuelve (url, motivo, status_code). No valida 'mismo día'; usa 12:00 fijo.
    """
    url = _snapshot_1200_id(original_url, fecha_str)
    try:
        # GET directo para comprobar que existe y devolver el mismo url (no seguimos redirects)
        resp = requests.get(url, timeout=WAYBACK_TIMEOUT, headers=HDRS, allow_redirects=False)
        return (url if resp.status_code == 200 else None,
                "OK" if resp.status_code == 200 else "HTTP != 200",
                resp.status_code)
    except Exception as e:
        log_error(f"[WB-1200] Error {url}: {e}")
        return (None, f"EXC: {e}", -1)

# =========================
# Extracción multi-estrategia
# =========================

def _clean_wayback_banner(soup: BeautifulSoup) -> None:
    for sel in ("#wm-ipp-base", "#wm-ipp-overlay", "#wm-ipp", ".wb-autocomplete-suggestions"):
        for node in soup.select(sel):
            node.decompose()

def _norm(txt: str) -> str:
    return " ".join((txt or "").split())

def _take_text(node) -> str:
    if node.name == "a":
        for attr in ("aria-label", "title"):
            v = node.get(attr)
            if v and len(_norm(v).split()) >= 3:
                return _norm(v)
    return _norm(node.get_text(" ", strip=True))

def _dedupe_and_filter(titulares: List[str], min_words: int = 3) -> List[str]:
    seen = set()
    out = []
    for t in titulares:
        t2 = _norm(t)
        if len(t2.split()) < min_words:
            continue
        if t2 in seen:
            continue
        seen.add(t2)
        out.append(t2)
    return out

def _site_specific_selectors(fuente: str) -> List[str]:
    f = fuente.upper()
    if f == "BBC":
        return [
            '[data-testid="card-headline"]',
            '[data-testid*=headline]',
            'a.gs-c-promo-heading__title',
            'h1, h2, h3',
        ]
    if f == "ABC":
        return [
            'h1, h2, h3',
            'h2 a, h3 a',
            '[class*="titular"] a', '[class*="headline"] a',
            '[class*="title"] a'
        ]
    if f == "EL PAIS":
        return [
            'h1, h2, h3',
            'h2 a, h3 a',
            '[data-dtm-region*="headline"]', '[class*="headline"]',
            '[class*="title"] a',
        ]
    if f == "BLOOMBERG":
        return [
            'h1, h2, h3',
            '[class*="headline"]', '[data-testid*="headline"]',
            'a[aria-label]', 'a[title]'
        ]
    if f == "THE TIMES":
        return [
            'h1, h2, h3',
            '[class*="Headline"]', '[data-testid*="headline"]',
            'a[aria-label]', 'a[title]'
        ]
    if f == "EXPANSION":
        return [
            'h1, h2, h3',
            'h2 a, h3 a',
            '[class*="titular"] a', '[class*="headline"] a',
            'a[aria-label]', 'a[title]'
        ]
    if f == "EL ECONOMISTA":
        return [
            'h1, h2, h3',
            'h2 a, h3 a',
            '[class*="titular"] a', '[class*="headline"] a',
            '[class*="title"] a', '[data-testid*="headline"]'
        ]
    return ['h1, h2, h3']

def _generic_safety_net_selectors() -> List[str]:
    return [
        "h1, h2, h3",
        "[role=heading]",
        "[data-testid*=headline], [class*=headline]",
        "[class*=title], [class*=Title], [class*=Heading]",
        "a[aria-label], a[title]"
    ]

def extraer_titulares(snapshot_url: str, fecha_str: str, fuente: Optional[str] = None) -> List[Dict]:
    titulares: List[Dict] = []
    try:
        res = requests.get(snapshot_url, timeout=SNAPSHOT_TIMEOUT, headers=HDRS, allow_redirects=False)
        status = res.status_code
        if status != 200:
            if VERBOSE:
                log_info(f"[{fuente}] GET {snapshot_url} → {status}")
            return []
        html = res.content
        soup = BeautifulSoup(html, "lxml")
        _clean_wayback_banner(soup)

        # robots
        body_text = soup.get_text(" ", strip=True)[:800].lower()
        if "blocked by robots" in body_text or "unavailable due to robots" in body_text:
            if VERBOSE:
                log_info(f"[{fuente}] Snapshot bloqueado por robots: {snapshot_url}")
            return []

        all_texts: List[str] = []

        # 1) site-specific primero (más agresivo)
        ssels = _site_specific_selectors(fuente or "")
        for sel in ssels:
            nodes = soup.select(sel)
            if not nodes:
                continue
            tsel = _dedupe_and_filter([_take_text(n) for n in nodes], min_words=3)
            if tsel:
                if VERBOSE:
                    log_info(f"[{fuente}] +{len(tsel)} por selector '{sel}'")
                all_texts.extend(tsel)

        # 2) safety net genérica
        if len(all_texts) < 10:
            for sel in _generic_safety_net_selectors():
                nodes = soup.select(sel)
                tsel = _dedupe_and_filter([_take_text(n) for n in nodes], min_words=3)
                if tsel:
                    if VERBOSE:
                        log_info(f"[{fuente}] +{len(tsel)} por genérico '{sel}'")
                    all_texts.extend(tsel)

        # dedupe final
        final_texts = _dedupe_and_filter(all_texts, min_words=3)

        if VERBOSE:
            size = len(html)
            log_info(f"[{fuente}] HTML {size} bytes — titulares totales: {len(final_texts)}")
            if final_texts:
                log_info(f"[{fuente}] Ejemplos: " + " | ".join(final_texts[:3]))

        for texto in final_texts:
            titulares.append({"fecha": fecha_str, "titular": texto, "url_archivo": snapshot_url})

    except Exception as e:
        log_error(f"[{fuente or 'GENERAL'}] Error extrayendo titulares: {e}")

    return titulares

# =========================
# Snowflake
# =========================

def sf_connect():
    log_info(f"[SF] Conectando → user={SNOWFLAKE_CONFIG['user']} account={SNOWFLAKE_CONFIG['account']} "
             f"db={SNOWFLAKE_CONFIG['database']} schema={SNOWFLAKE_CONFIG['schema']} (via {schema_from})")
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
        return 0
    finally:
        cs.close()
        ctx.close()

def obtener_ultima_fecha_en_snowflake(tabla: str) -> date:
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
    log_info(f"[DEBUG] ENTORNO Snowflake: account={SNOWFLAKE_CONFIG['account']} db={SNOWFLAKE_CONFIG['database']} schema={SNOWFLAKE_CONFIG['schema']} (via {schema_from}) warehouse={SNOWFLAKE_CONFIG['warehouse']}")

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

            snapshot_url, motivo, status = obtener_snapshot_url_mediodia(url, fecha_str)
            log_info(f"[{fuente}] {fecha_str} URL 12:00: {snapshot_url or '(none)'} → {status} ({motivo})")

            if snapshot_url:
                dias_con_snapshot += 1
                tits = extraer_titulares(snapshot_url, fecha_str, fuente=fuente)
                for t in tits:
                    t["fuente"] = fuente
                    t["idioma"] = idioma
                total_titulares += len(tits)
                log_info(f"[{fuente}] Titulares {fecha_str}: {len(tits)}")
                if VERBOSE and tits[:3]:
                    ejemplos = " | ".join([x["titular"] for x in tits[:3]])
                    log_info(f"[{fuente}] Ejemplos: {ejemplos}")
                resultados.extend(tits)
            else:
                log_info(f"[{fuente}] Sin snapshot utilizable a 12:00 para {fecha_str}")

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

    log_info("\n=== RESUMEN ===")
    log_info("Fuente | Días intentados | Días con snapshot | Titulares extraídos | Δ filas en tabla")
    for fuente, di, ds, tot, delta in resumen:
        log_info(f"{fuente:10} | {di:14} | {ds:17} | {tot:18} | {delta:14}")
    log_info("=== FIN SCRAPER ===")
