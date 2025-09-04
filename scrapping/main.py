# -*- coding: utf-8 -*-
"""
main.py — Scraper de titulares (Wayback directo + id_) → Snowflake
- Solo días faltantes: MAX(fecha)+1 .. ayer (Europe/Madrid). Si no hay datos: 2024-01-01.
- Wayback con 'id_' para HTML crudo (sin banner).
- Extracción multi-estrategia:
    1) h1/h2/h3
    2) Selectores por medio (BBC, ABC, EL_PAIS, BLOOMBERG, THE_TIMES, EXPANSION)
    3) Red genérica: data-testid/headline/title/role=heading, enlaces con aria-label/title
- MERGE por (fecha, titular). Logs detallados.

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
# Wayback directo (sin API) + id_
# =========================

def _timestamp_candidates_for_day(fecha_str: str) -> List[str]:
    return ["235959", "220000", "210000", "200000", "180000", "150000", "120000", "090000", "060000", "030000", "000000"]

def _to_id_variant(url: str) -> str:
    try:
        pre, post = url.split("/web/", 1)
        ts, rest = post.split("/", 1)
        if not ts.endswith("id_"):
            ts = ts + "id_"
        return pre + "/web/" + ts + "/" + rest
    except Exception:
        return url

def _is_same_day_snapshot(final_url: str, fecha_str: str) -> bool:
    try:
        ts = final_url.split("/web/")[1].split("/", 1)[0]
        if ts.endswith("id_"):
            ts = ts[:-3]
        return ts[:8] == fecha_str
    except Exception:
        return False

def obtener_snapshot_url_directo(original_url: str, fecha_str: str) -> Tuple[Optional[str], List[str]]:
    detail_logs = []
    base_url = original_url.strip("/")
    for hhmmss in _timestamp_candidates_for_day(fecha_str):
        candidate = f"https://web.archive.org/web/{fecha_str}{hhmmss}/{base_url}/"
        try:
            resp = requests.get(candidate, timeout=WAYBACK_TIMEOUT, allow_redirects=True, headers=HDRS)
            final_url = resp.url.replace("http://", "https://")
            final_id = _to_id_variant(final_url)
            detail_logs.append(f"  - Probar {candidate} → {resp.status_code} final={final_url} id_={final_id}")
            if resp.status_code == 200 and _is_same_day_snapshot(final_url, fecha_str):
                detail_logs.append(f"    ✓ Aceptado (mismo día {fecha_str})")
                return final_id, detail_logs
            else:
                detail_logs.append(f"    × Rechazado (no es {fecha_str})")
        except Exception as e:
            err = f"[WB-direct] Error acceso {candidate}: {e}"
            log_error(err)
            detail_logs.append(f"    ! {err}")
    return None, detail_logs

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
    # si el <a> tiene title/aria-label con texto útil
    if node.name == "a":
        for attr in ("aria-label", "title"):
            v = node.get(attr)
            if v and len(_norm(v).split()) >= 3:
                return _norm(v)
    return _norm(node.get_text(" ", strip=True))

def _dedupe_and_filter(titulares: List[str]) -> List[str]:
    seen = set()
    out = []
    for t in titulares:
        t2 = _norm(t)
        if len(t2.split()) < 3:
            continue
        if t2 in seen:
            continue
        seen.add(t2)
        out.append(t2)
    return out

def _site_specific_selectors(fuente: str) -> List[str]:
    f = fuente.upper()
    # Selectores razonables por medio (no exhaustivos, pero cubren la mayoría de portadas)
    if f == "BBC":
        return [
            '[data-testid="card-headline"]',
            '[data-testid*=headline]',
            'a.gs-c-promo-heading__title',
            'h3 a.gs-c-promo-heading',
        ]
    if f == "ABC":
        return [
            'h2 a', 'h3 a',
            '[class*="titular"] a', '[class*="headline"] a',
            '[class*="title"] a'
        ]
    if f == "EL PAIS":
        return [
            'h2 a', 'h3 a',
            '[data-dtm-region*="headline"]', '[class*="headline"]',
            '[class*="c_t"] a', '[class*="title"] a',
        ]
    if f == "BLOOMBERG":
        return [
            'h1', 'h2', 'h3',
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
            'h2 a', 'h3 a',
            '[class*="titular"] a', '[class*="headline"] a',
            'a[aria-label]', 'a[title]'
        ]
    # EL ECONOMISTA (portadas JSON y módulos), intentamos genéricos
    if f == "EL ECONOMISTA":
        return [
            'h2 a', 'h3 a',
            '[class*="titular"] a', '[class*="headline"] a',
            '[class*="title"] a', '[data-testid*="headline"]'
        ]
    return []

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
        res = requests.get(snapshot_url, timeout=SNAPSHOT_TIMEOUT, headers=HDRS)
        res.raise_for_status()
        html = res.content
        soup = BeautifulSoup(html, "lxml")
        _clean_wayback_banner(soup)

        # robots
        body_text = soup.get_text(" ", strip=True)[:600].lower()
        if "blocked by robots" in body_text or "unavailable due to robots" in body_text:
            if VERBOSE:
                log_info(f"[{fuente}] Snapshot bloqueado por robots: {snapshot_url}")
            return []

        all_texts: List[str] = []

        # 1) h1/h2/h3 directo
        h_nodes = soup.select("h1, h2, h3")
        h_texts = [_take_text(n) for n in h_nodes]
        h_texts = _dedupe_and_filter(h_texts)
        if VERBOSE:
            log_info(f"[{fuente}] h1/h2/h3 encontrados: {len(h_texts)}")

        all_texts.extend(h_texts)

        # 2) site-specific si poco o nada
        if len(all_texts) < 10 and fuente:
            ssels = _site_specific_selectors(fuente)
            for sel in ssels:
                nodes = soup.select(sel)
                if not nodes:
                    continue
                tsel = _dedupe_and_filter([_take_text(n) for n in nodes])
                if tsel:
                    if VERBOSE:
                        log_info(f"[{fuente}] +{len(tsel)} por selector '{sel}'")
                    all_texts.extend(tsel)

        # 3) safety net genérica
        if len(all_texts) < 10:
            for sel in _generic_safety_net_selectors():
                nodes = soup.select(sel)
                tsel = _dedupe_and_filter([_take_text(n) for n in nodes])
                if tsel:
                    if VERBOSE:
                        log_info(f"[{fuente}] +{len(tsel)} por genérico '{sel}'")
                    all_texts.extend(tsel)

        # dedupe final
        final_texts = _dedupe_and_filter(all_texts)

        if VERBOSE:
            log_info(f"[{fuente}] TOTAL titulares detectados: {len(final_texts)} en {snapshot_url}")
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
            log_info(f"[{fuente}] Día {fecha_str} — buscando snapshot del MISMO día (varias horas)")

            snapshot_url, detail_logs = obtener_snapshot_url_directo(url, fecha_str)
            for line in detail_logs:
                log_info(line)

            if snapshot_url:
                dias_con_snapshot += 1
                log_info(f"[{fuente}] Snapshot aceptado (id_): {snapshot_url}")
                tits = extraer_titulares(snapshot_url, fecha_str, fuente=fuente)
                for t in tits:
                    t["fuente"] = fuente
                    t["idioma"] = idioma
                total_titulares += len(tits)
                log_info(f"[{fuente}] Titulares {fecha_str}: {len(tits)}")
                resultados.extend(tits)
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

    log_info("\n=== RESUMEN ===")
    log_info("Fuente | Días intentados | Días con snapshot | Titulares extraídos | Δ filas en tabla")
    for fuente, di, ds, tot, delta in resumen:
        log_info(f"{fuente:10} | {di:14} | {ds:17} | {tot:18} | {delta:14}")
    log_info("=== FIN SCRAPER ===")
