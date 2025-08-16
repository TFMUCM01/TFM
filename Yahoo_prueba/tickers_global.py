# -*- coding: utf-8 -*-
# pip install requests pandas snowflake-connector-python beautifulsoup4 lxml

import re
import time
from typing import List, Dict, Set

import requests
import pandas as pd
from bs4 import BeautifulSoup
import snowflake.connector

# ========= CONFIGURA TU CONEXIÓN SNOWFLAKE =========
SNOWFLAKE_CONFIG = {
    'user': 'tfmgrupo4',
    'password': 'TFMgrupo4ucm01_01#',
    'account': 'WYNIFVB-YE01854',
    'warehouse': 'TFM_WH',
    'database': 'YAHOO_PRUEBA',
    'schema': 'IBEX',
    'table': 'LISTA_IBEX_35'
}

# ========= ÍNDICES A RASTREAR =========
INDEX_SPECS = [
    # España — IBEX 35
    dict(pais="España", index="IBEX 35",
         components_url="https://www.tradingview.com/symbols/BME-IBC/components/",
         accept_exchanges={"BME"}, tv_stock_exchange="BME", yahoo_suffix=".MC", min_count=30),

    # Alemania — DAX 40
    dict(pais="Alemania", index="DAX 40",
         components_url="https://www.tradingview.com/symbols/XETR-DAX/components/",
         accept_exchanges={"XETR"}, tv_stock_exchange="XETR", yahoo_suffix=".DE", min_count=30),

    # Francia — CAC 40
    dict(pais="Francia", index="CAC 40",
         components_url="https://www.tradingview.com/symbols/EURONEXT-PX1/components/",
         accept_exchanges={"EURONEXT"}, tv_stock_exchange="EURONEXT", yahoo_suffix=".PA", min_count=30),

    # Italia — FTSE MIB
    dict(pais="Italia", index="FTSE MIB",
        components_url="https://www.tradingview.com/symbols/INDEX-FTSEMIB/components/",
        accept_exchanges={"MIL"}, tv_stock_exchange="MIL", yahoo_suffix=".MI", min_count=30),

    # Países Bajos — AEX
    dict(pais="Países Bajos", index="AEX",
         components_url="https://www.tradingview.com/symbols/EURONEXT-AEX/components/",
         accept_exchanges={"EURONEXT"}, tv_stock_exchange="EURONEXT", yahoo_suffix=".AS", min_count=20),

    # Reino Unido — FTSE 100
    dict(pais="Reino Unido", index="FTSE 100",
         components_url="https://www.tradingview.com/symbols/FTSE-UKX/components/",
         accept_exchanges={"LSE"}, tv_stock_exchange="LSE", yahoo_suffix=".L", min_count=90),

    # Suecia — OMXS30
    dict(pais="Suecia", index="OMXS30",
         components_url="https://www.tradingview.com/symbols/NASDAQ-OMXS30/components/",
         accept_exchanges={"OMXSTO"}, tv_stock_exchange="OMXSTO", yahoo_suffix=".ST", min_count=25),

    # Suiza — SMI
    dict(pais="Suiza", index="SMI",
         components_url="https://www.tradingview.com/symbols/SIX-SMI/components/",
         accept_exchanges={"SIX"}, tv_stock_exchange="SIX", yahoo_suffix=".SW", min_count=18),
]

# ========= HTTP SESSION =========
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36")
})
TIMEOUT = 25

# ========= HELPERS DE SCRAPING =========
# Aceptamos letras, números, puntos, guiones y guión bajo en tickers.
SYMBOL_LINK_RE = re.compile(r'/symbols/([A-Z]+)-([A-Z0-9_.-]{1,20})/', re.I)
SYMBOL_TEXT_RE = re.compile(r'\b([A-Z]+):([A-Z0-9_.-]{1,20})\b', re.I)

def fetch_html(url: str) -> str:
    r = SESSION.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text

def extract_tickers_from_components(html: str, accept_exchanges: Set[str]) -> List[str]:
    found = set()

    # 1) Enlaces /symbols/EXCH-TICKER/
    for exch, ticker in SYMBOL_LINK_RE.findall(html):
        if exch.upper() in accept_exchanges:
            found.add(ticker)

    # 2) Textos EXCH:TICKER
    for exch, ticker in SYMBOL_TEXT_RE.findall(html):
        if exch.upper() in accept_exchanges:
            found.add(ticker)

    # Limpieza de tokens que no son símbolos
    bad_like = {"INDEX", "FX", "TVC"}
    found = {t for t in found if t.upper() not in bad_like}

    return sorted(found)

def clean_company_name(raw: str, ticker: str) -> str:
    if not raw:
        return ticker
    s = raw.strip()
    # Elimina sufijos de TradingView
    s = re.sub(r"\s*—\s*TradingView.*$", "", s, flags=re.I)
    # Elimina "(EXCH:TICKER)" si aparece
    s = re.sub(r"\(\s*[A-Z]+:[A-Z0-9_.-]{1,20}\s*\)", "", s)
    # Colapsa espacios y limpia guiones
    s = re.sub(r"\s{2,}", " ", s).strip(" -–—|·")
    return s or ticker

def extract_company_name_from_tv(exchange: str, ticker_for_tv_path: str) -> str:
    """
    Toma el <h1> de la página del valor en TradingView:
    https://www.tradingview.com/symbols/{exchange}-{ticker}/
    """
    url = f"https://www.tradingview.com/symbols/{exchange}-{ticker_for_tv_path}/"
    try:
        html = fetch_html(url)
        soup = BeautifulSoup(html, "lxml")
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True):
            return clean_company_name(h1.get_text(" ", strip=True), ticker_for_tv_path)
        og = soup.find("meta", {"property": "og:title"})
        if og and og.get("content"):
            return clean_company_name(og["content"], ticker_for_tv_path)
    except Exception:
        pass
    return ticker_for_tv_path  # fallback

def yahoo_ticker_from_local(local_ticker: str, pais: str) -> str:
    """
    Normaliza el símbolo local para Yahoo Finance según la bolsa/país.
    Sólo devuelve el "base", el sufijo se añade fuera.
    """
    t = local_ticker.strip().upper()

    if pais == "Reino Unido":
        # LSE usa puntos para clases/series: 'BT.A', 'RR.'
        # Yahoo usa guiones y suprime punto final: 'BT-A', 'RR'
        t = t.replace('.', '-').rstrip('-')
    elif pais == "Suecia":
        # OMXSTO usa guión bajo para series: 'INVE_B' -> Yahoo usa guión: 'INVE-B'
        t = t.replace('_', '-')
    # Los demás suelen ir tal cual
    return t

def scrape_country(spec: Dict) -> pd.DataFrame:
    html = fetch_html(spec["components_url"])
    tickers = extract_tickers_from_components(html, spec["accept_exchanges"])
    if len(tickers) < spec["min_count"]:
        raise RuntimeError(f"{spec['index']} ({spec['pais']}): sólo {len(tickers)} símbolos extraídos (<{spec['min_count']}).")

    rows = []
    for tk in tickers:
        # Ticker para construir la URL del valor en TradingView (tal cual viene en components)
        tv_ticker_for_path = tk

        # Ticker base para Yahoo (normalizado por país)
        base_for_yahoo = yahoo_ticker_from_local(tk, spec["pais"])
        yahoo = f"{base_for_yahoo}{spec['yahoo_suffix']}"

        # Nombre de empresa desde la página del valor
        nombre = extract_company_name_from_tv(spec["tv_stock_exchange"], tv_ticker_for_path)

        rows.append({
            "TICKER_YAHOO": yahoo,
            "NOMBRE": nombre,
            "PAIS": spec["pais"],
            "TICKET": base_for_yahoo  # local, sin sufijo Yahoo
        })

        # Pequeña pausa para no ser agresivos con el sitio
        time.sleep(0.15)

    df = pd.DataFrame(rows, columns=["TICKER_YAHOO", "NOMBRE", "PAIS", "TICKET"])
    return df

# ========= CARGA EN SNOWFLAKE (TRUNCATE + INSERT) =========
def df_to_rows(df: pd.DataFrame) -> List[tuple]:
    required = ["TICKER_YAHOO", "NOMBRE", "PAIS", "TICKET"]
    # Asegura columnas en mayúsculas
    df = df.rename(columns=str.upper)
    # Verifica columnas
    faltan = [c for c in required if c not in df.columns]
    if faltan:
        raise ValueError(f"Faltan columnas en el DataFrame: {faltan}. Columnas actuales: {list(df.columns)}")
    sub = df[required].fillna("")
    # Convierte a lista de tuplas (todo a str por seguridad)
    return [tuple(str(x).strip() for x in row) for row in sub.to_numpy().tolist()]

def overwrite_snowflake_sql(df: pd.DataFrame, cfg: Dict):
    rows = df_to_rows(df)
    if not rows:
        raise ValueError("El DataFrame está vacío; no se insertará nada.")

    conn = snowflake.connector.connect(
        user=cfg["user"],
        password=cfg["password"],
        account=cfg["account"],
        warehouse=cfg["warehouse"],
        database=cfg["database"],
        schema=cfg["schema"],
    )
    fq_table = f'{cfg["database"]}.{cfg["schema"]}.{cfg["table"]}'
    insert_sql = f"INSERT INTO {fq_table} (TICKER_YAHOO, NOMBRE, PAIS, TICKET) VALUES (%s, %s, %s, %s)"
    try:
        with conn.cursor() as cur:
            # Borrar contenido previo (sin histórico)
            cur.execute(f"TRUNCATE TABLE {fq_table}")
            # Insertar todas las filas
            cur.executemany(insert_sql, rows)
            # Validación
            cur.execute(f"SELECT COUNT(*) FROM {fq_table}")
            count = cur.fetchone()[0]
            conn.commit()
        print(f"✅ Snowflake: {count} filas en {fq_table}")
    finally:
        conn.close()

# ========= MAIN =========
if __name__ == "__main__":
    all_frames = []
    for spec in INDEX_SPECS:
        print(f"Raspando {spec['index']} ({spec['pais']}) ...")
        df_country = scrape_country(spec)
        print(f" - {spec['pais']}: {len(df_country)} tickers")
        all_frames.append(df_country)

    full_df = pd.concat(all_frames, ignore_index=True)

    # Normaliza columnas y elimina duplicados por ticker de Yahoo
    full_df = full_df.rename(columns=str.upper)
    full_df = full_df.drop_duplicates(subset=["TICKER_YAHOO"]).reset_index(drop=True)

    if full_df.empty:
        raise RuntimeError("No se obtuvieron símbolos; abortando carga.")

    # Debug corto (opcional)
    print("Muestra:", full_df.head(5).to_dict(orient="records"))

    # Carga en Snowflake (TRUNCATE + INSERT)
    overwrite_snowflake_sql(full_df, SNOWFLAKE_CONFIG)
    print("¡Hecho!")
