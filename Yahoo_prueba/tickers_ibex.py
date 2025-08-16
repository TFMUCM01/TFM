# pip install requests beautifulsoup4 lxml pandas snowflake-connector-python

import re
import time
import pandas as pd
import requests as rq
from bs4 import BeautifulSoup
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

# ---------- TradingView ----------
URL_COMPONENTS = "https://www.tradingview.com/symbols/BME-IBC/components/"
URL_STOCK = "https://www.tradingview.com/symbols/BME-{ticker}/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

# ---------- Snowflake (tu config) ----------
SNOWFLAKE_CONFIG = {
    'user': 'tfmgrupo4',
    'password': 'TFMgrupo4ucm01_01#',
    'account': 'WYNIFVB-YE01854',
    'warehouse': 'TFM_WH',
    'database': 'YAHOO_PRUEBA',
    'schema': 'IBEX',
    'table': 'LISTA_IBEX_35'
}

def fetch_component_tickers() -> list[str]:
    """Devuelve tickers BME sin .MC (p. ej., ['ITX','SAN',...])."""
    resp = rq.get(URL_COMPONENTS, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    html = resp.text
    # Patrón robusto: hrefs y textos tipo BME:<TICKER>
    tickers = set(re.findall(r"/symbols/BME-([A-Z0-9]{2,6})", html, flags=re.I))
    tickers |= set(re.findall(r"\bBME:([A-Z0-9]{2,6})\b", html, flags=re.I))
    tickers = {t.upper() for t in tickers if t.upper() != "IBC"}
    tickers = sorted(tickers)
    if len(tickers) < 34:
        raise RuntimeError(f"Lista incompleta: {len(tickers)} símbolos encontrados.")
    return tickers

def clean_company_name(txt: str, ticker: str) -> str:
    """Limpia el texto del <h1>/og:title> para dejar solo el nombre de empresa."""
    if not txt:
        return ticker
    s = txt.strip()
    # Elimina '— TradingView' u otros sufijos
    s = re.sub(r"\s*—\s*TradingView.*$", "", s, flags=re.I)
    # Elimina (BME:XXX) o variantes
    s = re.sub(r"\(\s*BME\s*:\s*[A-Z0-9]{2,6}\s*\)", "", s, flags=re.I)
    # Colapsa espacios
    s = re.sub(r"\s{2,}", " ", s).strip(" -–—|·")
    return s or ticker

def fetch_company_name(ticker: str) -> str:
    """Abre la página del valor y toma el <h1> o og:title como nombre."""
    url = URL_STOCK.format(ticker=ticker)
    resp = rq.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return clean_company_name(h1.get_text(" ", strip=True), ticker)
    og = soup.find("meta", {"property": "og:title"})
    if og and og.get("content"):
        return clean_company_name(og["content"], ticker)
    return ticker  # fallback

def build_dataframe(tickers: list[str]) -> pd.DataFrame:
    rows = []
    for t in tickers:
        time.sleep(0.15)  # cortesía con el sitio
        nombre = fetch_company_name(t)
        rows.append({
            "TICKER_YAHOO": f"{t}.MC",
            "NOMBRE": nombre,
            "PAIS": "España",
            "TICKET": t
        })
    return pd.DataFrame(rows, columns=["TICKER_YAHOO", "NOMBRE", "PAIS", "TICKET"])

def overwrite_snowflake(df: pd.DataFrame, cfg: dict) -> None:
    """TRUNCATE + INSERT (sin histórico). Crea la tabla si no existe."""
    conn = connect(
        account=cfg['account'],
        user=cfg['user'],
        password=cfg['password'],
        warehouse=cfg['warehouse'],
        database=cfg['database'],
        schema=cfg['schema'],
    )
    table = cfg['table']
    cs = conn.cursor()
    try:
        cs.execute(f'USE DATABASE {cfg["database"]}')
        cs.execute(f'USE SCHEMA {cfg["schema"]}')
        # Asegura la tabla (nombres sin comillas -> Snowflake los guarda en MAYÚSCULAS)
        cs.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                TICKER_YAHOO STRING,
                NOMBRE STRING,
                PAIS STRING,
                TICKET STRING
            )
        """)
        # Borrar contenido previo
        cs.execute(f"TRUNCATE TABLE {table}")
        # Insertar listado actual
        ok, _, nrows, _ = write_pandas(conn, df, table_name=table)
        if not ok or nrows != len(df):
            raise RuntimeError(f"Insert incompleto: inserted={nrows}, expected={len(df)}")
        # Validación opcional: deben ser 35 filas
        cs.execute(f"SELECT COUNT(*) FROM {table}")
        count = cs.fetchone()[0]
        if count < 34:
            raise RuntimeError(f"Advertencia: la tabla quedó con {count} filas (<34).")
        conn.commit()
    finally:
        cs.close()
        conn.close()

if __name__ == "__main__":
    tickers = fetch_component_tickers()
    df = build_dataframe(tickers)
    overwrite_snowflake(df, SNOWFLAKE_CONFIG)
    print(f"✅ Snowflake actualizado con {len(df)} componentes en "
          f"{SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{SNOWFLAKE_CONFIG['table']}")
