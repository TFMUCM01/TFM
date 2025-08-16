# -*- coding: utf-8 -*-
# pip install requests pandas snowflake-connector-python beautifulsoup4 lxml

import re
import time
from typing import List, Dict, Set, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup
import snowflake.connector

# ========= CONFIG SNOWFLAKE =========
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
    dict(pais="España",        index="IBEX 35",  components_url="https://www.tradingview.com/symbols/BME-IBC/components/",       accept_exchanges={"BME"},    tv_stock_exchange="BME",     yahoo_suffix=".MC", min_count=35),
    dict(pais="Alemania",      index="DAX 40",   components_url="https://www.tradingview.com/symbols/XETR-DAX/components/",      accept_exchanges={"XETR"},   tv_stock_exchange="XETR",    yahoo_suffix=".DE", min_count=40),
    dict(pais="Francia",       index="CAC 40",   components_url="https://www.tradingview.com/symbols/EURONEXT-PX1/components/",  accept_exchanges={"EURONEXT"}, tv_stock_exchange="EURONEXT", yahoo_suffix=".PA", min_count=40),
    dict(pais="Italia",        index="FTSE MIB", components_url="https://www.tradingview.com/symbols/INDEX-FTSEMIB/components/", accept_exchanges={"MIL"},    tv_stock_exchange="MIL",     yahoo_suffix=".MI", min_count=40),
    dict(pais="Países Bajos",  index="AEX",      components_url="https://www.tradingview.com/symbols/EURONEXT-AEX/components/",  accept_exchanges={"EURONEXT"}, tv_stock_exchange="EURONEXT", yahoo_suffix=".AS", min_count=50),
    dict(pais="Reino Unido",   index="FTSE 100", components_url="https://www.tradingview.com/symbols/FTSE-UKX/components/",      accept_exchanges={"LSE"},    tv_stock_exchange="LSE",     yahoo_suffix=".L",  min_count=100),
    dict(pais="Suecia",        index="OMXS30",   components_url="https://www.tradingview.com/symbols/NASDAQ-OMXS30/components/", accept_exchanges={"OMXSTO"}, tv_stock_exchange="OMXSTO",  yahoo_suffix=".ST", min_count=30),
    dict(pais="Suiza",         index="SMI",      components_url="https://www.tradingview.com/symbols/SIX-SMI/components/",        accept_exchanges={"SIX"},    tv_stock_exchange="SIX",     yahoo_suffix=".SW", min_count=21),
]

# ========= HTTP =========
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36")
})
TIMEOUT = 25

# ========= REGEX =========
SYMBOL_LINK_RE = re.compile(r'/symbols/([A-Z]+)-([A-Z0-9_.-]{1,20})/', re.I)
SYMBOL_TEXT_RE = re.compile(r'\b([A-Z]+):([A-Z0-9_.-]{1,20})\b', re.I)
OG_URL_RE      = re.compile(r'/symbols/([A-Z]+)-([A-Z0-9_.-]{1,20})/?$', re.I)

INVALID_KEYWORDS = (
    " INDEX", " ÍNDICE", " ETF", " ETN", " ETC",
    " FUTURE", " FUTURO", " FUTURES",
    " CFD", " FOREX", " CURRENCY", " CRYPTO",
    " BOND", " YIELD", " WARRANT", " OPTION"
)

def fetch_html(url: str) -> str:
    r = SESSION.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text

def extract_tickers_from_components(html: str, accept_exchanges: Set[str]) -> List[str]:
    found = set()
    for exch, ticker in SYMBOL_LINK_RE.findall(html):
        if exch.upper() in accept_exchanges:
            found.add(ticker)
    for exch, ticker in SYMBOL_TEXT_RE.findall(html):
        if exch.upper() in accept_exchanges:
            found.add(ticker)
    # eliminar tokens comunes que no son símbolos
    bad_like = {"INDEX", "FX", "TVC"}
    found = {t for t in found if t.upper() not in bad_like}
    return sorted(found)

def clean_company_name(raw: str, ticker: str) -> str:
    if not raw:
        return ticker
    s = raw.strip()
    s = re.sub(r"\s*—\s*TradingView.*$", "", s, flags=re.I)
    s = re.sub(r"\(\s*[A-Z]+:[A-Z0-9_.-]{1,20}\s*\)", "", s)
    s = re.sub(r"\s{2,}", " ", s).strip(" -–—|·")
    return s or ticker

def fetch_symbol_info(exchange: str, ticker_for_tv_path: str) -> Tuple[str, str, str, bool]:
    """
    Devuelve: (nombre_limpio, canonical_exchange, canonical_symbol, es_equity_valida)
    - es_equity_valida = False si detectamos Index/ETF/Futures/CFD/etc. o si el exchange canonical no coincide.
    """
    url = f"https://www.tradingview.com/symbols/{exchange}-{ticker_for_tv_path}/"
    try:
        html = fetch_html(url)
    except Exception:
        return (ticker_for_tv_path, exchange, ticker_for_tv_path, False)

    soup = BeautifulSoup(html, "lxml")

    # Nombre
    name = ""
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        name = clean_company_name(h1.get_text(" ", strip=True), ticker_for_tv_path)
    if not name:
        ogt = soup.find("meta", {"property": "og:title"})
        if ogt and ogt.get("content"):
            name = clean_company_name(ogt["content"], ticker_for_tv_path)

    # Meta textos para detección de tipo
    meta_texts = [name.upper()]
    ogd = soup.find("meta", {"property": "og:description"})
    if ogd and ogd.get("content"):
        meta_texts.append(ogd["content"].upper())
    title_tag = soup.find("title")
    if title_tag and title_tag.get_text():
        meta_texts.append(title_tag.get_text().upper())
    big_text = " ".join(meta_texts)

    # Exchange y símbolo canónicos desde og:url (si está)
    canonical_exchange, canonical_symbol = exchange, ticker_for_tv_path
    ogu = soup.find("meta", {"property": "og:url"})
    if ogu and ogu.get("content"):
        m = OG_URL_RE.search(ogu["content"])
        if m:
            canonical_exchange = m.group(1).upper()
            canonical_symbol   = m.group(2)

    # Heurística: descartar si parece índice/ETF/etc.
    is_invalid_kind = any(kw in big_text for kw in INVALID_KEYWORDS)

    # Válido sólo si NO es índice/ETF/... y el exchange canónico coincide
    es_equity_valida = (not is_invalid_kind) and (canonical_exchange == exchange)

    return (name or ticker_for_tv_path, canonical_exchange, canonical_symbol, es_equity_valida)

def yahoo_ticker_from_local(local_ticker: str, pais: str) -> str:
    t = local_ticker.strip().upper()
    if pais == "Reino Unido":
        t = t.replace('.', '-').rstrip('-')
    elif pais == "Suecia":
        t = t.replace('_', '-')
    return t

def scrape_country(spec: Dict) -> pd.DataFrame:
    html = fetch_html(spec["components_url"])
    candidates = extract_tickers_from_components(html, spec["accept_exchanges"])
    if len(candidates) < spec["min_count"]:
        raise RuntimeError(f"{spec['index']} ({spec['pais']}): sólo {len(candidates)} símbolos candidatos (<{spec['min_count']}).")

    rows = []
    kept, skipped = 0, 0
    for tk in candidates:
        # Info del símbolo (validación de tipo y exchange)
        nombre, exch_can, sym_can, ok = fetch_symbol_info(spec["tv_stock_exchange"], tk)
        if not ok:
            skipped += 1
            continue

        base_for_yahoo = yahoo_ticker_from_local(sym_can, spec["pais"])
        yahoo = f"{base_for_yahoo}{spec['yahoo_suffix']}"
        rows.append({
            "TICKER_YAHOO": yahoo,
            "NOMBRE": nombre,
            "PAIS": spec["pais"],
            "TICKET": base_for_yahoo
        })
        kept += 1
        time.sleep(0.12)  # cortesía

    if kept < spec["min_count"]:
        # si tras filtrar quedaron muy pocos, mejor avisar
        raise RuntimeError(f"{spec['index']} ({spec['pais']}): tras filtrar quedaron {kept} (<{spec['min_count']}). Revisa cambios de página.")
    print(f"   → filtrados: {kept} válidos, {skipped} descartados")

    df = pd.DataFrame(rows, columns=["TICKER_YAHOO", "NOMBRE", "PAIS", "TICKET"])
    return df

# ========= CARGA EN SNOWFLAKE (TRUNCATE + INSERT) =========
def df_to_rows(df: pd.DataFrame) -> List[tuple]:
    required = ["TICKER_YAHOO", "NOMBRE", "PAIS", "TICKET"]
    df = df.rename(columns=str.upper)
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas: {missing}.")
    sub = df[required].fillna("")
    return [tuple(str(x).strip() for x in row) for row in sub.to_numpy().tolist()]

def overwrite_snowflake_sql(df: pd.DataFrame, cfg: Dict):
    rows = df_to_rows(df)
    if not rows:
        raise ValueError("DF vacío; no se insertará nada.")
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
            cur.execute(f"TRUNCATE TABLE {fq_table}")
            cur.executemany(insert_sql, rows)
            cur.execute(f"SELECT COUNT(*) FROM {fq_table}")
            count = cur.fetchone()[0]
            conn.commit()
        print(f"✅ Snowflake: {count} filas en {fq_table}")
    finally:
        conn.close()

# ========= MAIN =========
if __name__ == "__main__":
    frames = []
    for spec in INDEX_SPECS:
        print(f"Raspando {spec['index']} ({spec['pais']}) ...")
        df_country = scrape_country(spec)
        print(f" - {spec['pais']}: {len(df_country)} tickers")
        frames.append(df_country)

    full_df = pd.concat(frames, ignore_index=True).rename(columns=str.upper)

    # Dedup por Yahoo (por si un emisor cotiza en más de un mercado con mismo ticker Yahoo)
    full_df = full_df.drop_duplicates(subset=["TICKER_YAHOO"]).reset_index(drop=True)

    if full_df.empty:
        raise RuntimeError("No se obtuvieron símbolos válidos; abortando.")

    print("Muestra:", full_df.head(8).to_dict(orient="records"))
    overwrite_snowflake_sql(full_df, SNOWFLAKE_CONFIG)
    print("¡Hecho!")
