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

# ========= ÍNDICES (con conteo exacto por país) =========
INDEX_SPECS = [
    dict(pais="España",        index="IBEX 35",  components_url="https://www.tradingview.com/symbols/BME-IBC/components/",       accept_exchanges={"BME"},     tv_stock_exchange="BME",     yahoo_suffix=".MC", expected_count=35),
    dict(pais="Alemania",      index="DAX 40",   components_url="https://www.tradingview.com/symbols/XETR-DAX/components/",      accept_exchanges={"XETR"},    tv_stock_exchange="XETR",    yahoo_suffix=".DE", expected_count=40),
    dict(pais="Francia",       index="CAC 40",   components_url="https://www.tradingview.com/symbols/EURONEXT-PX1/components/",  accept_exchanges={"EURONEXT"},tv_stock_exchange="EURONEXT", yahoo_suffix=".PA", expected_count=40),
    dict(pais="Italia",        index="FTSE MIB", components_url="https://www.tradingview.com/symbols/INDEX-FTSEMIB/components/", accept_exchanges={"MIL"},     tv_stock_exchange="MIL",     yahoo_suffix=".MI", expected_count=40),
    dict(pais="Países Bajos",  index="AEX",      components_url="https://www.tradingview.com/symbols/EURONEXT-AEX/components/",  accept_exchanges={"EURONEXT"},tv_stock_exchange="EURONEXT", yahoo_suffix=".AS", expected_count=25),
    dict(pais="Reino Unido",   index="FTSE 100", components_url="https://www.tradingview.com/symbols/FTSE-UKX/components/",      accept_exchanges={"LSE"},     tv_stock_exchange="LSE",     yahoo_suffix=".L",  expected_count=100),
    dict(pais="Suecia",        index="OMXS30",   components_url="https://www.tradingview.com/symbols/NASDAQ-OMXS30/components/", accept_exchanges={"OMXSTO"},  tv_stock_exchange="OMXSTO",  yahoo_suffix=".ST", expected_count=30),
    dict(pais="Suiza",         index="SMI",      components_url="https://www.tradingview.com/symbols/SIX-SMI/components/",        accept_exchanges={"SIX"},     tv_stock_exchange="SIX",     yahoo_suffix=".SW", expected_count=20),
]

# ========= HTTP =========
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36")
})
TIMEOUT = 25

# ========= PATRONES =========
HREF_SYMBOL_RE = re.compile(r'/symbols/([A-Z]+)-([A-Z0-9_.-]{1,20})/?$', re.I)
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

def class_contains(tag, fragment: str) -> bool:
    cls = tag.get("class", [])
    if isinstance(cls, str):
        cls = cls.split()
    return any(fragment in c for c in cls)

def extract_rows_precise(html: str, accept_exchanges: Set[str]) -> List[Tuple[str, str, str]]:
    """
    Devuelve lista de tuplas (EXCHANGE, SYMBOL, NAME) extrayendo
    - símbolo desde <a href="/symbols/EXCH-SYMBOL/">
    - nombre desde <sup class="... tickerDescription-..."> dentro de la MISMA fila.
    """
    soup = BeautifulSoup(html, "lxml")

    # 1) Encuentra todos los sup con la clase de DESCRIPCIÓN
    desc_nodes = soup.find_all("sup", class_=lambda v: v and "tickerDescription-" in " ".join(v if isinstance(v, list) else v.split()))
    rows = []
    for sup in desc_nodes:
        name = sup.get_text(strip=True)
        if not name:
            continue
        # 2) Sube hasta un contenedor de fila (div con clase que contenga 'row-')
        row = sup
        found = None
        for _ in range(8):
            row = row.parent
            if row is None:
                break
            if class_contains(row, "row-"):
                # 3) Dentro de esa fila, busca el link del símbolo
                a = row.find("a", href=True)
                if a:
                    m = HREF_SYMBOL_RE.search(a["href"])
                    if m:
                        exch = m.group(1).upper()
                        sym  = m.group(2)
                        if exch in accept_exchanges:
                            rows.append((exch, sym, name))
                break

    # Fallback: si por alguna razón no se detectan filas por la clase 'row-',
    # intenta emparejar por proximidad (hermanos) buscando el primer link ascendente.
    if not rows:
        for sup in desc_nodes:
            name = sup.get_text(strip=True)
            cur = sup
            for _ in range(8):
                cur = cur.parent
                if cur is None:
                    break
                link = cur.find("a", href=True)
                if link:
                    m = HREF_SYMBOL_RE.search(link["href"])
                    if m:
                        exch = m.group(1).upper()
                        sym  = m.group(2)
                        if exch in accept_exchanges:
                            rows.append((exch, sym, name))
                        break

    # Limpia: elimina duplicados y descarta si el nombre aparenta ser índice/ETF/etc.
    out = {}
    for exch, sym, name in rows:
        if any(kw in name.upper() for kw in INVALID_KEYWORDS):
            continue
        out[(exch, sym)] = name
    result = [(ex, sy, nm) for (ex, sy), nm in out.items()]
    return result

def clean_company_name(s: str, ticker: str) -> str:
    if not s: return ticker
    s = re.sub(r"\s*—\s*TradingView.*$", "", s, flags=re.I)
    s = re.sub(r"\(\s*[A-Z]+:[A-Z0-9_.-]{1,20}\s*\)", "", s)
    s = re.sub(r"\s{2,}", " ", s).strip(" -–—|·")
    return s or ticker

def yahoo_ticker_from_local(local_ticker: str, pais: str) -> str:
    t = local_ticker.strip().upper()
    if pais == "Reino Unido":
        t = t.replace('.', '-').rstrip('-')
    elif pais == "Suecia":
        t = t.replace('_', '-')
    return t

def scrape_country(spec: Dict) -> pd.DataFrame:
    html = fetch_html(spec["components_url"])
    pairs = extract_rows_precise(html, spec["accept_exchanges"])

    # Validación estricta de cantidad
    if len(pairs) != spec["expected_count"]:
        raise RuntimeError(f"{spec['index']} ({spec['pais']}): se extrajeron {len(pairs)} filas, se esperaban {spec['expected_count']}.")

    # Construye filas finales
    rows = []
    for exch, sym, name in pairs:
        base_for_yahoo = yahoo_ticker_from_local(sym, spec["pais"])
        yahoo = f"{base_for_yahoo}{spec['yahoo_suffix']}"
        rows.append({
            "TICKER_YAHOO": yahoo,
            "NOMBRE": clean_company_name(name, sym),
            "PAIS": spec["pais"],
            "TICKET": base_for_yahoo
        })
        time.sleep(0.05)  # cortesía (mínima, ya no abrimos páginas individuales)

    df = pd.DataFrame(rows, columns=["TICKER_YAHOO", "NOMBRE", "PAIS", "TICKET"])
    return df

# ========= CARGA EN SNOWFLAKE (TRUNCATE + INSERT) =========
def df_to_rows(df: pd.DataFrame):
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
        print(f" - {spec['pais']}: {len(df_country)} tickers (OK)")
        frames.append(df_country)

    full_df = pd.concat(frames, ignore_index=True).rename(columns=str.upper)
    # Dedup por Yahoo por seguridad (debería ser 0 si los conteos son exactos)
    full_df = full_df.drop_duplicates(subset=["TICKER_YAHOO"]).reset_index(drop=True)

    if full_df.empty:
        raise RuntimeError("No se obtuvieron símbolos válidos; abortando.")

    print("Muestra:", full_df.head(8).to_dict(orient="records"))
    overwrite_snowflake_sql(full_df, SNOWFLAKE_CONFIG)
    print("¡Hecho!")
