import re
import time
import requests
import pandas as pd
from typing import List, Dict, Set
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ==== CONFIGURA TU CONEXIÓN SNOWFLAKE (usa la tuya existente) ====
SNOWFLAKE_CONFIG = {
    'user': 'tfmgrupo4',
    'password': 'TFMgrupo4ucm01_01#',
    'account': 'WYNIFVB-YE01854',
    'warehouse': 'TFM_WH',
    'database': 'YAHOO_PRUEBA',
    'schema': 'IBEX',
    'table': 'LISTA_IBEX_35'
}

# ==== ESPECIFICACIONES POR ÍNDICE/PAÍS ====
INDEX_SPECS = [
    # España
    dict(pais="España", index="IBEX 35",
         components_url="https://www.tradingview.com/symbols/BME-IBC/components/",
         accept_exchanges={"BME"}, tv_stock_exchange="BME", yahoo_suffix=".MC", min_count=30),

    # Alemania
    dict(pais="Alemania", index="DAX 40",
         components_url="https://www.tradingview.com/symbols/XETR-DAX/components/",
         accept_exchanges={"XETR"}, tv_stock_exchange="XETR", yahoo_suffix=".DE", min_count=30),

    # Francia
    dict(pais="Francia", index="CAC 40",
         components_url="https://www.tradingview.com/symbols/EURONEXT-PX1/components/",
         accept_exchanges={"EURONEXT"}, tv_stock_exchange="EURONEXT", yahoo_suffix=".PA", min_count=30),

    # Italia
    dict(pais="Italia", index="FTSE MIB",
         components_url="https://www.tradingview.com/symbols/INDEX-FTSEMIB/components/",
         accept_exchanges={"MIL"}, tv_stock_exchange="MIL", yahoo_suffix=".MI", min_count=30),

    # Países Bajos
    dict(pais="Países Bajos", index="AEX",
         components_url="https://www.tradingview.com/symbols/EURONEXT-AEX/components/",
         accept_exchanges={"EURONEXT"}, tv_stock_exchange="EURONEXT", yahoo_suffix=".AS", min_count=20),

    # Reino Unido
    dict(pais="Reino Unido", index="FTSE 100",
         components_url="https://www.tradingview.com/symbols/FTSE-UKX/components/",
         accept_exchanges={"LSE"}, tv_stock_exchange="LSE", yahoo_suffix=".L", min_count=90),

    # Suecia
    dict(pais="Suecia", index="OMXS30",
         components_url="https://www.tradingview.com/symbols/NASDAQ-OMXS30/components/",
         accept_exchanges={"OMXSTO"}, tv_stock_exchange="OMXSTO", yahoo_suffix=".ST", min_count=25),

    # Suiza
    dict(pais="Suiza", index="SMI",
         components_url="https://www.tradingview.com/symbols/SIX-SMI/components/",
         accept_exchanges={"SIX"}, tv_stock_exchange="SIX", yahoo_suffix=".SW", min_count=18),
]

# ==== HTTP SESSION ====
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0 Safari/537.36"
})
TIMEOUT = 25

# ==== HELPERS ====
def fetch_html(url: str) -> str:
    r = SESSION.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text

SYMBOL_LINK_RE = re.compile(r'/symbols/([A-Z]+)-([A-Z0-9_.-]{1,20})/')
SYMBOL_TEXT_RE = re.compile(r'\b([A-Z]+):([A-Z0-9_]{1,20})\b')

def extract_tickers_from_components(html: str, accept_exchanges: Set[str]) -> List[str]:
    found = set()

    # 1) Enlaces del tipo /symbols/EXCH-TICKER/
    for exch, ticker in SYMBOL_LINK_RE.findall(html):
        if exch in accept_exchanges:
            found.add(ticker)

    # 2) Textos del tipo EXCH:TICKER
    for exch, ticker in SYMBOL_TEXT_RE.findall(html):
        if exch in accept_exchanges:
            found.add(ticker)

    # Limpieza rápida de casos no válidos
    bad_like = {"INDEX", "FX", "TVC"}
    found = {t for t in found if t.upper() not in bad_like}

    return sorted(found)

H1_RE = re.compile(r'<h1[^>]*>\s*(.*?)\s*</h1>', re.S)

def extract_company_name_from_tv(exchange: str, ticker_for_tv_path: str) -> str:
    """
    Lee el <h1> de la página del valor en TradingView, p.ej.:
    https://www.tradingview.com/symbols/LSE-BP./
    """
    url = f"https://www.tradingview.com/symbols/{exchange}-{ticker_for_tv_path}/"
    try:
        html = fetch_html(url)
        m = H1_RE.search(html)
        if m:
            # Quita etiquetas internas si las hubiera
            name = re.sub(r'<[^>]+>', '', m.group(1)).strip()
            if name:
                return name
    except Exception:
        pass
    return ticker_for_tv_path  # Fallback

def yahoo_ticker_from_local(local_ticker: str, pais: str) -> str:
    t = local_ticker.strip().upper()

    # Normalizaciones por país
    if pais == "Reino Unido":
        # En LSE, TradingView usa puntos para series/clases (p.ej. RR., BT.A)
        # En Yahoo: RR.L, BT-A.L -> regla: '.' -> '-' y si termina en '-', quítalo.
        t = t.replace('.', '-').rstrip('-')
    elif pais == "Suecia":
        # En OMXSTO se usa INVE_B, HM_B, ... -> en Yahoo se usa guion
        t = t.replace('_', '-')

    # Devuelve el ticker "base", el caller añadirá el suffix
    return t

def scrape_country(spec: Dict) -> pd.DataFrame:
    html = fetch_html(spec["components_url"])
    tickers = extract_tickers_from_components(html, spec["accept_exchanges"])

    if len(tickers) < spec["min_count"]:
        raise RuntimeError(f"{spec['index']} ({spec['pais']}): se extrajeron {len(tickers)} "
                           f"tickers, menos de lo esperado ({spec['min_count']}). "
                           f"Revisa la URL o los patrones.")

    rows = []
    for tk in tickers:
        # Para pedir el nombre en TradingView, debemos usar el ticker tal cual aparece en el path.
        tv_ticker_for_path = tk

        # Para Yahoo aplicamos normalización por país
        base_for_yahoo = yahoo_ticker_from_local(tk, spec["pais"])
        yahoo = f"{base_for_yahoo}{spec['yahoo_suffix']}"

        nombre = extract_company_name_from_tv(spec["tv_stock_exchange"], tv_ticker_for_path)
        rows.append({
            "Ticker_yahoo": yahoo,
            "Nombre": nombre,
            "Pais": spec["pais"],
            "Ticket": base_for_yahoo  # local, sin suffix de Yahoo
        })

        # Respeto a TradingView (pequeña pausa)
        time.sleep(0.15)

    df = pd.DataFrame(rows, columns=["TICKER_YAHOO", "NOMBRE", "PAIS", "TICKET"])
    return df

def overwrite_snowflake(df: pd.DataFrame, cfg: Dict):
    conn = snowflake.connector.connect(
        user=cfg["user"],
        password=cfg["password"],
        account=cfg["account"],
        warehouse=cfg["warehouse"],
        database=cfg["database"],
        schema=cfg["schema"],
        quote_identifiers=False
    )
    try:
        fq_table = f'{cfg["database"]}.{cfg["schema"]}.{cfg["table"]}'
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {fq_table}")
        ok, nchunks, nrows, _ = write_pandas(
            conn, df,
            table_name=cfg["table"],
            database=cfg["database"],
            schema=cfg["schema"]
        )
        if not ok:
            raise RuntimeError("write_pandas devolvió ok=False")
        print(f"Subidos {nrows} registros a {fq_table} en {nchunks} chunk(s).")
    finally:
        conn.close()

if __name__ == "__main__":
    all_frames = []
    for spec in INDEX_SPECS:
        print(f"Raspando {spec['index']} ({spec['pais']}) ...")
        df = scrape_country(spec)
        print(f" - {spec['pais']}: {len(df)} tickers")
        all_frames.append(df)

    full_df = pd.concat(all_frames, ignore_index=True)
    # Quita posibles duplicados exactos por seguridad
    full_df = full_df.drop_duplicates(subset=["Ticker_yahoo"]).reset_index(drop=True)

    # 2) Justo antes de subir todo a Snowflake (por si acaso):
    full_df.columns = [c.upper() for c in full_df.columns]


    print(f"TOTAL filas a subir: {len(full_df)}")
    overwrite_snowflake(full_df, SNOWFLAKE_CONFIG)
    print("¡Hecho!")
