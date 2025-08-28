# -*- coding: utf-8 -*-
# pip install requests pandas snowflake-connector-python beautifulsoup4 lxml yfinance

import os, re, time
from typing import List, Dict, Set, Tuple, Iterable
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

import requests
import pandas as pd
from bs4 import BeautifulSoup
import snowflake.connector
import yfinance as yf

# =======================
# Config por variables de entorno (secrets)
# Requeridas: SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT,
#             SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
# Opcionales: SNOWFLAKE_ROLE, TICKERS_TABLE, PRICES_TABLE, START_DATE
# =======================
SNOWFLAKE_USER      = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]          # ej: WYNIFVB-YE01854 (o con región)
SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
SNOWFLAKE_DATABASE  = os.environ["SNOWFLAKE_DATABASE"]         # ej: YAHOO_PRUEBA
SNOWFLAKE_SCHEMA    = os.environ["SNOWFLAKE_SCHEMA"]           # ej: IBEX
SNOWFLAKE_ROLE      = os.environ.get("SNOWFLAKE_ROLE")         # opcional

TICKERS_TABLE = os.environ.get("TICKERS_TABLE", f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.LISTA_IBEX_35")
PRICES_TABLE  = os.environ.get("PRICES_TABLE",  f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.TICKERS_INDEX")
START_DATE    = pd.to_datetime(os.environ.get("START_DATE", "2020-01-01")).date()

TZ = ZoneInfo("Europe/Madrid")

# ============ ÍNDICES A RASTREAR (conteo exacto) ============

# --8<-- [start:index]
INDEX_SPECS = [
    dict(pais="España",        index="IBEX 35",  components_url="https://www.tradingview.com/symbols/BME-IBC/components/",       accept_exchanges={"BME"},     yahoo_suffix=".MC", expected_count=35),
    dict(pais="Alemania",      index="DAX 40",   components_url="https://www.tradingview.com/symbols/XETR-DAX/components/",      accept_exchanges={"XETR"},    yahoo_suffix=".DE", expected_count=40),
    dict(pais="Francia",       index="CAC 40",   components_url="https://www.tradingview.com/symbols/EURONEXT-PX1/components/",  accept_exchanges={"EURONEXT"},yahoo_suffix=".PA", expected_count=39),
    dict(pais="Italia",        index="FTSE MIB", components_url="https://www.tradingview.com/symbols/INDEX-FTSEMIB/components/", accept_exchanges={"MIL"},     yahoo_suffix=".MI", expected_count=40),
    dict(pais="Países Bajos",  index="AEX",      components_url="https://www.tradingview.com/symbols/EURONEXT-AEX/components/",  accept_exchanges={"EURONEXT"},yahoo_suffix=".AS", expected_count=25),
    dict(pais="Reino Unido",   index="FTSE 100", components_url="https://www.tradingview.com/symbols/FTSE-UKX/components/",      accept_exchanges={"LSE"},     yahoo_suffix=".L",  expected_count=100),
    dict(pais="Suecia",        index="OMXS30",   components_url="https://www.tradingview.com/symbols/NASDAQ-OMXS30/components/", accept_exchanges={"OMXSTO"},  yahoo_suffix=".ST", expected_count=30),
    dict(pais="Suiza",         index="SMI",      components_url="https://www.tradingview.com/symbols/SIX-SMI/components/",        accept_exchanges={"SIX"},     yahoo_suffix=".SW", expected_count=21),
]
# --8<-- [end:index]
# ============ HTTP ============
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36")
})
TIMEOUT = 25

HREF_SYMBOL_RE = re.compile(r'/symbols/([A-Z]+)-([A-Z0-9_.-]{1,20})/?$', re.I)
INVALID_KEYWORDS = (
    " INDEX", " ÍNDICE", " ETF", " ETN", " ETC",
    " FUTURE", " FUTURO", " FUTURES",
    " CFD", " FOREX", " CURRENCY", " CRYPTO",
    " BOND", " YIELD", " WARRANT", " OPTION"
)

# ----------------- Utilidades scraping -----------------
def fetch_html(url: str) -> str:
    r = SESSION.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text

def class_contains(tag, fragment: str) -> bool:
    cls = tag.get("class", [])
    if isinstance(cls, str):
        cls = cls.split()
    return any(fragment in c for c in cls)

# --8<-- [start:extract_rows_precise]
def extract_rows_precise(html: str, accept_exchanges: Set[str]) -> List[Tuple[str, str, str]]:
    soup = BeautifulSoup(html, "lxml")
    desc_nodes = soup.find_all("sup", class_=lambda v: v and "tickerDescription-" in " ".join(v if isinstance(v, list) else v.split()))
    rows = []
    for sup in desc_nodes:
        name = sup.get_text(strip=True)
        if not name:
            continue
        row = sup
        for _ in range(8):
            row = row.parent
            if row is None:
                break
            if class_contains(row, "row-"):
                a = row.find("a", href=True)
                if a:
                    m = HREF_SYMBOL_RE.search(a["href"])
                    if m:
                        exch = m.group(1).upper()
                        sym  = m.group(2)
                        if exch in accept_exchanges:
                            rows.append((exch, sym, name))
                break
    # Limpieza y filtro de índices/ETF
    out = {}
    for exch, sym, name in rows:
        if any(kw in name.upper() for kw in INVALID_KEYWORDS):
            continue
        out[(exch, sym)] = name
    return [(ex, sy, nm) for (ex, sy), nm in out.items()]
# --8<-- [end:extract_rows_precise]

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
# --8<-- [start:scrape_country.py]
def scrape_country(spec: Dict) -> pd.DataFrame:
    html = fetch_html(spec["components_url"])
    pairs = extract_rows_precise(html, spec["accept_exchanges"])
    if len(pairs) != spec["expected_count"]:
        raise RuntimeError(f"{spec['index']} ({spec['pais']}): se extrajeron {len(pairs)} filas, se esperaban {spec['expected_count']}.")
    rows = []
    for exch, sym, name in pairs:
        base_for_yahoo = yahoo_ticker_from_local(sym, spec["pais"])
        yahoo = f"{base_for_yahoo}{spec['yahoo_suffix']}"
        rows.append({"TICKER_YAHOO": yahoo, "NOMBRE": clean_company_name(name, sym), "PAIS": spec["pais"], "TICKET": base_for_yahoo})
        time.sleep(0.03)
    return pd.DataFrame(rows, columns=["TICKER_YAHOO", "NOMBRE", "PAIS", "TICKET"])
# --8<-- [start:scrape_country.py]

# ----------------- Snowflake helpers -----------------
def sf_connect():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )

def ensure_db_schema(conn):
    with conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}")
        cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

def overwrite_tickers(conn, df: pd.DataFrame):
    if df.empty:
        raise ValueError("DF de tickers vacío.")
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TICKERS_TABLE} (
                TICKER_YAHOO STRING, NOMBRE STRING, PAIS STRING, TICKET STRING
            )
        """)
        cur.execute(f"TRUNCATE TABLE {TICKERS_TABLE}")
        rows = [tuple(map(lambda x: str(x).strip(), r))
                for r in df[["TICKER_YAHOO","NOMBRE","PAIS","TICKET"]].fillna("").to_numpy().tolist()]
        cur.executemany(
            f"INSERT INTO {TICKERS_TABLE} (TICKER_YAHOO,NOMBRE,PAIS,TICKET) VALUES (%s,%s,%s,%s)",
            rows
        )
        cur.execute(f"SELECT COUNT(*) FROM {TICKERS_TABLE}")
        print("Tickers en tabla:", cur.fetchone()[0])
# --8<-- [start:ensure_prices_table]
def ensure_prices_table(conn):
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {PRICES_TABLE} (
              TICKER STRING,
              CLOSE  FLOAT,
              HIGH   FLOAT,
              LOW    FLOAT,
              OPEN   FLOAT,
              VOLUME NUMBER,
              FECHA  DATE
            )
        """)
# --8<-- [end:ensure_prices_table]
def read_tickers(conn) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT TICKER_YAHOO FROM {TICKERS_TABLE} ORDER BY 1")
        return [str(r[0]).strip().upper() for r in cur.fetchall() if r[0]]

def read_last_dates(conn, tickers: List[str]) -> Dict[str, date]:
    if not tickers:
        return {}
    in_list = ",".join(f"'{t}'" for t in tickers)
    with conn.cursor() as cur:
        try:
            cur.execute(f"SELECT TICKER, MAX(FECHA) FROM {PRICES_TABLE} WHERE TICKER IN ({in_list}) GROUP BY 1")
            rows = cur.fetchall()
        except snowflake.connector.errors.ProgrammingError:
            return {}
    return {str(t).upper(): d for t, d in rows if t}

def chunked(seq: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def yesterday_madrid():
    y = (datetime.now(TZ) - timedelta(days=1)).date()
    return y, y + timedelta(days=1)

# ----------------- Descarga y MERGE de precios -----------------
# --8<-- [start:download_batch]
def download_batch(tickers: List[str], start_date, end_excl) -> pd.DataFrame:
    if not tickers:
        return pd.DataFrame(columns=["TICKER","CLOSE","HIGH","LOW","OPEN","VOLUME","FECHA"])
    df = yf.download(
        tickers, start=start_date, end=end_excl, interval="1d",
        group_by="ticker", auto_adjust=False, progress=False, threads=True
    )
    rows = []
    for t in tickers:
        try:
            if isinstance(df.columns, pd.MultiIndex):
                if t not in df.columns.get_level_values(0):
                    continue
                dft = df[t]
            else:
                if set(df.columns) & {"Open","High","Low","Close","Volume"}:
                    dft = df
                else:
                    continue
            dft = dft.reset_index().rename(columns={
                "Date":"FECHA","Open":"OPEN","High":"HIGH","Low":"LOW","Close":"CLOSE","Volume":"VOLUME"
            })
            dft["TICKER"] = t
            rows.append(dft[["TICKER","CLOSE","HIGH","LOW","OPEN","VOLUME","FECHA"]])
        except Exception:
            continue
    if not rows:
        return pd.DataFrame(columns=["TICKER","CLOSE","HIGH","LOW","OPEN","VOLUME","FECHA"])
    out = pd.concat(rows, ignore_index=True).dropna(subset=["CLOSE"])
    out["FECHA"] = pd.to_datetime(out["FECHA"]).dt.date
    for col in ["CLOSE","HIGH","LOW","OPEN"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["VOLUME"] = pd.to_numeric(out["VOLUME"], errors="coerce").astype("Int64")
    return out.dropna(subset=["CLOSE","HIGH","LOW","OPEN"])
# --8<-- [end:download_batch]

def merge_with_temp(conn, df: pd.DataFrame):
    """Inserta en chunks para evitar el límite de 200k expresiones y luego MERGE."""
    if df.empty:
        return

    insert_sql = f"""
        INSERT INTO TMP_PRICES (TICKER,CLOSE,HIGH,LOW,OPEN,VOLUME,FECHA)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """
    merge_sql = f"""
        MERGE INTO {PRICES_TABLE} t
        USING TMP_PRICES s
          ON t.TICKER = s.TICKER AND t.FECHA = s.FECHA
        WHEN MATCHED THEN UPDATE SET
          t.CLOSE = s.CLOSE, t.HIGH = s.HIGH, t.LOW = s.LOW, t.OPEN = s.OPEN, t.VOLUME = s.VOLUME
        WHEN NOT MATCHED THEN
          INSERT (TICKER, CLOSE, HIGH, LOW, OPEN, VOLUME, FECHA)
          VALUES (s.TICKER, s.CLOSE, s.HIGH, s.LOW, s.OPEN, s.VOLUME, s.FECHA)
    """

    MAX_EXPRESSIONS = 200000
    COLS = 7
    SAFETY = 1000
    BATCH_ROWS = min(25000, (MAX_EXPRESSIONS - SAFETY) // COLS)

    rows = [
        (
            str(r.TICKER),
            float(r.CLOSE),
            float(r.HIGH),
            float(r.LOW),
            float(r.OPEN),
            int(r.VOLUME) if pd.notna(r.VOLUME) else None,
            r.FECHA
        )
        for _, r in df.iterrows()
    ]

    with conn.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE TMP_PRICES LIKE {PRICES_TABLE}")

        total = len(rows)
        for i in range(0, total, BATCH_ROWS):
            chunk = rows[i:i+BATCH_ROWS]
            cur.executemany(insert_sql, chunk)

        cur.execute(merge_sql)
        conn.commit()

# ----------------- MAIN -----------------
if __name__ == "__main__":
    # 1) SCRAPE TICKERS
    frames = []
    for spec in INDEX_SPECS:
        print(f"Raspando {spec['index']} ({spec['pais']}) ...")
        df_country = scrape_country(spec)
        print(f" - {spec['pais']}: {len(df_country)} tickers (OK)")
        frames.append(df_country)
    tick_df = pd.concat(frames, ignore_index=True).rename(columns=str.upper)
    tick_df = tick_df.drop_duplicates(subset=["TICKER_YAHOO"]).reset_index(drop=True)

    # 2) CONEXIÓN SNOWFLAKE
    print("Conectando a Snowflake...")
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )

    with conn:
        ensure_db_schema(conn)

        # 3) Sobrescribe lista de tickers
        overwrite_tickers(conn, tick_df)

        # 4) Precios incrementales (2020 si no hay datos; si hay, desde max(FECHA)+1 hasta ayer)
        ensure_prices_table(conn)
        tickers = read_tickers(conn)
        last_dates = read_last_dates(conn, tickers)
        yday, end_excl = yesterday_madrid()

        # Plan por fecha de inicio
        plan: Dict[date, List[str]] = {}
        for t in tickers:
            last = last_dates.get(t)
            start = max(START_DATE, (last + timedelta(days=1)) if last else START_DATE)
            if start <= yday:
                plan.setdefault(start, []).append(t)

        total_rows = 0
        for start_date, group in sorted(plan.items()):
            print(f"Descargando {len(group)} tickers desde {start_date} → {yday}")
            for sub in chunked(group, 60):  # sub-lotes de tickers para yfinance
                part = download_batch(sub, start_date, end_excl)
                if part.empty:
                    continue
                part = part[(part["FECHA"] >= start_date) & (part["FECHA"] <= yday)]
                merge_with_temp(conn, part)   # MERGE por sub-lote (estable y sin límites)
                total_rows += len(part)

        print("✅ Filas nuevas/actualizadas:", total_rows)
