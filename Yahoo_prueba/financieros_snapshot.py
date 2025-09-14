# -*- coding: utf-8 -*-
# pip install "pandas>=2.0,<2.3" "snowflake-connector-python[pandas]>=3.5.0" pyarrow yfinance

import os, time, math, concurrent.futures
import pandas as pd
import yfinance as yf
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ====== ENV / secrets ======
SNOWFLAKE_USER      = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
SNOWFLAKE_DATABASE  = os.environ["SNOWFLAKE_DATABASE"]
SNOWFLAKE_SCHEMA    = os.environ["SNOWFLAKE_SCHEMA"]
SNOWFLAKE_ROLE      = os.environ.get("SNOWFLAKE_ROLE")

TICKERS_TABLE  = os.environ.get("TICKERS_TABLE", f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.LISTA_IBEX_35")
SNAPSHOT_TABLE = os.environ.get("SNAPSHOT_TABLE", f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.FIN_MARKET_SNAPSHOT")
MAX_WORKERS    = int(os.environ.get("SNAPSHOT_MAX_WORKERS", "8"))
SLEEP_SECS     = float(os.environ.get("SNAPSHOT_SLEEP", "0.05"))

COLS = [
    "TICKER",
    "PE_TRAILING","PE_FORWARD","PRICE_TO_BOOK","EV_TO_EBITDA",
    "DIVIDEND_YIELD","PAYOUT_RATIO","MARKET_CAP","ENTERPRISE_VALUE","SHARES_OUTSTANDING",
]

# ====== Snowflake helpers ======
def sf_connect():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER, password=SNOWFLAKE_PASSWORD, account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE, database=SNOWFLAKE_DATABASE, schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )

def ensure_table(conn):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {SNAPSHOT_TABLE} (
      TICKER             STRING,
      PE_TRAILING        FLOAT,
      PE_FORWARD         FLOAT,
      PRICE_TO_BOOK      FLOAT,
      EV_TO_EBITDA       FLOAT,
      DIVIDEND_YIELD     FLOAT,
      PAYOUT_RATIO       FLOAT,
      MARKET_CAP         FLOAT,
      ENTERPRISE_VALUE   FLOAT,
      SHARES_OUTSTANDING FLOAT
    )
    """
    with conn.cursor() as cur:
        cur.execute(ddl)

def read_tickers(conn):
    with conn.cursor() as cur:
        cur.execute(f"SELECT TICKER_YAHOO FROM {TICKERS_TABLE} ORDER BY 1")
        return [str(r[0]).strip().upper() for r in cur.fetchall() if r[0]]

# ====== yfinance snapshot (robusto) ======
def _latest_ebitda_from_financials(tk: yf.Ticker):
    # intenta EBITDA de financials (anual), toma la columna más reciente
    try:
        fin = tk.financials
        if fin is not None and not fin.empty:
            for key in ("Ebitda","EBITDA"):
                if key in fin.index:
                    s = fin.loc[key]
                    try:
                        return float(pd.to_numeric(s.iloc[0], errors="coerce"))
                    except Exception:
                        for v in s:
                            val = pd.to_numeric(v, errors="coerce")
                            if pd.notna(val):
                                return float(val)
    except Exception:
        pass
    return None

# --8<-- [start:fetch_snapshot_one]
def fetch_snapshot_one(ticker: str, retries: int = 3, pause: float = 0.6) -> dict:
    for i in range(retries):
        try:
            tk = yf.Ticker(ticker)
            # prefer get_info() (más estable en versiones recientes); fallback a .info
            info = {}
            try:
                info = tk.get_info() or {}
            except Exception:
                info = tk.info or {}

            pe_trailing   = info.get("trailingPE")
            pe_forward    = info.get("forwardPE")
            price_to_book = info.get("priceToBook")
            ev_to_ebitda  = info.get("enterpriseToEbitda")
            dividend_yld  = info.get("dividendYield")
            payout_ratio  = info.get("payoutRatio")
            market_cap    = info.get("marketCap")
            enterprise_v  = info.get("enterpriseValue")
            shares_out    = info.get("sharesOutstanding")

            # Si falta EV/EBITDA, calcula con enterpriseValue y EBITDA reciente
            if ev_to_ebitda in (None, 0, float("inf")):
                ebitda = info.get("ebitda")
                if ebitda is None:
                    ebitda = _latest_ebitda_from_financials(tk)
                if enterprise_v is not None and ebitda not in (None, 0):
                    ev_to_ebitda = float(enterprise_v) / float(ebitda)

            row = {
                "TICKER": ticker,
                "PE_TRAILING": pe_trailing,
                "PE_FORWARD": pe_forward,
                "PRICE_TO_BOOK": price_to_book,
                "EV_TO_EBITDA": ev_to_ebitda,
                "DIVIDEND_YIELD": dividend_yld,
                "PAYOUT_RATIO": payout_ratio,
                "MARKET_CAP": market_cap,
                "ENTERPRISE_VALUE": enterprise_v,
                "SHARES_OUTSTANDING": shares_out,
            }
            return row
        except Exception:
            if i < retries - 1:
                time.sleep(pause * (i + 1))
            else:
                # devuelve fila con NULOS si no se pudo obtener
                return {
                    "TICKER": ticker,
                    "PE_TRAILING": None,
                    "PE_FORWARD": None,
                    "PRICE_TO_BOOK": None,
                    "EV_TO_EBITDA": None,
                    "DIVIDEND_YIELD": None,
                    "PAYOUT_RATIO": None,
                    "MARKET_CAP": None,
                    "ENTERPRISE_VALUE": None,
                    "SHARES_OUTSTANDING": None,
                }
# --8<-- [end:fetch_snapshot_one]

# ====== Upsert ======
def upsert_snapshot(conn, df: pd.DataFrame):
    if df is None or df.empty:
        print("No hay filas para subir (snapshot).")
        return
    df = df.reindex(columns=COLS)
    # Cast numérico donde aplica
    for c in COLS:
        if c != "TICKER":
            df[c] = pd.to_numeric(df[c], errors="coerce")

    with conn.cursor() as cur:
        cur.execute(f"CREATE OR REPLACE TEMP TABLE TMP_SNAPSHOT LIKE {SNAPSHOT_TABLE}")
    ok, _, nrows, _ = write_pandas(conn, df, table_name="TMP_SNAPSHOT", quote_identifiers=False)
    if not ok:
        raise RuntimeError("write_pandas falló al cargar TMP_SNAPSHOT.")

    merge_sql = f"""
    MERGE INTO {SNAPSHOT_TABLE} t
    USING TMP_SNAPSHOT s
      ON t.TICKER = s.TICKER
    WHEN MATCHED THEN UPDATE SET
      t.PE_TRAILING = s.PE_TRAILING,
      t.PE_FORWARD = s.PE_FORWARD,
      t.PRICE_TO_BOOK = s.PRICE_TO_BOOK,
      t.EV_TO_EBITDA = s.EV_TO_EBITDA,
      t.DIVIDEND_YIELD = s.DIVIDEND_YIELD,
      t.PAYOUT_RATIO = s.PAYOUT_RATIO,
      t.MARKET_CAP = s.MARKET_CAP,
      t.ENTERPRISE_VALUE = s.ENTERPRISE_VALUE,
      t.SHARES_OUTSTANDING = s.SHARES_OUTSTANDING
    WHEN NOT MATCHED THEN
      INSERT (TICKER, PE_TRAILING, PE_FORWARD, PRICE_TO_BOOK, EV_TO_EBITDA,
              DIVIDEND_YIELD, PAYOUT_RATIO, MARKET_CAP, ENTERPRISE_VALUE, SHARES_OUTSTANDING)
      VALUES (s.TICKER, s.PE_TRAILING, s.PE_FORWARD, s.PRICE_TO_BOOK, s.EV_TO_EBITDA,
              s.DIVIDEND_YIELD, s.PAYOUT_RATIO, s.MARKET_CAP, s.ENTERPRISE_VALUE, s.SHARES_OUTSTANDING)
    """
    with conn.cursor() as cur:
        cur.execute(merge_sql)
        conn.commit()
    print(f"Upsert FIN_MARKET_SNAPSHOT: {nrows} filas")

# ====== MAIN ======
if __name__ == "__main__":
    conn = sf_connect()
    try:
        ensure_table(conn)
        tickers = read_tickers(conn)
        print("Tickers:", len(tickers))

        rows = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            for row in ex.map(fetch_snapshot_one, tickers):
                rows.append(row)
                time.sleep(SLEEP_SECS)

        df = pd.DataFrame(rows).reindex(columns=COLS)
        upsert_snapshot(conn, df)
        print("✅ Snapshot de indicadores de mercado actualizado.")
    finally:
        conn.close()
