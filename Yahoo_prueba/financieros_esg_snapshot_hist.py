# -*- coding: utf-8 -*-
# pip install "pandas>=2.0,<2.3" "snowflake-connector-python[pandas]>=3.5.0" pyarrow yfinance

import os, time, concurrent.futures
from datetime import datetime
from zoneinfo import ZoneInfo
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
ESG_HIST_TABLE = os.environ.get("ESG_HIST_TABLE", f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.FIN_ESG_SNAPSHOT_HIST")

MAX_WORKERS = int(os.environ.get("ESG_MAX_WORKERS", "8"))
SLEEP_SECS  = float(os.environ.get("ESG_SLEEP", "0.05"))

TZ = ZoneInfo("Europe/Madrid")

COLS = ["TICKER","SNAPSHOT_DATE","HAS_ESG","TOTAL_ESG","ENVIRONMENTAL","SOCIAL","GOVERNANCE","CONTROVERSY"]

# ====== Snowflake helpers ======
def sf_connect():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER, password=SNOWFLAKE_PASSWORD, account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE, database=SNOWFLAKE_DATABASE, schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )

def ensure_table(conn):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {ESG_HIST_TABLE} (
      TICKER        STRING,
      SNAPSHOT_DATE DATE,
      HAS_ESG       BOOLEAN,
      TOTAL_ESG     FLOAT,
      ENVIRONMENTAL FLOAT,
      SOCIAL        FLOAT,
      GOVERNANCE    FLOAT,
      CONTROVERSY   FLOAT,
      LOADED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """
    with conn.cursor() as cur:
        cur.execute(ddl)

def read_tickers(conn):
    with conn.cursor() as cur:
        cur.execute(f"SELECT TICKER_YAHOO FROM {TICKERS_TABLE} ORDER BY 1")
        return [str(r[0]).strip().upper() for r in cur.fetchall() if r[0]]

# ====== yfinance ESG (robusto) ======
ESG_KEYS = {
    "totalEsg": "TOTAL_ESG",
    "environmentScore": "ENVIRONMENTAL",
    "socialScore": "SOCIAL",
    "governanceScore": "GOVERNANCE",
    "highestControversy": "CONTROVERSY",
}

def _get_from_any_shape(df: pd.DataFrame, key: str):
    if df is None or df.empty:
        return None
    try:
        if key in df.index and df.shape[1] >= 1:
            return pd.to_numeric(df.iloc[df.index.get_loc(key), 0], errors="coerce")
    except Exception:
        pass
    try:
        t = df.T
        if key in t.columns:
            return pd.to_numeric(t.iloc[0][key], errors="coerce")
    except Exception:
        pass
    try:
        if hasattr(df, "get"):
            return pd.to_numeric(df.get(key), errors="coerce")
    except Exception:
        pass
    return None

def fetch_esg_one(ticker: str) -> dict:
    tk = yf.Ticker(ticker)
    try:
        sust = tk.sustainability
    except Exception:
        sust = None

    row = {"TICKER": ticker, "HAS_ESG": False,
           "TOTAL_ESG": None, "ENVIRONMENTAL": None, "SOCIAL": None, "GOVERNANCE": None, "CONTROVERSY": None}

    if sust is not None and not sust.empty:
        any_val = False
        for k_src, k_dst in ESG_KEYS.items():
            v = _get_from_any_shape(sust, k_src)
            row[k_dst] = float(v) if v is not None else None
            any_val = any_val or (v is not None)
        row["HAS_ESG"] = bool(any_val)
    return row

def upsert_hist(conn, df: pd.DataFrame, snap_date):
    if df is None or df.empty:
        print("No hay filas para subir (ESG hist).")
        return

    df = df.copy()
    df["SNAPSHOT_DATE"] = snap_date
    df = df.reindex(columns=COLS)

    # Cast numérico donde aplica
    for c in ["TOTAL_ESG","ENVIRONMENTAL","SOCIAL","GOVERNANCE","CONTROVERSY"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    with conn.cursor() as cur:
        cur.execute(f"CREATE OR REPLACE TEMP TABLE TMP_ESG_HIST LIKE {ESG_HIST_TABLE}")
    ok, _, nrows, _ = write_pandas(conn, df, table_name="TMP_ESG_HIST", quote_identifiers=False)
    if not ok:
        raise RuntimeError("write_pandas falló al cargar TMP_ESG_HIST.")

    merge_sql = f"""
    MERGE INTO {ESG_HIST_TABLE} t
    USING TMP_ESG_HIST s
      ON t.TICKER = s.TICKER AND t.SNAPSHOT_DATE = s.SNAPSHOT_DATE
    WHEN MATCHED THEN UPDATE SET
      t.HAS_ESG = s.HAS_ESG,
      t.TOTAL_ESG = s.TOTAL_ESG,
      t.ENVIRONMENTAL = s.ENVIRONMENTAL,
      t.SOCIAL = s.SOCIAL,
      t.GOVERNANCE = s.GOVERNANCE,
      t.CONTROVERSY = s.CONTROVERSY,
      t.LOADED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (TICKER, SNAPSHOT_DATE, HAS_ESG, TOTAL_ESG, ENVIRONMENTAL, SOCIAL, GOVERNANCE, CONTROVERSY)
      VALUES (s.TICKER, s.SNAPSHOT_DATE, s.HAS_ESG, s.TOTAL_ESG, s.ENVIRONMENTAL, s.SOCIAL, s.GOVERNANCE, s.CONTROVERSY)
    """
    with conn.cursor() as cur:
        cur.execute(merge_sql)
        conn.commit()
    print(f"Upsert FIN_ESG_SNAPSHOT_HIST: {nrows} filas")

if __name__ == "__main__":
    snap_date = datetime.now(TZ).date()  # fecha local Madrid
    conn = sf_connect()
    try:
        ensure_table(conn)
        tickers = read_tickers(conn)
        print(f"Tickers: {len(tickers)}  |  SNAPSHOT_DATE: {snap_date}")

        rows = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            for row in ex.map(fetch_esg_one, tickers):
                rows.append(row)
                time.sleep(SLEEP_SECS)

        df = pd.DataFrame(rows)
        upsert_hist(conn, df, snap_date)
        print("✅ ESG snapshot histórico actualizado.")
    finally:
        conn.close()
