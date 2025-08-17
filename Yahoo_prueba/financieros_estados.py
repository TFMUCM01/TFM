# -*- coding: utf-8 -*-
# Dependencias:
#   pip install "pandas>=2.0,<2.3" "snowflake-connector-python[pandas]>=3.5.0" pyarrow
#   pip install yfinance requests beautifulsoup4 lxml

import os, time, re
from typing import List, Iterable
import pandas as pd
import yfinance as yf
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ===== Config via secrets/variables de entorno =====
SNOWFLAKE_USER      = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
SNOWFLAKE_DATABASE  = os.environ["SNOWFLAKE_DATABASE"]
SNOWFLAKE_SCHEMA    = os.environ["SNOWFLAKE_SCHEMA"]
SNOWFLAKE_ROLE      = os.environ.get("SNOWFLAKE_ROLE")

TICKERS_TABLE = os.environ.get("TICKERS_TABLE", f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.LISTA_IBEX_35")
INCOME_TABLE  = os.environ.get("INCOME_TABLE",  f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.FIN_INCOME")
BAL_TABLE     = os.environ.get("BALANCE_TABLE", f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.FIN_BALANCE")
CF_TABLE      = os.environ.get("CASHFLOW_TABLE",f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.FIN_CASHFLOW")

BATCH_TICKERS = int(os.environ.get("FIN_BATCH_TICKERS", "40"))  # nº de tickers por lote

# ============ Conexión Snowflake ============
def sf_connect():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER, password=SNOWFLAKE_PASSWORD, account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE, database=SNOWFLAKE_DATABASE, schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )

def ensure_table(conn, table_name: str):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
      TICKER     STRING,
      PERIOD     DATE,
      FREQUENCY  STRING,   -- 'A' anual
      METRIC     STRING,
      VALUE      FLOAT
    )
    """
    with conn.cursor() as cur:
        cur.execute(ddl)

def read_tickers(conn) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT TICKER_YAHOO FROM {TICKERS_TABLE} ORDER BY 1")
        return [str(r[0]).strip().upper() for r in cur.fetchall() if r[0]]

def chunked(seq: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

# ============ Normalización/Transformación ============
METRIC_CLEAN_RE = re.compile(r"[^A-Z0-9]+")

def norm_metric(s: str) -> str:
    s = (s or "").upper()
    s = METRIC_CLEAN_RE.sub("_", s).strip("_")
    return s

def tidy_statement(df: pd.DataFrame, ticker: str, freq: str) -> pd.DataFrame:
    """
    yfinance: filas = métricas, columnas = periodos.
    Devuelve: TICKER, PERIOD(date), FREQUENCY, METRIC, VALUE
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["TICKER","PERIOD","FREQUENCY","METRIC","VALUE"])

    df = df.copy()
    df.index = df.index.astype(str)

    # columnas → fechas válidas
    valid_cols = []
    for c in df.columns:
        d = pd.to_datetime(str(c), errors="coerce")
        if pd.notna(d):
            valid_cols.append(c)
    if not valid_cols:
        return pd.DataFrame(columns=["TICKER","PERIOD","FREQUENCY","METRIC","VALUE"])

    long = df[valid_cols].stack().reset_index()
    long.columns = ["METRIC","PERIOD","VALUE"]
    long["PERIOD"] = pd.to_datetime(long["PERIOD"], errors="coerce").dt.date
    long = long[long["PERIOD"].notna()]
    long["METRIC"] = long["METRIC"].map(norm_metric)
    long = long[pd.to_numeric(long["VALUE"], errors="coerce").notna()]
    long["VALUE"] = long["VALUE"].astype(float)
    long["TICKER"] = ticker
    long["FREQUENCY"] = freq
    return long[["TICKER","PERIOD","FREQUENCY","METRIC","VALUE"]]

# ============ Descarga solo ANUAL ============
def get_income_annual(ticker: str) -> pd.DataFrame:
    try:
        return tidy_statement(yf.Ticker(ticker).financials, ticker, "A")
    except Exception:
        return pd.DataFrame(columns=["TICKER","PERIOD","FREQUENCY","METRIC","VALUE"])

def get_balance_annual(ticker: str) -> pd.DataFrame:
    try:
        return tidy_statement(yf.Ticker(ticker).balance_sheet, ticker, "A")
    except Exception:
        return pd.DataFrame(columns=["TICKER","PERIOD","FREQUENCY","METRIC","VALUE"])

def get_cashflow_annual(ticker: str) -> pd.DataFrame:
    try:
        return tidy_statement(yf.Ticker(ticker).cashflow, ticker, "A")
    except Exception:
        return pd.DataFrame(columns=["TICKER","PERIOD","FREQUENCY","METRIC","VALUE"])

def upsert_long_table(conn, df: pd.DataFrame, table_name: str):
    """Carga df a TEMP TABLE con write_pandas y MERGE en (TICKER, PERIOD, FREQUENCY, METRIC)."""
    if df is None or df.empty:
        return
    df = df[["TICKER","PERIOD","FREQUENCY","METRIC","VALUE"]].copy()
    df["PERIOD"] = pd.to_datetime(df["PERIOD"]).dt.date
    df["VALUE"]  = pd.to_numeric(df["VALUE"], errors="coerce")

    with conn.cursor() as cur:
        cur.execute(f"CREATE OR REPLACE TEMP TABLE TMP_FS LIKE {table_name}")
    ok, _, nrows, _ = write_pandas(conn, df, table_name="TMP_FS", quote_identifiers=False)
    if not ok:
        raise RuntimeError(f"write_pandas falló al cargar TMP_FS para {table_name}.")

    merge_sql = f"""
    MERGE INTO {table_name} t
    USING TMP_FS s
      ON  t.TICKER = s.TICKER
      AND t.PERIOD = s.PERIOD
      AND t.FREQUENCY = s.FREQUENCY
      AND t.METRIC = s.METRIC
    WHEN MATCHED THEN UPDATE SET
      t.VALUE = s.VALUE
    WHEN NOT MATCHED THEN
      INSERT (TICKER, PERIOD, FREQUENCY, METRIC, VALUE)
      VALUES (s.TICKER, s.PERIOD, s.FREQUENCY, s.METRIC, s.VALUE)
    """
    with conn.cursor() as cur:
        cur.execute(merge_sql)
        conn.commit()
    print(f"Upsert {table_name}: {nrows} filas")

# ============ MAIN ============
if __name__ == "__main__":
    conn = sf_connect()
    try:
        # Asegura tablas destino
        for tbl in (INCOME_TABLE, BAL_TABLE, CF_TABLE):
            ensure_table(conn, tbl)

        tickers = read_tickers(conn)
        print("Tickers para estados financieros (solo anual):", len(tickers))

        for batch in chunked(tickers, BATCH_TICKERS):
            print(f"Lote de {len(batch)} tickers...")
            inc_rows, bal_rows, cf_rows = [], [], []

            for tck in batch:
                inc = get_income_annual(tck)
                if not inc.empty: inc_rows.append(inc)
                bal = get_balance_annual(tck)
                if not bal.empty: bal_rows.append(bal)
                cf  = get_cashflow_annual(tck)
                if not cf.empty: cf_rows.append(cf)
                time.sleep(0.05)  # cortesía mínima a Yahoo

            if inc_rows:
                upsert_long_table(conn, pd.concat(inc_rows, ignore_index=True), INCOME_TABLE)
            if bal_rows:
                upsert_long_table(conn, pd.concat(bal_rows, ignore_index=True), BAL_TABLE)
            if cf_rows:
                upsert_long_table(conn, pd.concat(cf_rows, ignore_index=True), CF_TABLE)

        print("✅ Estados financieros ANUALES actualizados (sin exclusiones).")
    finally:
        conn.close()
