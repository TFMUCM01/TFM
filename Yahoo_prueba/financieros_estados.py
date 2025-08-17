# -*- coding: utf-8 -*-
# Dependencias:
#   pip install "pandas>=2.0,<2.3" "snowflake-connector-python[pandas]>=3.5.0" pyarrow
#   pip install yfinance requests beautifulsoup4 lxml

import os, re, time
from typing import List, Dict, Tuple, Iterable
from datetime import datetime, date
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

INCLUDE_QUARTERLY = os.environ.get("INCLUDE_QUARTERLY", "true").lower() in ("1","true","yes")
BATCH_TICKERS     = int(os.environ.get("FIN_BATCH_TICKERS", "40"))  # nº tickers por lote para descargar y subir

# ============ Conexión Snowflake ============
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

def ensure_table(conn, table_name: str):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
      TICKER     STRING,
      PERIOD     DATE,
      FREQUENCY  STRING,   -- 'A' anual, 'Q' trimestral
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
    yfinance devuelve un DataFrame con:
      - filas: métricas
      - columnas: periodos (DatetimeIndex o strings)
    Lo convertimos a largo: TICKER, PERIOD(date), FREQUENCY, METRIC, VALUE
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["TICKER","PERIOD","FREQUENCY","METRIC","VALUE"])

    df = df.copy()
    # Asegura índices/columnas manejables
    df.index = df.index.astype(str)

    # Asegura que las columnas sean fechas (date)
    periods: List[date] = []
    for c in df.columns:
        try:
            d = pd.to_datetime(c).date()
        except Exception:
            # si viene Timestamp ya sirve; si es string raro, intenta coerce
            d = pd.to_datetime(str(c), errors="coerce")
            d = d.date() if pd.notna(d) else None
        if d is None:
            # si no se puede parsear, lo saltamos
            continue
        periods.append(d)
    # mapea columnas a periods válidos
    valid_cols = [col for col in df.columns if pd.notna(pd.to_datetime(str(col), errors="coerce"))]
    if not valid_cols:
        return pd.DataFrame(columns=["TICKER","PERIOD","FREQUENCY","METRIC","VALUE"])

    # stack → largo
    long = df[valid_cols].stack().reset_index()
    long.columns = ["METRIC","PERIOD","VALUE"]

    # parse PERIOD a date
    long["PERIOD"] = pd.to_datetime(long["PERIOD"], errors="coerce").dt.date
    long = long[long["PERIOD"].notna()]
    # limpia métrica y NaNs
    long["METRIC"] = long["METRIC"].map(norm_metric)
    long = long[pd.to_numeric(long["VALUE"], errors="coerce").notna()]
    long["VALUE"] = long["VALUE"].astype(float)

    long["TICKER"] = ticker
    long["FREQUENCY"] = freq  # 'A' o 'Q'

    return long[["TICKER","PERIOD","FREQUENCY","METRIC","VALUE"]]

# ============ Descarga y upsert por reporte ============
def get_income_frames(ticker: str) -> List[pd.DataFrame]:
    t = yf.Ticker(ticker)
    frames = []
    try:
        frames.append(tidy_statement(t.financials, ticker, "A"))
    except Exception:
        pass
    if INCLUDE_QUARTERLY:
        try:
            frames.append(tidy_statement(t.quarterly_financials, ticker, "Q"))
        except Exception:
            pass
    return [f for f in frames if f is not None and not f.empty]

def get_balance_frames(ticker: str) -> List[pd.DataFrame]:
    t = yf.Ticker(ticker)
    frames = []
    try:
        frames.append(tidy_statement(t.balance_sheet, ticker, "A"))
    except Exception:
        pass
    if INCLUDE_QUARTERLY:
        try:
            frames.append(tidy_statement(t.quarterly_balance_sheet, ticker, "Q"))
        except Exception:
            pass
    return [f for f in frames if f is not None and not f.empty]

def get_cashflow_frames(ticker: str) -> List[pd.DataFrame]:
    t = yf.Ticker(ticker)
    frames = []
    try:
        frames.append(tidy_statement(t.cashflow, ticker, "A"))
    except Exception:
        pass
    if INCLUDE_QUARTERLY:
        try:
            frames.append(tidy_statement(t.quarterly_cashflow, ticker, "Q"))
        except Exception:
            pass
    return [f for f in frames if f is not None and not f.empty]

def upsert_long_table(conn, df: pd.DataFrame, table_name: str):
    """
    Carga df a TEMP TABLE con write_pandas y hace MERGE en:
      PK lógica: (TICKER, PERIOD, FREQUENCY, METRIC)
    """
    if df is None or df.empty:
        return

    # Asegura orden/columnas y tipos
    df = df[["TICKER","PERIOD","FREQUENCY","METRIC","VALUE"]].copy()
    df["PERIOD"] = pd.to_datetime(df["PERIOD"]).dt.date
    df["FREQUENCY"] = df["FREQUENCY"].astype(str)
    df["METRIC"] = df["METRIC"].astype(str)
    df["TICKER"] = df["TICKER"].astype(str)
    df["VALUE"]  = pd.to_numeric(df["VALUE"], errors="coerce")

    # TEMP y carga
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
        print("Tickers para estados financieros:", len(tickers))

        # Procesa por lotes para no saturar memoria/red
        for batch in chunked(tickers, BATCH_TICKERS):
            print(f"Procesando lote de {len(batch)} tickers...")
            inc_frames, bal_frames, cf_frames = [], [], []

            for t in batch:
                # Income
                inc_frames.extend(get_income_frames(t))
                # Balance
                bal_frames.extend(get_balance_frames(t))
                # Cash Flow
                cf_frames.extend(get_cashflow_frames(t))
                time.sleep(0.05)  # cortesía mínima para yfinance

            # Concatenar y subir cada reporte
            if inc_frames:
                df_inc = pd.concat(inc_frames, ignore_index=True)
                upsert_long_table(conn, df_inc, INCOME_TABLE)
            if bal_frames:
                df_bal = pd.concat(bal_frames, ignore_index=True)
                upsert_long_table(conn, df_bal, BAL_TABLE)
            if cf_frames:
                df_cf = pd.concat(cf_frames, ignore_index=True)
                upsert_long_table(conn, df_cf, CF_TABLE)

        print("✅ Estados financieros actualizados.")
    finally:
        conn.close()
