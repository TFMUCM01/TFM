# -*- coding: utf-8 -*-
# pip install yfinance "pandas>=2.0,<2.3" "snowflake-connector-python[pandas]>=3.5.0" pyarrow

import os, time, concurrent.futures
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ===== Config desde variables de entorno (secrets) =====
SNOWFLAKE_USER      = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
SNOWFLAKE_DATABASE  = os.environ["SNOWFLAKE_DATABASE"]
SNOWFLAKE_SCHEMA    = os.environ["SNOWFLAKE_SCHEMA"]
SNOWFLAKE_ROLE      = os.environ.get("SNOWFLAKE_ROLE")

TICKERS_TABLE = os.environ.get("TICKERS_TABLE", f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.LISTA_IBEX_35")
PROFILE_TABLE = os.environ.get("PROFILE_TABLE", f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.COMPANY_PROFILE")

TZ = ZoneInfo("Europe/Madrid")

def sf_connect():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER, password=SNOWFLAKE_PASSWORD, account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE, database=SNOWFLAKE_DATABASE, schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )

def ensure_profile_table(conn):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {PROFILE_TABLE} (
      TICKER            STRING,
      NOMBRE            STRING,
      SECTOR            STRING,
      INDUSTRIA         STRING,
      EMPLEADOS         NUMBER,
      PAIS              STRING,
      CIUDAD            STRING,
      DIRECCION         STRING,
      WEBSITE           STRING,
      TELEFONO          STRING,
      MONEDA            STRING,
      EXCHANGE          STRING,
      MARKET_CAP        NUMBER,
      RESUMEN           STRING,
      UPDATED_AT        TIMESTAMP_TZ
    )
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)

def read_tickers(conn):
    with conn.cursor() as cur:
        cur.execute(f"SELECT TICKER_YAHOO FROM {TICKERS_TABLE} ORDER BY 1")
        return [r[0] for r in cur.fetchall() if r[0]]

def fetch_profile_one(ticker: str, retries: int = 3, pause: float = 0.6) -> dict:
    for i in range(retries):
        try:
            info = yf.Ticker(ticker).get_info()
            # mapear campos (algunos pueden no existir)
            return {
                "TICKER": ticker,
                "NOMBRE": info.get("longName") or info.get("shortName"),
                "SECTOR": info.get("sector"),
                "INDUSTRIA": info.get("industry"),
                "EMPLEADOS": info.get("fullTimeEmployees"),
                "PAIS": info.get("country"),
                "CIUDAD": info.get("city"),
                "DIRECCION": info.get("address1"),
                "WEBSITE": info.get("website"),
                "TELEFONO": info.get("phone"),
                "MONEDA": info.get("currency"),
                "EXCHANGE": info.get("exchange") or info.get("fullExchangeName"),
                "MARKET_CAP": info.get("marketCap"),
                "RESUMEN": info.get("longBusinessSummary"),
                "UPDATED_AT": datetime.now(TZ),
            }
        except Exception:
            if i < retries - 1:
                time.sleep(pause * (i + 1))
            else:
                return {
                    "TICKER": ticker, "NOMBRE": None, "SECTOR": None, "INDUSTRIA": None,
                    "EMPLEADOS": None, "PAIS": None, "CIUDAD": None, "DIRECCION": None,
                    "WEBSITE": None, "TELEFONO": None, "MONEDA": None, "EXCHANGE": None,
                    "MARKET_CAP": None, "RESUMEN": None, "UPDATED_AT": datetime.now(TZ),
                }

def fetch_profiles(tickers, max_workers=8):
    rows = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        for res in ex.map(fetch_profile_one, tickers):
            rows.append(res)
    df = pd.DataFrame(rows)
    # opcional: filtra filas totalmente nulas (cuando no hay perfil)
    has_any = df.drop(columns=["TICKER", "UPDATED_AT"]).notna().any(axis=1)
    return df[has_any].reset_index(drop=True)

def merge_profiles(conn, df: pd.DataFrame):
    if df.empty:
        print("No hay perfiles para actualizar.")
        return
    # Carga en tabla temporal con write_pandas y luego MERGE
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TEMP TABLE TMP_PROFILE LIKE " + PROFILE_TABLE)
    ok, _, nrows, _ = write_pandas(conn, df, table_name="TMP_PROFILE", quote_identifiers=False)
    if not ok:
        raise RuntimeError("write_pandas falló al cargar TMP_PROFILE.")
    merge_sql = f"""
    MERGE INTO {PROFILE_TABLE} t
    USING TMP_PROFILE s
      ON t.TICKER = s.TICKER
    WHEN MATCHED THEN UPDATE SET
      t.NOMBRE = s.NOMBRE,
      t.SECTOR = s.SECTOR,
      t.INDUSTRIA = s.INDUSTRIA,
      t.EMPLEADOS = s.EMPLEADOS,
      t.PAIS = s.PAIS,
      t.CIUDAD = s.CIUDAD,
      t.DIRECCION = s.DIRECCION,
      t.WEBSITE = s.WEBSITE,
      t.TELEFONO = s.TELEFONO,
      t.MONEDA = s.MONEDA,
      t.EXCHANGE = s.EXCHANGE,
      t.MARKET_CAP = s.MARKET_CAP,
      t.RESUMEN = s.RESUMEN,
      t.UPDATED_AT = s.UPDATED_AT
    WHEN NOT MATCHED THEN
      INSERT (TICKER,NOMBRE,SECTOR,INDUSTRIA,EMPLEADOS,PAIS,CIUDAD,DIRECCION,WEBSITE,TELEFONO,MONEDA,EXCHANGE,MARKET_CAP,RESUMEN,UPDATED_AT)
      VALUES (s.TICKER,s.NOMBRE,s.SECTOR,s.INDUSTRIA,s.EMPLEADOS,s.PAIS,s.CIUDAD,s.DIRECCION,s.WEBSITE,s.TELEFONO,s.MONEDA,s.EXCHANGE,s.MARKET_CAP,s.RESUMEN,s.UPDATED_AT)
    """
    with conn.cursor() as cur:
        cur.execute(merge_sql)
        conn.commit()
    print(f"Perfiles actualizados/insertados: {len(df)} (cargados {nrows})")

if __name__ == "__main__":
    conn = sf_connect()
    try:
        ensure_profile_table(conn)
        tickers = read_tickers(conn)
        print("Tickers a perfilar:", len(tickers))
        df = fetch_profiles(tickers, max_workers=8)  # ajusta workers si ves rate limiting
        print("Perfiles obtenidos:", len(df))
        merge_profiles(conn, df)
    finally:
        conn.close()
