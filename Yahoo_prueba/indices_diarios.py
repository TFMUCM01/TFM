# -*- coding: utf-8 -*-
# pip install "pandas>=2.0,<2.3" "snowflake-connector-python[pandas]>=3.5.0" pyarrow yfinance

import os, time
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import pandas as pd
import yfinance as yf
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# === Secrets / ENV ===
SNOWFLAKE_USER      = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
SNOWFLAKE_DATABASE  = os.environ["SNOWFLAKE_DATABASE"]
SNOWFLAKE_SCHEMA    = os.environ["SNOWFLAKE_SCHEMA"]
SNOWFLAKE_ROLE      = os.environ.get("SNOWFLAKE_ROLE")

START_DATE = pd.to_datetime(os.environ.get("START_DATE", "2020-01-01")).date()
INDEX_TABLE = os.environ.get("INDEX_TABLE", f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.INDEX_DAILY")

TZ = ZoneInfo("Europe/Madrid")

INDEX_SPECS = [
    # market: etiqueta corta que te quedará en la tabla
    dict(market="IBEX",   index_name="IBEX 35",            pais="España",       symbol="^IBEX"),
    dict(market="DAX",    index_name="DAX 40",             pais="Alemania",     symbol="^GDAXI"),
    dict(market="CAC",    index_name="CAC 40",             pais="Francia",      symbol="^FCHI"),
    dict(market="MIB",    index_name="FTSE MIB",           pais="Italia",       symbol="FTSEMIB.MI"),
    dict(market="AEX",    index_name="AEX",                pais="Países Bajos", symbol="^AEX"),
    dict(market="FTSE",   index_name="FTSE 100",           pais="Reino Unido",  symbol="^FTSE"),
    dict(market="OMXS30", index_name="OMX Stockholm 30",   pais="Suecia",       symbol="^OMXS30"),
    dict(market="SMI",    index_name="SMI PR",             pais="Suiza",        symbol="^SSMI"),
]

def sf_connect():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER, password=SNOWFLAKE_PASSWORD, account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE, database=SNOWFLAKE_DATABASE, schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )

def ensure_table(conn):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {INDEX_TABLE} (
      SYMBOL      STRING,
      MARKET      STRING,
      INDEX_NAME  STRING,
      PAIS        STRING,
      FECHA       DATE,
      OPEN        FLOAT,
      HIGH        FLOAT,
      LOW         FLOAT,
      CLOSE       FLOAT
    )
    """
    with conn.cursor() as cur:
        cur.execute(ddl)

def read_last_dates(conn):
    with conn.cursor() as cur:
        try:
            cur.execute(f"SELECT SYMBOL, MAX(FECHA) FROM {INDEX_TABLE} GROUP BY 1")
            return {str(s): d for s, d in cur.fetchall() if s}
        except snowflake.connector.errors.ProgrammingError:
            return {}

def yesterday_madrid():
    y = (datetime.now(TZ) - timedelta(days=1)).date()
    return y, y + timedelta(days=1)

def fetch_index(symbol: str, start_d: date, end_excl: date) -> pd.DataFrame:
    df = yf.download(
        symbol,
        start=start_d,
        end=end_excl,
        interval="1d",
        auto_adjust=False,
        progress=False,
        group_by="ticker",
        threads=True,
    )
    if df is None or df.empty:
        return pd.DataFrame(columns=["FECHA","OPEN","HIGH","LOW","CLOSE"])

    # Si viene MultiIndex (por ejemplo con el símbolo en el nivel 0)
    if isinstance(df.columns, pd.MultiIndex):
        if symbol in df.columns.get_level_values(0):
            df = df.xs(symbol, level=0, axis=1)
        else:
            # a veces viene con un solo nivel implícito
            try:
                df.columns = [c[-1] if isinstance(c, tuple) else c for c in df.columns]
            except Exception:
                pass

    # Normaliza nombres disponibles (case-insensitive)
    colmap_lower = {c.lower(): c for c in df.columns}

    def pick(*candidates):
        for cand in candidates:
            # exacto
            if cand in df.columns:
                return cand
            # por lower
            lc = cand.lower()
            if lc in colmap_lower:
                return colmap_lower[lc]
        return None

    open_col  = pick("Open")
    high_col  = pick("High")
    low_col   = pick("Low")
    close_col = pick("Close", "Adj Close")  # <— usa Adj Close si falta Close

    # Construye salida
    out = pd.DataFrame(index=df.index.copy())
    out["FECHA"] = pd.to_datetime(out.index).date

    def col_to_float(colname):
        if colname is None or colname not in df.columns:
            return pd.Series([pd.NA]*len(df), index=df.index, dtype="Float64")
        return pd.to_numeric(df[colname], errors="coerce").astype("Float64")

    out["OPEN"]  = col_to_float(open_col)
    out["HIGH"]  = col_to_float(high_col)
    out["LOW"]   = col_to_float(low_col)
    out["CLOSE"] = col_to_float(close_col)

    # Reglas de limpieza mínimas
    out = out[["FECHA","OPEN","HIGH","LOW","CLOSE"]]
    out = out.dropna(subset=["CLOSE"])  # si no hay close/adj close, esa fila no sirve

    # Convierte FECHA a tipo date puro
    out["FECHA"] = pd.to_datetime(out["FECHA"]).dt.date
    return out.reset_index(drop=True)


def merge_chunk(conn, df_chunk: pd.DataFrame):
    if df_chunk.empty:
        return
    with conn.cursor() as cur:
        cur.execute(f"CREATE OR REPLACE TEMP TABLE TMP_INDEX LIKE {INDEX_TABLE}")
    ok, _, nrows, _ = write_pandas(conn, df_chunk, table_name="TMP_INDEX", quote_identifiers=False)
    if not ok:
        raise RuntimeError("write_pandas falló para TMP_INDEX")
    merge_sql = f"""
    MERGE INTO {INDEX_TABLE} t
    USING TMP_INDEX s
      ON  t.SYMBOL = s.SYMBOL AND t.FECHA = s.FECHA
    WHEN MATCHED THEN UPDATE SET
      t.OPEN = s.OPEN, t.HIGH = s.HIGH, t.LOW = s.LOW, t.CLOSE = s.CLOSE,
      t.MARKET = s.MARKET, t.INDEX_NAME = s.INDEX_NAME, t.PAIS = s.PAIS
    WHEN NOT MATCHED THEN
      INSERT (SYMBOL, MARKET, INDEX_NAME, PAIS, FECHA, OPEN, HIGH, LOW, CLOSE)
      VALUES (s.SYMBOL, s.MARKET, s.INDEX_NAME, s.PAIS, s.FECHA, s.OPEN, s.HIGH, s.LOW, s.CLOSE)
    """
    with conn.cursor() as cur:
        cur.execute(merge_sql)
        conn.commit()
    print(f"Subidas/actualizadas: {nrows}")

if __name__ == "__main__":
    yday, end_excl = yesterday_madrid()

    conn = sf_connect()
    try:
        ensure_table(conn)
        last = read_last_dates(conn)

        all_rows = []
        for spec in INDEX_SPECS:
            sym = spec["symbol"]
            start_d = max(START_DATE, (last[sym] + timedelta(days=1)) if sym in last and last[sym] else START_DATE)
            if start_d > yday:
                continue
            print(f"Descargando {spec['market']} ({sym}) desde {start_d} → {yday}")
            df = fetch_index(sym, start_d, end_excl)
            if df.empty:
                continue
            df = df[(df["FECHA"] >= start_d) & (df["FECHA"] <= yday)].copy()
            df["SYMBOL"] = sym
            df["MARKET"] = spec["market"]
            df["INDEX_NAME"] = spec["index_name"]
            df["PAIS"] = spec["pais"]
            all_rows.append(df[["SYMBOL","MARKET","INDEX_NAME","PAIS","FECHA","OPEN","HIGH","LOW","CLOSE"]])
            time.sleep(0.1)  # cortesía mínima

        if all_rows:
            big = pd.concat(all_rows, ignore_index=True)
            merge_chunk(conn, big)
        else:
            print("No hay nada nuevo para cargar.")
        print("✅ Índices actualizados.")
    finally:
        conn.close()



# valida el script tickers precio global para ver si los ticker estan bien