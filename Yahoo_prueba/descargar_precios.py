# -*- coding: utf-8 -*-
# pip install yfinance pandas snowflake-connector-python python-dateutil

import os, re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Iterable

import pandas as pd
import yfinance as yf
import snowflake.connector

# === Config Snowflake por variables de entorno ===
CFG = dict(
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'TFM_WH'),
    database=os.environ.get('SNOWFLAKE_DATABASE', 'YAHOO_FINANCE'),
    schema=os.environ.get('SNOWFLAKE_SCHEMA', 'MACHINE_LEARNING'),
)

# Tabla de TICKERS (origen)
TICKERS_TABLE = os.environ.get('TICKERS_TABLE', 'YAHOO_PRUEBA.IBEX.LISTA_IBEX_35')
# Tabla de precios (destino)
PRICES_TABLE  = os.environ.get('PRICES_TABLE',  'YAHOO_FINANCE.MACHINE_LEARNING.TICKERS_INDEX')
# Fecha mínima histórica
START_DATE = pd.to_datetime(os.environ.get('START_DATE', '2020-01-01')).date()

TZ = ZoneInfo("Europe/Madrid")

def yesterday_madrid():
    y = (datetime.now(TZ) - timedelta(days=1)).date()
    return y, (y + timedelta(days=1))  # end exclusivo para yfinance

def sanitize_ticker(t: str) -> str:
    t = str(t).upper().strip()
    if not re.fullmatch(r"[A-Z0-9.\-]+", t):
        return ""
    return t

def chunked(seq: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def ensure_prices_table(conn):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {PRICES_TABLE} (
      TICKER STRING,
      CLOSE  FLOAT,
      HIGH   FLOAT,
      LOW    FLOAT,
      OPEN   FLOAT,
      VOLUME NUMBER,
      FECHA  DATE
    )
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)

def read_tickers(conn) -> List[str]:
    sql = f"SELECT TICKER_YAHOO FROM {TICKERS_TABLE} ORDER BY TICKER_YAHOO"
    with conn.cursor() as cur:
        cur.execute(sql)
        raw = [sanitize_ticker(r[0]) for r in cur.fetchall()]
    return sorted({t for t in raw if t})

def read_last_dates(conn, tickers: List[str]) -> Dict[str, datetime.date]:
    """Devuelve dict ticker -> max(FECHA) si existe, si no, sin clave (nuevo)."""
    if not tickers:
        return {}
    # Sanitiza y arma el IN (...)
    in_list = ",".join(f"'{t}'" for t in tickers)
    sql = f"SELECT TICKER, MAX(FECHA) FROM {PRICES_TABLE} WHERE TICKER IN ({in_list}) GROUP BY TICKER"
    with conn.cursor() as cur:
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        except snowflake.connector.errors.ProgrammingError:
            # Si la tabla no existe aún
            return {}
    return {sanitize_ticker(t): d for t, d in rows if t}

def build_download_plan(tickers: List[str], last_dates: Dict[str, datetime.date], yday: datetime.date):
    """Agrupa tickers por fecha de inicio (start_date), calculada per-ticker."""
    plan: Dict[datetime.date, List[str]] = {}
    for t in tickers:
        last = last_dates.get(t)  # puede ser None
        if last:
            start = max(START_DATE, last + timedelta(days=1))
        else:
            start = START_DATE
        if start <= yday:
            plan.setdefault(start, []).append(t)
    return plan  # {start_date: [tickers...]}

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
                # Si el batch tiene 1 ticker, yfinance puede no usar MultiIndex
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
    # Normaliza
    for col in ["CLOSE","HIGH","LOW","OPEN"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["VOLUME"] = pd.to_numeric(out["VOLUME"], errors="coerce").astype("Int64")
    return out.dropna(subset=["CLOSE","HIGH","LOW","OPEN"])

def merge_with_temp(conn, df: pd.DataFrame):
    if df.empty:
        print("No hay filas nuevas para MERGE.")
        return

    create_tmp = f"CREATE TEMP TABLE TMP_PRICES LIKE {PRICES_TABLE}"
    insert_tmp = f"INSERT INTO TMP_PRICES (TICKER,CLOSE,HIGH,LOW,OPEN,VOLUME,FECHA) VALUES (%s,%s,%s,%s,%s,%s,%s)"
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

    # Construir lista de tuplas
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
        cur.execute(create_tmp)
        # inserta en lotes
        BATCH = 10000
        for i in range(0, len(rows), BATCH):
            cur.executemany(insert_tmp, rows[i:i+BATCH])
        cur.execute(merge_sql)
        conn.commit()

if __name__ == "__main__":
    yday, end_excl = yesterday_madrid()
    print("Hasta (ayer Madrid):", yday)
    with snowflake.connector.connect(**CFG) as conn:
        ensure_prices_table(conn)
        tickers = read_tickers(conn)
        print("Tickers en Snowflake:", len(tickers))
        last_dates = read_last_dates(conn, tickers)
        plan = build_download_plan(tickers, last_dates, yday)

        total_rows = 0
        for start_date, group in sorted(plan.items()):
            print(f"Descargando {len(group)} tickers desde {start_date} → {yday}")
            # Descarga en sublotes para ser estable con yfinance
            frames = []
            for sub in chunked(group, 60):
                part = download_batch(sub, start_date, end_excl)
                if not part.empty:
                    frames.append(part)
            if not frames:
                continue
            df_new = pd.concat(frames, ignore_index=True)
            # Filtro defensivo por rango exacto
            df_new = df_new[(df_new["FECHA"] >= start_date) & (df_new["FECHA"] <= yday)]
            merge_with_temp(conn, df_new)
            total_rows += len(df_new)

        print("Filas nuevas/actualizadas:", total_rows)
