# -*- coding: utf-8 -*-
# pip install "pandas>=2.0,<2.3" "snowflake-connector-python[pandas]>=3.5.0" pyarrow yfinance

import os, time
from typing import List, Iterable, Optional, Dict, Set
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ================== CONFIG (por secrets/env) ==================
SNOWFLAKE_USER      = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
SNOWFLAKE_DATABASE  = os.environ["SNOWFLAKE_DATABASE"]
SNOWFLAKE_SCHEMA    = os.environ["SNOWFLAKE_SCHEMA"]
SNOWFLAKE_ROLE      = os.environ.get("SNOWFLAKE_ROLE")

TICKERS_TABLE   = os.environ.get("TICKERS_TABLE", f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.LISTA_IBEX_35")
SUMMARY_TABLE   = os.environ.get("SUMMARY_TABLE", f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.FIN_SUMMARY_COMPACT_FS")
FIN_BATCH_TICKERS = int(os.environ.get("FIN_BATCH_TICKERS", "40"))

TZ = ZoneInfo("Europe/Madrid")
CURRENT_YEAR = datetime.now(TZ).year
START_YEAR = int(os.environ.get("SUMMARY_START_YEAR", "2021"))
END_YEAR   = int(os.environ.get("SUMMARY_END_YEAR", str(CURRENT_YEAR - 1)))  # años cerrados

# Columnas finales (sin snapshot de mercado)
COLS_ORDER = [
    "TICKER","YEAR",
    "ASSETS","LIABILITIES","EQUITY",
    "REVENUE","EXPENSES","NET_INCOME",
    "OPERATING_CF","INVESTING_CF","FINANCING_CF","FREE_CF",
    "NET_MARGIN","ROA","ROE","DEBT_EQUITY"
]

# ================== Snowflake helpers ==================
def sf_connect():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER, password=SNOWFLAKE_PASSWORD, account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE, database=SNOWFLAKE_DATABASE, schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )

def ensure_table(conn):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {SUMMARY_TABLE} (
      TICKER         STRING,
      YEAR           NUMBER(4,0),
      ASSETS         FLOAT,
      LIABILITIES    FLOAT,
      EQUITY         FLOAT,
      REVENUE        FLOAT,
      EXPENSES       FLOAT,
      NET_INCOME     FLOAT,
      OPERATING_CF   FLOAT,
      INVESTING_CF   FLOAT,
      FINANCING_CF   FLOAT,
      FREE_CF        FLOAT,
      NET_MARGIN     FLOAT,
      ROA            FLOAT,
      ROE            FLOAT,
      DEBT_EQUITY    FLOAT
    )
    """
    with conn.cursor() as cur:
        cur.execute(ddl)

def read_tickers(conn) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT TICKER_YAHOO FROM {TICKERS_TABLE} ORDER BY 1")
        return [str(r[0]).strip().upper() for r in cur.fetchall() if r[0]]

def read_existing_years(conn) -> Dict[str, Set[int]]:
    """
    Devuelve {ticker: {years ya existentes en [START_YEAR..END_YEAR] }}.
    """
    with conn.cursor() as cur:
        try:
            cur.execute(
                f"SELECT TICKER, YEAR FROM {SUMMARY_TABLE} "
                f"WHERE YEAR BETWEEN %s AND %s",
                (START_YEAR, END_YEAR)
            )
            rows = cur.fetchall()
        except snowflake.connector.errors.ProgrammingError:
            return {}
    out: Dict[str, Set[int]] = {}
    for t, y in rows:
        if t is None or y is None: 
            continue
        t = str(t).upper().strip()
        out.setdefault(t, set()).add(int(y))
    return out

def chunked(seq: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

# ================== Helpers financieros ==================
def safe_get_cell(df: Optional[pd.DataFrame], keys: List[str], col) -> Optional[float]:
    if df is None or df.empty:
        return None
    for k in keys:
        if k in df.index and col in df.columns:
            try:
                return float(df.at[k, col])
            except Exception:
                return None
    return None

ASSETS_KEYS    = ["Total Assets"]
LIABS_KEYS     = ["Total Liabilities Net Minority Interest", "Total Liabilities"]
EQUITY_KEYS    = ["Stockholders Equity", "Total Equity Gross Minority Interest"]

REVENUE_KEYS   = ["Total Revenue", "Operating Revenue"]
NETINC_KEYS    = ["Net Income"]
OPINC_KEYS     = ["Operating Income", "Operating Income Before Depreciation"]

OPCF_KEYS      = ["Operating Cash Flow", "Net Cash Provided by Operating Activities",
                  "Cash Flow From Continuing Operating Activities"]
INVCF_KEYS     = ["Investing Cash Flow", "Net Cash Used for Investing Activities",
                  "Cash Flow From Continuing Investing Activities"]
FINCF_KEYS     = ["Financing Cash Flow", "Net Cash Provided by (Used for) Financing Activities",
                  "Cash Flow From Continuing Financing Activities"]
FCF_KEYS       = ["Free Cash Flow"]
CAPEX_KEYS     = ["Capital Expenditure", "Purchase of Property, Plant & Equipment", "Purchase Of Fixed Assets"]

def summarize_missing_years(ticker: str, allowed_years: Set[int]) -> pd.DataFrame:
    """
    Devuelve sólo las filas de los años en allowed_years (ya filtrado por rango).
    """
    if not allowed_years:
        return pd.DataFrame(columns=COLS_ORDER)

    tk = yf.Ticker(ticker)
    balance = tk.balance_sheet        # anual
    income  = tk.financials           # anual
    cf      = tk.cashflow             # anual

    # periodos combinados
    cols = set()
    for df in (balance, income, cf):
        if df is not None and not df.empty:
            cols |= set(df.columns)
    if not cols:
        return pd.DataFrame(columns=COLS_ORDER)

    rows = []
    for col in cols:
        year = int(pd.Timestamp(col).year)
        if year not in allowed_years:
            continue

        assets = safe_get_cell(balance, ASSETS_KEYS, col)
        liabs  = safe_get_cell(balance, LIABS_KEYS,  col)
        equity = safe_get_cell(balance, EQUITY_KEYS, col)
        if equity is None and assets is not None and liabs is not None:
            equity = assets - liabs

        revenue     = safe_get_cell(income, REVENUE_KEYS,  col)
        net_income  = safe_get_cell(income, NETINC_KEYS,   col)
        op_income   = safe_get_cell(income, OPINC_KEYS,    col)
        expenses    = (revenue - net_income) if (revenue is not None and net_income is not None) else None

        op_cf   = safe_get_cell(cf, OPCF_KEYS,  col)
        inv_cf  = safe_get_cell(cf, INVCF_KEYS, col)
        fin_cf  = safe_get_cell(cf, FINCF_KEYS, col)
        free_cf = safe_get_cell(cf, FCF_KEYS,   col)
        if free_cf is None:
            capex = safe_get_cell(cf, CAPEX_KEYS, col)
            if op_cf is not None and capex is not None:
                free_cf = op_cf + capex  # CapEx suele ser negativo

        net_margin  = (net_income / revenue) if (net_income not in (None, 0) and revenue not in (None, 0)) else None
        roa         = (net_income / assets)  if (net_income not in (None, 0) and assets  not in (None, 0)) else None
        roe         = (net_income / equity)  if (net_income not in (None, 0) and equity  not in (None, 0)) else None
        debt_equity = (liabs / equity)       if (liabs not in (None, 0) and equity  not in (None, 0)) else None

        rows.append({
            "TICKER": ticker,
            "YEAR": int(year),
            "ASSETS": assets,
            "LIABILITIES": liabs,
            "EQUITY": equity,
            "REVENUE": revenue,
            "EXPENSES": expenses,
            "NET_INCOME": net_income,
            "OPERATING_CF": op_cf,
            "INVESTING_CF": inv_cf,
            "FINANCING_CF": fin_cf,
            "FREE_CF": free_cf,
            "NET_MARGIN": round(net_margin, 3) if net_margin is not None else None,
            "ROA": round(roa, 3) if roa is not None else None,
            "ROE": round(roe, 3) if roe is not None else None,
            "DEBT_EQUITY": round(debt_equity, 3) if debt_equity is not None else None,
        })

    if not rows:
        return pd.DataFrame(columns=COLS_ORDER)

    df = pd.DataFrame(rows)
    df = df.reindex(columns=COLS_ORDER)
    df["TICKER"] = df["TICKER"].astype(str)
    df["YEAR"] = pd.to_numeric(df["YEAR"], errors="coerce").astype("Int64")
    for c in [x for x in COLS_ORDER if x not in ("TICKER","YEAR")]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values(["TICKER","YEAR"])

def insert_only_missing(conn, df: pd.DataFrame):
    """
    Inserta solo nuevas filas (sin actualizar existentes).
    Además incluye una segunda protección en SQL con NOT EXISTS.
    """
    if df is None or df.empty:
        print("No hay filas nuevas para insertar.")
        return

    df = df.reindex(columns=COLS_ORDER)
    with conn.cursor() as cur:
        cur.execute(f"CREATE OR REPLACE TEMP TABLE TMP_SUMMARY LIKE {SUMMARY_TABLE}")
    ok, _, nrows, _ = write_pandas(conn, df, table_name="TMP_SUMMARY", quote_identifiers=False)
    if not ok:
        raise RuntimeError("write_pandas falló al cargar TMP_SUMMARY.")

    insert_sql = f"""
    INSERT INTO {SUMMARY_TABLE} ({", ".join(COLS_ORDER)})
    SELECT {", ".join("s."+c for c in COLS_ORDER)} 
    FROM TMP_SUMMARY s
    WHERE NOT EXISTS (
      SELECT 1 FROM {SUMMARY_TABLE} t
      WHERE t.TICKER = s.TICKER AND t.YEAR = s.YEAR
    )
    """
    with conn.cursor() as cur:
        cur.execute(insert_sql)
        conn.commit()
    print(f"Insert (solo faltantes) cargados desde TMP_SUMMARY: {nrows} filas candidatas")

# ================== MAIN ==================
if __name__ == "__main__":
    print(f"Años objetivo: {START_YEAR}…{END_YEAR} (solo agregar faltantes)")
    conn = sf_connect()
    try:
        ensure_table(conn)
        tickers = read_tickers(conn)
        existing = read_existing_years(conn)

        target_years = set(range(START_YEAR, END_YEAR + 1))
        print("Tickers:", len(tickers))

        all_rows = []
        for batch in chunked(tickers, FIN_BATCH_TICKERS):
            print(f"Lote de {len(batch)} tickers…")
            for tck in batch:
                have = existing.get(tck, set())
                missing_years = target_years - have
                if not missing_years:
                    continue
                try:
                    df = summarize_missing_years(tck, missing_years)
                    if not df.empty:
                        all_rows.append(df)
                except Exception:
                    pass
                time.sleep(0.05)

        if all_rows:
            big = pd.concat(all_rows, ignore_index=True)
            insert_only_missing(conn, big)
        else:
            print("No hay años faltantes que insertar.")
        print("✅ Financial summary: solo años faltantes agregados.")
    finally:
        conn.close()
