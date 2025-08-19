# -*- coding: utf-8 -*-
# pip install "pandas>=2.0,<2.3" "snowflake-connector-python[pandas]>=3.5.0" pyarrow yfinance

import os, time
from typing import List, Iterable, Optional
from datetime import datetime, date
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
SUMMARY_TABLE   = os.environ.get("SUMMARY_TABLE", f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.FIN_SUMMARY_COMPACT")
FIN_BATCH_TICKERS = int(os.environ.get("FIN_BATCH_TICKERS", "40"))

TZ = ZoneInfo("Europe/Madrid")
CURRENT_YEAR = datetime.now(TZ).year
START_YEAR = int(os.environ.get("SUMMARY_START_YEAR", "2021"))
END_YEAR   = int(os.environ.get("SUMMARY_END_YEAR", str(CURRENT_YEAR - 1)))  # años cerrados

# Orden y nombres EXACTOS que se grabarán en Snowflake
COLS_ORDER = [
    "TICKER","YEAR",
    "ASSETS","LIABILITIES","EQUITY",
    "REVENUE","EXPENSES","NET_INCOME",
    "OPERATING_CF","INVESTING_CF","FINANCING_CF","FREE_CF",
    "NET_MARGIN","ROA","ROE","DEBT_EQUITY",
    "PE_TRAILING","PE_FORWARD","PRICE_TO_BOOK","EV_TO_EBITDA",
    "DIVIDEND_YIELD","PAYOUT_RATIO","MARKET_CAP","ENTERPRISE_VALUE","SHARES_OUTSTANDING"
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
      TICKER              STRING,
      YEAR                NUMBER(4,0),
      ASSETS              FLOAT,
      LIABILITIES         FLOAT,
      EQUITY              FLOAT,
      REVENUE             FLOAT,
      EXPENSES            FLOAT,
      NET_INCOME          FLOAT,
      OPERATING_CF        FLOAT,
      INVESTING_CF        FLOAT,
      FINANCING_CF        FLOAT,
      FREE_CF             FLOAT,
      NET_MARGIN          FLOAT,
      ROA                 FLOAT,
      ROE                 FLOAT,
      DEBT_EQUITY         FLOAT,
      PE_TRAILING         FLOAT,
      PE_FORWARD          FLOAT,
      PRICE_TO_BOOK       FLOAT,
      EV_TO_EBITDA        FLOAT,
      DIVIDEND_YIELD      FLOAT,
      PAYOUT_RATIO        FLOAT,
      MARKET_CAP          FLOAT,
      ENTERPRISE_VALUE    FLOAT,
      SHARES_OUTSTANDING  FLOAT
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
EBITDA_KEYS    = ["Ebitda", "EBITDA"]

OPCF_KEYS      = ["Operating Cash Flow", "Net Cash Provided by Operating Activities",
                  "Cash Flow From Continuing Operating Activities"]
INVCF_KEYS     = ["Investing Cash Flow", "Net Cash Used for Investing Activities",
                  "Cash Flow From Continuing Investing Activities"]
FINCF_KEYS     = ["Financing Cash Flow", "Net Cash Provided by (Used for) Financing Activities",
                  "Cash Flow From Continuing Financing Activities"]
FCF_KEYS       = ["Free Cash Flow"]
CAPEX_KEYS     = ["Capital Expenditure", "Purchase of Property, Plant & Equipment", "Purchase Of Fixed Assets"]

def summarize_by_year_compact(ticker: str) -> pd.DataFrame:
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

    # snapshot de mercado
    info = {}
    try:
        # yfinance moderno: .get_info() es más estable que .info
        info = tk.get_info() or {}
    except Exception:
        try:
            info = tk.info or {}
        except Exception:
            info = {}

    pe_trailing   = info.get("trailingPE")
    pe_forward    = info.get("forwardPE")
    price_to_book = info.get("priceToBook")
    ev_to_ebitda  = info.get("enterpriseToEbitda")
    dividend_yld  = info.get("dividendYield")
    payout_ratio  = info.get("payoutRatio")
    market_cap    = info.get("marketCap")
    enterprise_v  = info.get("enterpriseValue")
    shares_out    = info.get("sharesOutstanding")

    rows = []
    # recorre años (más reciente primero)
    for col in sorted(cols, key=lambda x: pd.Timestamp(x), reverse=True):
        year = pd.Timestamp(col).year
        if not (START_YEAR <= year <= END_YEAR):
            continue

        assets = safe_get_cell(balance, ASSETS_KEYS, col)
        liabs  = safe_get_cell(balance, LIABS_KEYS,  col)
        equity = safe_get_cell(balance, EQUITY_KEYS, col)
        if equity is None and assets is not None and liabs is not None:
            equity = assets - liabs

        revenue     = safe_get_cell(income, REVENUE_KEYS,  col)
        net_income  = safe_get_cell(income, NETINC_KEYS,   col)
        op_income   = safe_get_cell(income, OPINC_KEYS,    col)
        ebitda      = safe_get_cell(income, EBITDA_KEYS,   col)
        expenses    = (revenue - net_income) if (revenue is not None and net_income is not None) else None

        op_cf   = safe_get_cell(cf, OPCF_KEYS,  col)
        inv_cf  = safe_get_cell(cf, INVCF_KEYS, col)
        fin_cf  = safe_get_cell(cf, FINCF_KEYS, col)
        free_cf = safe_get_cell(cf, FCF_KEYS,   col)
        if free_cf is None:
            capex = safe_get_cell(cf, CAPEX_KEYS, col)
            if op_cf is not None and capex is not None:
                free_cf = op_cf + capex  # CapEx suele ser negativo

        # ratios anuales
        net_margin  = (net_income / revenue) if (net_income not in (None, 0) and revenue not in (None, 0)) else None
        roa         = (net_income / assets)  if (net_income not in (None, 0) and assets  not in (None, 0)) else None
        roe         = (net_income / equity)  if (net_income not in (None, 0) and equity  not in (None, 0)) else None
        debt_equity = (liabs / equity)       if (liabs not in (None, 0) and equity  not in (None, 0)) else None

        # EV/EBITDA efectivo si no vino en snapshot
        ev_ebitda_eff = ev_to_ebitda
        if ev_ebitda_eff is None and enterprise_v is not None and ebitda not in (None, 0):
            ev_ebitda_eff = enterprise_v / ebitda

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
            "PE_TRAILING": round(pe_trailing, 3) if pe_trailing is not None else None,
            "PE_FORWARD": round(pe_forward, 3) if pe_forward is not None else None,
            "PRICE_TO_BOOK": round(price_to_book, 3) if price_to_book is not None else None,
            "EV_TO_EBITDA": round(ev_ebitda_eff, 3) if ev_ebitda_eff is not None else None,
            "DIVIDEND_YIELD": round(dividend_yld, 3) if dividend_yld is not None else None,
            "PAYOUT_RATIO": round(payout_ratio, 3) if payout_ratio is not None else None,
            "MARKET_CAP": market_cap,
            "ENTERPRISE_VALUE": enterprise_v,
            "SHARES_OUTSTANDING": shares_out
        })

    if not rows:
        return pd.DataFrame(columns=COLS_ORDER)

    df = pd.DataFrame(rows)
    # Asegura columnas/orden/tipos
    df = df.reindex(columns=COLS_ORDER)
    df["TICKER"] = df["TICKER"].astype(str)
    df["YEAR"] = pd.to_numeric(df["YEAR"], errors="coerce").astype("Int64")
    for c in [x for x in COLS_ORDER if x not in ("TICKER","YEAR")]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values(["TICKER","YEAR"])

def upsert_summary(conn, df: pd.DataFrame):
    if df is None or df.empty:
        return
    df = df.reindex(columns=COLS_ORDER)
    with conn.cursor() as cur:
        cur.execute(f"CREATE OR REPLACE TEMP TABLE TMP_SUMMARY LIKE {SUMMARY_TABLE}")
    ok, _, nrows, _ = write_pandas(conn, df, table_name="TMP_SUMMARY", quote_identifiers=False)
    if not ok:
        raise RuntimeError("write_pandas falló al cargar TMP_SUMMARY.")
    merge_sql = f"""
    MERGE INTO {SUMMARY_TABLE} t
    USING TMP_SUMMARY s
      ON  t.TICKER = s.TICKER AND t.YEAR = s.YEAR
    WHEN MATCHED THEN UPDATE SET
      t.ASSETS = s.ASSETS,
      t.LIABILITIES = s.LIABILITIES,
      t.EQUITY = s.EQUITY,
      t.REVENUE = s.REVENUE,
      t.EXPENSES = s.EXPENSES,
      t.NET_INCOME = s.NET_INCOME,
      t.OPERATING_CF = s.OPERATING_CF,
      t.INVESTING_CF = s.INVESTING_CF,
      t.FINANCING_CF = s.FINANCING_CF,
      t.FREE_CF = s.FREE_CF,
      t.NET_MARGIN = s.NET_MARGIN,
      t.ROA = s.ROA,
      t.ROE = s.ROE,
      t.DEBT_EQUITY = s.DEBT_EQUITY,
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
      INSERT (TICKER, YEAR, ASSETS, LIABILITIES, EQUITY, REVENUE, EXPENSES, NET_INCOME,
              OPERATING_CF, INVESTING_CF, FINANCING_CF, FREE_CF,
              NET_MARGIN, ROA, ROE, DEBT_EQUITY,
              PE_TRAILING, PE_FORWARD, PRICE_TO_BOOK, EV_TO_EBITDA,
              DIVIDEND_YIELD, PAYOUT_RATIO, MARKET_CAP, ENTERPRISE_VALUE, SHARES_OUTSTANDING)
      VALUES (s.TICKER, s.YEAR, s.ASSETS, s.LIABILITIES, s.EQUITY, s.REVENUE, s.EXPENSES, s.NET_INCOME,
              s.OPERATING_CF, s.INVESTING_CF, s.FINANCING_CF, s.FREE_CF,
              s.NET_MARGIN, s.ROA, s.ROE, s.DEBT_EQUITY,
              s.PE_TRAILING, s.PE_FORWARD, s.PRICE_TO_BOOK, s.EV_TO_EBITDA,
              s.DIVIDEND_YIELD, s.PAYOUT_RATIO, s.MARKET_CAP, s.ENTERPRISE_VALUE, s.SHARES_OUTSTANDING)
    """
    with conn.cursor() as cur:
        cur.execute(merge_sql)
        conn.commit()
    print(f"Upsert FIN_SUMMARY_COMPACT: {nrows} filas")

# ================== MAIN ==================
if __name__ == "__main__":
    print(f"Rango de años: {START_YEAR} … {END_YEAR} (anuales, cerrados)")
    conn = sf_connect()
    try:
        ensure_table(conn)
        tickers = read_tickers(conn)
        print("Tickers:", len(tickers))

        all_rows = []
        for batch in chunked(tickers, FIN_BATCH_TICKERS):
            print(f"Lote de {len(batch)} tickers…")
            for tck in batch:
                try:
                    df = summarize_by_year_compact(tck)
                    if not df.empty:
                        all_rows.append(df)
                except Exception:
                    pass
                time.sleep(0.05)
        if all_rows:
            big = pd.concat(all_rows, ignore_index=True)
            upsert_summary(conn, big)
        else:
            print("No hubo datos para el rango solicitado.")
        print("✅ Resumen financiero ANUAL (compacto) actualizado.")
    finally:
        conn.close()
