# pip install requests pandas beautifulsoup4 lxml

import re
import time
import requests as rq
import pandas as pd
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; IBEX35-components/1.0)"}

def get_from_yahoo() -> list[str]:
    """
    Raspa los componentes del ^IBEX desde Yahoo Finance pidiendo count=100
    para evitar la paginación de 30 filas por defecto.
    Devuelve tickers en formato Yahoo: XXX.MC
    """
    urls = [
        "https://finance.yahoo.com/quote/%5EIBEX/components?p=%5EIBEX&count=100",
        "https://es.finance.yahoo.com/quote/IB.MC/components?p=IB.MC&count=100",
    ]
    for url in urls:
        html = rq.get(url, headers=HEADERS, timeout=30).text
        try:
            tables = pd.read_html(html)
        except ValueError:
            continue
        table = None
        for df in tables:
            cols = [str(c).strip().lower() for c in df.columns]
            if "symbol" in cols or "símbolo" in cols:
                table = df
                break
        if table is None:
            continue
        sym_col = [c for c in table.columns if str(c).strip().lower() in ("symbol", "símbolo")][0]
        syms = (
            table[sym_col]
            .dropna()
            .astype(str)
            .str.strip()
            .tolist()
        )
        syms = [s for s in syms if s.endswith(".MC")]
        if len(syms) >= 34:  # normalmente 35
            return sorted(set(syms))
    raise RuntimeError("Yahoo no devolvió la lista completa.")

def get_from_tradingview() -> list[str]:
    """
    Respaldo: toma los componentes desde TradingView y mapea BME:XXX -> XXX.MC
    """
    url = "https://www.tradingview.com/symbols/BME-IBC/components/"
    html = rq.get(url, headers=HEADERS, timeout=30).text
    soup = BeautifulSoup(html, "lxml")
    tickers = set()
    # La tabla de TV tiene links a cada símbolo bajo /symbols/BME-<TICKER>/
    for a in soup.select("a[href*='/symbols/BME-']"):
        text = (a.get_text() or "").strip().upper()
        if re.fullmatch(r"[A-Z0-9]{2,6}", text):
            tickers.add(text + ".MC")
    if len(tickers) >= 34:
        return sorted(tickers)
    raise RuntimeError("TradingView no devolvió la lista completa.")

def get_ibex35_tickers() -> list[str]:
    """
    Intenta Yahoo y, si falla, TradingView. Verifica tamaño.
    """
    for fn in (get_from_yahoo, get_from_tradingview):
        try:
            tickers = fn()
            if len(tickers) >= 34:
                return tickers
        except Exception as e:
            print(f"[WARN] {fn.__name__} falló: {e}")
            time.sleep(1)
    raise RuntimeError("No se pudo obtener la lista automática de componentes del IBEX 35.")

if __name__ == "__main__":
    tickers = get_ibex35_tickers()
    print(f"✅ Componentes IBEX35 detectados ({len(tickers)}):")
    print(tickers)
