# pip install requests beautifulsoup4 lxml

import re
import requests as rq
from bs4 import BeautifulSoup

URL = "https://www.tradingview.com/symbols/BME-IBC/components/"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; IBEX35-components/1.0)"}

def get_ibex35_components_tradingview() -> list[str]:
    """
    Raspa el listado completo de componentes del IBEX 35 desde TradingView
    y devuelve tickers BME (sin sufijo), p. ej., ['ACS','ACX','AENA',...].
    """
    resp = rq.get(URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    tickers = set()

    # 1) Método principal: enlaces de cada valor /symbols/BME-<TICKER>/
    for a in soup.select("a[href*='/symbols/BME-']"):
        href = a.get("href", "")
        m = re.search(r"/symbols/BME-([A-Z0-9]+)", href)
        if not m:
            continue
        sym = m.group(1).upper()
        if sym != "IBC" and 2 <= len(sym) <= 6 and sym.isalnum():
            tickers.add(sym)

    # 2) Fallback defensivo: por si cambian el DOM y quedan textos planos
    if len(tickers) < 34:
        for el in soup.find_all(string=re.compile(r"^[A-Z0-9]{2,6}$")):
            sym = el.strip().upper()
            if sym != "IBC" and sym.isalnum():
                tickers.add(sym)

    tickers = sorted(tickers)

    if len(tickers) < 34:  # normalmente deben ser 35
        raise RuntimeError(f"Lista incompleta desde TradingView: {len(tickers)} símbolos extraídos.")

    return tickers

if __name__ == "__main__":
    bme = get_ibex35_components_tradingview()
    yahoo = [t + ".MC" for t in bme]
    print(f"✅ Componentes IBEX35 (BME): {len(bme)}")
    print(bme)
    print(f"\n✅ Componentes IBEX35 (Yahoo .MC): {len(yahoo)}")
    print(yahoo)
