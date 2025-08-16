# pip install requests beautifulsoup4 lxml pandas

import re
import time
from pathlib import Path

import pandas as pd
import requests as rq
from bs4 import BeautifulSoup

URL_COMPONENTS = "https://www.tradingview.com/symbols/BME-IBC/components/"
URL_STOCK = "https://www.tradingview.com/symbols/BME-{ticker}/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

def fetch_component_tickers() -> list[str]:
    """Raspa la página de componentes y devuelve tickers BME (sin .MC)."""
    resp = rq.get(URL_COMPONENTS, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    html = resp.text

    # 1) símbolos en los href: /symbols/BME-<TICKER>/
    tickers = set(re.findall(r"/symbols/BME-([A-Z0-9]{2,6})", html, flags=re.I))

    # 2) refuerza con apariciones del patrón 'BME:<TICKER>' en alt/textos
    tickers |= set(re.findall(r"\bBME:([A-Z0-9]{2,6})\b", html, flags=re.I))

    # limpia y filtra
    tickers = {t.upper() for t in tickers if t.upper() != "IBC"}
    tickers = sorted(tickers)

    if len(tickers) < 34:  # normalmente 35
        raise RuntimeError(f"Lista incompleta: {len(tickers)} símbolos encontrados.")
    return tickers

def fetch_company_name(ticker: str) -> str:
    """Abre la página del valor y toma el <h1> como nombre de la empresa."""
    url = URL_STOCK.format(ticker=ticker)
    resp = rq.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    # 1) h1 (caso más estable)
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(strip=True)

    # 2) og:title (fallback)
    og = soup.find("meta", {"property": "og:title"})
    if og and og.get("content"):
        name = og["content"]
        name = re.sub(r"\s*—\s*TradingView.*$", "", name).strip()
        if name:
            return name

    # 3) último recurso: deja vacío (o podrías devolver el propio ticker)
    return ""

def save_csv(rows: list[dict], filename: str = "ticker_ibex_35.csv") -> str:
    df = pd.DataFrame(rows, columns=["NOMBRE_EMPRESA", "PAIS", "INDEX", "TICKER"])
    try:
        base_dir = Path(__file__).resolve().parent
    except NameError:
        base_dir = Path.cwd()
    out_path = base_dir / filename
    df.to_csv(out_path, index=False, encoding="utf-8")
    return str(out_path)

if __name__ == "__main__":
    tickers = fetch_component_tickers()
    rows = []
    for t in tickers:
        # Pequeña pausa para ser amables con el sitio
        time.sleep(0.15)
        nombre = fetch_company_name(t)
        rows.append({
            "NOMBRE_EMPRESA": nombre,
            "PAIS": "España",
            "INDEX": "IBEX35",
            "TICKER": t,  # sin .MC
        })
    out = save_csv(rows)
    print(f"✅ Extraídos {len(rows)} componentes IBEX35 y guardados en: {out}")
