# pip install requests beautifulsoup4 lxml pandas

import re
from pathlib import Path
import requests as rq
import pandas as pd
from bs4 import BeautifulSoup

URL = "https://www.tradingview.com/symbols/BME-IBC/components/"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; IBEX35-components/1.1)"}

def fetch_ibex_components() -> pd.DataFrame:
    """
    Raspa símbolos (BME) y nombres de empresa del IBEX-35 desde TradingView,
    arma un DataFrame con columnas: NOMBRE_EMPRESA, PAIS, INDEX, TICKER (sin .MC).
    """
    resp = rq.get(URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    data = {}  # ticker -> nombre empresa

    # Cada fila tiene un <a href="/symbols/BME-<TICKER>/">TICKER</a> seguido del nombre de empresa
    for a in soup.select('a[href*="/symbols/BME-"]'):
        href = a.get("href", "")
        m = re.search(r"/symbols/BME-([A-Z0-9]+)", href.upper())
        if not m:
            continue
        ticker = m.group(1)
        if ticker == "IBC":
            continue  # evita el propio índice

        # Intenta tomar el texto inmediatamente después del enlace (el nombre de la empresa)
        node = a.next_sibling
        # salta espacios vacíos
        while node and isinstance(node, str) and not node.strip():
            node = node.next_sibling

        if isinstance(node, str):
            raw = " ".join(node.split())
        else:
            raw = " ".join(node.get_text(" ", strip=True).split()) if node else ""

        # Recorta hasta antes del primer número (la tabla sigue con cap. bursátil, precio, etc.)
        name_match = re.match(r"([^\d]+)", raw)
        name = (name_match.group(1) if name_match else raw).strip(" -–—|·")

        # Fallback: si quedó vacío, usa el texto del contenedor sin el ticker y corta antes de números
        if not name:
            container_text = a.parent.get_text(" ", strip=True)
            container_text = re.sub(rf"\b{re.escape(ticker)}\b", "", container_text).strip()
            name = re.split(r"\s+\d", container_text)[0].strip(" -–—|·")

        if name:
            name = re.sub(r"\s{2,}", " ", name)
            data[ticker] = name

    if len(data) < 34:  # deberían salir 35; si no, falla para que lo veas en tu scheduler
        raise RuntimeError(f"Lista incompleta desde TradingView: {len(data)} símbolos extraídos.")

    rows = [{
        "NOMBRE_EMPRESA": name,
        "PAIS": "España",
        "INDEX": "IBEX35",
        "TICKER": ticker,   # sin .MC
    } for ticker, name in sorted(data.items())]

    return pd.DataFrame(rows, columns=["NOMBRE_EMPRESA", "PAIS", "INDEX", "TICKER"])

def save_csv(df: pd.DataFrame, filename: str = "ticker_ibex_35.csv") -> str:
    # Guarda al lado del .py; si no existe __file__ (p.ej. REPL), usa cwd()
    try:
        base_dir = Path(__file__).resolve().parent
    except NameError:
        base_dir = Path.cwd()
    out_path = base_dir / filename
    df.to_csv(out_path, index=False, encoding="utf-8")
    return str(out_path)

if __name__ == "__main__":
    df = fetch_ibex_components()
    out = save_csv(df)
    print(f"✅ Guardado {len(df)} componentes en {out}")
