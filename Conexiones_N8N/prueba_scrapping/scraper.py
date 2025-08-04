# scraper.py

import requests
from bs4 import BeautifulSoup
from datetime import datetime
from config import WAYBACK_TIMEOUT, SNAPSHOT_TIMEOUT

# Obtiene la URL archivada más cercana desde Wayback Machine
def obtener_snapshot_url(original_url, fecha_str):
    wayback_api = f'https://archive.org/wayback/available?url={original_url}&timestamp={fecha_str}'
    res = requests.get(wayback_api, timeout=WAYBACK_TIMEOUT)
    data = res.json()
    if 'archived_snapshots' in data and data['archived_snapshots']:
        snapshot_url = data['archived_snapshots']['closest']['url']
        return snapshot_url.replace("http://", "https://")
    return None

# Extrae titulares en función del noticiero
def extraer_titulares(snapshot_url, fecha_str, fuente=None):
    titulares = []
    try:
        page = requests.get(snapshot_url, timeout=SNAPSHOT_TIMEOUT)
        soup = BeautifulSoup(page.content, 'html.parser')
        encabezados = soup.find_all(['h1', 'h2', 'h3'])

        for t in encabezados:
            texto = t.get_text(strip=True)
            clases = " ".join(t.get('class', [])) if t.get('class') else ""

            # Si es BBC, aplicar clases específicas
            if fuente == "BBC":
                if texto and (any(c in clases for c in [
                    'gs-c-promo-heading__title',
                    'lx-stream-post__header-title',
                    'ssrcss-6arcww-PromoHeadline'
                ]) or len(texto.split()) > 3):
                    titulares.append({
                        "fecha": fecha_str,
                        "titular": texto,
                        "url_archivo": snapshot_url
                    })
            else:
                # Para los demás medios: aceptar titulares con más de 3 palabras
                if texto and len(texto.split()) > 3:
                    titulares.append({
                        "fecha": fecha_str,
                        "titular": texto,
                        "url_archivo": snapshot_url
                    })
    except Exception as e:
        log_error(f"[{fuente or 'GENERAL'}] Error accediendo a snapshot: {e}")

    return titulares

# Guarda errores en archivo log
def log_error(mensaje):
    with open("scraping_log.txt", "a", encoding="utf-8") as f:
        f.write(f"{datetime.now()} - {mensaje}\n")
