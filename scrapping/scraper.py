# scraper.py

import requests
from bs4 import BeautifulSoup
from datetime import datetime
from config import WAYBACK_TIMEOUT, SNAPSHOT_TIMEOUT

# --8<-- [start:obtener_snapshot_url]
def obtener_snapshot_url(original_url, fecha_str):
    wayback_api = f'https://archive.org/wayback/available?url={original_url}&timestamp={fecha_str}'
    try:
        res = requests.get(wayback_api, timeout=WAYBACK_TIMEOUT)
        data = res.json()
        snapshots = data.get('archived_snapshots', {})
        if snapshots and 'closest' in snapshots:
            url_snapshot = snapshots['closest']['url']
            return url_snapshot.replace("http://", "https://")
        else:
            print(f"No snapshot disponible para {original_url} en {fecha_str}")
            return None
    except Exception as e:
        log_error(f"Error consultando Wayback API para {original_url} en {fecha_str}: {e}")
        return None
# --8<-- [end:obtener_snapshot_url]

# --8<-- [start:extraer_titulares]
def extraer_titulares(snapshot_url, fecha_str, fuente=None):
    titulares = []
    try:
        res = requests.get(snapshot_url, timeout=SNAPSHOT_TIMEOUT)
        soup = BeautifulSoup(res.content, 'html.parser')
        encabezados = soup.find_all(['h1', 'h2', 'h3'])
        print(f"[{fuente}] {len(encabezados)} encabezados encontrados en {snapshot_url}")

        for t in encabezados:
            texto = t.get_text(strip=True)
            clases = " ".join(t.get('class', [])) if t.get('class') else ""

            if fuente == "THE TIMES":
                if any(cls in clases for cls in [
                    'responsive__HeadlineContainer-sc-3t8ix5-3',
                    'responsive__Heading-sc-1k9kzho-1',
                    'responsive__Title-sc-1ij0d4n-5'
                ]) or len(texto.split()) > 3:
                    titulares.append({
                        "fecha": fecha_str,
                        "titular": texto,
                        "url_archivo": snapshot_url
                    })
            else:
                if texto and len(texto.split()) > 3:
                    titulares.append({
                        "fecha": fecha_str,
                        "titular": texto,
                        "url_archivo": snapshot_url
                    })

    except Exception as e:
        log_error(f"[{fuente or 'GENERAL'}] Error accediendo a snapshot: {e}")

    return titulares
# --8<-- [end:extraer_titulares]

def log_error(mensaje):
    with open("scraping_log.txt", "a", errors="ignore") as f:
        f.write(f"{datetime.now()} - {mensaje}\n")
        
def obtener_snapshot_url_directo(original_url, fecha_str):
    # Usa directamente la estructura estándar del snapshot con hora fija (12:00:00)
    snapshot_url = f"https://web.archive.org/web/{fecha_str}120000/{original_url.strip('/')}/"
    return snapshot_url
