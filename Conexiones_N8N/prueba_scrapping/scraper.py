# scraper.py

import requests
from bs4 import BeautifulSoup
from datetime import datetime
from config import WAYBACK_TIMEOUT, SNAPSHOT_TIMEOUT

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
            print(f"⚠️ No snapshot disponible para {original_url} en {fecha_str}")
            return None
    except Exception as e:
        log_error(f"❌ Error consultando Wayback API para {original_url} en {fecha_str}: {e}")
        return None


def extraer_titulares(snapshot_url, fecha_str, fuente=None):
    titulares = []
    try:
        res = requests.get(snapshot_url, timeout=SNAPSHOT_TIMEOUT)
        soup = BeautifulSoup(res.content, 'html.parser')  # ⚠️ usar .content, no .text
        encabezados = soup.find_all(['h1', 'h2', 'h3'])
        print(f"🔬 [{fuente}] {len(encabezados)} encabezados encontrados en {snapshot_url}")

        for t in encabezados:
            texto = t.get_text(strip=True)
            if texto:
                print(f"📝 {texto[:80]}")  # muestra ejemplo de titular recogido
                titulares.append({
                    "fecha": fecha_str,
                    "titular": texto,
                    "url_archivo": snapshot_url
                })

    except Exception as e:
        log_error(f"[{fuente or 'GENERAL'}] Error accediendo a snapshot: {e}")

    return titulares

def log_error(mensaje):
    with open("scraping_log.txt", "a", errors="ignore") as f:
        f.write(f"{datetime.now()} - {mensaje}\n")
