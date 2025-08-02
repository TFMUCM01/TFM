from datetime import timedelta
import pandas as pd
import os
import time

from config import *
from scraper import obtener_snapshot_url, extraer_titulares, log_error
from drive_utils import subir_a_drive

resultados = []
fecha = FECHA_INICIO

# Cargar datos previos si existen
if os.path.exists("bbc_news_2025.csv"):
    df_existente = pd.read_csv("bbc_news_2025.csv")
    fechas_procesadas = set(df_existente['fecha'])
else:
    df_existente = pd.DataFrame()
    fechas_procesadas = set()

while fecha <= FECHA_FIN:
    fecha_str = fecha.strftime("%Y%m%d")
    if fecha_str in fechas_procesadas:
        print(f"{fecha_str} ya procesado, se omite.")
        fecha += timedelta(days=1)
        continue

    print(f"Procesando {fecha_str}...")
    success = False
    for intento in range(RETRIES):
        try:
            snapshot_url = obtener_snapshot_url(BBC_URL, fecha_str)
            if snapshot_url:
                titulares = extraer_titulares(snapshot_url, fecha_str)
                resultados.extend(titulares)
                success = True
                break
            else:
                log_error(f"No snapshot para {fecha_str}")
                success = True
                break
        except Exception as e:
            log_error(f"Error en {fecha_str} (intento {intento+1}): {e}")
            time.sleep(3)
    
    if not success:
        log_error(f"Fallo persistente en {fecha_str}, se omite el día.")

    time.sleep(SLEEP_BETWEEN_DIAS)
    fecha += timedelta(days=1)

# Guardar archivo
df_nuevo = pd.DataFrame(resultados)
df_total = pd.concat([df_existente, df_nuevo]).drop_duplicates(subset=["titular"])
df_total.to_csv("bbc_news_2025.csv", index=False)
print(f"Total de titulares recopilados: {len(df_total)}")

# Subir a Google Drive
if not df_total.empty:
    subir_a_drive("bbc_news_2025.csv", "bbc_news_2025.csv")
