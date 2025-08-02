from datetime import timedelta
import pandas as pd
import os
import time

from config import *
from scraper import obtener_snapshot_url, extraer_titulares, log_error
from drive_utils import subir_a_drive

resultados = []
fecha = FECHA_INICIO

# Verificar si existe un CSV válido
if os.path.exists("bbc_news_2025.csv") and os.path.getsize("bbc_news_2025.csv") > 0:
    try:
        df_existente = pd.read_csv("bbc_news_2025.csv")
        fechas_procesadas = set(df_existente['fecha'])
        print(f"Archivo existente encontrado con {len(df_existente)} registros.")
    except Exception as e:
        log_error(f"Error leyendo CSV existente: {e}")
        df_existente = pd.DataFrame()
        fechas_procesadas = set()
else:
    df_existente = pd.DataFrame()
    fechas_procesadas = set()

# Loop sobre fechas
while fecha <= FECHA_FIN:
    fecha_str = fecha.strftime("%Y%m%d")
    if fecha_str in fechas_procesadas:
        print(f"{fecha_str} ya procesado, se omite.")
        fecha += timedelta(days=1)
        continue

    print(f"📅 Procesando {fecha_str}...")
    success = False
    for intento in range(RETRIES):
        try:
            snapshot_url = obtener_snapshot_url(BBC_URL, fecha_str)
            if snapshot_url:
                titulares = extraer_titulares(snapshot_url, fecha_str)
                if titulares:
                    print(f"✅ {len(titulares)} titulares encontrados.")
                else:
                    print(f"⚠️ No se encontraron titulares en el snapshot.")
                resultados.extend(titulares)
                success = True
                break
            else:
                print(f"⚠️ No hay snapshot disponible para {fecha_str}")
                success = True
                break
        except Exception as e:
            log_error(f"Error en {fecha_str} (intento {intento+1}): {e}")
            time.sleep(3)

    if not success:
        log_error(f"Fallo persistente en {fecha_str}, se omite el día.")

    time.sleep(SLEEP_BETWEEN_DIAS)
    fecha += timedelta(days=1)

# Guardar resultados si hay titulares nuevos
if resultados:
    df_nuevo = pd.DataFrame(resultados)
    df_total = pd.concat([df_existente, df_nuevo]).drop_duplicates(subset=["titular"])
    df_total.to_csv("bbc_news_2025.csv", index=False)
    print(f"📝 Total de titulares guardados: {len(df_total)}")
    print(f"⬆️ Subiendo archivo actualizado a Google Drive...")
    subir_a_drive("bbc_news_2025.csv", "bbc_news_2025.csv")
else:
    print("⚠️ No se encontraron titulares nuevos. No se actualiza ni sube el archivo.")
