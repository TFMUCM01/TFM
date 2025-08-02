from datetime import timedelta
import pandas as pd
import os
import time

from config import *
from scraper import obtener_snapshot_url, extraer_titulares, log_error
from snowflake_utils import subir_a_snowflake

resultados = []
fecha = FECHA_INICIO

# Verificar si existe un CSV válido y cargarlo
if os.path.exists("bbc_news_2025.csv") and os.path.getsize("bbc_news_2025.csv") > 0:
    try:
        df_existente = pd.read_csv("bbc_news_2025.csv")
        fechas_procesadas = set(df_existente['fecha'])
        print(f"📁 Archivo existente con {len(df_existente)} titulares.")
    except Exception as e:
        log_error(f"Error leyendo CSV existente: {e}")
        df_existente = pd.DataFrame()
        fechas_procesadas = set()
else:
    df_existente = pd.DataFrame()
    fechas_procesadas = set()

# Scraping por fecha
while fecha <= FECHA_FIN:
    fecha_str = fecha.strftime("%Y%m%d")
    if fecha_str in fechas_procesadas:
        print(f"⏩ {fecha_str} ya procesado, se omite.")
        fecha += timedelta(days=1)
        continue

    print(f"📆 Procesando {fecha_str}...")
    success = False
    for intento in range(RETRIES):
        try:
            snapshot_url = obtener_snapshot_url(BBC_URL, fecha_str)
            if snapshot_url:
                titulares = extraer_titulares(snapshot_url, fecha_str)
                if titulares:
                    print(f"✅ {len(titulares)} titulares encontrados.")
                else:
                    print("⚠️ Snapshot encontrado pero sin titulares.")
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
        log_error(f"❌ Fallo persistente en {fecha_str}, se omite el día.")

    time.sleep(SLEEP_BETWEEN_DIAS)
    fecha += timedelta(days=1)

# Guardar resultados y subir a Snowflake
if resultados:
    df_nuevo = pd.DataFrame(resultados)
    df_total = pd.concat([df_existente, df_nuevo]).drop_duplicates(subset=["titular"])
    df_total.to_csv("bbc_news_2025.csv", index=False)
    print(f"\n📝 Total de titulares guardados: {len(df_total)}")
    
    # Subir a Snowflake
    subir_a_snowflake(df_nuevo, SNOWFLAKE_CONFIG)
else:
    print("\n⚠️ No se encontraron titulares nuevos. No se sube nada a Snowflake.")
