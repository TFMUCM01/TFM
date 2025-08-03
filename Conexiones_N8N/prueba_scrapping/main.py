# main.py

from datetime import datetime, timedelta
import pandas as pd
import os
import time

from config import *
from scraper import obtener_snapshot_url, extraer_titulares, log_error
from snowflake_utils import subir_a_snowflake, obtener_ultima_fecha_en_snowflake

# 📅 Definir fechas
FECHA_INICIO = obtener_ultima_fecha_en_snowflake(SNOWFLAKE_CONFIG)  # datetime.date
FECHA_FIN = (datetime.today() - timedelta(days=1)).date()           # datetime.date

print(f"📆 Fecha de inicio: {FECHA_INICIO}")
print(f"📆 Fecha de fin:    {FECHA_FIN}")

resultados = []
fecha = FECHA_INICIO

# Cargar CSV existente si lo hay
if os.path.exists("bbc_news_2025.csv") and os.path.getsize("bbc_news_2025.csv") > 0:
    df_existente = pd.read_csv("bbc_news_2025.csv")
    fechas_procesadas = set(pd.to_datetime(df_existente['fecha']).dt.date)
else:
    df_existente = pd.DataFrame()
    fechas_procesadas = set()

# Loop de scraping por fecha
while fecha <= FECHA_FIN:
    if fecha in fechas_procesadas:
        print(f"⏩ {fecha} ya procesado.")
        fecha += timedelta(days=1)
        continue

    fecha_str = fecha.strftime("%Y%m%d")
    print(f"🔍 Procesando {fecha_str}...")

    success = False
    for intento in range(RETRIES):
        try:
            snapshot_url = obtener_snapshot_url(BBC_URL, fecha_str)
            if snapshot_url:
                titulares = extraer_titulares(snapshot_url, fecha_str)
                if titulares:
                    print(f"✅ {len(titulares)} titulares encontrados.")
                else:
                    print("⚠️ Snapshot sin titulares.")
                resultados.extend(titulares)
                success = True
                break
            else:
                print(f"⚠️ No hay snapshot para {fecha_str}")
                success = True
                break
        except Exception as e:
            log_error(f"Error en {fecha_str} (intento {intento+1}): {e}")
            time.sleep(3)

    if not success:
        log_error(f"❌ Fallo persistente en {fecha_str}")

    time.sleep(SLEEP_BETWEEN_DIAS)
    fecha += timedelta(days=1)

# Guardar y subir resultados
if resultados:
    df_nuevo = pd.DataFrame(resultados)
    df_nuevo.drop_duplicates(subset=["fecha", "titular"], inplace=True)

    # Convertir fechas a formato real antes de guardar
    df_nuevo["fecha"] = pd.to_datetime(df_nuevo["fecha"], format="%Y%m%d").dt.date

    # Actualizar CSV local
    if not df_existente.empty:
        df_existente["fecha"] = pd.to_datetime(df_existente["fecha"]).dt.date
    df_total = pd.concat([df_existente, df_nuevo], ignore_index=True)
    df_total.drop_duplicates(subset=["fecha", "titular"], inplace=True)
    df_total.to_csv("bbc_news_2025.csv", index=False)
    print(f"📝 Total de titulares en CSV: {len(df_total)}")

    # Cargar a Snowflake
    subir_a_snowflake(df_nuevo, SNOWFLAKE_CONFIG)
else:
    print("⚠️ No se encontraron titulares nuevos.")
