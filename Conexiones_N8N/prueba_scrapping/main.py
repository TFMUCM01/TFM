# main.py

from datetime import datetime, timedelta
import pandas as pd
import time

from config import *
from scraper import obtener_snapshot_url, extraer_titulares, log_error
from snowflake_utils import subir_a_snowflake, obtener_ultima_fecha_en_snowflake

# 📅 Rango de fechas: desde última fecha en Snowflake hasta ayer
FECHA_INICIO = obtener_ultima_fecha_en_snowflake(SNOWFLAKE_CONFIG)  # datetime.date
FECHA_FIN = (datetime.today() - timedelta(days=1)).date()           # datetime.date

print(f"📆 Fecha de inicio: {FECHA_INICIO}")
print(f"📆 Fecha de fin:    {FECHA_FIN}")

resultados = []
fecha = FECHA_INICIO

# Loop scraping diario
while fecha <= FECHA_FIN:
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

# Cargar a Snowflake
if resultados:
    df_nuevo = pd.DataFrame(resultados)
    df_nuevo.drop_duplicates(subset=["fecha", "titular"], inplace=True)
    df_nuevo["fecha"] = pd.to_datetime(df_nuevo["fecha"], format="%Y%m%d").dt.date

    subir_a_snowflake(df_nuevo, SNOWFLAKE_CONFIG)
else:
    print("⚠️ No se encontraron titulares nuevos.")
