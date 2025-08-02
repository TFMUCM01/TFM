# main.py

from datetime import datetime, timedelta
import pandas as pd
import os
import time

from config import *
from scraper import obtener_snapshot_url, extraer_titulares, log_error
from snowflake_utils import subir_a_snowflake, obtener_ultima_fecha_en_snowflake

# Fechas dinámicas
FECHA_INICIO = obtener_ultima_fecha_en_snowflake(SNOWFLAKE_CONFIG)
FECHA_FIN = datetime.today()

print(f"📆 Fecha de inicio: {FECHA_INICIO.strftime('%Y-%m-%d')}")
print(f"📆 Fecha de fin:    {FECHA_FIN.strftime('%Y-%m-%d')}")

resultados = []
fecha = FECHA_INICIO

# Cargar CSV si existe
if os.path.exists("bbc_news_2025.csv") and os.path.getsize("bbc_news_2025.csv") > 0:
    df_existente = pd.read_csv("bbc_news_2025.csv")
    fechas_procesadas = set(df_existente['fecha'])
else:
    df_existente = pd.DataFrame()
    fechas_procesadas = set()

# Loop por días
while fecha <= FECHA_FIN:
    fecha_str = fecha.strftime("%Y%m%d")
    if fecha_str in fechas_procesadas:
        print(f"⏩ {fecha_str} ya procesado.")
        fecha += timedelta(days=1)
        continue

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
    df_total = pd.concat([df_existente, df_nuevo]).drop_duplicates(subset=["titular"])
    df_total.to_csv("bbc_news_2025.csv", index=False)
    print(f"📝 Total de titulares en CSV: {len(df_total)}")
    subir_a_snowflake(df_nuevo, SNOWFLAKE_CONFIG)
else:
    print("⚠️ No se encontraron titulares nuevos.")
