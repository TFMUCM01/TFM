# main.py

from datetime import datetime, timedelta
import pandas as pd
import time

from config import NOTICIEROS, SNOWFLAKE_CONFIG, RETRIES, SLEEP_BETWEEN_DIAS
from scraper import obtener_snapshot_url, extraer_titulares, log_error
from snowflake_utils import subir_a_snowflake, obtener_ultima_fecha_en_snowflake

for medio in NOTICIEROS:
    nombre = medio["nombre"]
    url = medio["url"]
    fuente = medio["fuente"]
    idioma = medio["idioma"]
    tabla = medio["tabla"]

    print(f"\n📡 Procesando noticiero: {nombre} ({fuente})")

    # Obtener rango de fechas
    FECHA_INICIO = obtener_ultima_fecha_en_snowflake(SNOWFLAKE_CONFIG, tabla)
    FECHA_FIN = (datetime.today() - timedelta(days=1)).date()

    print(f"📆 Fecha de inicio: {FECHA_INICIO}")
    print(f"📆 Fecha de fin:    {FECHA_FIN}")

    resultados = []
    fecha = FECHA_INICIO

    while fecha <= FECHA_FIN:
        fecha_str = fecha.strftime("%Y%m%d")
        print(f"🔍 [{fuente}] Procesando {fecha_str}...")

        success = False
        for intento in range(RETRIES):
            try:
                snapshot_url = obtener_snapshot_url(url, fecha_str)
                if snapshot_url:
                    titulares = extraer_titulares(snapshot_url, fecha_str)
                    for t in titulares:
                        t["fuente"] = fuente
                        t["idioma"] = idioma
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
                log_error(f"[{fuente}] Error en {fecha_str} (intento {intento+1}): {e}")
                time.sleep(3)

        if not success:
            log_error(f"[{fuente}] ❌ Fallo persistente en {fecha_str}")

        time.sleep(SLEEP_BETWEEN_DIAS)
        fecha += timedelta(days=1)

    # Subir resultados
    if resultados:
        df_nuevo = pd.DataFrame(resultados)
        df_nuevo.drop_duplicates(subset=["fecha", "titular"], inplace=True)
        subir_a_snowflake(df_nuevo, SNOWFLAKE_CONFIG, tabla)
    else:
        print(f"⚠️ No se encontraron titulares nuevos para {fuente}.")
