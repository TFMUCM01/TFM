from datetime import datetime, timedelta
import pandas as pd
import time
import sys

from config import NOTICIEROS, SNOWFLAKE_CONFIG, RETRIES, SLEEP_BETWEEN_DIAS
from scraper import obtener_snapshot_url_directo, extraer_titulares, log_error
from snowflake_utils import subir_a_snowflake, obtener_ultima_fecha_en_snowflake

# Mostrar config básica (sin password) para depuración
print("=== CONFIG SCRAPER ===")
print(f"Account:   {SNOWFLAKE_CONFIG.get('account')}")
print(f"Database:  {SNOWFLAKE_CONFIG.get('database')}")
print(f"Schema:    {SNOWFLAKE_CONFIG.get('schema')}  (esperado desde SNOWFLAKE_SCHEMA1)")
print(f"Warehouse: {SNOWFLAKE_CONFIG.get('warehouse')}")
print("=======================")

if not SNOWFLAKE_CONFIG.get('schema'):
    print("[ERROR] El schema está vacío. Asegúrate de definir el secret SNOWFLAKE_SCHEMA1.", file=sys.stderr)
    sys.exit(1)

# --8<-- [start:configfechas-noticieros]
for medio in NOTICIEROS:
    nombre = medio["nombre"]
    url = medio["url"]
    fuente = medio["fuente"]
    idioma = medio["idioma"]
    tabla = medio["tabla"]

    print(f"\nProcesando noticiero: {nombre} ({fuente})")

    FECHA_INICIO = obtener_ultima_fecha_en_snowflake(SNOWFLAKE_CONFIG, tabla)
    FECHA_FIN = datetime.today().date() - timedelta(days=1)

    print(f"Fecha de inicio: {FECHA_INICIO}")
    print(f"Fecha de fin:    {FECHA_FIN}")

    fecha = datetime.combine(FECHA_INICIO, datetime.min.time())
    fecha_fin_dt = datetime.combine(FECHA_FIN, datetime.min.time())
# --8<-- [end:configfechas-noticieros]

    resultados = []
# --8<-- [start:extraer-titulares]
    while fecha <= fecha_fin_dt:
        fecha_str = fecha.strftime("%Y%m%d")
        print(f"[{fuente}] Procesando {fecha_str}...")

        snapshot_url = obtener_snapshot_url_directo(url, fecha_str)
        print(f"Snapshot for {fecha_str}: {snapshot_url}")

        try:
            titulares = extraer_titulares(snapshot_url, fecha_str, fuente=fuente)
            for t in titulares:
                t["fuente"] = fuente
                t["idioma"] = idioma
            if titulares:
                print(f"{len(titulares)} titulares encontrados.")
            else:
                print("Snapshot sin titulares.")
            resultados.extend(titulares)
        except Exception as e:
            log_error(f"[{fuente}] Error en {fecha_str}: {e}")
            print(f"[{fuente}] Error en {fecha_str}: {e}")

        time.sleep(SLEEP_BETWEEN_DIAS)
        fecha += timedelta(days=1)
# --8<-- [end:extraer-titulares]

# --8<-- [start:subida-snowflake]
    if resultados:
        df_nuevo = pd.DataFrame(resultados)
        df_nuevo.drop_duplicates(subset=["fecha", "titular"], inplace=True)
        print(f"Subiendo {len(df_nuevo)} filas a {SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{tabla} ...")
        subir_a_snowflake(df_nuevo, SNOWFLAKE_CONFIG, tabla)
        print(f"Total titulares subidos para {fuente}: {len(df_nuevo)}")
    else:
        print(f"No se encontraron titulares nuevos para {fuente}.")
# --8<-- [end:subida-snowflake]
