"""
main.py
========
Este módulo actúa como **orquestador** del proyecto.  
Su función principal es coordinar el proceso de scraping y la carga de datos en Snowflake.
-----------------------------
Explicación por módulo dependencia:
- **config.py**  
  Contiene parámetros de configuración global:
  - `NOTICIEROS`: lista de medios con sus URLs, fuentes, idioma y tabla destino.
  - `SNOWFLAKE_CONFIG`: credenciales y parámetros de conexión a Snowflake.
  - `RETRIES`, `SLEEP_BETWEEN_DIAS`: control de reintentos y pausas.

- **scraper.py**  
  Funciones para interactuar con snapshots de páginas:
  - `obtener_snapshot_url_directo(url, fecha)`: devuelve la URL archivada.
  - `extraer_titulares(url, fecha, fuente)`: extrae titulares de un snapshot.
  - `log_error(msg)`: registra errores durante el scraping.

- **snowflake_utils.py**  
  Funciones para interactuar con la base de datos Snowflake:
  - `obtener_ultima_fecha_en_snowflake(config, tabla)`: consulta última fecha ya cargada.
  - `subir_a_snowflake(df, config, tabla)`: inserta nuevos datos.

Flujo general
-------------

1. Para cada noticiero definido en `config.py`, se obtiene la última fecha almacenada.
2. Se recorren los días pendientes hasta el día anterior al actual.
3. Para cada fecha:
   - Se obtiene la URL del snapshot.
   - Se extraen los titulares.
   - Se limpian duplicados y se enriquecen con metadatos.
4. Finalmente, se suben a Snowflake.

Este script **actúa como coordinador** de las funciones auxiliares de los otros módulos.
"""

from datetime import datetime, timedelta
import pandas as pd
import time

from config import NOTICIEROS, SNOWFLAKE_CONFIG, RETRIES, SLEEP_BETWEEN_DIAS
from scraper import obtener_snapshot_url_directo, extraer_titulares, log_error
from snowflake_utils import subir_a_snowflake, obtener_ultima_fecha_en_snowflake

for medio in NOTICIEROS:
    nombre = medio["nombre"]
    url = medio["url"]
    fuente = medio["fuente"]
    idioma = medio["idioma"]
    tabla = medio["tabla"]

    print(f"\n📡 Procesando noticiero: {nombre} ({fuente})")

    FECHA_INICIO = obtener_ultima_fecha_en_snowflake(SNOWFLAKE_CONFIG, tabla)
    FECHA_FIN = datetime.today().date() - timedelta(days=1)

    print(f"📆 Fecha de inicio: {FECHA_INICIO}")
    print(f"📆 Fecha de fin:    {FECHA_FIN}")

    fecha = datetime.combine(FECHA_INICIO, datetime.min.time())
    fecha_fin_dt = datetime.combine(FECHA_FIN, datetime.min.time())

    resultados = []

    while fecha <= fecha_fin_dt:
        fecha_str = fecha.strftime("%Y%m%d")
        print(f"🔍 [{fuente}] Procesando {fecha_str}...")

        snapshot_url = obtener_snapshot_url_directo(url, fecha_str)
        print(f"📄 Snapshot for {fecha_str}: {snapshot_url}")

        try:
            titulares = extraer_titulares(snapshot_url, fecha_str, fuente=fuente)
            for t in titulares:
                t["fuente"] = fuente
                t["idioma"] = idioma
            if titulares:
                print(f"✅ {len(titulares)} titulares encontrados.")
            else:
                print("⚠️ Snapshot sin titulares.")
            resultados.extend(titulares)
        except Exception as e:
            log_error(f"[{fuente}] Error en {fecha_str}: {e}")

        time.sleep(SLEEP_BETWEEN_DIAS)
        fecha += timedelta(days=1)

    if resultados:
        df_nuevo = pd.DataFrame(resultados)
        df_nuevo.drop_duplicates(subset=["fecha", "titular"], inplace=True)
        subir_a_snowflake(df_nuevo, SNOWFLAKE_CONFIG, tabla)
        print(f"📥 Total titulares subidos para {fuente}: {len(df_nuevo)}")
    else:
        print(f"⚠️ No se encontraron titulares nuevos para {fuente}.")
