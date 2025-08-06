# snowflake_utils.py

import snowflake.connector
import pandas as pd
from datetime import datetime, timedelta
#prueba
def obtener_ultima_fecha_en_snowflake(config, tabla):
    ctx = snowflake.connector.connect(
        user=config['user'],
        password=config['password'],
        account=config['account'],
        warehouse=config['warehouse'],
        database=config['database'],
        schema=config['schema']
    )
    cs = ctx.cursor()
    try:
        tabla_completa = f"{config['database']}.{config['schema']}.{tabla}"
        cs.execute(f"SELECT MAX(fecha) FROM {tabla_completa}")
        resultado = cs.fetchone()
        if resultado and resultado[0]:
            ultima_fecha = resultado[0]
            print(f"📌 Última fecha en Snowflake para {tabla}: {ultima_fecha}")
            return ultima_fecha + timedelta(days=1)
        else:
            print(f"⚠️ No se encontraron registros en {tabla}. Iniciando desde 2024-01-01.")
            return datetime.strptime("20240101", "%Y%m%d").date()
    finally:
        cs.close()
        ctx.close()

def subir_a_snowflake(df, config, tabla):
    if df.empty:
        print(f"⚠️ No hay datos para subir a {tabla}.")
        return

    # Convertir 'fecha' a datetime.date
    df["fecha"] = pd.to_datetime(df["fecha"], format="%Y%m%d").dt.date

    ctx = snowflake.connector.connect(
        user=config['user'],
        password=config['password'],
        account=config['account'],
        warehouse=config['warehouse'],
        database=config['database'],
        schema=config['schema']
    )
    cs = ctx.cursor()
    try:
        tabla_completa = f"{config['database']}.{config['schema']}.{tabla}"

        # Crear tabla si no existe
        cs.execute(f"""
            CREATE TABLE IF NOT EXISTS {tabla_completa} (
                fecha DATE,
                titular STRING,
                url_archivo STRING,
                fuente STRING,
                idioma STRING
            );
        """)

        insert_query = f"""
            INSERT INTO {tabla_completa} (fecha, titular, url_archivo, fuente, idioma)
            VALUES (%s, %s, %s, %s, %s)
        """
        rows_to_insert = df[["fecha", "titular", "url_archivo", "fuente", "idioma"]].values.tolist()
        cs.executemany(insert_query, rows_to_insert)

        print(f"✅ {len(df)} filas insertadas en {tabla}.")
    finally:
        cs.close()
        ctx.close()
