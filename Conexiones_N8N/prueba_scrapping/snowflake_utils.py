# snowflake_utils.py

import snowflake.connector
import pandas as pd
from datetime import datetime, timedelta

def obtener_ultima_fecha_en_snowflake(config):
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
        tabla = f"{config['database']}.{config['schema']}.{config['table']}"
        cs.execute(f"SELECT MAX(fecha) FROM {tabla}")
        resultado = cs.fetchone()
        if resultado and resultado[0]:
            ultima_fecha = datetime.strptime(resultado[0], "%Y%m%d")
            return ultima_fecha + timedelta(days=1)
        else:
            return datetime.strptime("20250101", "%Y%m%d")
    finally:
        cs.close()
        ctx.close()

def subir_a_snowflake(df, config):
    if df.empty:
        print("⚠️ No hay datos para subir a Snowflake.")
        return

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
        tabla_completa = f"{config['database']}.{config['schema']}.{config['table']}"

        cs.execute(f"""
            CREATE TABLE IF NOT EXISTS {tabla_completa} (
                fecha STRING,
                titular STRING,
                url_archivo STRING
            );
        """)

        for _, row in df.iterrows():
            cs.execute(f"""
                INSERT INTO {tabla_completa} (fecha, titular, url_archivo)
                VALUES (%s, %s, %s)
            """, (row['fecha'], row['titular'], row['url_archivo']))

        print(f"✅ {len(df)} filas insertadas en Snowflake.")
    finally:
        cs.close()
        ctx.close()
