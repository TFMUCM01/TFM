import snowflake.connector
import pandas as pd

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
        # Establece el contexto
        cs.execute(f"USE DATABASE {config['database']}")
        cs.execute(f"USE SCHEMA {config['schema']}")

        # Crea la tabla si no existe
        cs.execute(f"""
            CREATE TABLE IF NOT EXISTS {config['database']}.{config['schema']}.{config['table']} (
                fecha STRING,
                titular STRING,
                url_archivo STRING
            );
        """)

        # Inserta datos nuevos
        for _, row in df.iterrows():
            cs.execute(f"""
                INSERT INTO {config['database']}.{config['schema']}.{config['table']} (fecha, titular, url_archivo)
                VALUES (%s, %s, %s)
            """, (row['fecha'], row['titular'], row['url_archivo']))

        print(f"✅ {len(df)} filas insertadas en Snowflake.")
    finally:
        cs.close()
        ctx.close()
