import snowflake.connector
import pandas as pd

def subir_a_snowflake(df, config):
    if df.empty:
        print("⚠️ No hay datos para subir a Snowflake.")
        return

    # Conexión
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

        # Crear tabla si no existe (nombre completamente calificado)
        cs.execute(f"""
            CREATE TABLE IF NOT EXISTS {tabla_completa} (
                fecha STRING,
                titular STRING,
                url_archivo STRING
            );
        """)

        # Insertar datos
        for _, row in df.iterrows():
            cs.execute(f"""
                INSERT INTO {tabla_completa} (fecha, titular, url_archivo)
                VALUES (%s, %s, %s)
            """, (row['fecha'], row['titular'], row['url_archivo']))

        print(f"✅ {len(df)} filas insertadas en Snowflake.")
    finally:
        cs.close()
        ctx.close()
