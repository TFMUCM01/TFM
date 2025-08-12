import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import textwrap
import snowflake.connector

# Solicitar fechas y tickers al usuario
fecha_inicio = "2020-01-01"
fecha_fin = "2024-12-31"
tickers = ["BBVA.MC"]


# Conectar a Snowflake
conn = snowflake.connector.connect(
    user='TFMGRUPO4',
    password='TFMgrupo4ucm01_01#',
    account='WYNIFVB-YE01854',
    warehouse='COMPUTE_WH',
    database='YAHOO_FINANCE',
    schema='MACHINE_LEARNING',
    role='ACCOUNTADMIN'
)

# Mostrar tickers disponibles
query_tickers = """
    SELECT DISTINCT TICKER 
    FROM TICKERS_INDEX
    ORDER BY TICKER
"""
cursor = conn.cursor()
cursor.execute(query_tickers)
available_tickers = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
#print("Tickers disponibles:")
#print(available_tickers)

# Consulta para extraer precios
query_data = f"""
    SELECT TICKER, FECHA, CLOSE, OPEN, HIGH, LOW, VOLUME
    FROM TICKERS_INDEX
    WHERE FECHA BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    AND TICKER IN ({','.join([f"'{s}'" for s in tickers])})
    ORDER BY FECHA
"""
cursor.execute(query_data)
df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

cursor.close()
conn.close()