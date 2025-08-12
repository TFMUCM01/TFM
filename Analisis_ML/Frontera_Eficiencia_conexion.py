import numpy as np
import pandas as pd
import snowflake.connector
import matplotlib.pyplot as plt 
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px


# Conexión a Snowflake
conn = snowflake.connector.connect(
    user='TFMGRUPO4',
    password='TFMgrupo4ucm01_01#',
    account='WYNIFVB-YE01854',
    warehouse='COMPUTE_WH',
    database='YAHOO_FINANCE',
    schema='MACHINE_LEARNING',
    role='ACCOUNTADMIN'
)

# Tickers del IBEX 35 (5 principales)
tickers = [
    "IBE.MC",    # Iberdrola (Energía)
    "ITX.MC",    # Inditex (Textil/Retail)
    "TEF.MC",    # Telefónica (Telecomunicaciones)
    "BBVA.MC",   # BBVA (Banca)
    "SAN.MC",    # Santander (Banca)
    "REP.MC",    # Repsol (Petróleo y Gas)
    "AENA.MC",   # Aena (Aeropuertos/Infraestructura)
    "IAG.MC",    # International Airlines Group (Aerolíneas)
    "ENG.MC",    # Enagás (Infraestructura energética)
    "ACS.MC",    # ACS (Construcción e Infraestructura)
    "FER.MC",    # Ferrovial (Infraestructura y Construcción)
    "CABK.MC",   # CaixaBank (Banca)
    "ELE.MC",    # Endesa (Energía eléctrica)
    "MAP.MC"     # Mapfre (Seguros)
]
fecha_inicio = "2020-01-01"
fecha_fin = "2024-12-31"


# Query para extraer datos de Snowflake
quoted_tickers = ",".join([f"'{ticker}'" for ticker in tickers])
query = f"""
SELECT TICKER, FECHA, CLOSE
FROM TICKERS_INDEX
WHERE TICKER IN ({quoted_tickers})
  AND FECHA BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
  AND CLOSE IS NOT NULL
ORDER BY FECHA, TICKER
"""

# Ejecutar query usando cursor (elimina la advertencia)
cursor = conn.cursor()
cursor.execute(query)
column_names = [desc[0] for desc in cursor.description]
df_prices = pd.DataFrame(cursor.fetchall(), columns=column_names)
cursor.close()
conn.close()
