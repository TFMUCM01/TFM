from datetime import datetime

# Rango de fechas (ajústalo para testear primero)
FECHA_INICIO = datetime.strptime("20250725", "%Y%m%d")
FECHA_FIN = datetime.strptime("20250727", "%Y%m%d")

BBC_URL = "https://www.bbc.com/news"

WAYBACK_TIMEOUT = 30
SNAPSHOT_TIMEOUT = 30
RETRIES = 3
SLEEP_BETWEEN_DIAS = 2

# Snowflake connection details
SNOWFLAKE_CONFIG = {
    'user': 'tfmgrupo4',
    'password': 'TFMgrupo4ucm01_01#',
    'account': 'WYNIFVB-YE01854.snowflakecomputing.com',          # ejemplo: xy12345.eu-central-1
    'warehouse': 'TFM_WH',
    'database': 'TFM',
    'schema': 'SCRAPING',
    'table': 'BBC_TITULARES'
}
