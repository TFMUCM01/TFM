BBC_URL = "https://www.bbc.com/news"

WAYBACK_TIMEOUT = 30
SNAPSHOT_TIMEOUT = 30
RETRIES = 3
SLEEP_BETWEEN_DIAS = 2

# Snowflake connection details
SNOWFLAKE_CONFIG = {
    'user': 'tfmgrupo4',
    'password': 'TFMgrupo4ucm01_01#',
    'account': 'WYNIFVB-YE01854',          # ejemplo: xy12345.eu-central-1
    'warehouse': 'TFM_WH',
    'database': 'NOTICIAS_PRUEBA',
    'schema': 'SCRAPING',
    'table': 'BBC_TITULARES'
}
