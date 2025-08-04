# config.py

# Tiempo entre días de scraping (en segundos)
SLEEP_BETWEEN_DIAS = 2
RETRIES = 3
WAYBACK_TIMEOUT = 30
SNAPSHOT_TIMEOUT = 30

# Lista de noticieros
NOTICIEROS = [
    {
        "nombre": "BBC",
        "url": "https://www.bbc.com/news",
        "fuente": "BBC",
        "idioma": "en",
        "tabla": "BBC_TITULARES"
    },
    {
        "nombre": "ABC",
        "url": "https://www.abc.es/economia/",
        "fuente": "ABC",
        "idioma": "es",
        "tabla": "ABC_TITULARES"
    },
    {
        "nombre": "EL_ECONOMISTA",
        "url": "https://www.eleconomista.es/economia/",
        "fuente": "EL ECONOMISTA",
        "idioma": "es",
        "tabla": "EL_ECONOMISTA_TITULARES"
    },
    {
        "nombre": "BLOOMBERG",
        "url": "https://www.bloomberg.com/europe",
        "fuente": "BLOOMBERG",
        "idioma": "en",
        "tabla": "BLOOMBERG_TITULARES"
    },
    {
        "nombre": "EL_PAIS",
        "url": "https://elpais.com/economia/",
        "fuente": "EL PAIS",
        "idioma": "es",
        "tabla": "EL_PAIS_TITULARES"
    },
    {
        "nombre": "THE_TIMES",
        "url": "https://www.thetimes.com/section/business/",
        "fuente": "THE TIMES",
        "idioma": "en",
        "tabla": "TIME_TITULARES"
    },
    {
        "nombre": "EXPANSION",
        "url": "https://www.expansion.com/",
        "fuente": "EXPANSION",
        "idioma": "es",
        "tabla": "EXPANSION_TITULARES"
    }
]
# Snowflake connection details
SNOWFLAKE_CONFIG = {
    'user': 'tfmgrupo4',
    'password': 'TFMgrupo4ucm01_01#',
    'account': 'WYNIFVB-YE01854',          # ejemplo: xy12345.eu-central-1
    'warehouse': 'TFM_WH',
    'database': 'NOTICIAS_PRUEBA',
    'schema': 'SCRAPING'
}
