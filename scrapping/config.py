# config.py

"""
config.py
==========

Archivo de configuración del proyecto de scraping de titulares.  
Contiene parámetros globales que controlan el comportamiento del scraping
y la conexión a Snowflake.

Parámetros de ejecución
----------------------
- SLEEP_BETWEEN_DIAS (int): tiempo de espera entre días de scraping (en segundos).
- RETRIES (int): número de reintentos en caso de fallo de descarga.
- WAYBACK_TIMEOUT (int): tiempo máximo de espera para obtener un snapshot del Wayback Machine.
- SNAPSHOT_TIMEOUT (int): tiempo máximo de espera para procesar un snapshot.

Lista de noticieros
------------------
NOTICIEROS (list[dict]): cada diccionario representa un medio de comunicación y contiene:
- nombre (str): nombre del medio.
- url (str): URL base del medio.
- fuente (str): identificador único de la fuente.
- idioma (str): idioma de los titulares.
- tabla (str): nombre de la tabla destino en Snowflake.
"""

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
        "url": "https://www.thetimes.com/",
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
"""
Configuración de Snowflake
--------------------------
SNOWFLAKE_CONFIG (dict): parámetros de conexión a la base de datos Snowflake:
- user (str): usuario de la base de datos.
- password (str): contraseña.
- account (str): identificador de la cuenta Snowflake.
- warehouse (str): warehouse a usar.
- database (str): nombre de la base de datos.
- schema (str): esquema donde se almacenan los datos.
"""
SNOWFLAKE_CONFIG = {
    'user': 'tfmgrupo4',
    'password': 'TFMgrupo4ucm01_01#',
    'account': 'WYNIFVB-YE01854',          # ejemplo: xy12345.eu-central-1
    'warehouse': 'TFM_WH',
    'database': 'NOTICIAS_PRUEBA',
    'schema': 'SCRAPING'
}
