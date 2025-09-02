# config.py

"""
Archivo de configuración del scraping de titulares.
Ahora las credenciales de Snowflake se leen de variables de entorno
(proporcionadas por GitHub Secrets en CI/CD).
"""

import os

# Parámetros de ejecución
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

# Configuración de Snowflake desde variables de entorno
SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA1')
}
