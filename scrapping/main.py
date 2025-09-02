# config.py
"""
Configuración del proyecto de scraping de titulares.

- Lee credenciales de Snowflake desde variables de entorno (ideal para GitHub Actions).
- Valida que no falte ninguna variable crítica y falla con mensaje claro.
- Soporta .env en local (si python-dotenv está instalado).
- Normaliza SNOWFLAKE_ACCOUNT para evitar pasar dominios completos.

Variables de entorno esperadas:
  SNOWFLAKE_USER
  SNOWFLAKE_PASSWORD
  SNOWFLAKE_ACCOUNT        # p. ej. wynifvb-ye01854 (sin .snowflakecomputing.com)
  SNOWFLAKE_WAREHOUSE
  SNOWFLAKE_DATABASE
  SNOWFLAKE_SCHEMA
"""

from __future__ import annotations
import os
import sys

# ---- Soporte opcional de .env en local (no requerido en GitHub Actions)
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()  # solo carga si existe un .env
except Exception:
    # Si no está instalado dotenv, simplemente seguimos.
    pass

# ---- Parámetros de ejecución
SLEEP_BETWEEN_DIAS = 2
RETRIES = 3
WAYBACK_TIMEOUT = 30
SNAPSHOT_TIMEOUT = 30

# ---- Lista de noticieros
NOTICIEROS = [
    {
        "nombre": "BBC",
        "url": "https://www.bbc.com/news",
        "fuente": "BBC",
        "idioma": "en",
        "tabla": "BBC_TITULARES",
    },
    {
        "nombre": "ABC",
        "url": "https://www.abc.es/economia/",
        "fuente": "ABC",
        "idioma": "es",
        "tabla": "ABC_TITULARES",
    },
    {
        "nombre": "EL_ECONOMISTA",
        "url": "https://www.eleconomista.es/economia/",
        "fuente": "EL ECONOMISTA",
        "idioma": "es",
        "tabla": "EL_ECONOMISTA_TITULARES",
    },
    {
        "nombre": "BLOOMBERG",
        "url": "https://www.bloomberg.com/europe",
        "fuente": "BLOOMBERG",
        "idioma": "en",
        "tabla": "BLOOMBERG_TITULARES",
    },
    {
        "nombre": "EL_PAIS",
        "url": "https://elpais.com/economia/",
        "fuente": "EL PAIS",
        "idioma": "es",
        "tabla": "EL_PAIS_TITULARES",
    },
    {
        "nombre": "THE_TIMES",
        "url": "https://www.thetimes.com/",
        "fuente": "THE TIMES",
        "idioma": "en",
        "tabla": "THE_TIMES_TITULARES",   # <- corregido (antes decía TIME_TITULARES)
    },
    {
        "nombre": "EXPANSION",
        "url": "https://www.expansion.com/",
        "fuente": "EXPANSION",
        "idioma": "es",
        "tabla": "EXPANSION_TITULARES",
    },
]

# ---- Utilidades de validación y normalización
def _must_get(env_name: str) -> str:
    """Obtiene una variable de entorno o lanza error explicando qué falta."""
    v = os.getenv(env_name)
    if not v:
        print(
            f"[CONFIG] Falta la variable de entorno {env_name}. "
            f"Define el Secret en GitHub Actions (Settings → Secrets and variables → Actions) "
            f"o crea un .env en local.",
            file=sys.stderr,
        )
        raise RuntimeError(f"Missing env var: {env_name}")
    return v

def _norm_account(account: str) -> str:
    """
    Normaliza el identificador de cuenta de Snowflake:
    - Quita sufijos como '.snowflakecomputing.com'
    - Quita protocolos, si los hubiera (https://)
    - Devuelve en minúsculas
    """
    acc = account.strip().lower()
    # elimina protocolo si lo pusieron
    if "://" in acc:
        acc = acc.split("://", 1)[1]
    # si vino con dominio completo, nos quedamos con la parte previa
    acc = acc.split("/", 1)[0]  # por si trajera paths
    acc = acc.replace(".snowflakecomputing.com", "")
    return acc

# ---- Configuración de Snowflake
SNOWFLAKE_CONFIG = {
    "user": _must_get("SNOWFLAKE_USER"),
    "password": _must_get("SNOWFLAKE_PASSWORD"),
    "account": _norm_account(_must_get("SNOWFLAKE_ACCOUNT")),  # ej: wynifvb-ye01854
    "warehouse": _must_get("SNOWFLAKE_WAREHOUSE"),
    "database": _must_get("SNOWFLAKE_DATABASE"),
    "schema": _must_get("SNOWFLAKE_SCHEMA"),
}

# (Opcional) Depuración controlada por env (no imprime password)
if os.getenv("DEBUG_CONFIG", "").lower() in ("1", "true", "yes"):
    safe_cfg = {k: ("***" if k == "password" else v) for k, v in SNOWFLAKE_CONFIG.items()}
    print("[DEBUG] SNOWFLAKE_CONFIG:", safe_cfg)
