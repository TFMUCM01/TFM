Este módulo actúa como **orquestador** del proyecto. Su función principal es coordinar el proceso de scraping y la carga de datos en Snowflake.
# `main_borrar.py`
---

## Módulos y dependencias

- **`config.py`**  
  - `NOTICIEROS`: lista de medios (URL, fuente, idioma, tabla destino)  
  - `SNOWFLAKE_CONFIG`: credenciales y parámetros de conexión  
  - `RETRIES`, `SLEEP_BETWEEN_DIAS`: reintentos y pausas

- **`scraper.py`**  
  - `obtener_snapshot_url_directo(url, fecha)`  
  - `extraer_titulares(url, fecha, fuente)`  
  - `log_error(msg)`

- **`snowflake_utils.py`**  
  - `obtener_ultima_fecha_en_snowflake(config, tabla)`  
  - `subir_a_snowflake(df, config, tabla)`

## Flujo general

1. Para cada noticiero en `config.py`, se obtiene la última fecha almacenada.  
2. Se recorren los días pendientes hasta el día anterior al actual.  
3. Para cada fecha:
   - Se obtiene la URL del snapshot.  
   - Se extraen los titulares.  
   - Se limpian duplicados y se añaden metadatos.  
4. Se suben los nuevos registros a Snowflake.

---

## Preparación del procesamiento por medio

En primer lugar, recorremos el documento `NOTICIEROS` donde tenemos todas nuevas url para el analiosos y extraemos `nombre`, `url`, `fuente`, `idioma` y tabla para parametrizar el flujo por periódico. Luego verificamos la ultima fecha de solicitud que esta en Snowflake para no repetir noticias de dia anteriores evitando duplicados siempre teniendo en cuenta que se esta extrayendo noticias de ayer

```{literalinclude} ../../main_borrar.py
:language: python
:linenos:
:start-after: --8<-- [start:configfechas-noticieros]
:end-before: --8<-- [end:configfechas-noticieros]

## Extracción de titulares

Creamos un bucle para ir en cada uno de los url de los noticiero descargando los titulares yverificando que estos esten en el formato correco dia por dia. 

```{literalinclude} ../../main_borrar.py
:language: python
:linenos:
:start-after: --8<-- [start:extraer-titulares]
:end-before: --8<-- [end:extraer-titulares]

## Carga en snowflake

Por ultimo, cargaremos las extracciones en Snowflake para el analisis de las tablas de nos noticieros. Esto para luego analisar los titulos y como influyen en el precio de las acciones.

```{literalinclude} ../../main_borrar.py
:language: python
:linenos:
:start-after: --8<-- [start:subida-snowflake]
:end-before: --8<-- [end:subida-snowflake]