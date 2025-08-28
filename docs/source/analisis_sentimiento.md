# Datos y Preparación

Este trabajo integra dos fuentes complementarias para anticipar el comportamiento del precio de acciones europeas: señales de sentimiento extraídas de titulares de prensa y información cuantitativa de mercado y estados financieros. Los titulares se obtuvieron mediante scraping y de medios generalistas y económicos —BBC, ABC, El Economista, Bloomberg Europa, El País, The Times y Expansión—. Sobre estos textos se aplicó una depuración sistemática (eliminación de duplicados y de ruidos como elementos de navegación, normalización de caracteres/idiomas y control de fechas) para construir indicadores diarios de sentimiento por medio e instrumento.

En paralelo, los datos de mercado (OHLCV por ticker) se descargaron y consolidaron en Snowflake, armonizando formatos de fecha y símbolos, verificando huecos y coherencia (festivos, sesiones sin volumen) y, cuando procede, incorporando información contable resumida por compañía. Finalmente, se integraron ambas fuentes en un único dataset analítico a nivel (ticker, fecha) que incluye rendimientos y rezagos, volumen y métricas de sentimiento agregadas. Este conjunto sirve de base para los modelos predictivos, combinando la dimensión informativa de las noticias con la evidencia numérica del mercado para obtener señales de compra/venta más robustas.

## Titulares de noticieros

Como primera fase, se recopilaron titulares de noticieros financieros y generalistas de acceso público —entre ellos El País, The Times y Bloomberg— a través de sus respectivas fuentes web y archivos digitales. Posteriormente, estos textos fueron procesados mediante técnicas de análisis de texto <span style="color:red">[aquí se puede especificar el método exacto]</span> , con el objetivo de identificar noticias relevantes capaces de influir en el comportamiento bursátil de las acciones.

Ejemplo de base da datos de titulares:

| fecha    | titular                               | url_archivo                                                                                       |
|----------|----------------------------------------|----------------------------------------------------------------------------------------------------|
| 20250101 | Accessibility Links                    | https://web.archive.org/web/20250101234554/https://www.thetimes.com/                               |
| 20250101 | MiCA, entre la proteccion al usuario y la soberania monetaria  | https://web.archive.org/web/20241227062856/https://elpais.com/economia/    |
| 20250101 | The Year AI Broke Into Music           | https://web.archive.org/web/20250102035656/https://www.bloomberg.com/europe                        |

![Noticia times](../../Imagenes/thetimes.png)

### Descarga de titulos de noticias

Crearemos un **orquestador** del proyecto para coordina el scraping por medio y la carga en Snowflake sin ejecutar código desde la documentación. Esto para generar una base de datos de las noticias sobre las empresas que cotizan en la bolsa de valores europea

**Módulos y dependencias**

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

**Flujo general**

1. Para cada noticiero en `config.py`, se obtiene la última fecha almacenada.  
2. Se recorren los días pendientes hasta el día anterior al actual.  
3. Para cada fecha: se resuelve el snapshot, se extraen titulares y se enriquecen con metadatos.  
4. Se suben los nuevos registros a Snowflake.

**Preparación del procesamiento por medio**
Primeramente es es necesario recuperar el estado histórico de páginas web —por ejemplo, tasas de interés, condiciones de préstamos, o términos y condiciones de productos financieros— tal como estaban publicadas en una fecha específica. La función se conecta a la Wayback Machine, un archivo de internet, para buscar y obtener la versión más cercana de una URL pública en una fecha dada (fecha_str). Si existe un snapshot disponible, devuelve la URL del contenido archivado (en https para mayor seguridad). Esto permite validar o comparar condiciones actuales con las pasadas, asegurando trazabilidad y respaldo en auditorías o investigaciones. Si no se encuentra un snapshot o hay un error técnico, se maneja de forma controlada, registrando el fallo para análisis posterior.

```{literalinclude} ../../scraper.py
:language: python
:linenos:
:start-after: --8<-- [start:obtener_snapshot_url]
:end-before: --8<-- [end:obtener_snapshot_url]
```

**Extracción de titulares**

Esta función forma parte de un sistema de monitoreo financiero que recopila titulares históricos de medios digitales desde snapshots archivados, lo cual permite analizar cómo evolucionó la narrativa mediática sobre eventos económicos relevantes (como crisis, inflación, tasas de interés, etc.). A partir de una URL archivada (snapshot_url) y una fecha (fecha_str), la función descarga el contenido HTML y extrae encabezados (h1, h2, h3) usando BeautifulSoup. Si la fuente es un medio específico como THE TIMES, aplica filtros adicionales para identificar titulares relevantes según clases CSS particulares. En cualquier otro caso, guarda los encabezados con más de 3 palabras como posibles titulares. Esto permite generar una línea temporal informativa y confiable para estudios financieros, análisis de percepción pública o validación documental.

```{literalinclude} ../../scraper.py
:language: python
:linenos:
:start-after: --8<-- [start:extraer_titulares]
:end-before: --8<-- [end:extraer_titulares]
```

Solicitud de titulares por fecha
Ahora se necesita que la solicitud de titulares sea al dia anterior y que verifique desde cuando no se hace la consulta, para esto creamos una funcion conectada con snowflake que verifique la ultima fecha de carga dentro de la base de datos, y recuperar la última fecha (MAX(fecha)) registrada. Esto permite determinar desde qué día continuar cargando nuevos datos, asegurando que no se dupliquen registros ni se pierda información. Si encuentra una fecha, suma un día para usarla como nuevo punto de partida. Si no hay datos aún, arranca desde una fecha base fija (1 de enero de 2024).

```{literalinclude} ../../snowflake_utils.py
:language: python
:linenos:
:start-after: --8<-- [start:obtener_ultima_fecha_en_snowflake]
:end-before: --8<-- [end:obtener_ultima_fecha_en_snowflake]
```

**Carga en Snowflake**


Por último se valida si el DataFrame contiene información; si está vacío y no realiza ninguna acción. Convierte la columna fecha al tipo adecuado (datetime.date) y se conecta a Snowflake usando las credenciales del entorno. Si la tabla de destino no existe, la crea con un esquema definido que incluye campos como fecha, titular de noticia, URL del snapshot, fuente e idioma. Luego, inserta en bloque todos los registros del DataFrame, lo que permite almacenar eficientemente los titulares extraídos de medios archivados para su posterior análisis o visualización.

```{literalinclude} ../../snowflake_utils.py
:language: python
:linenos:
:start-after: --8<-- [start:subir_a_snowflake]
:end-before: --8<-- [end:subir_a_snowflake]
```

## Precios por tickers

En el analisis financieron primeramente se necesita los tickers en los que vamos hacer el analisis, por nuestra parte nos hemos dedicado a solicitar los tickers de **Bolsas de valores europeas** que pas podemos ver abajo en total son 8 con sus diferentes tickers y tambien limitaremos solo a las acciones tienen dentro de su portafolio los indices de estos mercados. 

| País           | Índice     | Exchange Aceptado | Sufijo Yahoo | Número Esperado |
|----------------|------------|-------------------|---------------|------------------|
| España         | IBEX 35    | BME               | .MC           | 35               |
| Alemania       | DAX 40     | XETR              | .DE           | 0                |
| Francia        | CAC 40     | EURONEXT          | .PA           | 39               |
| Italia         | FTSE MIB   | MIL               | .MI           | 40               |
| Países Bajos   | AEX        | EURONEXT          | .AS           | 25               |
| Reino Unido    | FTSE 100   | LSE               | .L            | 100              |
| Suecia         | OMXS30     | OMXSTO            | .ST           | 30               |
| Suiza          | SMI        | SIX               | .SW           | 21               |

### Extracción precisa de componentes bursátiles

La función extract_rows_precise toma como entrada un HTML y un conjunto de bolsas aceptadas (accept_exchanges) y extrae de forma precisa una lista de empresas válidas listadas en esas bolsas. Para ello, analiza el HTML con BeautifulSoup, busca descripciones asociadas a acciones (en etiquetas <sup>), navega por el DOM para encontrar enlaces con información del símbolo y la bolsa, filtra entradas no deseadas como índices o ETFs, y devuelve una lista sin duplicados con tuplas que contienen el exchange, el símbolo y el nombre de la empresa. Esta función es útil para extraer componentes limpios y verificados de un índice bursátil desde una fuente web como TradingView. Ha esta funcion le agregaremos los tickers sacado  de la funicion 

```{literalinclude} ../../tickers_global.py
:language: python
:linenos:
:start-after: --8<-- [start:extract_rows_precise]
:end-before: --8<-- [end:extract_rows_precise]
```
## solicitud de 










