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

Se ha creado un **orquestador** con el objetivo de coordinar el proceso de *scraping* por cada medio de comunicación y la posterior carga de los datos en **Snowflake**, evitando la ejecución manual de código desde la documentación. Este flujo tiene como finalidad construir una base de datos de noticias relacionadas con las empresas que cotizan en las principales bolsas de valores europeas.  

Para la orquestación se ha utilizado la herramienta **n8n**, una plataforma de automatización en la que se centraliza el código encargado de realizar las solicitudes a las distintas API’s y de aplicar los modelos de análisis de sentimiento. En particular, se han empleado los siguientes modelos:  

- **bert-base-multilingual-uncased**, para el análisis de noticias en español.  
- **DistilRoberta-financial-sentiment**, especializado en noticias financieras en inglés.  

Estos modelos generan una probabilidad asociada a cada noticia, lo que permite clasificarla como **positiva**, **negativa** o **neutral**. Aunque el detalle de la aplicación de los modelos se desarrolla en una sección posterior, en este apartado se describe el procedimiento de descarga de titulares desde las API’s y la construcción del *data lake* en **Snowflake**.  


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
En primer lugar, es necesario verificar la última fecha de carga en **Snowflake**, tomando como referencia el período comprendido desde **enero de 2024 hasta el día inmediatamente anterior**. De esta manera se garantiza la continuidad del proceso y se evitan posibles duplicidades. Este enfoque asegura la disponibilidad de comentarios recientes, lo que permite realizar un análisis actualizado y contribuye a mejorar la capacidad de predicción sobre la evolución del valor de las acciones.  

```{literalinclude} ../../scrapping/snowflake_utils.py
:language: python
:linenos:
:start-after: --8<-- [start:obtener_ultima_fecha_en_snowflake]
:end-before: --8<-- [end:obtener_ultima_fecha_en_snowflake]
```

Adicionalmente, es necesario extraer las URL correctas por cada noticiero. Para ello, se ha desarrollado la función **`obtener_snapshot_url(original_url, fecha_str)`**, la cual consulta la API de **Wayback Machine** con el objetivo de recuperar la versión archivada más cercana de una página web en una fecha determinada. El procedimiento consiste en construir la URL de la API a partir de la dirección original y la fecha solicitada, realizar una petición `GET` y procesar la respuesta en formato **JSON**.  
Si existe un *snapshot* disponible, la función devuelve la URL más próxima a la fecha indicada en formato `https`; en caso contrario, retorna `None` mostrando un mensaje informativo.  

```{literalinclude} ../../scrapping/scraper.py
:language: python
:linenos:
:start-after: --8<-- [start:obtener_snapshot_url]
:end-before: --8<-- [end:obtener_snapshot_url]
```

**Extracción de titulares**

Una vez verificada la última fecha de carga y obtenidas las URL de las API’s, se procede con la solicitud de la información. Ahora se crea la función está diseñada para obtener los titulares de noticias desde una página web archivada en la Wayback Machine. Para ello, accede al contenido del snapshot, identifica los encabezados relevantes dentro del documento HTML y aplica reglas de filtrado para asegurar que los textos extraídos correspondan efectivamente a titulares. En función del medio, se utilizan criterios específicos de validación, y los resultados se almacenan en una lista estructurada.

Módulo de descarga en API’s 
```{literalinclude} ../../scrapping/scraper.py
:language: python
:linenos:
:start-after: --8<-- [start:extraer_titulares]
:end-before: --8<-- [end:extraer_titulares]
```

**Carga en Snowflake**

Por último, se valida si el **DataFrame** contiene información; en caso de estar vacío, no se ejecuta ninguna acción.  
En caso contrario, se transforma la columna de fecha al tipo de dato adecuado (`datetime.date`) y se establece la conexión con **Snowflake** mediante las credenciales configuradas en el entorno.  

Si la tabla de destino no existe, esta se crea con un esquema predefinido que incluye los campos: **fecha**, **titular de la noticia**, **URL del snapshot**, **fuente** e **idioma**. Posteriormente, se realiza una inserción en bloque de todos los registros del DataFrame, lo que permite almacenar de forma eficiente los titulares extraídos de los medios archivados, garantizando su disponibilidad para procesos posteriores de análisis o visualización.   

| FECHA      | TITULAR                                                                 | URL_ARCHIVO                                                                                     | FUENTE | IDIOMA |
|------------|-------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|--------|--------|
| 2024-01-16 | España desoye a Europa y se resiste a crear un consejo de la productividad | [Link](https://web.archive.org/web/20240119085343/https://www.abc.es/economia/)                 | ABC    | es     |
| 2024-01-16 | Un patinazo de Montoro aboca a Hacienda a pagar devoluciones millonarias a las grandes empresas | [Link](https://web.archive.org/web/20240119085343/https://www.abc.es/economia/) | ABC    | es     |
| 2024-01-16 | Cataluña enfila 2024 con 10.000 empresas fugadas tras el 'procés'       | [Link](https://web.archive.org/web/20240119085343/https://www.abc.es/economia/)                 | ABC    | es     |




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










