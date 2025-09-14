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

Se ha creado un **orquestador** con el objetivo de coordinar el proceso de *scraping* por cada medio de comunicación y la posterior carga de los datos en **Snowflake**, evitando la ejecución manual de código desde la documentación. Este flujo tiene como finalidad construir una base de datos de noticias relacionadas con las empresas que cotizan en las principales bolsas de valores europeas.  

Para la orquestación se ha utilizado la herramienta **n8n**, una plataforma de automatización en la que se centraliza el código encargado de realizar las solicitudes a las distintas API’s y de aplicar los modelos de análisis de sentimiento. En particular, se han empleado los siguientes modelos:  

- **bert-base-multilingual-uncased**, para el análisis de noticias en español.  
- **DistilRoberta-financial-sentiment**, especializado en noticias financieras en inglés.  

Estos modelos generan una probabilidad asociada a cada noticia, lo que permite clasificarla como **positiva**, **negativa** o **neutral**. Aunque el detalle de la aplicación de los modelos se desarrolla en una sección posterior, en este apartado se describe el procedimiento de descarga de titulares desde las API’s y la construcción del *data lake* en **Snowflake**.  

---
### Módulos y dependencias

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

---
### Flujo general

1. Para cada noticiero en `config.py`, se obtiene la última fecha almacenada.  
2. Se recorren los días pendientes hasta el día anterior al actual.  
3. Para cada fecha: se resuelve el snapshot, se extraen titulares y se enriquecen con metadatos.  
4. Se suben los nuevos registros a Snowflake.

---
### Preparación del procesamiento por medio
En primer lugar, es necesario verificar la última fecha de carga en **Snowflake**, tomando como referencia el período comprendido desde **enero de 2024 hasta el día inmediatamente anterior**. De esta manera se garantiza la continuidad del proceso y se evitan posibles duplicidades. Este enfoque asegura la disponibilidad de comentarios recientes, lo que permite realizar un análisis actualizado y contribuye a mejorar la capacidad de predicción sobre la evolución del valor de las acciones.  

```{literalinclude} ../../scrapping/snowflake_utils.py
:language: python
:linenos:
:start-after: --8<-- [start:obtener_ultima_fecha_en_snowflake]
:end-before: --8<-- [end:obtener_ultima_fecha_en_snowflake]
```
---

Adicionalmente, es necesario extraer las URL correctas por cada noticiero. Para ello, se ha desarrollado la función **`obtener_snapshot_url(original_url, fecha_str)`**, la cual consulta la API de **Wayback Machine** con el objetivo de recuperar la versión archivada más cercana de una página web en una fecha determinada. El procedimiento consiste en construir la URL de la API a partir de la dirección original y la fecha solicitada, realizar una petición `GET` y procesar la respuesta en formato **JSON**.  
Si existe un *snapshot* disponible, la función devuelve la URL más próxima a la fecha indicada en formato `https`; en caso contrario, retorna `None` mostrando un mensaje informativo.  

```{literalinclude} ../../scrapping/scraper.py
:language: python
:linenos:
:start-after: --8<-- [start:obtener_snapshot_url]
:end-before: --8<-- [end:obtener_snapshot_url]
```
---

### Extracción de titulares

Una vez verificada la última fecha de carga y obtenidas las URL de las API’s, se procede con la solicitud de la información. Ahora se crea la función está diseñada para obtener los titulares de noticias desde una página web archivada en la Wayback Machine. Para ello, accede al contenido del snapshot, identifica los encabezados relevantes dentro del documento HTML y aplica reglas de filtrado para asegurar que los textos extraídos correspondan efectivamente a titulares. En función del medio, se utilizan criterios específicos de validación, y los resultados se almacenan en una lista estructurada.

Módulo de descarga en API’s 
```{literalinclude} ../../scrapping/scraper.py
:language: python
:linenos:
:start-after: --8<-- [start:extraer_titulares]
:end-before: --8<-- [end:extraer_titulares]
```
---

### Carga del DataFrame de titulares de noticieros en el DataLake

Por último, se valida si el **DataFrame** contiene información; en caso de estar vacío, no se ejecuta ninguna acción.  
En caso contrario, se transforma la columna de fecha al tipo de dato adecuado (`datetime.date`) y se establece la conexión con **Snowflake** mediante las credenciales configuradas en el entorno.  

Si la tabla de destino no existe, esta se crea con un esquema predefinido que incluye los campos: **fecha**, **titular de la noticia**, **URL del snapshot**, **fuente** e **idioma**. Posteriormente, se realiza una inserción en bloque de todos los registros del DataFrame, lo que permite almacenar de forma eficiente los titulares extraídos de los medios archivados, garantizando su disponibilidad para procesos posteriores de análisis o visualización.   

| FECHA      | TITULAR                                                                 | URL_ARCHIVO                                                                                     | FUENTE | IDIOMA |
|------------|-------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|--------|--------|
| 2024-01-16 | España desoye a Europa y se resiste a crear un consejo de la productividad | [Link](https://web.archive.org/web/20240119085343/https://www.abc.es/economia/)                 | ABC    | es     |
| 2024-01-16 | Un patinazo de Montoro aboca a Hacienda a pagar devoluciones millonarias a las grandes empresas | [Link](https://web.archive.org/web/20240119085343/https://www.abc.es/economia/) | ABC    | es     |
| 2024-01-16 | Cataluña enfila 2024 con 10.000 empresas fugadas tras el 'procés'       | [Link](https://web.archive.org/web/20240119085343/https://www.abc.es/economia/)                 | ABC    | es     |


___

## Precios por tickers

En el análisis financiero, en primer lugar se necesitan los tickers sobre los que se realizará el estudio. Para este trabajo se han recopilado los tickers de bolsas de valores europeas, que se muestran a continuación. En total son ocho, cada una con sus respectivos tickers, y el análisis se limitará únicamente a las acciones que forman parte de los índices de dichos mercados.

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

### Scraping de índices europeos en TradingView

En una primera etapa resulta necesario extraer los tickers representativos de las principales bolsas europeas. Para ello se emplea la API de TradingView, ampliamente utilizada en el ámbito del análisis financiero, implementando la función scrape_country como componente orquestador del proceso. Esta función integra, por un lado, `fetch_html`, encargada de recuperar el código HTML de las páginas de TradingView, y por otro, `extract_rows_precise`, que a través de la librería BeautifulSoup identifica los nombres y tickers de las compañías, filtra los resultados según el mercado de interés, elimina duplicados y descarta instrumentos no relevantes como ETFs o futuros. Finalmente, scrape_country consolida ambos procedimientos para cada índice bursátil, generando un DataFrame estandarizado con el ticker en formato Yahoo Finance, el nombre depurado de la empresa, el país y el símbolo local, lo que permite conformar una base de datos estructurada y preparada para su posterior almacenamiento en Snowflake y para los análisis financieros avanzados.

```{literalinclude} ../../Yahoo_prueba/tickers_precios_global.py
:language: python
:linenos:
:start-after: --8<-- [start:scrape_country]
:end-before: --8<-- [end:scrape_country]
```

Ejemplos de DataFrame de stickers:

| NOMBRE_EMPRESA                                       | PAIS   | INDEX  | TICKER |
|------------------------------------------------------|--------|--------|--------|
| ACS, ACTIVIDADES DE CONSTRUCCION Y SERVICIOS, S.A.   | España | IBEX35 | ACS    |
| ACERINOX, S.A.                                       | España | IBEX35 | ACX    |
| AENA, S.M.E., S.A.                                   | España | IBEX35 | AENA   |

### Descarga de precios por empresa diaria

Una vez obtenida la lista de tickers, se procede a la consulta de la información histórica en Yahoo Finance mediante la función `download_batch`. Dicha función permite recopilar de forma automatizada los precios diarios de apertura, cierre, máximo y mínimo, además del volumen de acciones negociadas, vinculando cada registro con su fecha correspondiente. Posteriormente, los datos son transformados y estandarizados en un formato homogéneo, lo que garantiza su integración sin inconsistencias dentro del flujo de almacenamiento en el DataLake y asegura la disponibilidad de un conjunto de información fiable para el análisis y la modelización posteriores.

```{literalinclude} ../../Yahoo_prueba/tickers_precios_global.py
:language: python
:linenos:
:start-after: --8<-- [start:download_batch]
:end-before: --8<-- [end:download_batch]
```

Ejemplos de DataFrame de Precios por accion 

| TICKER | CLOSE       | HIGH        | LOW         | OPEN        | VOLUME   | FECHA     |
|--------|-------------|-------------|-------------|-------------|----------|-----------|
| INGA.AS | 6.59100008 | 6.656000137 | 6.429999828 | 6.5         | 17736691 | 10/23/20  |
| GLE.PA  | 23.84499931| 24.03000069 | 23.65500069 | 23.87000084 | 2111582  | 10/28/24  |
| LAND.L  | 686.2000122| 697.2000122 | 686.2000122 | 690.5999756 | 1574957  | 06/17/21  |

### Carga de los DataFrame de precios por tickers en el DataLake

Finalmente, se lleva a cabo un proceso de verificación de la última fecha de actualización registrada en el DataLake, tanto para los precios como para los tickers disponibles. A partir de dicha referencia temporal, se descargan únicamente los datos faltantes hasta el día inmediatamente anterior, con el fin de mantener una ingesta incremental que garantice la actualización diaria del repositorio. Este enfoque asegura la coherencia y completitud del histórico financiero, a la vez que proporciona una base de datos confiable, estructurada y permanentemente actualizada para los posteriores análisis y modelado mediante técnicas de machine learning.

```{literalinclude} ../../Yahoo_prueba/tickers_precios_global.py
:language: python
:linenos:
:start-after: --8<-- [start:merge_with_temp]
:end-before: --8<-- [end:merge_with_temp]
```

## Estados Financieros por tickers

### Ratios Financieros

Los principales ratios bursátiles constituyen herramientas fundamentales para evaluar cómo el mercado valora a una empresa y qué expectativas existen sobre su desempeño. El PER (Price to Earnings Ratio), tanto en su versión histórica (trailing) como en la proyectada (forward), mide cuántas veces los inversores están pagando las utilidades de la compañía; un valor elevado puede reflejar expectativas de crecimiento, mientras que uno reducido puede sugerir infravaloración. El Price to Book (P/B) compara el precio de mercado con el valor contable, permitiendo identificar si la acción cotiza por encima o por debajo de sus activos netos. Por su parte, el EV/EBITDA es un ratio muy utilizado para comparar empresas dentro de un mismo sector, ya que relaciona el valor total de la compañía (incluyendo deuda) con su capacidad operativa de generar beneficios.

El script desarrollado en este proyecto automatiza la obtención de estos indicadores bursátiles desde Yahoo Finance y los almacena en Snowflake de forma estructurada y actualizada. Este procedimiento permite disponer de un repositorio centralizado de ratios de valoración, rentabilidad y dividendos, listo para ser utilizado en análisis posteriores. Al integrarse con los otros módulos implementados, se conforma una base de datos integral que combina perspectivas de mercado, solidez fundamental y sostenibilidad, lo que proporciona una visión completa para la toma de decisiones financieras y de inversión.

```{literalinclude} ../../Yahoo_prueba/financieros_snapshot.py
:language: python
:linenos:
:start-after: --8<-- [start:fetch_snapshot_one]
:end-before: --8<-- [end:fetch_snapshot_one]
```

| TICKER | PE_TRAILING | PE_FORWARD | PRICE_TO_BOOK | EV_TO_EBITDA | DIVIDEND_YIELD | PAYOUT_RATIO | MARKET_CAP   | ENTERPRISE_VALUE | SHARES_OUTSTANDING |
|--------|-------------|------------|---------------|--------------|----------------|--------------|--------------|------------------|--------------------|
| A2A.MI | 8.319231    | 12.016666  | 1.406372      | 6.135        | 4.62           | 0.3868       | 6767140864   | 12491133952      | 3128590080         |
| AAF.L  | 32.62857    | 19.033333  | 321.69016     | 6.584        | 2.15           | 0.71650004   | 8328902656   | 14142440448      | 3646629888         |
| AAL.L  |             | 15.484849  | 157.49245     | 6.569        | 0.94           | 5.6102004    | 27304775680  | 45547601920      | 1068680000         |


### Estados Financieros

Este script es importante porque automatiza la recopilación y almacenamiento de estados financieros anuales (balance, cuenta de resultados y flujos de caja) desde Yahoo Finance, asegurando que en Snowflake solo se guarden los años faltantes y evitando duplicados. Los estados financieros son la base del análisis fundamental, ya que muestran la salud económica de una empresa: el balance indica qué posee y qué debe, la cuenta de resultados revela si es rentable y el estado de flujos de efectivo refleja su liquidez real. A partir de estos datos se calculan ratios clave como ROA, ROE, margen neto y deuda/equity, fundamentales para evaluar la solidez, rentabilidad y riesgos de una compañía, lo que convierte al script en una pieza esencial para integrar análisis financiero sólido dentro de tu proyecto.

```{literalinclude} ../../Yahoo_prueba/financieros_resumen_anual.py
:language: python
:linenos:
:start-after: --8<-- [start:summarize_missing_years]
:end-before: --8<-- [end:summarize_missing_years]
```

| TICKER | YEAR | ASSETS       | LIABILITIES   | EQUITY      | REVENUE      | EXPENSES     | NET_INCOME  | OPERATING_CF | INVESTING_CF | FINANCING_CF | FREE_CF     | NET_MARGIN | ROA   | ROE   | DEBT_EQUITY |
|--------|------|--------------|---------------|-------------|--------------|--------------|-------------|--------------|--------------|--------------|-------------|------------|-------|-------|-------------|
| A2A.MI | 2021 | 18008000000  | 13705000000   | 3760000000  | 11352000000  | 10848000000  | 504000000   | 1135000000   | -1595000000  | 412000000    | 61000000    | 0.044      | 0.028 | 0.134 | 3.645       |
| A2A.MI | 2022 | 21367000000  | 16900000000   | 3899000000  | 22938000000  | 22537000000  | 401000000   | 1260000000   | -1142000000  | 1502000000   | 20000000    | 0.017      | 0.019 | 0.103 | 4.334       |
| A2A.MI | 2023 | 18798000000  | 13996000000   | 4240000000  | 14492000000  | 13833000000  | 659000000   | 1040000000   | -1359000000  | -636000000   | -336000000  | 0.045      | 0.035 | 0.155 | 3.301       |

### Análisis de Sostenibilidad ESG 

ESG son las siglas de Environmental, Social and Governance (Medioambiental, Social y Gobernanza). Es un conjunto de criterios que se usan para evaluar a las empresas más allá de sus resultados financieros.
1. Environmental (Medioambiental): mide el impacto que tiene la empresa sobre el medio ambiente. Ejemplos: emisiones de CO₂, gestión de residuos, eficiencia energética, uso de energías renovables.

2. Social (Social): evalúa cómo la empresa se relaciona con empleados, clientes, comunidades y sociedad en general. Ejemplos: condiciones laborales, diversidad e inclusión, derechos humanos, impacto en la comunidad.

3. Governance (Gobernanza): analiza cómo se gestiona y dirige la empresa. Ejemplos: independencia del consejo de administración, ética corporativa, transparencia, políticas contra la corrupción.

```{literalinclude} ../../Yahoo_prueba/financieros_esg_snapshot.py
:language: python
:linenos:
:start-after: --8<-- [start:fetch_esg_one]
:end-before: --8<-- [end:fetch_esg_one]
```

| TICKER | HAS_ESG | TOTAL_ESG | ENVIRONMENTAL | SOCIAL | GOVERNANCE | CONTROVERSY |
|--------|---------|-----------|---------------|--------|------------|-------------|
| A2A.MI | TRUE    | 20.10     | 10.59         | 4.90   | 4.61       | 1           |
| AAF.L  | TRUE    | 22.69     | 6.52          | 11.82  | 4.35       | 2           |
| AAL.L  | TRUE    | 25.79     | 14.22         | 8.96   | 2.62       | 3           |
