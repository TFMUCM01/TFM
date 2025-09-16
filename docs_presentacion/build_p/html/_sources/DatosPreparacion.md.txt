# Datos y Preparación

Este trabajo combina dos fuentes de información complementarias para anticipar el comportamiento de acciones europeas:

Señales de sentimiento obtenidas mediante scraping de titulares de medios generalistas y económicos (BBC, ABC, El Economista, Bloomberg Europa, El País, The Times y Expansión), tras un proceso de depuración y normalización.

Datos cuantitativos de mercado y financieros (OHLCV y estados contables), descargados y consolidados en Snowflake con controles de coherencia.

Ambas fuentes se integraron en un único dataset analítico a nivel (ticker, fecha), que incorpora rendimientos, rezagos, volumen y métricas de sentimiento agregadas. Este dataset constituye la base de los modelos predictivos, aportando señales de compra/venta más robustas al combinar información textual y numérica.

## Titulares de noticieros

Como primera fase, se llevó a cabo la recopilación de titulares procedentes de noticieros financieros y generalistas de acceso público mediante sus plataformas web y archivos digitales, los cuales fueron posteriormente procesados a través de técnicas de análisis de texto con el fin de filtrar e identificar aquellas noticias con mayor relevancia y potencial de impacto en el comportamiento bursátil de las acciones.

Ejemplo de base da datos de titulares:

| fecha    | titular                               | url_archivo                                                                                       |
|----------|----------------------------------------|----------------------------------------------------------------------------------------------------|
| 20250101 | Accessibility Links                    | https://web.archive.org/web/20250101234554/https://www.thetimes.com/                               |
| 20250101 | MiCA, entre la proteccion al usuario y la soberania monetaria  | https://web.archive.org/web/20241227062856/https://elpais.com/economia/    |
| 20250101 | The Year AI Broke Into Music           | https://web.archive.org/web/20250102035656/https://www.bloomberg.com/europe                        |

![Noticia times](../../Imagenes/thetimes.png)


Para la orquestación se ha utilizado la herramienta **n8n**, una plataforma de automatización en la que se centraliza el código encargado de realizar las solicitudes a las distintas API’s y de aplicar los modelos de análisis de sentimiento. En particular, se han empleado los siguientes modelos:  

- **bert-base-multilingual-uncased**, para el análisis de noticias en español.  
- **DistilRoberta-financial-sentiment**, especializado en noticias financieras en inglés.  

Estos modelos generan una probabilidad asociada a cada noticia, lo que permite clasificarla como **positiva**, **negativa** o **neutral**. Aunque el detalle de la aplicación de los modelos se desarrolla en una sección posterior, en este apartado se describe el procedimiento de descarga de titulares desde las API’s y la construcción del *data lake* en **Snowflake**.  

[![Diagrama de Sentimientos](../../Imagenes/DigSentimientos.png)](../../Imagenes/DigSentimientos.png)


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
En caso contrario, se transforma la columna de fecha al tipo de dato adecuado y se establece la conexión con **Snowflake** mediante las credenciales configuradas en el entorno.  

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
| Alemania       | DAX 40     | XETR              | .DE           | 40                |
| Francia        | CAC 40     | EURONEXT          | .PA           | 39               |
| Italia         | FTSE MIB   | MIL               | .MI           | 40               |
| Países Bajos   | AEX        | EURONEXT          | .AS           | 25               |
| Reino Unido    | FTSE 100   | LSE               | .L            | 100              |
| Suecia         | OMXS30     | OMXSTO            | .ST           | 30               |
| Suiza          | SMI        | SIX               | .SW           | 21               |

### Scraping de índices europeos en TradingView
Fuente principal: TradingView, empleada para obtener los tickers de referencia de las principales bolsas europeas.

Ejemplos de DataFrame de stickers:

| NOMBRE_EMPRESA                                       | PAIS   | INDEX  | TICKER |
|------------------------------------------------------|--------|--------|--------|
| ACS, ACTIVIDADES DE CONSTRUCCION Y SERVICIOS, S.A.   | España | IBEX35 | ACS    |
| ACERINOX, S.A.                                       | España | IBEX35 | ACX    |
| AENA, S.M.E., S.A.                                   | España | IBEX35 | AENA   |


PONER FOTO DE TRAIDINGVIEW


### Estados Financieros

Este script es importante porque automatiza la recopilación y almacenamiento de estados financieros anuales (balance, cuenta de resultados y flujos de caja) desde Yahoo Finance, asegurando que en Snowflake solo se guarden los años faltantes y evitando duplicados. Los estados financieros son la base del análisis fundamental, ya que muestran la salud económica de una empresa: el balance indica qué posee y qué debe, la cuenta de resultados revela si es rentable y el estado de flujos de efectivo refleja su liquidez real. A partir de estos datos se calculan ratios clave como ROA, ROE, margen neto y deuda/equity, fundamentales para evaluar la solidez, rentabilidad y riesgos de una compañía, lo que convierte al script en una pieza esencial para integrar análisis financiero sólido dentro de tu proyecto.

PONER LO DE API

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

| TICKER | HAS_ESG | TOTAL_ESG | ENVIRONMENTAL | SOCIAL | GOVERNANCE | CONTROVERSY |
|--------|---------|-----------|---------------|--------|------------|-------------|
| A2A.MI | TRUE    | 20.10     | 10.59         | 4.90   | 4.61       | 1           |
| AAF.L  | TRUE    | 22.69     | 6.52          | 11.82  | 4.35       | 2           |
| AAL.L  | TRUE    | 25.79     | 14.22         | 8.96   | 2.62       | 3           |
