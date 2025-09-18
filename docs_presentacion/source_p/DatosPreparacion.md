# Datos y Preparación

Este trabajo integra dos fuentes para anticipar el comportamiento de acciones europeas:

- Sentimiento obtenido de titulares de medios económicos y generalistas, tras depuración y normalización.

- Datos de mercado y financieros (OHLCV y contables), consolidados en Snowflake con controles de calidad.

```{figure} ../../Imagenes/FlujoScraping.jpeg
:alt: FlujoScraping 
:width: 100%
:align: center
**Figura 5.** Flujograma del modelo de scrapping
```

## Titulares de noticieros

Como primera fase, se llevó a cabo la recopilación de titulares procedentes de noticieros financieros y generalistas de acceso público mediante sus plataformas web y archivos digitales, los cuales fueron posteriormente procesados a través de técnicas de análisis de texto con el fin de identificar aquellas noticias con mayor relevancia y potencial de impacto en el comportamiento bursátil de las acciones.

```{figure} ../../Imagenes/thetimes.png
:alt: Noticia times
:width: 80%
:align: center
**Figura 6.** Representación de noticieros extraidos
```

Estos modelos generan una probabilidad asociada a cada noticia, lo que permite clasificarla como **positiva**, **negativa** o **neutral**. Aunque el detalle de la aplicación de los modelos se desarrolla en una sección posterior, en este apartado se describe el procedimiento de descarga de titulares desde las API’s y la construcción del *data lake* en **Snowflake**.  

```{figure} ../../Imagenes/DigSentimientos.png
:alt: Diagrama de Sentimientos
:width: 110%
:align: center
**Figura 7.** Flujo de proceso de scrapping
```
En este proceso tenemos el data lake el cual se alimenta nuestros análisis a diario y lo integra en uno solo para que el análisis tanto del visualizador como el bot en telegram tengan la información correspondiente para el cliente.

```{figure} ../../Imagenes/Snowflake_noticias.png
:alt: Snowflake_noticias
:width: 100%
:align: center
:name: fig:snowflake-noticias

**Figura 8.** Base de datos en **Snowflake**
```

Tabla: Noticieros descargados:

| ID                                   | FECHA    | TITULAR                                                         | URL_ARCHIVO                                                                 |
|--------------------------------------|----------|-----------------------------------------------------------------|----------------------------------------------------------------------------|
| cfee822f-4744-413c-b175-06b74a31b269 | 09/15/25 | Why India's Supreme Court is 'a men's club'                     | [Link](https://web.archive.org/web/20250915120000/https://www.bbc.com/news/) |
| 27a933d1-634f-442e-aa1f-e47bfdad666d | 09/15/25 | Pilar 2 y el papel de la OCDE en el panorama fiscal internacional | [Link](https://web.archive.org/web/20250915120000/https://www.expansion.com/) |
| c1d7c991-e8cd-4e90-955f-1c5fe8456739 | 09/15/25 | Así fomenta el empleo el mercado de vehículos de ocasión         | [Link](https://web.archive.org/web/20250915120000/https://elpais.com/economia/) |

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

```{figure} ../../Imagenes/FlujoYahoo.jpeg
:alt: FlujoYahoo 
:width: 100%
:align: center
**Figura 9.** Flujograma del modelo de extracción de datos en Yahoo Finance
```


Ejemplos de DataFrame de stickers:

| NOMBRE_EMPRESA                                       | PAIS   | INDEX  | TICKER |
|------------------------------------------------------|--------|--------|--------|
| ACS, ACTIVIDADES DE CONSTRUCCION Y SERVICIOS, S.A.   | España | IBEX35 | ACS    |
| ACERINOX, S.A.                                       | España | IBEX35 | ACX    |
| AENA, S.M.E., S.A.                                   | España | IBEX35 | AENA   |

```{figure} ../../Imagenes/TV_componentes.gif
:alt: tradingview
:align: center
:width: 100%
**Figura 10.** Acciones que actualmente pertenecen en al IBEX35. Ejemplo de los componentes que hemos extraídos.
```

---

### Estados Financieros

En nuestras bases de dato necesitamos para el análisis técnico los estados financieros de las empresas analizadas y en esto se reúne la información contable clave de la empresa, organizada por año. Incluye el balance (activos, pasivos y patrimonio), la cuenta de resultados (ingresos, gastos y beneficio neto) y los flujos de efectivo (operativo, de inversión y de financiación), además de indicadores derivados como el margen neto, el ROA, el ROE y el ratio deuda/patrimonio. Esta estructura no solo permite analizar la evolución histórica de la compañía, sino también evaluar su solidez financiera, rentabilidad y nivel de endeudamiento de manera comparativa y objetiva.

Tabla: Ratios descargados:

| TICKER | YEAR | ASSETS       | LIABILITIES   | EQUITY      | REVENUE      | EXPENSES     | NET_INCOME  | OPERATING_CF | INVESTING_CF | FINANCING_CF | FREE_CF     | NET_MARGIN | ROA   | ROE   | DEBT_EQUITY |
|--------|------|--------------|---------------|-------------|--------------|--------------|-------------|--------------|--------------|--------------|-------------|------------|-------|-------|-------------|
| A2A.MI | 2021 | 18008000000  | 13705000000   | 3760000000  | 11352000000  | 10848000000  | 504000000   | 1135000000   | -1595000000  | 412000000    | 61000000    | 0.044      | 0.028 | 0.134 | 3.645       |
| A2A.MI | 2022 | 21367000000  | 16900000000   | 3899000000  | 22938000000  | 22537000000  | 401000000   | 1260000000   | -1142000000  | 1502000000   | 20000000    | 0.017      | 0.019 | 0.103 | 4.334       |
| A2A.MI | 2023 | 18798000000  | 13996000000   | 4240000000  | 14492000000  | 13833000000  | 659000000   | 1040000000   | -1359000000  | -636000000   | -336000000  | 0.045      | 0.035 | 0.155 | 3.301       |

```{figure} ../../Imagenes/Docker.jpeg
:alt: Docker
:align: center
:width: 100%
**Figura 11.** Encendida del control de Docker.
```

### Ratios Financieros

Esta tabla muestra los principales ratios de análisis financiero. Incluye indicadores de valoración como el PER y el Price to Book, métricas de rentabilidad como el EV/EBITDA, así como el dividend yield y el payout ratio para entender la política de dividendos. Además, aporta información sobre la capitalización bursátil, el valor de empresa y las acciones en circulación, lo que permite evaluar el tamaño y la posición de la compañía en el mercado.

| TICKER | PE_TRAILING | PE_FORWARD | PRICE_TO_BOOK | EV_TO_EBITDA | DIVIDEND_YIELD | PAYOUT_RATIO | MARKET_CAP   | ENTERPRISE_VALUE | SHARES_OUTSTANDING |
|--------|-------------|------------|---------------|--------------|----------------|--------------|--------------|------------------|--------------------|
| A2A.MI | 8.253846    | 11.922221  | 1.3953185     | 6.109        | 4.66           | 0.3868       | 6804686848   | 12437948416      | 3128590080         |
| AAF.L  | 32.05714    | 18.7       | 316.05634     | 6.516        | 2.19           | 0.71650004   | 8183037440   | 13996040192      | 3646629888         |
| AAL.L  |             | 15.612122  | 158.78691     | 6.605        | 0.94           | 5.6102004    | 27529195520  | 45797920768      | 1068680000         |


### Análisis de Sostenibilidad ESG 

ESG son las siglas de Environmental, Social and Governance (Medioambiental, Social y Gobernanza). Es un conjunto de criterios que se usan para evaluar a las empresas más allá de sus resultados financieros.
1. Environmental (Medioambiental): mide el impacto que tiene la empresa sobre el medio ambiente. Ejemplos: emisiones de CO₂, gestión de residuos, eficiencia energética, uso de energías renovables.

2. Social (Social): evalúa cómo la empresa se relaciona con empleados, clientes, comunidades y sociedad en general. Ejemplos: condiciones laborales, diversidad e inclusión, derechos humanos, impacto en la comunidad.

3. Governance (Gobernanza): analiza cómo se gestiona y dirige la empresa. Ejemplos: independencia del consejo de administración, ética corporativa, transparencia, políticas contra la corrupción.

Tabla: ESG descargados:

| TICKER | HAS_ESG | TOTAL_ESG | ENVIRONMENTAL | SOCIAL | GOVERNANCE | CONTROVERSY |
|--------|---------|-----------|---------------|--------|------------|-------------|
| A2A.MI | TRUE    | 20.10     | 10.59         | 4.90   | 4.61       | 1           |
| AAF.L  | TRUE    | 22.69     | 6.52          | 11.82  | 4.35       | 2           |
| AAL.L  | TRUE    | 25.79     | 14.22         | 8.96   | 2.62       | 3           |
