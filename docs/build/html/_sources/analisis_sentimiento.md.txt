# Datos y Preparación

Este trabajo integra dos fuentes complementarias para anticipar el comportamiento del precio de acciones europeas: señales de sentimiento extraídas de titulares de prensa y información cuantitativa de mercado y estados financieros. Los titulares se obtuvieron mediante scraping y de medios generalistas y económicos —BBC, ABC, El Economista, Bloomberg Europa, El País, The Times y Expansión—. Sobre estos textos se aplicó una depuración sistemática (eliminación de duplicados y de ruidos como elementos de navegación, normalización de caracteres/idiomas y control de fechas) para construir indicadores diarios de sentimiento por medio e instrumento.

En paralelo, los datos de mercado (OHLCV por ticker) se descargaron y consolidaron en Snowflake, armonizando formatos de fecha y símbolos, verificando huecos y coherencia (festivos, sesiones sin volumen) y, cuando procede, incorporando información contable resumida por compañía. Finalmente, se integraron ambas fuentes en un único dataset analítico a nivel (ticker, fecha) que incluye rendimientos y rezagos, volumen y métricas de sentimiento agregadas. Este conjunto sirve de base para los modelos predictivos, combinando la dimensión informativa de las noticias con la evidencia numérica del mercado para obtener señales de compra/venta más robustas.

## Analisis de sentimiento

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

Recorremos `NOTICIEROS`, extraemos metadatos del medio y fijamos el rango de fechas (desde la última cargada hasta ayer).

Verificacion de las ultimas fechas cargada
```{literalinclude} ../../scraper.py
:language: python
:linenos:
:start-after: --8<-- [start:obtener_ultima_fecha_en_snowflake]
:end-before: --8<-- [end:obtener_ultima_fecha_en_snowflake]
```

Configuracion de las fechas y url
```{literalinclude} ../../main.py
:language: python
:linenos:
:start-after: --8<-- [start:configfechas-noticieros]
:end-before: --8<-- [end:configfechas-noticieros]
```

**Extracción de titulares**

Bucle diario: resolvemos el snapshot del día, extraemos titulares y los enriquecemos con metadatos; errores se registran sin detener el flujo.

Modulo de descarga en API's
```{literalinclude} ../../scraper.py
:language: python
:linenos:
:start-after: --8<-- [start:extraer_titulares]
:end-before: --8<-- [end:extraer_titulares]
```

Solicitud de titulares por fecha
```{literalinclude} ../../main.py
:language: python
:linenos:
:start-after: --8<-- [start:extraer-titulares]
:end-before: --8<-- [end:extraer-titulares]
```

**Carga en Snowflake**

Consolidamos resultados, eliminamos duplicados y subimos a la tabla destino.

Modulo de carga en Snowflaje

```{literalinclude} ../../snowflake_utils.py
:language: python
:linenos:
:start-after: --8<-- [start:subir_a_snowflake]
:end-before: --8<-- [end:subir_a_snowflake]
```