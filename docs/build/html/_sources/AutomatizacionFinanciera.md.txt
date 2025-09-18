# Automatización Financiera

## Modelos de análisis de sentimiento 

Se ha decidido utilizar un modelo predictivo para estimar la probabilidad de que un titular sea *negativo, positivo o neutral*. El modelo seleccionado es **`distilroberta-finetuned-financial-news-sentiment-analysis`**, especializado en el ámbito financiero en idioma inglés, elegido por su buen rendimiento en clasificación.  

### Modelo en inglés: *mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis*  

Este modelo corresponde a una versión ajustada de **DistilRoBERTa-base**, entrenada específicamente en el conjunto de datos *Financial PhraseBank*, que contiene 4.840 frases procedentes de noticias financieras en inglés. Dicho corpus fue anotado por entre 5 y 8 expertos, categorizando cada enunciado según su polaridad de sentimiento (positivo, negativo o neutral).  

A nivel arquitectónico, DistilRoBERTa es una versión comprimida de RoBERTa-base que conserva su rendimiento con menor complejidad computacional. Consta de **6 capas, 768 dimensiones y 12 cabezales de atención**, sumando un total aproximado de **82 millones de parámetros** (frente a los 125 millones de RoBERTa-base). Esta reducción permite que el modelo sea, en promedio, **el doble de rápido** en comparación con RoBERTa-base, manteniendo un desempeño competitivo.  

En cuanto a resultados, el modelo alcanzó un **accuracy del 98,23 %** y una pérdida de validación de **0,1116** en el conjunto de evaluación, lo que demuestra un desempeño sobresaliente en la clasificación de sentimiento en el ámbito financiero.  

La principal ventaja de este modelo es su **alta especialización en finanzas**, lo que le permite detectar matices en titulares y frases económicas con gran precisión. No obstante, presenta la limitación de estar restringido exclusivamente al inglés.  

[Ver HuggingFace - Modelo Sentimento Inglés ](https://huggingface.co/mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis )

Ahora podemos ver el flujo de trabajo para procesar titulares de noticias financieras: desde su extracción y verificación, pasando por el análisis de sentimiento con un modelo predictivo, hasta la integración de los resultados en un DataLake, garantizando que solo se analicen y almacenen los titulares nuevos.

```{figure} ../../Imagenes/DigSentimientos.png
:alt: Diagrama de Sentimientos 
:width: 110%
:align: center
**Figura 12.** Flujograma del modelo de scrapping
```

### Automatización del flujo de trabajo con n8n

En la automatización de la ingesta y el procesamiento de datos se ha empleado **n8n**, una herramienta de *workflow automation* de código abierto. Su función principal es permitir la integración entre múltiples servicios, APIs y bases de datos mediante la creación de flujos de trabajo visuales que se ejecutan de manera automática en función de determinados eventos o programaciones. A partir del trigger que en nuestro caso de tiempo (00:00 todos los días), los nodos se van ejecutando en orden lógico, permitiendo que la información fluya automáticamente entre servicios sin intervención manual. En conjunto, n8n se convierte en un componente clave para la **orquestación de datos** en este proyecto, permitiendo que el análisis de sentimiento y financiero se sustente en información 

```{figure} ../../Imagenes/FlujoN8N.jpeg
:alt: FlujoN8N 
:width: 100%
:align: center
**Figura 13.** Flujograma del modelo de scrapping en N8N
```

### Flujo de utilizacion de los modelos

Dentro del *datalake* definido en el módulo de **Datos y Preparación**, se encuentra la tabla unificada denominada `NOTICIAS_ANALIZADAS`. A partir de esta base de datos se extraen los titulares procedentes de los distintos noticieros. Posteriormente, se aplica el modelo de predicción correspondiente, previamente entrenado para el análisis de sentimiento.  

### Generación de las columnas con tipo de títulos  

Una vez aplicado el modelo de análisis de sentimiento, se generan cuatro variables principales: `SENTIMIENTO_RESULTADO`, `PROBABILIDAD_POSITIVO`, `PROBABILIDAD_NEGATIVA` y `PROBABILIDAD_NEUTRAL`. Estas variables permiten determinar de manera objetiva la clasificación final del texto en función de su polaridad. Posteriormente, los resultados se incorporan a la tabla principal de **Noticias_Analizadas** y se almacenan nuevamente en Snowflake, lo que garantiza su disponibilidad para futuros análisis, tanto de carácter técnico como de integración con otros indicadores financieros.  

```{figure} ../../Imagenes/Correo_Titulares.png
:alt: correo_titulares 
:width: 100%
:align: center
**Figura 14.** Notificaciones automáticas de la actualización de la base de datos y aplicación del modelo a los titulares
```

## Análisis  de Frontera de eficiencia

Para la selección de nuestra cartera emplearemos el modelo de media-varianza de Markowitz, considerado la base de la Teoría Moderna de Carteras. Este enfoque es ampliamente utilizado en finanzas porque permite encontrar la combinación óptima de activos equilibrando riesgo y rendimiento esperado.
El procedimiento consiste en simular 50.000 carteras aleatorias con diferentes ponderaciones de activos. Posteriormente, se identifican aquellas que cumplen con dos criterios clave:
- Cartera de mínima varianza: la que presenta el menor nivel de riesgo posible.
- Cartera con ratio de Sharpe máximo: la que ofrece la mejor relación entre rendimiento y riesgo ajustado por la tasa libre de riesgo.

    Ver teoría en {ref}`modelo-de-markowitz`

El primer aspecto para considerar es que los resultados de nuestro modelo se generan siempre con datos actualizados hasta el día anterior en todas las bases de datos. Dichos resultados se almacenan en un datalake implementado en Snowflake, el cual se encuentra en constante actualización. Por ello, la primera fase de nuestro análisis consiste en establecer la conexión con Snowflake y asegurar la correcta creación, configuración y mantenimiento del datalake.

Una vez completada esta etapa, el modelo se vincula con las acciones seleccionadas por el usuario en función de su análisis fundamental. Para cada activo elegido, se verifican las tendencias históricas y se realizan diferentes simulaciones con variaciones en los porcentajes de asignación.

De este modo, el modelo permite seleccionar la combinación más eficiente considerando no solo los criterios de diversificación cuantitativa, sino también el análisis fundamental, el análisis descriptivo de los datos históricos y el análisis de sentimiento obtenido a partir de noticias sectoriales e industriales.

El cálculo de los rendimientos de los activos es una etapa fundamental previa a cualquier análisis de carteras. A partir de los precios históricos, se obtienen las variaciones porcentuales que reflejan cómo evoluciona el valor de cada activo en el tiempo.

```{literalinclude} ../../Analisis_Financiero/Frontera_Eficiencia.py
:language: python
:linenos:
:start-after: --8<-- [start:rendimientos]
:end-before: --8<-- [end:rendimientos]
```

Una vez obtenidos los datos necesarios para el análisis principal, procedemos a la simulación de 50.000 portafolios aleatorios. Este proceso nos permite explorar un amplio rango de combinaciones posibles entre los activos y, de este modo, identificar aquellas carteras que ofrecen el mejor equilibrio entre riesgo y rendimiento esperado.

```{literalinclude} ../../Analisis_Financiero/Frontera_Eficiencia.py
:language: python
:linenos:
:start-after: --8<-- [start:iteracioncarteras]
:end-before: --8<-- [end:iteracioncarteras]
```

Con este procedimiento se han generado 50.000 carteras simuladas, cada una con su respectivo nivel de riesgo y rendimiento esperado. A partir de estos resultados, y considerando una tasa libre de riesgo del 3%, se calcula el ratio de Sharpe, lo que nos permite identificar la combinación de acciones más eficiente en términos de la relación rentabilidad–riesgo. La siguiente tabla muestra algunos ejemplos representativos de estas carteras:

| Rendimientos | Riesgos  | PesoACS.MC | PesoAENA.MC | PesoBBVA.MC | PesoCABK.MC | PesoELE.MC | PesoENG.MC | PesoFER.MC | PesoIAG.MC | PesoIBE.MC | PesoITX.MC | PesoMAP.MC | PesoREP.MC | PesoSAN.MC | PesoTEF.MC |
|--------------|----------|------------|-------------|-------------|-------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|
| 0.094535     | 0.236939 | 0.056633   | 0.050586    | 0.146062    | 0.077725    | 0.046691   | 0.035503   | 0.113892   | 0.069035   | 0.062491   | 0.131826   | 0.036659   | 0.013266   | 0.052721   | 0.106909   |
| 0.088686     | 0.247779 | 0.150650   | 0.047172    | 0.018697    | 0.065942    | 0.009546   | 0.004347   | 0.037379   | 0.134567   | 0.145345   | 0.096339   | 0.017419   | 0.038019   | 0.119666   | 0.114913   |    
| 0.076873     | 0.232206 | 0.130143   | 0.112952    | 0.103699    | 0.046427    | 0.117658   | 0.066646   | 0.027429   | 0.084714   | 0.030029   | 0.094975   | 0.118723   | 0.011767   | 0.007015   | 0.047825   |


```{literalinclude} ../../Analisis_Financiero/Frontera_Eficiencia.py
:language: python
:linenos:
:start-after: --8<-- [start:varianza_minima]
:end-before: --8<-- [end:varianza_minima]
```

### Gráfica de la frontera de eficiencia

Este gráfico es un ejemplo con una cartera del IBEX35 el cual representa la frontera eficiente obtenida a partir de la simulación de 50.000 combinaciones de activos. En el eje horizontal tenemos el riesgo del portafolio medido como volatilidad y en el eje vertical tenemos el rendimiento esperado por el portafolio. La nube de puntos son los portafolios creados aleatoriamente que combina cada uno de los pesos de las acciones de cada portafolio.

La estrella verde, abajo a la izquierda, es la cartera con el menor riesgo posible, aunque su rentabilidad es moderada y la estrella azul, arriba a la derecha es la cartera más eficiente en términos de relación riesgo–rentabilidad, considerando la tasa libre de riesgo.

```{figure} ../../Imagenes/FronteraEficiencia_porta.gif
:alt: Frontera Eficiencia
:align: center
**Figura 15.** Muestra de frontera de eficiencia para la elección de la cartera más eficiente
```