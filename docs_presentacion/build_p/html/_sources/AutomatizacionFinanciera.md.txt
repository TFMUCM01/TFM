# Automatización Financiera

## Análisis de sentimiento 

Se ha decidido utilizar un modelo predictivo para estimar la probabilidad de que un titular sea *negativo, positivo o neutral*. El modelo seleccionado es **`distilroberta-finetuned-financial-news-sentiment-analysis`**, especializado en el ámbito financiero en idioma inglés, elegido por su buen rendimiento en clasificación.  

```{figure} ../../Imagenes/DigSentimientos.png
:alt: Diagrama de Sentimientos 
:width: 110%
:align: center
**Figura 12.** Flujograma del modelo de scrapping
```

### Automatización del flujo de trabajo con N8N

N8N es la herramienta de automatización que integra servicios, APIs y bases de datos mediante flujos de trabajo programados (ej. cada día a las 00:00). Permite que la información fluya sin intervención manual, convirtiéndose en un elemento clave para la orquestación de datos y garantizando que el análisis se realice siempre con información actualizada. 

```{figure} ../../Imagenes/FlujoN8N.jpeg
:alt: FlujoN8N 
:width: 100%
:align: center
**Figura 13.** Flujograma del modelo de scrapping en N8N
```

### Generación de las columnas con tipo de títulos 

Aplicando el análisis de sentimiento, se generan cuatro variables principales: `SENTIMIENTO_RESULTADO`, `PROBABILIDAD_POSITIVO`, `PROBABILIDAD_NEGATIVA` y `PROBABILIDAD_NEUTRAL`. Estas variables permiten determinar de manera objetiva la clasificación final del texto en función de su polaridad. Posteriormente, los resultados se incorporan a la tabla principal de **Noticias_Analizadas** y se almacenan nuevamente en Snowflake, lo que garantiza su disponibilidad para futuros análisis, tanto de carácter técnico como de integración con otros indicadores financieros.  

```{figure} ../../Imagenes/Correo_Titulares.png
:alt: correo_titulares 
:width: 100%
:align: center
**Figura 14.** Notificaciones automáticas de la actualización de la base de datos y aplicación del modelo a los titulares
```

## Análisis de Frontera de eficiencia

Para selección de nuestra cartera emplearemos el modelo de media-varianza de Markowitz, considerado la base de la Teoría Moderna de Carteras. Este enfoque es ampliamente utilizado en finanzas porque permite encontrar la combinación óptima de activos equilibrando riesgo y rendimiento esperado.
El procedimiento consiste en simular 50.000 carteras aleatorias con diferentes ponderaciones de activos. Posteriormente, se identifican aquellas que cumplen con dos criterios clave:
- Cartera de mínima varianza: la que presenta el menor nivel de riesgo posible.
- Cartera con ratio de Sharpe máximo: la que ofrece la mejor relación entre rendimiento y riesgo ajustado por la tasa libre de riesgo.

    Ver teoría en {ref}`modelo-de-markowitz`

| Rendimientos | Riesgos  | PesoACS.MC | PesoAENA.MC | PesoBBVA.MC | PesoCABK.MC | PesoELE.MC | PesoENG.MC | PesoFER.MC | PesoIAG.MC | PesoIBE.MC | PesoITX.MC | PesoMAP.MC | PesoREP.MC | PesoSAN.MC | PesoTEF.MC |
|--------------|----------|------------|-------------|-------------|-------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|------------|
| 0.094535     | 0.236939 | 0.056633   | 0.050586    | 0.146062    | 0.077725    | 0.046691   | 0.035503   | 0.113892   | 0.069035   | 0.062491   | 0.131826   | 0.036659   | 0.013266   | 0.052721   | 0.106909   |
| 0.088686     | 0.247779 | 0.150650   | 0.047172    | 0.018697    | 0.065942    | 0.009546   | 0.004347   | 0.037379   | 0.134567   | 0.145345   | 0.096339   | 0.017419   | 0.038019   | 0.119666   | 0.114913   |    
| 0.076873     | 0.232206 | 0.130143   | 0.112952    | 0.103699    | 0.046427    | 0.117658   | 0.066646   | 0.027429   | 0.084714   | 0.030029   | 0.094975   | 0.118723   | 0.011767   | 0.007015   | 0.047825   |

### Gráfica de la frontera de eficiencia

Este gráfico es un ejemplo con una cartera del IBEX35 el cual representa la frontera eficiente obtenida a partir de la simulación de 50.000 combinaciones de activos. En el eje horizontal tenemos el riesgo del portafolio medido como volatilidad y en el eje vertical tenemos el rendimiento esperado por el portafolio. La nube de puntos son los portafolios creados aleatoriamente que combina cada uno de los pesos de las acciones de cada portafolio.

La estrella verde, abajo a la izquierda, es la cartera con el menor riesgo posible, aunque su rentabilidad es moderada y la estrella azul, arriba a la derecha es la cartera más eficiente en términos de relación riesgo–rentabilidad, considerando la tasa libre de riesgo.

```{figure} ../../Imagenes/FronteraEficiencia_porta.gif
:alt: Frontera Eficiencia
:align: center
**Figura 15.** Muestra de frontera de eficiencia para la elección de la cartera más eficiente
```