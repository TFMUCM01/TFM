# Descripción Problema

El análisis de los mercados bursátiles combina distintos enfoques: el fundamental, basado en datos financieros para valorar el potencial de una empresa; el técnico, que estudia precios y volúmenes pasados para anticipar tendencias; y el de sentimiento, que mide percepciones colectivas a partir de noticias y redes sociales. A estos se suma el cuantitativo, que mediante algoritmos y machine learning integra grandes volúmenes de información. La unión de los cuatro ofrece una visión más completa y precisa para la toma de decisiones en inversión.

---

## Hipótesis del estudio

- **Hipótesis nula (H₀):**  
    No existe relación entre el uso del asistente financiero automatizado y una mejora en el análisis del mercado europeo o en la toma de decisiones de inversión.

- **Hipótesis alternativa (H₁):**  
    Existe una relación entre el uso del asistente financiero automatizado y una mejora en el análisis del mercado europeo o en la toma de decisiones de inversión.

---

## Alcance del proyecto (temporal, geográfico, sectorial)

El proyecto abarca el periodo 2020-2025, marcado por la COVID-19, la inflación y la guerra en Ucrania, con foco en el mercado bursátil europeo (IBEX 35, DAX 40, CAC 40, FTSE MIB, AEX, FTSE 100, OMXS30 y SMI). Se centra en empresas cotizadas, complementando con métricas ESG y noticias económicas, y analiza los cuatro pilares financieros: sentimiento, técnico, fundamental y cuantitativo. Se excluyen productos de renta fija, ETFs e instrumentos mixtos, aunque se contemplan como posibles extensiones futuras.

**Delimitación:**  

El análisis abarca el periodo de enero de 2020 a septiembre de 2025, un intervalo marcado por la pandemia de COVID-19, la recuperación económica, la inflación y la guerra en Ucrania, factores que generaron alta volatilidad e intensa producción informativa. Sin embargo, los registros de noticias solo están disponibles desde enero de 2024, lo que limita la amplitud del histórico aunque asegura la relevancia del marco temporal. A nivel técnico, el proyecto se enfrentó a restricciones en la descarga y homogeneización de datos financieros, limitaciones en el uso de modelos por consumo de tokens, migraciones de infraestructura por costes en la nube y tiempos de ejecución elevados en descargas masivas. Asimismo, la integración de GitHub Actions con n8n mediante cloudflared permitió la automatización, aunque con mayor complejidad operativa. Estas limitaciones condicionan el alcance del estudio, pero confirman su pertinencia y aplicabilidad al contexto económico actual.


---

## Recursos

- **Comparación de estado de resultados financieros**  
  - Fuentes:
    - Yahoo Finance 
    - TradingView 

- **Noticias (text mining + scrapping):**  
  - ABC
  - BBC 
  - El Pais
  - Expansion 
  - The times
  - Bloomberg 
  - El economista

- **Escalabilidad del producto: productivizar el modelo**  
 - Ambientes:
    - Visual Studio  
    - Google Colab  
    - n8n  
    - Cloudflared  
    - Tableau  

  - Programas
    - Docker  
    - Snowflake  
    - Google Drive  

  - Lenguajes de programación
    - SQL  
    - Python  
    - JavaScript  
    - HTML  

  - Librerías / Frameworks
    - Pandas  
    - NumPy  
    - Transformers  
    - Torch (PyTorch)  
    - Snowflake Connector  
    - Matplotlib  
    - Seaborn  
    - Plotly (Graph Objects, Express)  
    - Statsmodels  

  - Herramientas
    - Git / GitHub  
    - OpenAI  
    - Hugging Face  
    - Telegram Bots  

