# Descripción Problema

## Definición del problema a resolver

En el análisis financiero de los mercados bursátiles, existen dos enfoques principales: el análisis fundamental y el técnico. El análisis fundamental se basa en datos financieros concretos como ganancias, pérdidas y márgenes operativos para evaluar la salud y el rendimiento de una empresa. Este enfoque es ideal para inversiones a mediano y largo plazo, aunque no es infalible, ya que puede verse afectado por la manipulación de cifras o decisiones macroeconómicas como los tipos de interés.

Por otro lado, el análisis técnico se enfoca en el comportamiento del precio, sin considerar los datos financieros tradicionales. Utiliza herramientas gráficas como los gráficos de velas y patrones chartistas para anticipar movimientos futuros del mercado, siendo más útil para el corto plazo o decisiones basadas en tendencias.

Sin embargo, en los mercados actuales, los precios de las acciones no dependen exclusivamente de estos factores tradicionales. También están influenciados por el comportamiento y percepción de los inversores, moldeados por la información que circula en medios, redes sociales y foros. Estos flujos de información generan cambios en el sentimiento del mercado, que pueden anticipar movimientos alcistas o bajistas.

Ante esta realidad, surge una necesidad creciente de incorporar técnicas de procesamiento del lenguaje natural (NLP) y análisis de texto, que permitan cuantificar estas percepciones y combinarlas con indicadores financieros clásicos. El objetivo es desarrollar modelos predictivos más robustos, capaces de identificar patrones entre lo que se dice sobre sectores, empresas o temas financieros, y el comportamiento posterior de sus precios en bolsa.

Este enfoque integral busca mejorar la capacidad de predicción en los mercados bursátiles, integrando análisis fundamental, técnico y de sentimiento.

---

## ¿Qué es el mercado bursátil?

El mercado bursátil es el espacio donde se negocian instrumentos financieros como acciones y bonos. Su función principal es facilitar la inversión y la financiación de empresas, permitiendo que los precios reflejen las expectativas de los participantes.

Estos precios no solo responden a datos económicos, sino también a factores emocionales y percepciones colectivas influenciadas por noticias, redes sociales y medios especializados. Por eso, el análisis de sentimiento aplicado a textos puede ser clave para anticipar movimientos del mercado, al detectar señales tempranas de optimismo o temor entre los inversores.

El mercado bursátil europeo es el conjunto de bolsas de valores que operan en los países de Europa, donde se negocian instrumentos financieros como acciones, bonos, derivados y fondos cotizados. Este mercado desempeña un papel fundamental en la economía del continente, ya que permite a empresas europeas captar inversión y a los inversores gestionar su capital a través de activos financieros.

Las principales bolsas europeas incluyen:

- **Euronext** (con presencia en París, Ámsterdam, Bruselas, Lisboa, Dublín, entre otras),
- **Deutsche Börse** (Fráncfort, Alemania),
- **London Stock Exchange (LSE)** (Reino Unido),
- **BME – Bolsas y Mercados Españoles** (España),
- **Swiss Exchange (SIX)** (Suiza).

Estas bolsas están interconectadas y son reguladas por entidades nacionales e internacionales que garantizan transparencia, legalidad y eficiencia en las transacciones.

El funcionamiento del mercado bursátil europeo se basa en la ley de oferta y demanda, y sus precios se ven influenciados no solo por indicadores económicos y financieros, sino también por factores externos como noticias políticas, eventos geopolíticos y, especialmente, la percepción colectiva de los inversores.

En este contexto, el análisis de sentimiento se ha convertido en una herramienta poderosa para los analistas e inversionistas. Al analizar lo que se comunica en medios financieros, redes sociales y portales especializados en Europa, es posible detectar cambios en el estado de ánimo del mercado (optimismo, temor, incertidumbre) y anticipar posibles movimientos alcistas o bajistas en los precios de las acciones y otros activos. 

---

## Hipótesis del estudio

- **Hipótesis nula (H₀):**  
  El sentimiento expresado en los titulares de prensa **NO** tiene un efecto significativo en la variación de los precios de las acciones sobre el mercado bursátil europeo.

- **Hipótesis alternativa (H₁):**  
  El sentimiento expresado en la prensa **sí tiene** un efecto significativo en la variación de los precios de las acciones sobre el mercado bursátil europeo.

---

## Alcance del proyecto (temporal, geográfico, sectorial)

Este proyecto tiene como objetivo analizar el impacto del sentimiento expresado en titulares de la prensa económica europea sobre la evolución del mercado bursátil, con un enfoque específico en España como caso de estudio. Se busca determinar si el análisis de contenido textual relacionado con noticias financieras permite predecir con mayor precisión el comportamiento de los precios de las acciones, principalmente del índice IBEX 35.

**Delimitación temporal:**  
El análisis se desarrollará utilizando datos comprendidos entre enero de 2020 y agosto de 2025, un periodo caracterizado por eventos económicos de gran impacto (como la pandemia de COVID-19, la recuperación post-crisis, la inflación y la guerra en Ucrania), que generaron abundante información periodística y fuertes fluctuaciones en los mercados.

**Delimitación geográfica:**  
La investigación se centrará en titulares de la prensa económica del ámbito europeo, con prioridad en medios españoles y fuentes oficiales como el Instituto Nacional de Estadística (INE), la Comisión Nacional del Mercado de Valores (CNMV), el Banco de España y plataformas informativas como *El Economista*, *Expansión*, *Reuters*, *Bloomberg EU*, entre otros.

**Delimitación técnica:**  
No se contemplan limitaciones técnicas relevantes, ya que se utilizarán herramientas y lenguajes de programación de acceso abierto. Se aplicarán técnicas de procesamiento de lenguaje natural (NLP), análisis de sentimiento y modelado predictivo, en conjunto con datos históricos bursátiles, para construir modelos que permitan identificar patrones y relaciones significativas entre la percepción del mercado y el comportamiento real de los activos.

---

## ¿Cómo lo haremos?

- **Comparación de estado de resultados financieros de pymes**  
  - *Fuentes:*  
    - Yahoo Finance  
    - Finance Database  

- **Análisis de proyección de deuda e inversión, modelado de datos**  
  - *Fuentes:*  
    - Finance Toolkits  

- **Análisis de percepción del mercado: text mining y scrapping**  
  - *Fuentes (scrapping):*  
    - Data GOB – España  

- **Noticias (text mining + scrapping):**  
  - Cinco Días  
  - Expansión  
  - El Economista  
  - Wayback  

- **Escalabilidad del producto: productivizar el modelo**  
  - *Herramientas:*  
    - Azure / Google Cloud  
    - PyPortfolioOpt  
    - mplfinance  
    - Quant-Finance-Resources  
