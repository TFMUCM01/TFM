# Conclusiones

El proyecto desarrollado integra distintas tecnologías de **procesamiento de lenguaje natural (NLP)**, **automatización de flujos de trabajo** y **almacenamiento en la nube**, con el propósito de construir una herramienta capaz de facilitar el análisis financiero a partir de diferentes fuentes de datos. Durante el proceso se abordaron múltiples fases: desde la recolección de información mediante *scraping* de noticias y descarga de datos financieros con APIs, hasta la limpieza, transformación y almacenamiento de dicha información en **Snowflake**. Además, se implementaron módulos especializados para incorporar métricas **fundamentales** (estados financieros), **de mercado** (ratios bursátiles) y **ESG** (sostenibilidad), conformando así una base de datos integral.  

Sobre esta infraestructura, se diseñó un flujo en **N8N** que permite interactuar con los datos a través de un **bot en Telegram**, el cual procesa las consultas de los usuarios con modelos de **OpenAI**, genera sentencias SQL, ejecuta las consultas en Snowflake y devuelve los resultados acompañados de un breve análisis interpretativo. Este enfoque no solo resuelve la parte técnica de la integración de datos, sino que también ofrece un **asesor virtual de inteligencia artificial (AsesorIA)** que democratiza el acceso a información financiera avanzada.  

La experiencia adquirida durante la implementación permitió identificar tanto los **beneficios** como los **desafíos** de este tipo de soluciones, destacando aspectos técnicos, económicos y estratégicos que serán determinantes para la continuidad y evolución de la herramienta.  

En síntesis, las principales conclusiones alcanzadas son:  
- La implementación de la herramienta ha supuesto un reto importante en las fases de **obtención, limpieza y transformación de datos**, lo que evidencia la necesidad de procesos sólidos de ingeniería de datos en proyectos de este tipo.  

- La herramienta representa un aporte significativo al permitir que **usuarios sin conocimientos técnicos financieros** puedan iniciarse en el análisis de mercados, reduciendo barreras de entrada y facilitando la democratización del acceso a la información financiera.  

- Los **costos asociados a plataformas y servicios** como Hugging Face, OpenAI y Snowflake constituyen un factor crítico a considerar en el desarrollo, escalabilidad y sostenibilidad de la solución.  

- La **selección de un modelo NLP** debe realizarse en función de parámetros técnicos como **precisión y exactitud**, garantizando resultados fiables y adecuados al contexto financiero.  

- Para la **elaboración, implementación y desarrollo** de la herramienta es necesario contar con **conocimientos técnicos previos**, que permitan evaluar las tecnologías, optimizar su uso y asegurar la correcta integración de los diferentes componentes.  
