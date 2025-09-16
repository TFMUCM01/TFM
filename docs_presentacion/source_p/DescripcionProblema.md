# Descripción Problema

El análisis de los mercados bursátiles combina distintos enfoques: el fundamental, basado en datos financieros para valorar el potencial de una empresa; el técnico, que estudia precios y volúmenes pasados para anticipar tendencias; y el de sentimiento, que mide percepciones colectivas a partir de noticias y redes sociales. A estos se suma el cuantitativo, que mediante algoritmos y machine learning integra grandes volúmenes de información. La unión de los cuatro ofrece una visión más completa y precisa para la toma de decisiones en inversión.

---

## Alcance del proyecto (temporal, geográfico, sectorial)

El proyecto abarca el periodo 2020-2025, marcado por la COVID-19, la inflación y la guerra en Ucrania, con foco en el mercado bursátil europeo (IBEX 35, DAX 40, CAC 40, FTSE MIB, AEX, FTSE 100, OMXS30 y SMI). Se centra en empresas cotizadas, complementando con métricas ESG y noticias económicas, y analiza los cuatro pilares financieros: sentimiento, técnico, fundamental y cuantitativo. Se excluyen productos de renta fija, ETFs e instrumentos mixtos, aunque se contemplan como posibles extensiones futuras.

**Delimitación:**  

## Alcance y Limitaciones del Estudio  

- **Periodo analizado**: enero 2020 – septiembre 2025  
  - Contexto: COVID-19, recuperación económica, inflación y guerra en Ucrania  
  - Consecuencia: alta volatilidad e intensa producción informativa  

- **Disponibilidad de noticias**: solo desde enero 2024  
  - Menor histórico, pero marco temporal altamente relevante  

- **Retos técnicos**:  
  - Restricciones en descarga y homogeneización de datos financieros  
  - Limitaciones en uso de modelos por consumo de tokens  
  - Migraciones de infraestructura por costes en la nube  
  - Descargas masivas con tiempos de ejecución elevados  

- **Automatización**:  
  - Integración de GitHub Actions con n8n vía *cloudflared*  
  - Mayor complejidad operativa, pero con éxito en la ejecución  

- **Conclusión**:  
  - Las limitaciones condicionan el alcance  
  - El estudio mantiene **pertinencia y aplicabilidad** en el contexto económico actual  



---

## Recursos

| Fuentes / Noticias | Ambientes / Programas | Lenguajes / Librerías / Herramientas |
|--------------------|------------------------|--------------------------------------|
| Yahoo Finance | Visual Studio | SQL |
| TradingView | Google Colab | Python |
| ABC | n8n | JavaScript |
| BBC | Cloudflared | HTML |
| El País | Tableau | Pandas |
| Expansión | Docker | NumPy |
| The Times | Snowflake | Transformers |
| Bloomberg | Google Drive | Torch (PyTorch) |
| El Economista |   | Snowflake Connector |
|   |   | Matplotlib |
|   |   | Seaborn |
|   |   | Plotly (Graph Objects, Express) |
|   |   | Statsmodels |
|   |   | Git / GitHub |
|   |   | OpenAI |
|   |   | Hugging Face |
|   |   | Telegram Bots |

