# Modelos Teoricos

(modelo-de-markowitz)=
## Modelo de Markowitz
El **modelo de Markowitz**, también conocido como **teoría moderna de carteras (Modern Portfolio Theory, MPT)**, fue desarrollado por **Harry Markowitz en 1952**.  

Es un modelo matemático de inversión que busca optimizar la asignación de activos en una cartera, equilibrando **riesgo y rentabilidad esperada**.  

![SML](../../Imagenes/capm.jpg)

### Ideas principales
- **Diversificación**: Al combinar activos con diferentes comportamientos, se reduce el riesgo total sin sacrificar necesariamente la rentabilidad.  
- **Rentabilidad esperada**: cada activo tiene un rendimiento medio esperado.  
- **Riesgo**: se mide con la **varianza o desviación estándar** de los rendimientos.  
- **Correlación**: el modelo tiene en cuenta cómo se mueven los activos entre sí. Activos con correlación baja o negativa reducen el riesgo global.  
- **Frontera eficiente**: es el conjunto de carteras óptimas que ofrecen la **mayor rentabilidad para un nivel de riesgo dado** (o el menor riesgo para una rentabilidad deseada).  

### Fórmulas

1. Rentabilidad esperada de la cartera  

La rentabilidad media ponderada de los activos que componen la cartera se define como:

$$
E(R_p) = \sum_{i=1}^n w_i \cdot E(R_i)
$$

donde:  
- Wi → peso del activo (i) en la cartera  
- E(Ri) → rentabilidad esperada del activo (i)

2. Varianza de la cartera (riesgo)  

El riesgo total de la cartera se mide a través de su varianza:

$$
\sigma_p^2 = \sum_{i=1}^n \sum_{j=1}^n w_i w_j \sigma_{ij}
$$

donde:  
- σ(ij) → covarianza entre los activos \(i\) y \(j\)  
- Si (i = j), entonces σ(ij) corresponde a la varianza del activo \(i\).  

---


## Modelo de Valoración de Activos Financieros  

El **Modelo de Valoración de Activos Financieros** (Security Market Line (SML)) es una representación gráfica de la relación entre el riesgo sistemático de un activo y su rentabilidad esperada, según el modelo CAPM. Es un instrumento clave para entender cómo el mercado valora (o debería valorar) la compensación por riesgo que exige un inversor.

Usando esta herramientas debemos tomar en cuenta que:

1. Beta (β): mide la sensibilidad del activo respecto al mercado.

    Si β > 1 → el activo es más volátil que el mercado.

    Si β < 1 → menos volátil.

    i β = 1 → sensibilidad igual al mercado.

2. Interpretaciones prácticas:

- Si un activo está **encima de la SML** → está **infravalorado** (ofrece más rentabilidad de la que debería para su nivel de riesgo).  
- Si un activo está **debajo de la SML** → está **sobrevalorado** (da menos rentabilidad de la que debería para su riesgo).  
- La SML sirve para comparar activos de **distinto riesgo sistemático**, y se aplica tanto a acciones como a carteras.  

3. Supuestos del modelo CAPM / SML:
- Inversores racionales, aversos al riesgo, que maximizan la utilidad esperada.
- Mercados eficientes (información disponible, sin costes de transacción, etc.).
- Existencia de un activo libre de riesgo al que todos tienen acceso.
- Sólo el riesgo sistemático (no diversificable) es recompensado; los riesgos idiosincráticos pueden diversificarse.

Distribución normal de retornos (o al menos, que las expectativas y varianzas-covarianzas son suficientes para describir el riesgo esperado).

### Descripción del gráfico
- **Eje X:** Riesgo sistemático de un activo, medido por su **beta (β)**.  
- **Eje Y:** Rentabilidad esperada (**E[R]**) del activo.  

El gráfico muestra la **Security Market Line (SML)**, que representa la relación entre el **riesgo sistemático** de un activo, medido por la beta βi , y su **rentabilidad esperada** E(R). En el eje horizontal se sitúa la beta, mientras que en el eje vertical se encuentra la rentabilidad esperada. La recta parte de la **tasa libre de riesgo** Rf, en este caso un **3%**, y su pendiente corresponde a la **prima de riesgo del mercado** (Rm - Rf).  

Según el modelo **CAPM**, un activo con βi = 1 debería tener una rentabilidad esperada igual a la del mercado, que en este ejemplo es del **10%**. A medida que la beta aumenta, la rentabilidad exigida por los inversores también crece, ya que el activo está más expuesto a los movimientos del mercado. Por el contrario, un activo con βi < 1 tendría una rentabilidad inferior, porque asume un menor riesgo sistemático.  


![SML](../../Imagenes/SML_Graph.jpg)

---

### Fórmula

```{math}
E(R_i) = R_f + \beta_i \cdot (R_m - R_f)
```

donde:  
- E(Ri): rentabilidad esperada del activo *i*.  
- Rf: tasa libre de riesgo (ej. bonos del Estado).  
- βi: sensibilidad del activo frente al mercado.  
- Rm: rentabilidad esperada del mercado.  
- (Rm - Rf): prima de riesgo del mercado.  

