# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import snowflake.connector
import matplotlib.pyplot as plt 
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px

# ================================
# Conexión a Snowflake
# ================================
conn = snowflake.connector.connect(
    user='TFMGRUPO4',
    password='TFMgrupo4ucm01_01#',
    account='WYNIFVB-YE01854',
    warehouse='COMPUTE_WH',
    database='YAHOO_PRUEBA',   # <--- ya ajustado
    schema='IBEX',             # <--- ya ajustado
    role='ACCOUNTADMIN'
)

# ================================
# Tickers y fechas
# ================================
tickers = [
    "IBE.MC", "ITX.MC", "TEF.MC", "BBVA.MC", "SAN.MC", "REP.MC",
    "AENA.MC", "IAG.MC", "ENG.MC", "ACS.MC", "FER.MC", "CABK.MC",
    "ELE.MC", "MAP.MC"
]
fecha_inicio = "2020-01-01"
fecha_fin    = "2024-12-31"

# ================================
# Query de datos
# ================================
quoted_tickers = ",".join([f"'{ticker}'" for ticker in tickers])
query = f"""
SELECT TICKER, FECHA, CLOSE
FROM TICKERS_INDEX
WHERE TICKER IN ({quoted_tickers})
  AND FECHA BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
  AND CLOSE IS NOT NULL
ORDER BY FECHA, TICKER
"""

cursor = conn.cursor()
cursor.execute(query)
column_names = [desc[0] for desc in cursor.description]
df_prices = pd.DataFrame(cursor.fetchall(), columns=column_names)
cursor.close()
conn.close()

df_prices['CLOSE'] = df_prices['CLOSE'].astype(float)
df_prices['FECHA'] = pd.to_datetime(df_prices['FECHA'])

prices = df_prices.pivot(index='FECHA', columns='TICKER', values='CLOSE').sort_index()

# ================================
# Cálculo de rendimientos
# ================================
# --8<-- [start:rendimientos]
rendimientos = (prices - prices.shift(1)) / prices.shift(1)
# --8<-- [end:rendimientos]
numero_activos = len(rendimientos.columns)

rendimientos_portafolio = []
riesgo_portafolio = []
peso_portafolio = []

# --8<-- [start:iteracioncarteras]
for x in range(50000):
    pesos = np.random.random(numero_activos)
    pesos /= np.sum(pesos)
    rendimiento = np.sum(pesos * rendimientos.mean()) * 252
    riesgo = np.sqrt(np.dot(pesos.T, np.dot(rendimientos.cov() * 252, pesos)))
    rendimientos_portafolio.append(rendimiento)
    riesgo_portafolio.append(riesgo)
    peso_portafolio.append(pesos)
# --8<-- [end:iteracioncarteras]
# ================================
# Matriz de portafolios
# ================================
portafolios = {
    'Rendimientos': rendimientos_portafolio,
    'Riesgos': riesgo_portafolio,
}
for contador, ticker in enumerate(rendimientos.columns.tolist()):
    portafolios["Peso" + ticker] = [W[contador] for W in peso_portafolio]

Matriz_portafolios = pd.DataFrame(portafolios)

# ================================
# Portafolios destacados
# ================================

# --8<-- [start:varianza_minima]
varianza_minima = Matriz_portafolios.iloc[Matriz_portafolios['Riesgos'].idxmin()]

risk_free = 0.03
portafolio_optimo = Matriz_portafolios.iloc[
    ((Matriz_portafolios['Rendimientos'] - risk_free) / Matriz_portafolios['Riesgos']).idxmax()
]
# --8<-- [end:varianza_minima]

# ================================
# Funciones auxiliares
# ================================
def crear_info_composicion(row_index):
    composicion = []
    for ticker in rendimientos.columns:
        peso_col = f"Peso{ticker}"
        peso = Matriz_portafolios.iloc[row_index][peso_col]
        if peso > 0.01:  # >1%
            composicion.append(f"{ticker}: {peso:.1%}")
    return "<br>".join(composicion[:8])

def crear_composicion_detallada(portafolio):
    info = []
    for ticker in rendimientos.columns:
        peso_col = f"Peso{ticker}"
        peso = portafolio[peso_col]
        if peso > 0.005:  # >0.5%
            info.append(f"{ticker}: {peso:.1%}")
    return "<br>".join(info)

# ================================
# Gráfico frontera de eficiencia
# ================================
fig = go.Figure()

# Scatter de todos los portafolios
customdata_info = []
for i in range(len(Matriz_portafolios)):
    sharpe = (Matriz_portafolios.iloc[i]['Rendimientos'] - risk_free) / Matriz_portafolios.iloc[i]['Riesgos']
    composicion = crear_info_composicion(i)
    customdata_info.append([sharpe, composicion])

fig.add_trace(go.Scatter(
    x=Matriz_portafolios['Riesgos'],
    y=Matriz_portafolios['Rendimientos'],
    mode='markers',
    marker=dict(size=5, color=Matriz_portafolios['Rendimientos'], colorscale='Viridis', opacity=0.7,
                colorbar=dict(title="Rendimiento<br>Esperado", titleside="right")),
    name='Portafolios simulados',
    hovertemplate='<b>📊 Portafolio Simulado</b><br>'
                  '<b>Riesgo:</b> %{x:.3f}<br>'
                  '<b>Rendimiento:</b> %{y:.3f}<br>'
                  '<b>Ratio Sharpe:</b> %{customdata[0]:.3f}<br>'
                  '<br><b>💼 Composición del Portafolio:</b><br>'
                  '%{customdata[1]}<br>'
                  '<extra></extra>',
    customdata=customdata_info
))

# Mínima varianza
composicion_mv = crear_composicion_detallada(varianza_minima)
fig.add_trace(go.Scatter(
    x=[varianza_minima['Riesgos']], y=[varianza_minima['Rendimientos']],
    mode='markers', marker=dict(symbol='star', size=18, color='green', line=dict(color='darkgreen', width=2)),
    name='Mínima varianza',
    hovertemplate='<b>🌟 Portafolio de Mínima Varianza</b><br>'
                  '<b>Riesgo:</b> %{x:.4f}<br>'
                  '<b>Rendimiento:</b> %{y:.4f}<br>'
                  '<br><b>💼 Composición del Portafolio:</b><br>' + f'{composicion_mv}<br>'
                  '<extra></extra>'
))

# Sharpe máximo
sharpe_ratio = (portafolio_optimo['Rendimientos'] - risk_free) / portafolio_optimo['Riesgos']
composicion_opt = crear_composicion_detallada(portafolio_optimo)
fig.add_trace(go.Scatter(
    x=[portafolio_optimo['Riesgos']], y=[portafolio_optimo['Rendimientos']],
    mode='markers', marker=dict(symbol='star', size=18, color='blue', line=dict(color='darkblue', width=2)),
    name='Sharpe máximo',
    hovertemplate='<b>🎯 Portafolio Óptimo (Sharpe máximo)</b><br>'
                  '<b>Riesgo:</b> %{x:.4f}<br>'
                  '<b>Rendimiento:</b> %{y:.4f}<br>'
                  f'<b>Ratio Sharpe:</b> {sharpe_ratio:.4f}<br>'
                  '<br><b>💼 Composición del Portafolio:</b><br>' + f'{composicion_opt}<br>'
                  '<extra></extra>'
))

# Layout
fig.update_layout(
    title=dict(text='📈 Frontera de Eficiencia - Portafolios IBEX 35', x=0.5,
               font=dict(size=16, family="Arial", color='darkblue')),
    xaxis=dict(title='Riesgo (Volatilidad)', titlefont=dict(size=12), tickfont=dict(size=10),
               gridcolor='lightgray', gridwidth=0.5, showgrid=True),
    yaxis=dict(title='Rendimiento Esperado', titlefont=dict(size=12), tickfont=dict(size=10),
               gridcolor='lightgray', gridwidth=0.5, showgrid=True),
    plot_bgcolor='white', paper_bgcolor='white',
    hovermode='closest', width=1000, height=700, showlegend=True,
    legend=dict(x=0.02, y=0.98, bgcolor='rgba(255,255,255,0.9)', bordercolor='gray', borderwidth=1, font=dict(size=10))
)

# Anotaciones
fig.add_annotation(x=varianza_minima['Riesgos'], y=varianza_minima['Rendimientos'],
                   text="🌟 Mínima<br>Varianza", showarrow=True, arrowhead=2, arrowsize=1,
                   arrowwidth=2, arrowcolor="green", ax=30, ay=-40, font=dict(size=10, color="green"))

fig.add_annotation(x=portafolio_optimo['Riesgos'], y=portafolio_optimo['Rendimientos'],
                   text="🎯 Sharpe<br>Máximo", showarrow=True, arrowhead=2, arrowsize=1,
                   arrowwidth=2, arrowcolor="blue", ax=30, ay=-40, font=dict(size=10, color="blue"))

fig.show()
