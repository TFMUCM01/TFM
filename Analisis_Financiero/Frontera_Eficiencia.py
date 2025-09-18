# --8<-- [start:inicio]
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
    account='VLNVLDD-WJ67583',   # << org-locator de tu URL
    warehouse='COMPUTE_WH',
    database='TFM',   # <--- CAMBIO
    schema='YAHOO_FINANCE',      # << Schema de la URL
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

# ---------- helpers ----------
def crear_info_composicion(row_index, top_n=8, thr=0.01):
    comp = []
    row = Matriz_portafolios.iloc[row_index]
    for t in rendimientos.columns:
        w = row.get(f"Peso{t}", 0)
        if w > thr:
            comp.append(f"{t}: {w:.1%}")
    return "<br>".join(comp[:top_n])

def crear_composicion_detallada(port, thr=0.005):
    comp = []
    for t in rendimientos.columns:
        w = float(port.get(f"Peso{t}", 0))
        if w > thr:
            comp.append(f"{t}: {w:.1%}")
    return "<br>".join(comp)

# ---------- datos base ----------
x = Matriz_portafolios['Riesgos']
y = Matriz_portafolios['Rendimientos']
rf_used = float(rf_anual if 'rf_anual' in globals() else (rf if 'rf' in globals() else 0.03))

# sharpe de cada portafolio (sobre rf_used)
sh_col = (y - rf_used) / x
customdata_info = np.stack([
    np.array(sh_col, dtype=float),
    np.array([crear_info_composicion(i) for i in range(len(Matriz_portafolios))], dtype=object)
], axis=-1)

# ---------- nube de portafolios ----------
fig.add_trace(go.Scatter(
    x=x, y=y, mode='markers',
    marker=dict(
        size=6,
        color=y,                 # colorear por rendimiento esperado
        colorscale='Viridis',
        showscale=True,
            colorbar=dict(title="Rendimiento<br>esperado")
    ),
    showlegend=False,
    hovertemplate=(
        "<b>📊 Portafolio</b><br>"
        "Riesgo = %{x:.3f}<br>"
        "Rendimiento = %{y:.3f}<br>"
        "Sharpe = %{customdata[0]:.3f}<br><br>"
        "<b>💼 Composición</b><br>%{customdata[1]}<extra></extra>"
    ),
    customdata=customdata_info,
    cliponaxis=False
))

# ---------- puntos especiales ----------
comp_mv = crear_composicion_detallada(varianza_minima)
fig.add_trace(go.Scatter(
    x=[varianza_minima['Riesgos']], y=[varianza_minima['Rendimientos']],
    mode='markers',
    marker=dict(symbol='star', size=18, color='green',
                line=dict(width=2, color='darkgreen')),
    showlegend=False,
    hovertemplate=(
        "<b>🌟 Mínima varianza</b><br>"
        "Riesgo = %{x:.4f}<br>"
        "Rendimiento = %{y:.4f}<br><br>"
        "<b>Composición</b><br>" + comp_mv + "<extra></extra>"
    ),
    cliponaxis=False
))

sh_opt = (portafolio_optimo['Rendimientos'] - rf_used) / portafolio_optimo['Riesgos']
comp_opt = crear_composicion_detallada(portafolio_optimo)
fig.add_trace(go.Scatter(
    x=[portafolio_optimo['Riesgos']], y=[portafolio_optimo['Rendimientos']],
    mode='markers',
    marker=dict(symbol='star', size=18, color='blue',
                line=dict(width=2, color='darkblue')),
    showlegend=False,
    hovertemplate=(
        "<b>🎯 Sharpe máximo</b><br>"
        f"Sharpe = {sh_opt:.4f}<br>"
        "Riesgo = %{x:.4f}<br>"
        "Rendimiento = %{y:.4f}<br><br>"
        "<b>Composición</b><br>" + comp_opt + "<extra></extra>"
    ),
    cliponaxis=False
))

# ---------- padding y rangos para que no se corte ----------
x_min, x_max = float(np.nanmin(x)), float(np.nanmax(x))
y_min, y_max = float(np.nanmin(y)), float(np.nanmax(y))
x_pad = (x_max - x_min) * 0.06 if x_max > x_min else 0.02
y_pad = (y_max - y_min) * 0.10 if y_max > y_min else 0.02

# ---------- líneas guía opcionales ----------
# Línea horizontal en Rf
fig.add_hline(
    y=rf_used, line_dash="dash", line_width=1.5,
    annotation_text=f"Rf = {rf_used:.2%}",
    annotation_position="bottom right", annotation_yshift=-6
)

# ---------- anotaciones tipo “caja” dentro del gráfico ----------
periodo_txt = ""
if 'start_date' in globals() and 'end_date' in globals():
    periodo_txt = f"{start_date} → {end_date}"
info_caja_top = (
    "<b>Frontera de Eficiencia</b>"
    f"<br>Rf = {rf_used:.2%}"
    f"<br>N portafolios = {len(Matriz_portafolios):,}"
    + (f"<br>Periodo: {periodo_txt}" if periodo_txt else "")
)
fig.add_annotation(
    xref="paper", yref="paper",
    x=0.99, y=0.98, xanchor="right", yanchor="top",
    text=info_caja_top, showarrow=False, align="right",
    bgcolor="rgba(255,255,255,0.90)",
    bordercolor="rgba(0,0,0,0.15)", borderwidth=1, borderpad=6,
    font=dict(size=11)
)

nota_caja_bottom = (
    "<b>Marcadores</b>: "
    "<span>⭐ Mínima varianza</span> · "
    "<span>⭐ Sharpe máximo</span>"
    "<br>Color: rendimiento esperado"
)
fig.add_annotation(
    xref="paper", yref="paper",
    x=0.98, y=0.06, xanchor="right", yanchor="bottom",
    text=nota_caja_bottom, showarrow=False, align="right",
    bgcolor="rgba(255,255,255,0.90)",
    bordercolor="rgba(0,0,0,0.15)", borderwidth=1, borderpad=6,
    font=dict(size=11)
)

# ---------- layout (estilo simple_white, márgenes generosos) ----------
fig.update_layout(
    template='simple_white',
    title="Frontera de Eficiencia — Portafolios IBEX 35",
    xaxis_title="Riesgo (Volatilidad)",
    yaxis_title="Rendimiento esperado",
    margin=dict(l=60, r=170, t=90, b=80),   # r grande para la barra de color
    hovermode='closest',
    showlegend=False,
    autosize=True,
    width=None,
    height=720
)

# ejes con rango y grid suave
fig.update_xaxes(
    range=[x_min - x_pad, x_max + x_pad],
    showline=True, linecolor="#888",
    gridcolor="#E5E5E5", gridwidth=0.5
)
fig.update_yaxes(
    range=[y_min - y_pad, y_max + y_pad],
    showline=True, linecolor="#888",
    gridcolor="#E5E5E5", gridwidth=0.5,
    tickformat=".2%"
)

# ---------- etiquetas puntuales (opcional) ----------
# Si quieres texto encima de los puntos especiales:
fig.add_annotation(
    x=varianza_minima['Riesgos'], y=varianza_minima['Rendimientos'],
    text="Mínima varianza", showarrow=True, ax=24, ay=-28,
    arrowhead=2, arrowcolor="green", font=dict(size=10, color="green")
)
fig.add_annotation(
    x=portafolio_optimo['Riesgos'], y=portafolio_optimo['Rendimientos'],
    text="Sharpe máximo", showarrow=True, ax=24, ay=-28,
    arrowhead=2, arrowcolor="blue", font=dict(size=10, color="blue")
)

fig.show(config={"responsive": True, "displayModeBar": True})
# --8<-- [end:fin]