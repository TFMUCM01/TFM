import pandas as pd
import numpy as np
import snowflake.connector
import statsmodels.api as sm
import plotly.graph_objects as go


# Configuración (ANUAL)

start_date   = '2020-01-01'
end_date     = '2024-12-31'
ticker_ibex  = 'IBEX 35'          # usamos el nombre del índice en INDEX_DAILY
rf_anual     = 0.03               # 3% anual
freq_label   = 'anual'
periodo_label = 'años'


tickers = [
    "IBE.MC", "ITX.MC", "TEF.MC", "BBVA.MC", "SAN.MC", "REP.MC",
    "AENA.MC", "IAG.MC", "ENG.MC", "ACS.MC", "FER.MC", "CABK.MC",
    "ELE.MC", "MAP.MC"
]

# Conexión Snowflake (DB/Schema correctos)
# -----------------------------
conn = snowflake.connector.connect(
    user='TFMGRUPO4',
    password='TFMgrupo4ucm01_01#',
    account='VLNVLDD-WJ67583',   # <<<<<< usa el account de tu nueva URL
    warehouse='COMPUTE_WH',
    database='TFM',              # <<<<<< Database
    schema='YAHOO_FINANCE',      # <<<<<< Schema
    role='ACCOUNTADMIN'
)
cursor = conn.cursor()

# IBEX (mercado) - INDEX_DAILY
# -----------------------------
q_ibex = f"""
    SELECT FECHA, CLOSE
    FROM INDEX_DAILY
    WHERE INDEX_NAME = '{ticker_ibex}'
      AND FECHA BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY FECHA
"""
cursor.execute(q_ibex)
df_ibex = pd.DataFrame(cursor.fetchall(), columns=['FECHA', 'IBEX'])
df_ibex['FECHA'] = pd.to_datetime(df_ibex['FECHA'])
df_ibex['IBEX'] = pd.to_numeric(df_ibex['IBEX'], errors='coerce').astype(float)
df_ibex.rename(columns={'FECHA': 'DATE'}, inplace=True)  # mantener tu índice con 'DATE'
df_ibex.set_index('DATE', inplace=True)
df_ibex = df_ibex.sort_index()

# Activos (precios) - TICKERS_INDEX
# -----------------------------
dfs = {'IBEX': df_ibex['IBEX']}
for tk in tickers:
    q = f"""
        SELECT FECHA, CLOSE
        FROM TICKERS_INDEX
        WHERE TICKER = '{tk}'
          AND FECHA BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY FECHA
    """
    cursor.execute(q)
    df = pd.DataFrame(cursor.fetchall(), columns=['FECHA', tk])
    df['FECHA'] = pd.to_datetime(df['FECHA'])
    df[tk] = pd.to_numeric(df[tk], errors='coerce').astype(float)
    df.set_index('FECHA', inplace=True)
    df = df.sort_index()
    dfs[tk] = df[tk]

cursor.close()
conn.close()


panel = pd.concat(dfs, axis=1, join='outer').sort_index()

def last_valid(series: pd.Series):
    s = series.dropna()
    return s.iloc[-1] if not s.empty else np.nan

# Fin de año ('Y'): tomamos el último precio válido del año
panel_y = panel.resample('Y').apply(last_valid)
rets    = panel_y.pct_change(fill_method=None)     # retornos anuales


# rf del periodo (ANUAL) para excesos
# -----------------------------
rf_period = rf_anual  # k = 1


def geometric_annualized(r: pd.Series, periods_per_year=1):
    r = r.dropna()
    n = r.shape[0]
    if n == 0:
        return np.nan
    gross = (1 + r).prod()
    return gross**(periods_per_year / n) - 1  # k=1 ⇒ media geométrica anual

def estimate_beta_alpha_r2(ri: pd.Series, rm: pd.Series, rf_p: float):
    """OLS en EXCESOS con constante: (Ri - Rf) = α + β (Rm - Rf) + ε"""
    pair = pd.concat([rm, ri], axis=1, join='inner').dropna()
    pair.columns = ['Rm', 'Ri']
    nobs = pair.shape[0]
    if nobs < 3:  # mínimo 3 retornos anuales
        return None, None, None, nobs
    X = pair['Rm'] - rf_p
    y = pair['Ri'] - rf_p
    Xc = sm.add_constant(X)
    res = sm.OLS(y, Xc).fit()
    beta  = float(res.params.get('Rm', np.nan))
    alpha = float(res.params.get('const', np.nan))
    r2    = float(res.rsquared)
    return beta, alpha, r2, nobs


# E[Rm] (geom) anual y prima de mercado
E_Rm_ann = geometric_annualized(rets['IBEX'], periods_per_year=1)
if pd.isna(E_Rm_ann) or rets['IBEX'].dropna().shape[0] < 3:
    raise RuntimeError("No hay años suficientes para β anual. Amplía el rango o revisa la cobertura del IBEX.")
market_premium = E_Rm_ann - rf_anual

# Chequeo: en β=0, SML debe valer Rf
print(f"Chequeo SML: y(β=0) = {rf_anual + market_premium*0.0:.2%} (Rf = {rf_anual:.2%})")


#β anual, α, R² y E[Ri] anual por activo

rows = []
for tk in tickers:
    beta, alpha, r2, nobs = estimate_beta_alpha_r2(rets[tk], rets['IBEX'], rf_period)
    if beta is None:
        print(f"[AVISO] {tk}: años insuficientes tras alinear (n={nobs}). Se omite.")
        continue

    pair = pd.concat([rets['IBEX'], rets[tk]], axis=1, join='inner').dropna()
    E_Ri_ann = geometric_annualized(pair[tk], periods_per_year=1)

    E_Ri_capm = rf_anual + beta * (E_Rm_ann - rf_anual)   # Predicción CAPM anual
    mispricing = E_Ri_ann - E_Ri_capm                     # Real - CAPM

    rows.append({
        'Ticker': tk,
        'Beta': beta,
        'Alpha_excesos': alpha,
        'R2': r2,
        'N_obs': nobs,
        'E_Ri_ann_geom': E_Ri_ann,
        'E_Ri_CAPM': E_Ri_capm,
        'Mispricing': mispricing
    })

df_points = pd.DataFrame(rows)
if df_points.empty:
    raise RuntimeError("No hay puntos para graficar (β anual). Amplía el rango o revisa cobertura.")
df_points = df_points.sort_values('Beta').reset_index(drop=True)


# Clasificación TP / FP / FN / TN
above_mean = df_points['E_Ri_ann_geom'] >= E_Rm_ann
above_sml  = df_points['E_Ri_ann_geom'] >= df_points['E_Ri_CAPM']

def _class_row(am, asml):
    if am and asml:        return 'TP'  # arriba media y arriba SML
    if am and not asml:    return 'FP'  # arriba media pero abajo SML
    if (not am) and asml:  return 'FN'  # abajo media pero arriba SML
    return 'TN'                           # abajo media y abajo SML

df_points['Class'] = [_class_row(am, asml) for am, asml in zip(above_mean, above_sml)]


# Colores y símbolos por clase
class_color  = {'TP':'#2ca02c', 'FP':'#ff7f0e', 'FN':'#1f77b4', 'TN':'#d62728'}
class_symbol = {'TP':'circle',  'FP':'diamond', 'FN':'triangle-up', 'TN':'x'}

# Conteos para anotación
cnt = df_points['Class'].value_counts().to_dict()
cTP = cnt.get('TP', 0); cFP = cnt.get('FP', 0); cFN = cnt.get('FN', 0); cTN = cnt.get('TN', 0)


# SML y rangos de ejes (forzamos que arranque en β=0)
# -----------------------------
beta_min = 0.0
beta_max = max(1.5, float(df_points['Beta'].max() + 0.2))
betas_line = np.linspace(beta_min, beta_max, 200)
E_R_line   = rf_anual + market_premium * betas_line

y_vals = np.concatenate([
    df_points['E_Ri_ann_geom'].values,
    [rf_anual, E_Rm_ann, E_R_line.min(), E_R_line.max()]
])
y_min = float(np.nanmin(y_vals)) - 0.03
y_max = float(np.nanmax(y_vals)) + 0.03


# GRÁFICO Plotly — SML ANUAL (uniforme con el mensual)
# -----------------------------
fig = go.Figure()

# SML (desde β=0)
fig.add_trace(go.Scatter(
    x=betas_line, y=E_R_line, mode='lines',
    line=dict(width=3),
    showlegend=False,
    name='SML'
))

# Y medio del mercado (E[Rm]) — etiqueta a la DERECHA
fig.add_hline(
    y=E_Rm_ann, line_dash="dash", line_width=2,
    annotation_text=f"Ȳ = E[Rm] {E_Rm_ann:.2%}",
    annotation_position="top right", annotation_yshift=6
)

# Rf (β=0)
fig.add_trace(go.Scatter(
    x=[0], y=[rf_anual], mode='markers',
    marker=dict(symbol='x', size=12, line=dict(width=1.5)),
    showlegend=False,
    name='Rf',
    hovertemplate="<b>Activo sin riesgo</b><br>β=0.00<br>Rendimiento= %{y:.2%}<extra></extra>"
))

# Mercado (β=1)
fig.add_trace(go.Scatter(
    x=[1], y=[E_Rm_ann], mode='markers',
    marker=dict(size=12, line=dict(width=1.5)),
    showlegend=False,
    name='Mercado',
    hovertemplate="<b>Mercado</b><br>β=1.00<br>E[Rm]= %{y:.2%}<extra></extra>"
))

# Activos por clase (4 trazas): colores/símbolos distintos, sin leyenda
for cls in ['TP','FP','FN','TN']:
    sub = df_points[df_points['Class'] == cls]
    if sub.empty:
        continue
    fig.add_trace(go.Scatter(
        x=sub['Beta'],
        y=sub['E_Ri_ann_geom'],
        mode='markers',
        marker=dict(
            size=10,
            symbol=class_symbol[cls],
            color=class_color[cls],
            line=dict(width=1, color='rgba(0,0,0,0.45)')
        ),
        showlegend=False,
        name=f'Activos {cls}',
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>"
            f"Clase = {cls} "
            "(%{customdata[8]})<br>"
            f"n = %{{customdata[7]}} {periodo_label}<br>"
            "β = %{x:.3f} | E[Ri] (geom, anual) = %{y:.2%}<br>"
            "E[Rm] = %{customdata[1]:.2%} | Rf = %{customdata[2]:.2%}<br>"
            "<b>CAPM</b>: E[Ri]_CAPM = Rf + β·(E[Rm]−Rf) = "
            "%{customdata[2]:.2%} + %{x:.3f}·(%{customdata[1]:.2%} − %{customdata[2]:.2%}) "
            "= %{customdata[3]:.2%}<br>"
            "α (excesos) = %{customdata[4]:.2%} | R² = %{customdata[5]:.3f}<br>"
            "Mispricing = E[Ri] − E[Ri]_CAPM = %{customdata[6]:.2%}"
            "<extra></extra>"
        ),
        customdata=np.stack([
            sub['Ticker'],
            np.full(len(sub), E_Rm_ann),
            np.full(len(sub), rf_anual),
            sub['E_Ri_CAPM'],
            sub['Alpha_excesos'],
            sub['R2'],
            sub['Mispricing'],
            sub['N_obs'],
            np.where(
                (sub['E_Ri_ann_geom'] >= E_Rm_ann) & (sub['E_Ri_ann_geom'] >= sub['E_Ri_CAPM']),
                "↑media & ↑SML",
                np.where(
                    (sub['E_Ri_ann_geom'] >= E_Rm_ann) & (sub['E_Ri_ann_geom'] < sub['E_Ri_CAPM']),
                    "↑media & ↓SML",
                    np.where(
                        (sub['E_Ri_ann_geom'] < E_Rm_ann) & (sub['E_Ri_ann_geom'] >= sub['E_Ri_CAPM']),
                        "↓media & ↑SML",
                        "↓media & ↓SML"
                    )
                )
            )
        ], axis=-1)
    ))

# Etiquetas (texto sobre cada punto)
fig.add_trace(go.Scatter(
    x=df_points['Beta'],
    y=df_points['E_Ri_ann_geom'],
    mode='text',
    text=df_points['Ticker'],
    textposition='top center',
    textfont=dict(size=10),
    showlegend=False,
    name='Etiquetas'
))

# -----------------------------
# Caja de FÓRMULA — ARRIBA-DERECHA (dentro del gráfico)
# -----------------------------
nota_formula = (
    "CAPM:  E[Ri] = Rf + β·(E[Rm]−Rf)"
    f"<br>Rf = {rf_anual:.2%} · E[Rm] (geom) = {E_Rm_ann:.2%}"
    f"<br>Prima de mercado = {market_premium:.2%}"
)
fig.add_annotation(
    xref="paper", yref="paper",
    x=0.99, y=0.98, xanchor="right", yanchor="top",
    text=nota_formula, showarrow=False, align="right",
    bordercolor="rgba(0,0,0,0.15)", borderwidth=1,
    bgcolor="rgba(255,255,255,0.90)", font=dict(size=11),
    borderpad=6
)

# -----------------------------
# Caja de CLASES — ABAJO-DERECHA (dentro del gráfico)
# -----------------------------
nota_clases = (
    "<b>Clases</b>: "
    f"<span style='color:{class_color['TP']}'>■ TP</span> "
    f"<span style='color:{class_color['FP']}'>■ FP</span> "
    f"<span style='color:{class_color['FN']}'>■ FN</span> "
    f"<span style='color:{class_color['TN']}'>■ TN</span>"
    f"<br>TP={cTP} · FP={cFP} · FN={cFN} · TN={cTN}"
)
fig.add_annotation(
    xref="paper", yref="paper",
    x=0.98, y=0.06,                # abajo-derecha
    xanchor="right", yanchor="bottom",
    text=nota_clases, showarrow=False, align="right",
    bordercolor="rgba(0,0,0,0.15)", borderwidth=1,
    bgcolor="rgba(255,255,255,0.90)", font=dict(size=11),
    borderpad=6
)

# Layout (sin leyenda) con márgenes moderados
fig.update_layout(
    template='simple_white',
    title=("SML (CAPM):  E[Ri] = Rf + β(E[Rm]−Rf)  ·  "
           f"Periodo: {start_date} → {end_date}  ·  Frecuencia β: {freq_label}"),
    xaxis_title="Beta (β)",
    yaxis_title="Rendimiento esperado ANUAL",
    margin=dict(l=40, r=60, t=90, b=80),
    hovermode='closest',
    showlegend=False
)

# Ejes (β desde 0 para que la recta parta en Rf visible)
fig.update_xaxes(
    range=[beta_min, beta_max],
    zeroline=True, zerolinewidth=1, zerolinecolor="#B0B0B0",
    showline=True, linecolor="#888", dtick=0.25
)
fig.update_yaxes(
    range=[y_min, y_max],
    zeroline=True, zerolinewidth=1, zerolinecolor="#B0B0B0",
    showline=True, linecolor="#888", tickformat=".0%"
)

fig.show()





