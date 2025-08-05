### 1. Configuración Global
import pandas as pd
import numpy as np
import getpass
import snowflake.connector
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objs as go

# Variables globales
stocks = ['BBVA.MC', 'IBE.MC', 'ITX.MC', 'REP.MC']
start_date = '2020-01-01'
end_date = '2024-12-31'
NUM_PORTFOLIOS = 10000
NUM_TRADING_DAYS = 252
RISK_FREE_RATE = 0.02


### 2. Conexión y Consulta a Snowflake
def conectar_snowflake():
    password = getpass.getpass("🔒 Introduce tu contraseña de Snowflake: ")
    conn = snowflake.connector.connect(
        user='TFMGRUPO4',
        password=password,
        account='WYNIFVB-YE01854',
        warehouse='COMPUTE_WH',
        database='YAHOO_FINANCE',
        schema='MACHINE_LEARNING',
        role='ACCOUNTADMIN'
    )
    return conn

def obtener_precios(stocks, start_date, end_date):
    conn = conectar_snowflake()
    cursor = conn.cursor()

    tickers_str = ', '.join(f"'{ticker}'" for ticker in stocks)
    query = f"""
        SELECT TICKER, FECHA, CLOSE
        FROM TICKERS_INDEX
        WHERE TICKER IN ({tickers_str})
          AND FECHA BETWEEN '{start_date}' AND '{end_date}'
    """

    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    cursor.close()
    conn.close()

    df['FECHA'] = pd.to_datetime(df['FECHA'])
    df_prices = df.pivot(index='FECHA', columns='TICKER', values='CLOSE')
    df_prices = df_prices.sort_index().fillna(method='ffill').dropna()
    return df_prices


### 3. Cálculo de Retornos y Estadísticas
def calcular_retorno_log(df_prices):
    return np.log(df_prices / df_prices.shift(1)).dropna()

def calcular_estadisticas_portafolio(pesos, mean_returns, cov_matrix):
    retorno = np.sum(mean_returns * pesos) * NUM_TRADING_DAYS
    volatilidad = np.sqrt(np.dot(pesos.T, np.dot(cov_matrix * NUM_TRADING_DAYS, pesos)))
    sharpe_ratio = (retorno - RISK_FREE_RATE) / volatilidad
    return retorno, volatilidad, sharpe_ratio


### 4. Simulación de Portafolios
def simular_portafolios(mean_returns, cov_matrix):
    num_activos = len(mean_returns)
    resultados = np.zeros((NUM_PORTFOLIOS, 3 + num_activos))

    for i in range(NUM_PORTFOLIOS):
        pesos = np.random.random(num_activos)
        pesos /= np.sum(pesos)

        retorno, volatilidad, sharpe = calcular_estadisticas_portafolio(pesos, mean_returns, cov_matrix)

        resultados[i, 0] = retorno
        resultados[i, 1] = volatilidad
        resultados[i, 2] = sharpe
        resultados[i, 3:] = pesos

    columnas = ['Retorno', 'Volatilidad', 'Sharpe'] + list(mean_returns.index)
    return pd.DataFrame(resultados, columns=columnas)


### 5. Identificación de Portafolios Óptimos
def obtener_portafolios_optimos(df_resultados):
    max_sharpe = df_resultados.loc[df_resultados['Sharpe'].idxmax()]
    min_vol = df_resultados.loc[df_resultados['Volatilidad'].idxmin()]
    return max_sharpe, min_vol


### 6. Visualizaciones Interactivas
def graficar_frontera_eficiente(df_resultados, max_sharpe, min_vol):
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_resultados['Volatilidad'],
        y=df_resultados['Retorno'],
        mode='markers',
        marker=dict(color=df_resultados['Sharpe'], colorscale='Viridis', size=5, showscale=True),
        name='Portafolios simulados'
    ))
    fig.add_trace(go.Scatter(
        x=[max_sharpe['Volatilidad']],
        y=[max_sharpe['Retorno']],
        mode='markers',
        marker=dict(color='red', size=10, symbol='star'),
        name='Mayor Sharpe'
    ))
    fig.add_trace(go.Scatter(
        x=[min_vol['Volatilidad']],
        y=[min_vol['Retorno']],
        mode='markers',
        marker=dict(color='blue', size=10, symbol='diamond'),
        name='Menor Volatilidad'
    ))
    fig.update_layout(title='Frontera Eficiente', xaxis_title='Volatilidad', yaxis_title='Retorno Esperado')
    fig.show()

def graficar_sharpe_ratio(df_resultados):
    fig = go.Figure()
    fig.add_trace(go.Scatter(y=df_resultados['Sharpe'], mode='lines', name='Sharpe Ratio'))
    fig.update_layout(title='Sharpe Ratio por Portafolio Simulado', xaxis_title='Simulación', yaxis_title='Sharpe Ratio')
    fig.show()

def graficar_correlacion(df_log_returns):
    plt.figure(figsize=(10, 6))
    sns.heatmap(df_log_returns.corr(), annot=True, cmap='coolwarm')
    plt.title('Correlación entre activos')
    plt.show()


### 7. Ejecución del Análisis
df_prices = obtener_precios(stocks, start_date, end_date)
df_log_returns = calcular_retorno_log(df_prices)

mean_returns = df_log_returns.mean()
cov_matrix = df_log_returns.cov()

resultados = simular_portafolios(mean_returns, cov_matrix)
max_sharpe, min_vol = obtener_portafolios_optimos(resultados)

graficar_frontera_eficiente(resultados, max_sharpe, min_vol)
graficar_sharpe_ratio(resultados)
graficar_correlacion(df_log_returns)


### 8. Notas de Interpretación
# - Frontera eficiente: cada punto es un portafolio. El color indica el Sharpe Ratio.
# - Punto rojo: mejor Sharpe (mejor relación retorno/riesgo).
# - Punto azul: menor volatilidad (portafolio más estable).
# - El heatmap permite ver correlaciones entre activos.


### 9. Extensiones posibles
# - Uso de cvxpy para optimización con restricciones.
# - Límites mínimos/máximos de pesos.
# - Inclusión de activos libres de riesgo o benchmarks.
# - Optimizar en base a retornos objetivo o tracking error.
