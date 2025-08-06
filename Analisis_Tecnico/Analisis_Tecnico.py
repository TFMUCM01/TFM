# -*- coding: utf-8 -*-
"""
Análisis Técnico básico para Mercados Financieros

Este documento presenta un análisis técnico básico para mercados financieros utilizando Python y Snowflake. 
El objetivo es proporcionar una herramienta para visualizar datos históricos de precios, calcular indicadores 
técnicos populares y generar gráficos que ayuden a identificar tendencias y patrones en el mercado.
"""

# Importar Librerías Necesarias
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import textwrap
import snowflake.connector

# Solicitar fechas y tickers al usuario
fecha_inicio = "2020-01-01"
fecha_fin = "2024-12-31"
tickers = ["BBVA.MC"]

# Conectar a Snowflake
conn = snowflake.connector.connect(
    user='TFMGRUPO4',
    password='TFMgrupo4ucm01_01#',
    account='WYNIFVB-YE01854',
    warehouse='COMPUTE_WH',
    database='YAHOO_FINANCE',
    schema='MACHINE_LEARNING',
    role='ACCOUNTADMIN'
)

# Mostrar tickers disponibles
query_tickers = """
    SELECT DISTINCT TICKER 
    FROM TICKERS_INDEX
    ORDER BY TICKER
"""
cursor = conn.cursor()
cursor.execute(query_tickers)
available_tickers = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
print("Tickers disponibles:")
print(available_tickers)

# Consulta para extraer precios
query_data = f"""
    SELECT TICKER, FECHA, CLOSE, OPEN, HIGH, LOW, VOLUME
    FROM TICKERS_INDEX
    WHERE FECHA BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    AND TICKER IN ({','.join([f"'{s}'" for s in tickers])})
    ORDER BY FECHA
"""
cursor.execute(query_data)
df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])

cursor.close()
conn.close()

# Convertir las columnas numéricas de 'df' a tipo float y 'VOLUME' a int
df['CLOSE'] = df['CLOSE'].astype(float)
df['OPEN'] = df['OPEN'].astype(float)
df['HIGH'] = df['HIGH'].astype(float)
df['LOW'] = df['LOW'].astype(float)
df['VOLUME'] = df['VOLUME'].astype(int)

# Filtrar solo el ticker que necesitas y establecer FECHA como índice
acciones = df[df['TICKER'] == tickers[0]].set_index('FECHA').drop('TICKER', axis=1)

# Pivotear para obtener precios por columna de ticker
acciones_cierre = df.pivot(index='FECHA', columns='TICKER', values='CLOSE')

# Definir una función para calcular el Índice de Fuerza Relativa (RSI)
def calcular_rsi(datos, ventana):
    delta = datos.diff()
    ganancia = (delta.where(delta > 0, 0)).rolling(window=ventana).mean()
    perdida = (-delta.where(delta < 0, 0)).rolling(window=ventana).mean()
    rs = ganancia / perdida
    rsi = 100 - (100 / (1 + rs))
    return rsi

# Definir una función para calcular la Convergencia/Divergencia de Medias Móviles (MACD)
def calcular_macd(datos, ventana_corta=12, ventana_larga=26, ventana_senal=9):
    ema_corta = datos.ewm(span=ventana_corta, adjust=False).mean()
    ema_larga = datos.ewm(span=ventana_larga, adjust=False).mean()
    macd = ema_corta - ema_larga
    senal = macd.ewm(span=ventana_senal, adjust=False).mean()
    return macd, senal

def calcular_mfi(datos, ventana=14):
    """Calcula el Índice de Flujo de Dinero (MFI)."""
    typical_price = (datos['HIGH'] + datos['LOW'] + datos['CLOSE']) / 3
    money_flow = typical_price * datos['VOLUME']
    
    # Calcular el flujo de dinero positivo y negativo
    delta_tp = typical_price.diff()
    positive_flow = money_flow.where(delta_tp > 0, 0)
    negative_flow = money_flow.where(delta_tp < 0, 0)
    
    # Sumar los flujos en la ventana
    positive_flow_sum = positive_flow.rolling(window=ventana).sum()
    negative_flow_sum = negative_flow.rolling(window=ventana).sum()
    
    # Calcular MFR y MFI
    mfr = positive_flow_sum / negative_flow_sum
    mfi = 100 - (100 / (1 + mfr))
    
    return mfi

def calcular_estocastico(datos, ventana_k=14, ventana_d=3):
    """Calcula el oscilador estocástico (%K y %D)."""
    lowest_low = datos['LOW'].rolling(window=ventana_k).min()
    highest_high = datos['HIGH'].rolling(window=ventana_k).max()
    k_percent = 100 * ((datos['CLOSE'] - lowest_low) / (highest_high - lowest_low))
    d_percent = k_percent.rolling(window=ventana_d).mean()
    return k_percent, d_percent

# Gráficos de Tendencias

# Gráfico de Velas
for ticker in tickers:
    if 'OPEN' in acciones.columns:
        # Crear el gráfico de velas
        grafico_velas = go.Figure(data=[go.Candlestick(x=acciones.index,
                                                       open=acciones['OPEN'],
                                                       high=acciones['HIGH'],
                                                       low=acciones['LOW'],
                                                       close=acciones['CLOSE'])])

        # Actualizar el diseño del gráfico
        grafico_velas.update_layout(xaxis_rangeslider_visible=False, title=f'Gráfico de Velas de {ticker}')
        grafico_velas.update_xaxes(title_text='Fecha')
        grafico_velas.update_yaxes(title_text=f'Precio de Cierre de {ticker}', tickprefix='$')

        # Mostrar el gráfico
        grafico_velas.show()

# Gráfico de velas personalizado
for ticker in tickers:
    if 'OPEN' in acciones.columns:
        # Crear el gráfico de velas
        c_candlestick = go.Figure(data=[go.Candlestick(x=acciones.index,
                                                            open=acciones['OPEN'],
                                                            high=acciones['HIGH'],
                                                            low=acciones['LOW'],
                                                            close=acciones['CLOSE'])])
        c_candlestick.update_xaxes(
                    title_text='Date',
                    rangeslider_visible=True,
                    rangeselector=dict(
                        buttons=list([
                            dict(count=1, label='1M', step='month', stepmode='backward'),
                            dict(count=6, label='6M', step='month', stepmode='backward'),
                            dict(count=1, label='YTD', step='year', stepmode='todate'),
                            dict(count=1, label='1Y', step='year', stepmode='backward'),
                            dict(step='all')])))

        c_candlestick.update_layout(
                    title={
                        'text': f'{ticker} Customized Candlestick Chart',
                        'y': 0.9,
                        'x': 0.5,
                        'xanchor': 'center',
                        'yanchor': 'top'})

        c_candlestick.update_yaxes(title_text=f'{ticker} Close Price', tickprefix='$')
        c_candlestick.show()

# Gráfico OHLC
for ticker in tickers:
    if 'OPEN' in acciones.columns:
        # Crear el gráfico OHLC
        grafico_ohlc = go.Figure(data=[go.Ohlc(x=acciones.index,
                                               open=acciones['OPEN'],
                                               high=acciones['HIGH'],
                                               low=acciones['LOW'],
                                               close=acciones['CLOSE'])])

        # Actualizar el diseño del gráfico
        grafico_ohlc.update_layout(xaxis_rangeslider_visible=False, title=f'Gráfico OHLC de {ticker}')
        grafico_ohlc.update_xaxes(title_text='Fecha')
        grafico_ohlc.update_yaxes(title_text=f'Precio de Cierre de {ticker}', tickprefix='$')

        # Mostrar el gráfico
        grafico_ohlc.show()

# Gráfico OHLC personalizado
for ticker in tickers:
    if 'OPEN' in acciones.columns:
        c_ohlc = go.Figure(data=[go.Ohlc(x=acciones.index,
                                         open=acciones['OPEN'],
                                         high=acciones['HIGH'],
                                         low=acciones['LOW'],
                                         close=acciones['CLOSE'])])

        c_ohlc.update_xaxes(
            title_text='Date',
            rangeslider_visible=True,
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label='1M', step='month', stepmode='backward'),
                    dict(count=6, label='6M', step='month', stepmode='backward'),
                    dict(count=1, label='YTD', step='year', stepmode='todate'),
                    dict(count=1, label='1Y', step='year', stepmode='backward'),
                    dict(step='all')])))

        c_ohlc.update_layout(
            title={
                'text': f'{ticker} Customized OHLC Chart',
                'y': 0.9,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top'})
        c_ohlc.update_yaxes(title_text=f'{ticker} Close Price', tickprefix='$')
        c_ohlc.show()

# Gráfico de Medias Móviles
for ticker in tickers:
    if 'CLOSE' in acciones.columns:
        # Calcular las medias móviles de 20, 50 y 200 días
        acciones[f'SMA20_{ticker}'] = acciones['CLOSE'].rolling(window=20).mean()
        acciones[f'SMA50_{ticker}'] = acciones['CLOSE'].rolling(window=50).mean()
        acciones[f'SMA200_{ticker}'] = acciones['CLOSE'].rolling(window=200).mean()

        # Crear el gráfico de medias móviles
        grafico_ma = go.Figure()
        grafico_ma.add_trace(go.Scatter(x=acciones.index, y=acciones['CLOSE'], mode='lines', name=f'Precio de Cierre de {ticker}'))
        grafico_ma.add_trace(go.Scatter(x=acciones.index, y=acciones[f'SMA20_{ticker}'], mode='lines', name='SMA 20 días'))
        grafico_ma.add_trace(go.Scatter(x=acciones.index, y=acciones[f'SMA50_{ticker}'], mode='lines', name='SMA 50 días'))
        grafico_ma.add_trace(go.Scatter(x=acciones.index, y=acciones[f'SMA200_{ticker}'], mode='lines', name='SMA 200 días'))

        # Actualizar el diseño del gráfico
        grafico_ma.update_layout(title=f'Medias Móviles de {ticker}', xaxis_title='Fecha', yaxis_title='Precio')
        grafico_ma.update_yaxes(tickprefix='$')

        # Mostrar el gráfico
        grafico_ma.show()

# Gráficos de Volumen
for ticker in tickers:
    if 'VOLUME' in acciones.columns:
        # Crear el gráfico de volumen
        grafico_volumen = go.Figure()
        grafico_volumen.add_trace(go.Bar(x=acciones.index, y=acciones['VOLUME'], name='Volumen', marker_color='darkred'))

        # Actualizar el diseño del gráfico
        grafico_volumen.update_layout(title=f'Volumen de {ticker}', xaxis_title='Fecha', yaxis_title='Volumen')

        # Mostrar el gráfico
        grafico_volumen.show()

# Gráficos de Fuerza (Osciladores)

# Gráfico de RSI
for ticker in tickers:
    if 'CLOSE' in acciones.columns:
        # Calcular el RSI
        acciones[f'RSI_{ticker}'] = calcular_rsi(acciones['CLOSE'], ventana=14)

        # Crear el gráfico de RSI
        grafico_rsi = go.Figure()
        grafico_rsi.add_trace(go.Scatter(x=acciones.index, y=acciones[f'RSI_{ticker}'], mode='lines', name='RSI'))
        grafico_rsi.add_hline(y=70, line_dash="dash", line_color="red")
        grafico_rsi.add_hline(y=30, line_dash="dash", line_color="green")

        # Actualizar el diseño del gráfico
        grafico_rsi.update_layout(title=f'RSI de {ticker}', xaxis_title='Fecha', yaxis_title='RSI')

        # Mostrar el gráfico
        grafico_rsi.show()

# Gráfico de MACD
for ticker in tickers:
    if 'CLOSE' in acciones.columns:
        # Calcular el MACD y la señal
        acciones[f'MACD_{ticker}'], acciones[f'Signal_{ticker}'] = calcular_macd(acciones['CLOSE'])

        # Crear el gráfico de MACD
        grafico_macd = go.Figure()
        grafico_macd.add_trace(go.Scatter(x=acciones.index, y=acciones[f'MACD_{ticker}'], mode='lines', name='MACD'))
        grafico_macd.add_trace(go.Scatter(x=acciones.index, y=acciones[f'Signal_{ticker}'], mode='lines', name='Señal'))

        # Actualizar el diseño del gráfico
        grafico_macd.update_layout(title=f'MACD de {ticker}', xaxis_title='Fecha', yaxis_title='MACD')

        # Mostrar el gráfico
        grafico_macd.show()

# Gráfico de Bandas de Bollinger
for ticker in tickers:
    if 'CLOSE' in acciones.columns:
        # Calcular las bandas de Bollinger
        acciones[f'BB_upper_{ticker}'] = acciones['CLOSE'].rolling(window=20).mean() + (acciones['CLOSE'].rolling(window=20).std() * 2)
        acciones[f'BB_lower_{ticker}'] = acciones['CLOSE'].rolling(window=20).mean() - (acciones['CLOSE'].rolling(window=20).std() * 2)

        # Crear el gráfico de bandas de Bollinger
        grafico_bb = go.Figure()
        grafico_bb.add_trace(go.Scatter(x=acciones.index, y=acciones['CLOSE'], mode='lines', name=f'Precio de Cierre de {ticker}'))
        grafico_bb.add_trace(go.Scatter(x=acciones.index, y=acciones[f'BB_upper_{ticker}'], mode='lines', name='Banda Superior'))
        grafico_bb.add_trace(go.Scatter(x=acciones.index, y=acciones[f'BB_lower_{ticker}'], mode='lines', name='Banda Inferior'))

        # Actualizar el diseño del gráfico
        grafico_bb.update_layout(title=f'Bandas de Bollinger de {ticker}', xaxis_title='Fecha', yaxis_title='Precio')
        grafico_bb.update_yaxes(tickprefix='$')

        # Mostrar el gráfico
        grafico_bb.show()

# Gráfico de MFI
for ticker in tickers:
    if 'CLOSE' in acciones.columns:
        # Calcular el MFI
        acciones[f'MFI_{ticker}'] = calcular_mfi(acciones, ventana=14)

        # Crear el gráfico de MFI
        grafico_mfi = go.Figure()
        grafico_mfi.add_trace(go.Scatter(x=acciones.index, y=acciones[f'MFI_{ticker}'], mode='lines', name='MFI'))
        grafico_mfi.add_hline(y=80, line_dash="dash", line_color="red")  # Sobrecompra
        grafico_mfi.add_hline(y=20, line_dash="dash", line_color="green")  # Sobreventa

        # Actualizar el diseño del gráfico
        grafico_mfi.update_layout(title=f'MFI de {ticker}', xaxis_title='Fecha', yaxis_title='MFI')

        # Mostrar el gráfico
        grafico_mfi.show()

# Gráfico de Estocástico
for ticker in tickers:
    if 'CLOSE' in acciones.columns:
        # Calcular el Estocástico
        acciones[f'Stochastic_K_{ticker}'], acciones[f'Stochastic_D_{ticker}'] = calcular_estocastico(acciones, ventana_k=14)

        # Crear el gráfico del Estocástico
        grafico_estocastico = go.Figure()
        grafico_estocastico.add_trace(go.Scatter(x=acciones.index, y=acciones[f'Stochastic_K_{ticker}'], mode='lines', name='%K'))
        grafico_estocastico.add_trace(go.Scatter(x=acciones.index, y=acciones[f'Stochastic_D_{ticker}'], mode='lines', name='%D'))
        grafico_estocastico.add_hline(y=80, line_dash="dash", line_color="red")  # Sobrecompra
        grafico_estocastico.add_hline(y=20, line_dash="dash", line_color="green")  # Sobreventa

        # Actualizar el diseño del gráfico
        grafico_estocastico.update_layout(title=f'Estocástico de {ticker}', xaxis_title='Fecha', yaxis_title='Estocástico')

        # Mostrar el gráfico
        grafico_estocastico.show()

# Conclusión del análisis
print("="*60)
print("ANÁLISIS TÉCNICO COMPLETADO")
print("="*60)
print(f"Ticker analizado: {tickers[0]}")
print(f"Período: {fecha_inicio} a {fecha_fin}")
print()
print("Gráficos generados:")
print("- Gráfico de Velas (básico y personalizado)")
print("- Gráfico OHLC (básico y personalizado)")
print("- Gráfico de Medias Móviles (SMA 20, 50, 200)")
print("- Gráfico de Volumen")
print("- Gráfico RSI (Índice de Fuerza Relativa)")
print("- Gráfico MACD (Convergencia/Divergencia de Medias Móviles)")
print("- Gráfico de Bandas de Bollinger")
print("- Gráfico MFI (Índice de Flujo de Dinero)")
print("- Gráfico Estocástico (%K y %D)")
print()
print("IMPORTANTE:")
print("- El análisis técnico debe complementarse con análisis fundamental")
print("- Los indicadores no son infalibles")
print("- El trading conlleva riesgos")
print("- Gestione su riesgo de manera responsable")
print("="*60)
