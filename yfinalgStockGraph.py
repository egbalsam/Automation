#===============================================
#SOURCE: https://levelup.gitconnected.com/how-i-tripled-my-return-on-bitcoin-using-mathematics-algorithms-and-python-347edd9b5625
#===============================================
import numpy as np
import pandas as pd
#Data Source
import yfinance as yf
#Data viz
import plotly.graph_objs as go

print('===============================================')
print('TICKERS:')
print('===============================================')

tick = input('Enter ticker: ')


sma = 7
lma = 30

#Importing market data
print('===============================================')
print('INTERVALS:')
print('===============================================')
print('   MINUTES:       HOURS:          MONTHS:')
print('   1m             1h              1mo')
print('   2m             DAYS:           3mo')
print('   5m             1d')
print('   15m            5d')
print('   30m            WEEKS:')
print('   90m            1wk')
print('===============================================')

timeint = input('Enter time interval: ')

if timeint == '1m':
    data = yf.download(tickers= tick ,period = '1d', interval = timeint)
    sma = 630
    lma = 2700
elif timeint == '2m':
    data = yf.download(tickers= tick ,period = '2d', interval = timeint)
    sma = 315
    lma = 1350
elif timeint == '5m':
    data = yf.download(tickers= tick ,period = '5d', interval = timeint)
    sma = 123
    lma = 540
elif timeint == '15m':
    data = yf.download(tickers= tick ,period = '15d', interval = timeint)
    sma = 41
    lma = 180
elif timeint == '30m':
    data = yf.download(tickers= tick ,period = '30d', interval = timeint)
    sma = 21
    lma = 90
elif timeint == '90m':
    data = yf.download(tickers= tick ,period = '60d', interval = timeint)
    sma = 7
    lma = 30
elif timeint == '1h':
    data = yf.download(tickers= tick ,period = '60d', interval = timeint)
    sma = 5
    lma = 20
elif timeint == '1d':
    data = yf.download(tickers= tick ,period = '365d', interval = timeint)
elif timeint == '5d':
    data = yf.download(tickers= tick ,period = '365d', interval = timeint)
elif timeint == '1w':
    data = yf.download(tickers= tick ,period = '3650d', interval = timeint)
elif timeint == '1mo':
    data = yf.download(tickers= tick ,period = '3650d', interval = timeint)
elif timeint == '3mo':
    data = yf.download(tickers= tick ,period = '3650d', interval = timeint)
else:
    print('invalid time period entry.')
    exit()

#===============================================
#TICKERS:
#===============================================
#BTC = BITCOIN
#ETH = ETHER
#XLM = LUMENS
#ADA = CARDANO
#DOGE = DOGECOIN
#===============================================

#===============================================
#INTERVALS:
#===============================================
#   MINUTES:       HOURS:          MONTHS:
#   1m             1h              1mo
#   2m             DAYS:           3mo
#   5m             1d
#   15m            5d
#   30m            WEEKS:
#   90m            1wk
#===============================================

#Adding Moving average calculated field
data['MA'+str(sma)] = data['Close'].rolling(sma).mean()
data['MA'+str(lma)] = data['Close'].rolling(lma).mean()

#declare figure
fig = go.Figure()

#Candlestick
fig.add_trace(go.Candlestick(x=data.index,
                open=data['Open'],
                high=data['High'],
                low=data['Low'],
                close=data['Close'], name = 'market data'))
print(go.Candlestick(x=data.index,
                open=data['Open'],
                high=data['High'],
                low=data['Low'],
                close=data['Close'], name = 'market data'))

#Add Moving average on the graph
fig.add_trace(go.Scatter(x=data.index, y= data['MA'+str(lma)],line=dict(color='blue', width=1.5), name = str(lma)+'P-SMA'))
fig.add_trace(go.Scatter(x=data.index, y= data['MA'+str(sma)],line=dict(color='orange', width=1.5), name = str(sma)+'P-SMA'))

#Updating X axis and graph
# X-Axes
fig.update_xaxes(
    rangeslider_visible=True,
    rangeselector=dict(
        buttons=list([
            dict(count=3, label="3d", step="day", stepmode="backward"),
            dict(count=5, label="5d", step="day", stepmode="backward"),
            dict(count=7, label="WTD", step="day", stepmode="todate"),
            dict(count=30, label="MTD", step="day", stepmode="todate"),
            dict(step="all")
        ])
    )
)

#Show
#fig.show()
fig.write_html('first_figure_'+tick+'.html', auto_open=True)