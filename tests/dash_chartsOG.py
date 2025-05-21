import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from sqlalchemy import create_engine
import datetime

# Connect to your SQLite database
engine = create_engine('sqlite:///EOD_data.db')

# Fetch data from the database
def fetch_data(engine, table_name):
    today = datetime.datetime.now().strftime('%Y/%m/%d')
    query = f"SELECT * FROM {table_name} WHERE substr(timestamp, 1, 10) = '{today}'"
    return pd.read_sql(query, engine, parse_dates=['timestamp'])

# Fetch signals from the database, including stop loss and buy close
def fetch_trade_signals(engine):
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    
    # Fetch Sell-Short signals
    sell_query = f"SELECT * FROM TradeSignal WHERE substr(time, 1, 10) = '{today}'"
    sell_signals = pd.read_sql(sell_query, engine, parse_dates=['time'])
    
    return sell_signals
    
# Fetch Stop Loss and Buy Close signals from StopMarket and BuyMarket
def fetch_stop_buy_signals(engine):
    today = datetime.datetime.now().strftime('%Y-%m-%d')

    # Fetch Stop Loss signals based on date and time columns
    stop_loss_query = f"SELECT * FROM StopMarket WHERE date = '{today}'"
    stop_loss_signals = pd.read_sql(stop_loss_query, engine)

    # Combine 'date' and 'time' columns into a full datetime
    stop_loss_signals['datetime'] = pd.to_datetime(stop_loss_signals['date'] + ' ' + stop_loss_signals['time'])
    
    

    # Fetch Buy Close signals based on date and time columns
    buy_close_query = f"SELECT * FROM BuyMarket WHERE date = '{today}'"
    buy_close_signals = pd.read_sql(buy_close_query, engine)

    # Combine 'date' and 'time' columns into a full datetime
    buy_close_signals['datetime'] = pd.to_datetime(buy_close_signals['date'] + ' ' + buy_close_signals['time'])
    
    

    return stop_loss_signals, buy_close_signals




# Calculate VWAP
def calculate_vwap(df):
    q = df['volume']
    p = df[['open', 'high', 'low', 'close']].mean(axis=1)
    df.loc[:, 'vwap'] = (p * q).cumsum() / q.cumsum()  # Use .loc to avoid SettingWithCopyWarning
    return df

# Plot chart
def plot_chart(df, sell_signals, stop_loss_signals, buy_close_signals, title):
    df = df.copy()  # Add this to avoid SettingWithCopyWarning
    
    df = df.sort_values(by='timestamp')

    
    fig = make_subplots(
        rows=2, 
        cols=1, 
        shared_xaxes=True, 
        vertical_spacing=0.1, 
        row_heights=[0.75, 0.25],  # Adjust the heights: 75% for candlestick, 25% for volume
        subplot_titles=('Price', 'Volume')
    )

    # Add candlestick chart to subplot 1
    fig.add_trace(go.Candlestick(
        x=df['timestamp'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='Price'
    ), row=1, col=1)

    # Calculate VWAP
    df = calculate_vwap(df)

    # Add VWAP line to subplot 1
    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['vwap'],
        mode='lines',
        name='VWAP',
        line=dict(color='yellow', width=2)
    ), row=1, col=1)

    # Add volume bars to subplot 2
    colors = ['green' if df['close'].iloc[i] > df['open'].iloc[i] else 'red' for i in range(len(df))]
    fig.add_trace(go.Bar(
        x=df['timestamp'],
        y=df['volume'],
        name='Volume',
        marker_color=colors,
        opacity=0.5
    ), row=2, col=1)

    # Add Sell-Short signals
    for _, signal in sell_signals.iterrows():
        fig.add_trace(go.Scatter(
            x=[signal['time']],
            y=[signal['price']],
            mode='markers+text',
            marker=dict(color='red', size=12),
            text=['Sell-Short'],
            textposition='bottom center'
        ), row=1, col=1)
    
    # Add Stop Loss signals
    for _, signal in stop_loss_signals.iterrows():
        fig.add_trace(go.Scatter(
            x=[signal['datetime']],
            y=[signal['price']],
            mode='markers+text',
            marker=dict(color='orange', size=12),
            text=['Stop Loss'],
            textposition='bottom center'
        ), row=1, col=1)
    
    # Add Buy Close signals (only when trade is closed)
    for _, signal in buy_close_signals.iterrows():
        fig.add_trace(go.Scatter(
            x=[signal['datetime']],
            y=[signal['price']],
            mode='markers+text',
            marker=dict(color='blue', size=12),
            text=['Buy Close'],
            textposition='bottom center'
        ), row=1, col=1)


    market_open = datetime.datetime.strptime(df['timestamp'].iloc[0].strftime('%Y-%m-%d') + ' 09:30:00', '%Y-%m-%d %H:%M:%S')
    market_close = datetime.datetime.strptime(df['timestamp'].iloc[0].strftime('%Y-%m-%d') + ' 16:00:00', '%Y-%m-%d %H:%M:%S')

    fig.add_vrect(
        x0=market_open, x1=market_close,
        fillcolor="LightGray", opacity=0.3,
        layer="below", line_width=0
    )

    fig.update_layout(
        title=title,
        yaxis_title='Price',
        xaxis_title='Time',
        xaxis_rangeslider_visible=True,  # Enable range slider
        showlegend=False,
        yaxis=dict(side="right"),
        yaxis2=dict(title='Volume', side='left'),
        autosize=True
    )

    fig.update_yaxes(showgrid=False, row=1, col=1)
    fig.update_yaxes(range=[0, df['volume'].max() * 1.1], row=2, col=1)

    return fig

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div([
    dcc.Store(id='selected-chart', data={'ticker': None, 'interval': '1min'}),
    dcc.Interval(
        id='interval-component',
        interval=60*1000,  # in milliseconds (update every 1 minute)
        n_intervals=0
    ),
    html.Div(id='chart-viewer', style={'height': '90vh'}),
    html.Div(id='chart-links', style={'display': 'flex', 'flexWrap': 'wrap', 'justifyContent': 'center', 'gap': '10px'})
], style={'height': '100vh'})

@app.callback(
    Output('chart-links', 'children'),
    Input('interval-component', 'n_intervals')
)
def generate_chart_links(n):
    ohlc_1min = fetch_data(engine, 'ohlc_1min')
    ohlc_5min = fetch_data(engine, 'ohlc_5min')
    
    # Unpack the signals tuple returned by fetch_signals
    sell_signals = fetch_trade_signals(engine)
    stop_loss_signals, buy_close_signals = fetch_stop_buy_signals(engine)
    
    tickers = ohlc_1min['ticker'].unique()
    links = []
    for ticker in tickers:
        # Filter signals for this ticker
        signals_1min = sell_signals[(sell_signals['ticker'] == ticker) & 
                                    (sell_signals['strategy'].isin(['1Minco', '1Minde']))]
        signals_5min = sell_signals[(sell_signals['ticker'] == ticker) & 
                                    (sell_signals['strategy'].isin(['5Minco', '5Minde']))]
        
        link_style_1min = {'color': 'red'} if not signals_1min.empty else {}
        link_style_5min = {'color': 'red'} if not signals_5min.empty else {}
        
        links.append(html.Div([
            html.Button(f'View {ticker} 1-Minute', id={'type': 'view-btn', 'index': f'{ticker}_1min'}, n_clicks=0, style=link_style_1min),
            html.Button(f'View {ticker} 5-Minute', id={'type': 'view-btn', 'index': f'{ticker}_5min'}, n_clicks=0, style=link_style_5min),
        ], style={'display': 'flex', 'gap': '10px'}))
    return links

@app.callback(
    Output('selected-chart', 'data'),
    [Input({'type': 'view-btn', 'index': dash.dependencies.ALL}, 'n_clicks')],
    [State('selected-chart', 'data')]
)
def update_selected_chart(n_clicks, selected_chart):
    ctx = dash.callback_context

    if not ctx.triggered:
        return dash.no_update

    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if 'back-btn' in button_id:
        return {'ticker': None, 'interval': '1min'}

    button_id = eval(button_id)['index']
    ticker, interval = button_id.split('_')
    return {'ticker': ticker, 'interval': interval}

@app.callback(
    Output('chart-viewer', 'children'),
    [Input('interval-component', 'n_intervals'),
     Input('selected-chart', 'data')]
)
def update_charts(n, selected_chart):
    ohlc_1min = fetch_data(engine, 'ohlc_1min')
    ohlc_5min = fetch_data(engine, 'ohlc_5min')
    sell_signals = fetch_trade_signals(engine)
    stop_loss_signals, buy_close_signals = fetch_stop_buy_signals(engine)
    
    tickers = ohlc_1min['ticker'].unique()

    if not selected_chart['ticker']:
        selected_chart = {'ticker': tickers[0], 'interval': '1min'}

    ticker = selected_chart['ticker']
    interval = selected_chart['interval']

    if interval == '1min':
        df = ohlc_1min[ohlc_1min['ticker'] == ticker].copy()
        fig = plot_chart(df, sell_signals[sell_signals['ticker'] == ticker], 
                         stop_loss_signals[stop_loss_signals['ticker'] == ticker], 
                         buy_close_signals[buy_close_signals['ticker'] == ticker], 
                         f'{ticker} 1-Minute Chart')
    else:
        df = ohlc_5min[ohlc_5min['ticker'] == ticker].copy()
        fig = plot_chart(df, sell_signals[sell_signals['ticker'] == ticker], 
                         stop_loss_signals[stop_loss_signals['ticker'] == ticker], 
                         buy_close_signals[buy_close_signals['ticker'] == ticker], 
                         f'{ticker} 5-Minute Chart')

    return html.Div([
        dcc.Graph(figure=fig, config={'scrollZoom': True}, style={'height': '90vh', 'width': '100%'}),
    ], style={'height': '90vh', 'display': 'flex', 'alignItems': 'center', 'justifyContent': 'center'})

if __name__ == '__main__':
    app.run_server(debug=True, port=8050)
