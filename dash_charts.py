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
    df = df.copy()
    df = df.sort_values(by='timestamp')
    
    # Calculate SMAs
    df['10_sma'] = df['close'].rolling(window=10).mean()
    df['20_sma'] = df['close'].rolling(window=20).mean()
    
    # Create subplots with shared x-axis
    combined_fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                                 row_heights=[0.7, 0.3],
                                 vertical_spacing=0.05)
    
    # Add candlestick chart to the first row
    combined_fig.add_trace(go.Candlestick(
        x=df['timestamp'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='Price'
    ), row=1, col=1)

    # Calculate VWAP
    df = calculate_vwap(df)

    # Add VWAP line to the first row
    combined_fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['vwap'],
        mode='lines',
        name='VWAP',
        line=dict(color='yellow', width=2)
    ), row=1, col=1)
    
    # Add 10 SMA to the first row
    combined_fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['10_sma'],
        mode='lines',
        name='10 SMA',
        line=dict(color='red', width=1.5)
    ), row=1, col=1)

    # Add 20 SMA to the first row
    combined_fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['20_sma'],
        mode='lines',
        name='20 SMA',
        line=dict(color='blue', width=1.5)
    ), row=1, col=1)

    # Add Sell-Short, Stop Loss, and Buy Close signals to the first row
    for _, signal in sell_signals.iterrows():
        combined_fig.add_trace(go.Scatter(
            x=[signal['time']],
            y=[signal['price']],
            mode='markers+text',
            marker=dict(color='red', size=12),
            text=[signal['strategy']],
            textposition='bottom center',
            name=signal['strategy'] 
        ), row=1, col=1)
    for _, signal in stop_loss_signals.iterrows():
        combined_fig.add_trace(go.Scatter(
            x=[signal['datetime']],
            y=[signal['price']],
            mode='markers+text',
            marker=dict(color='orange', size=12),
            text=[f"SL - {signal['strategy']}"],  # Dynamically format the text
            textposition='bottom center',
            name=signal['strategy']
        ), row=1, col=1)
    for _, signal in buy_close_signals.iterrows():
        combined_fig.add_trace(go.Scatter(
            x=[signal['datetime']],
            y=[signal['price']],
            mode='markers+text',
            marker=dict(color='blue', size=12),
            text=['Buy Close'],
            textposition='bottom center'
        ), row=1, col=1)
        
    market_open = datetime.datetime.strptime(df['timestamp'].iloc[0].strftime('%Y-%m-%d') + ' 09:30:00', '%Y-%m-%d %H:%M:%S')
    market_close = datetime.datetime.strptime(df['timestamp'].iloc[0].strftime('%Y-%m-%d') + ' 16:00:00', '%Y-%m-%d %H:%M:%S')

    combined_fig.add_vrect(
        x0=market_open, x1=market_close,
        fillcolor="Orange", opacity=0.3,
        layer="below", line_width=0
    )    
        

    # Add volume bar chart to the second row
    colors = ['green' if df['close'].iloc[i] > df['open'].iloc[i] else 'red' for i in range(len(df))]
    combined_fig.add_trace(go.Bar(
        x=df['timestamp'],
        y=df['volume'],
        name='Volume',
        marker_color=colors,
        opacity=0.5
    ), row=2, col=1)

    # Update layout
    combined_fig.update_layout(
        title=title,
        xaxis_title='Time',
        yaxis_title='Price',
        xaxis2_title='Time',
        yaxis2_title='Volume',
        xaxis_rangeslider_visible=False,
        dragmode='pan',
        yaxis=dict(fixedrange=False),
        showlegend=False,
        height=900,
        autosize=True
    )
    combined_fig.update_yaxes(showgrid=False, row=1, col=1)
    combined_fig.update_yaxes(showgrid=True, row=2, col=1)

    return combined_fig


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
                                    (sell_signals['strategy'].isin(['1Min', '1Minco', '1Minde']))]
        signals_5min = sell_signals[(sell_signals['ticker'] == ticker) & 
                                    (sell_signals['strategy'].isin(['5Min', '5Minco', '5Minde', 'limit']))]
        
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
        
        # Filter sell signals for 1-minute chart (based on strategy)
        filtered_sell_signals = sell_signals[
            (sell_signals['ticker'] == ticker) & 
            (sell_signals['strategy'].isin(['1Min', '1Minco', '1Minde']))
        ]
        
        # Do not filter Stop Loss and Buy Close signals by strategy
        filtered_stop_loss_signals = stop_loss_signals[stop_loss_signals['ticker'] == ticker]
        filtered_buy_close_signals = buy_close_signals[buy_close_signals['ticker'] == ticker]
        
        fig = plot_chart(
            df, 
            filtered_sell_signals, 
            filtered_stop_loss_signals, 
            filtered_buy_close_signals, 
            f'{ticker} 1-Minute Chart'
        )
    else:
        df = ohlc_5min[ohlc_5min['ticker'] == ticker].copy()
        
        # Filter sell signals for 5-minute chart (based on strategy)
        filtered_sell_signals = sell_signals[
            (sell_signals['ticker'] == ticker) & 
            (sell_signals['strategy'].isin(['5Min', '5Minco', '5Minde', 'Limit']))
        ]
        
        # Do not filter Stop Loss and Buy Close signals by strategy
        filtered_stop_loss_signals = stop_loss_signals[stop_loss_signals['ticker'] == ticker]
        filtered_buy_close_signals = buy_close_signals[buy_close_signals['ticker'] == ticker]
        
        fig = plot_chart(
            df, 
            filtered_sell_signals, 
            filtered_stop_loss_signals, 
            filtered_buy_close_signals, 
            f'{ticker} 5-Minute Chart'
        )

    return html.Div([
        dcc.Graph(figure=fig, config={'scrollZoom': True}, style={'height': '90vh', 'width': '100%'}),
    ], style={'height': '90vh', 'display': 'flex', 'alignItems': 'center', 'justifyContent': 'center'})

if __name__ == '__main__':
    app.run_server(debug=True, port=8050)
