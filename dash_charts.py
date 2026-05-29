import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from sqlalchemy import create_engine
import datetime
from datetime import timedelta
import config  # Import config.py for DB_CONFIGfrom datetime import timedelta


# Connect to PostgreSQL database
engine = create_engine(
    f"postgresql+psycopg2://{config.DB_CONFIG['user']}:{config.DB_CONFIG['password']}@{config.DB_CONFIG['host']}:{config.DB_CONFIG['port']}/{config.DB_CONFIG['dbname']}"
)


# Fetch data from the database
def fetch_data(engine, table_name):
    today = datetime.datetime.now().strftime('%Y/%m/%d')
    query = f"SELECT * FROM {table_name.lower()} WHERE substring(timestamp, 1, 10) = %s"
    return pd.read_sql(query, engine, params=(today,), parse_dates=['timestamp'])

def fetch_trade_signals(engine):
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    query = "SELECT * FROM tradesignal WHERE substring(time, 1, 10) = %s"
    return pd.read_sql(query, engine, params=(today,), parse_dates=['time'])

def fetch_stop_buy_signals(engine, ticker):
    today = datetime.date.today().strftime('%Y-%m-%d')
    ticker = str(ticker)

    # 1. Buy-close (unchanged)
    buy_close = pd.read_sql(
        "SELECT * FROM buymarket WHERE date = %s AND ticker = %s",
        engine, params=(today, ticker)
    )

    # 2. ALL possible stop tables
    stop_tables = ['stopmarket', 'executedstop', 'canceledstop']
    stops = []

    for table in stop_tables:
        df = pd.read_sql(
            f"SELECT * FROM {table} WHERE date = %s AND ticker = %s",
            engine, params=(today, ticker)
        )
        if len(df) > 0:
            df['source_table'] = table
            stops.append(df)

    stop_loss_signals = pd.concat(stops, ignore_index=True) if stops else pd.DataFrame()

    # 3. Add proper datetime to BOTH frames (exactly like your working code)
    for df in [stop_loss_signals, buy_close]:
        if not df.empty and 'date' in df.columns and 'time' in df.columns:
            df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'], errors='coerce')
            df.dropna(subset=['datetime'], inplace=True)

    return stop_loss_signals, buy_close




# Calculate VWAP
def calculate_vwap(df):
    q = df['volume']
    p = df[['open', 'high', 'low', 'close']].mean(axis=1)
    df.loc[:, 'vwap'] = (p * q).cumsum() / q.cumsum()  # Use .loc to avoid SettingWithCopyWarning
    return df


def plot_chart(df, sell_signals, stop_loss_signals, buy_close_signals, title):
    df = df.copy()
    df = df.sort_values(by='timestamp')
    
    # Calculate SMAs
    df['10_sma'] = df['close'].rolling(window=10).mean()
    df['20_sma'] = df['close'].rolling(window=20).mean()
    df['50_sma'] = df['close'].rolling(window=50).mean()
    
    # Calculate VWAP
    df = calculate_vwap(df)

    # Create subplots
    combined_fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, 
        row_heights=[0.7, 0.3],
        vertical_spacing=0.05
    )
    
    # Add candlestick
    combined_fig.add_trace(go.Candlestick(
        x=df['timestamp'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='Price'
    ), row=1, col=1)

    # Add VWAP and SMAs
    combined_fig.add_trace(go.Scatter(x=df['timestamp'], y=df['vwap'], mode='lines', name='VWAP', line=dict(color='yellow', width=2)), row=1, col=1)
    combined_fig.add_trace(go.Scatter(x=df['timestamp'], y=df['10_sma'], mode='lines', name='10 SMA', line=dict(color='red', width=1.5)), row=1, col=1)
    combined_fig.add_trace(go.Scatter(x=df['timestamp'], y=df['20_sma'], mode='lines', name='20 SMA', line=dict(color='blue', width=1.5)), row=1, col=1)
    combined_fig.add_trace(go.Scatter(x=df['timestamp'], y=df['50_sma'], mode='lines', name='50 SMA', line=dict(color='purple', width=1.5)), row=1, col=1)
    
    
    if not df.empty:
        hod_price = df['high'].max()
        hod_row = df[df['high'] == hod_price].iloc[0]  # first occurrence
        hod_time = hod_row['timestamp']
        
        # Format price to 2 decimals
        price_label = f"{hod_price:.2f}"
   
        # Marker + "HOD" label directly above the candle
        combined_fig.add_trace(go.Scatter(
            x=[hod_time],
            y=[hod_price],
            mode='markers+text',
            marker=dict(
                color='green',
                size=10,
                symbol='circle',
                line=dict(width=1, color='white')
            ),
            text=[f"HOD ({price_label})"],  # ← HOD + price in label
            textposition='top center',
            textfont=dict(color='green', size=12, family="Arial Black"),
            name='HOD',
            hovertemplate=f"<b>HOD</b><br><b>Price: {hod_price:.2f}</b><extra></extra>",
            showlegend=False
        ), row=1, col=1)
 
    # === SELL SIGNALS ===
    for _, sig in sell_signals.iterrows():
        strat = sig['strategy']
        price = sig['price']
        combined_fig.add_trace(go.Scatter(
            x=[sig['time']],
            y=[sig['price']],
            mode='markers+text',
            marker=dict(color='red', size=12, symbol='triangle-down'),
            text=[strat],
            textposition='bottom center',
            name=strat,
            hovertemplate=(
                f"<b>{strat}</b><br>"
                f"<b>Sell Short</b><br>"
                f"<b>Price: {price:.2f}</b>"
                "<extra></extra>"
            ),
            showlegend=False
        ), row=1, col=1)


    # === STOP-LOSS SIGNALS (TIME-ALIGNED) ===
    interval = 1 if '1min' in title.lower() else 5

    for _, signal in stop_loss_signals.iterrows():
        stop_time = signal['datetime']
        stop_price = signal['price']
        strat = signal['strategy']  # ← Define strat here
        
        # Floor to candle start
        if interval == 1:
            candle_time = stop_time.floor('T')  # e.g., 10:16:23 → 10:16:00
        else:
            minutes = stop_time.minute
            floored_minute = (minutes // interval) * interval
            candle_time = stop_time.replace(minute=floored_minute, second=0, microsecond=0)

        # Use candle time if it exists
        plot_x = candle_time if candle_time in df['timestamp'].values else stop_time

        combined_fig.add_trace(go.Scatter(
            x=[plot_x],
            y=[stop_price],
            mode='markers+text',
            marker=dict(color='orange', size=12, symbol='triangle-down'),
            text=[f"SL {strat}"],
            textposition='bottom center',
            name=f"SL-{strat}",
            hovertemplate=(
                "<b>SL</b><br>"
                f"<b>{strat}</b><br>"
                f"<b>Price: {stop_price:.2f}</b>"
                "<extra></extra>"
            ),
            showlegend=False
        ), row=1, col=1)

    # === BUY CLOSE SIGNALS ===
    for _, sig in buy_close_signals.iterrows():
        buy_time = sig['datetime']
        strat = sig['strategy']
        price = sig['price']
        
        # Floor to candle start (same as stop-loss)
        if interval == 1:
            candle_time = buy_time.floor('T')
        else:
            mins = buy_time.minute
            floored = (mins // interval) * interval
            candle_time = buy_time.replace(minute=floored, second=0, microsecond=0)
            
        # Use candle time if it exists in chart data
        plot_x = candle_time if candle_time in df['timestamp'].values else buy_time    
        
        combined_fig.add_trace(go.Scatter(
            x=[plot_x],
            y=[price],
            mode='markers+text',
            marker=dict(color='blue', size=12, symbol='triangle-up'),
            text=[strat],
            textposition='top center',
            name=strat,
            hovertemplate=(
                f"<b>{strat}</b><br>"
                f"<b>Buy Close</b><br>"
                f"<b>Price: {price:.2f}</b>"
                "<extra></extra>"
            ),
            showlegend=False
        ), row=1, col=1)

    # === VOLUME BAR ===
    colors = ['green' if c > o else 'red' for c, o in zip(df['close'], df['open'])]
    combined_fig.add_trace(go.Bar(
        x=df['timestamp'], y=df['volume'],
        name='Volume', marker_color=colors, opacity=0.5
    ), row=2, col=1)

    # === MARKET HOURS SHADING ===
    today = df['timestamp'].iloc[0].date()
    market_open = pd.Timestamp(today) + pd.Timedelta('9:30:00')
    market_close = pd.Timestamp(today) + pd.Timedelta('16:00:00')
    combined_fig.add_vrect(
        x0=market_open, x1=market_close,
        fillcolor="Orange", opacity=0.3,
        layer="below", line_width=0
    )

    # === LAYOUT ===
    combined_fig.update_layout(
        title=title,
        xaxis_title='Time', yaxis_title='Price',
        xaxis2_title='Time', yaxis2_title='Volume',
        xaxis_rangeslider_visible=False,
        dragmode='pan',
        yaxis=dict(fixedrange=False),
        showlegend=False,
        height=900,
        autosize=True,
        
        # ADD CROSSHAIR HERE
        hovermode='x unified',           # Shows one tooltip for all traces
        spikedistance=1000             # How far the spike extends
    )
    combined_fig.update_xaxes(
        showspikes=True,
        spikemode='across',
        spikesnap='cursor',
        spikecolor='gray',
        spikethickness=1,
        showgrid=False, row=1, col=1
    )

    combined_fig.update_yaxes(
        showspikes=True,
        spikemode='across',
        spikesnap='cursor',
        spikecolor='gray',
        spikethickness=1,
        showgrid=False, row=1, col=1
    )

    return combined_fig

# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div([
    dcc.Store(id='selected-chart', data={'ticker': None, 'interval': '1min'}),
    
     # Top: Chart area
    html.Div(id='chart-viewer', style={'height': '80vh', 'width': '100%'}),

    # Bottom: Always-visible ticker buttons
    html.Div(id='chart-links', style={
        'position': 'fixed',
        'bottom': '0',
        'left': '0',
        'width': '100%',
        'backgroundColor': 'white',
        'padding': '10px',
        'borderTop': '2px solid #ccc',
        'overflowX': 'auto',
        'zIndex': 1000,
        'display': 'flex',
        'flexWrap': 'wrap',
        'gap': '10px',
        'justifyContent': 'center'
    }),
    dcc.Interval(
        id='interval-component',
        interval=60*1000,  # in milliseconds (update every 1 minute)
        n_intervals=0
    ),
    
], style={'height': '100vh', 'paddingBottom': '100px'})  # Make room for fixed bar

@app.callback(
    Output('chart-links', 'children'),
    Input('interval-component', 'n_intervals')
)
def generate_chart_links(n):
    ohlc_1min = fetch_data(engine, 'ohlc_1min')
    ohlc_5min = fetch_data(engine, 'ohlc_5min')
    
    # Unpack the signals tuple returned by fetch_signals
    sell_signals = fetch_trade_signals(engine)
    
    
    tickers = ohlc_1min['ticker'].unique()
    links = []
    for ticker in tickers:
        # Filter signals for this ticker
        signals_1min = sell_signals[(sell_signals['ticker'] == ticker) & 
                                    (sell_signals['strategy'].isin(['1Min', '1Minco', '1Minde', 'limit', '1Min-2g2r', '1Min-belowsma', 'stop']))]
        signals_5min = sell_signals[(sell_signals['ticker'] == ticker) & 
                                    (sell_signals['strategy'].isin(['5Min', '5Minco', '5Minde', 'limit', '5Min-2g2r', '5Min-belowsma', 'market', 'stop']))]
        
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

    ticker = selected_chart.get('ticker')
    interval = selected_chart.get('interval', '1min')

    # Default ticker if none selected
    if not ticker or ticker not in ohlc_1min['ticker'].unique():
        if ohlc_1min.empty:
            return html.Div("No data available", style={'textAlign': 'center', 'marginTop': '100px'})
        ticker = ohlc_1min['ticker'].iloc[0]

    stop_loss_signals, buy_close_signals = fetch_stop_buy_signals(engine, ticker)

    
    

    # Build chart
    if interval == '1min':
        df = ohlc_1min[ohlc_1min['ticker'] == ticker].copy()
        filtered_sell_signals = sell_signals[
            (sell_signals['ticker'] == ticker) & 
            (sell_signals['strategy'].isin(['1Min', '1Minco', '1Minde', 'limit', '1Min-2g2r', '1Min-belowsma', 'market', 'stop']))
        ]
    else:
        df = ohlc_5min[ohlc_5min['ticker'] == ticker].copy()
        filtered_sell_signals = sell_signals[
            (sell_signals['ticker'] == ticker) & 
            (sell_signals['strategy'].isin(['5Min', '5Minco', '5Minde', 'limit', '5Min-2g2r', '5Min-belowsma', 'market', 'stop']))
        ]

    # No need to re-filter stop/buy — already filtered by ticker in fetch
    fig = plot_chart(
        df, 
        filtered_sell_signals, 
        stop_loss_signals, 
        buy_close_signals, 
        f'{ticker} {interval.capitalize()} Chart'
    )

    return dcc.Graph(
        figure=fig,
        config={'scrollZoom': True, 'displayModeBar': True},
        style={'height': '100%', 'width': '100%'}
    )

if __name__ == '__main__':
    app.run_server(debug=True, port=8050)
