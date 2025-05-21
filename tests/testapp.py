import logging

logging.basicConfig(level=logging.DEBUG)
logging.debug("Starting app.py")

import os
from flask import Flask, request, jsonify, render_template
from flask_socketio import SocketIO, emit
import sqlite3
from datetime import datetime, timedelta
import threading
import time
from candle_fetch_module import CandleFetch
from get_hi_module import run_update_trade_parameters
from api_connection import APIConnection
from ticker_monitor import TickerMonitor
from strategy_logic import StrategyLogic
from risk_management import RiskManagement
from order_execution import OrderExecution
import pytz

app = Flask(__name__)
socketio = SocketIO(app)

logging.basicConfig(level=logging.DEBUG)

# Create a custom logger
logger = logging.getLogger(__name__)

# Create handlers
c_handler = logging.StreamHandler()
c_handler.setLevel(logging.ERROR)  # Set level to ERROR to show only errors in the terminal

# Create formatters and add it to handlers
c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
c_handler.setFormatter(c_format)

# Add handlers to the logger
logger.addHandler(c_handler)

# Initialize the candle fetch module
candle_fetch = CandleFetch()

# Initialize the API connection
api_connection = APIConnection()

# Initialize the OrderExecution instance
order_execution = OrderExecution()

# Initialize the RiskManagement module
risk_management = RiskManagement(order_execution)

# Initialize the StrategyLogic module with risk management
strategy_logic = StrategyLogic(risk_management)

# Root route
@app.route('/')
def index():
    return render_template('index.html')

# Route to get tickers
@app.route('/get_tickers', methods=['GET'])
def get_tickers():
    with sqlite3.connect('tms_data.db') as conn:
        c = conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        c.execute('SELECT TICKER, DATE, TIME, RISK_1MIN, RISK_5MIN, RSI_1MIN, RSI_5MIN FROM TradeParameters WHERE DATE = ?', (today,))
        tickers = [{'ticker': row[0], 'date': row[1], 'time': row[2], 'risk_1m': row[3], 'risk_5m': row[4], 'rsi_1m': row[5], 'rsi_5m': row[6]} for row in c.fetchall()]
        # socketio.emit('update_tickers', {'tickers': tickers})
    return jsonify({'tickers': tickers})

# Route to add tickers
@app.route('/add_ticker', methods=['POST'])
def add_ticker():
    data = request.form
    tickers = data.getlist('tickers[]')
    risk_1m = data.getlist('risk_1m[]')
    risk_5m = data.getlist('risk_5m[]')
    rsi_1m = data.getlist('rsi_1m[]')
    rsi_5m = data.getlist('rsi_5m[]')

    attempts = 5
    for attempt in range(attempts):
        try:
            with sqlite3.connect('tms_data.db') as conn:
                c = conn.cursor()
                date = datetime.now().strftime('%Y-%m-%d')
                time = datetime.now().strftime('%H:%M:%S')
                for ticker, r1m, r5m, r1r, r5r in zip(tickers, risk_1m, risk_5m, rsi_1m, rsi_5m):
                    c.execute('''
                        INSERT INTO TradeParameters (TICKER, DATE, TIME, RISK_1MIN, RISK_5MIN, RSI_1MIN, RSI_5MIN)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (ticker, date, time, r1m, r5m, r1r, r5r))
                conn.commit()
                break
        except sqlite3.OperationalError as e:
            if 'database is locked' in str(e):
                time.sleep(1)
            else:
                raise
    else:
        return jsonify({'status': 'failure', 'message': 'Database is locked, could not add tickers'}), 500

    # Emit an event to update the frontend
    socketio.emit('update_tickers')

    # Start the modules after adding tickers
    start_modules()

    return jsonify({'status': 'success'})

# Route to remove a ticker
@app.route('/remove_ticker', methods=['POST'])
def remove_ticker():
    ticker = request.form.get('ticker')

    with sqlite3.connect('tms_data.db') as conn:
        c = conn.cursor()
        c.execute('DELETE FROM TradeParameters WHERE TICKER = ?', (ticker,))
        c.execute('UPDATE TradeSignal SET status = ? WHERE ticker = ?', ('Canceled', ticker))
        conn.commit()

    # Emit an event to update the frontend and notify modules
    socketio.emit('ticker_removed', {'ticker': ticker})

    return jsonify({'status': 'success'})

# Routes for other tables
@app.route('/get_trade_signals', methods=['GET'])
def get_trade_signals():
    today = datetime.now().strftime('%Y-%m-%d')
    with sqlite3.connect('tms_data.db') as conn:
        c = conn.cursor()
        c.execute('SELECT tradeID, time, strategy, ticker, price, rsi, atr_highest, hi, status FROM TradeSignal WHERE DATE(time) = ?', (today,))
        trade_signals = [{'tradeID': row[0], 'time': row[1], 'strategy': row[2], 'ticker': row[3], 'price': row[4], 'rsi': row[5], 'atr_highest': row[6], 'hi': row[7], 'status': row[8]} for row in c.fetchall()]
        socketio.emit('update_trade_signals', {'trade_signals': trade_signals})
    return jsonify({'trade_signals': trade_signals})

@app.route('/get_sell_market', methods=['GET'])
def get_sell_market():
    today = datetime.now().strftime('%Y-%m-%d')
    with sqlite3.connect('tms_data.db') as conn:
        c = conn.cursor()
        c.execute('SELECT tradeID, time, ticker, shares, price, action, status, act_status, notes FROM SellMarket WHERE date = ?', (today,))
        sell_market = [{'tradeID': row[0], 'time': row[1], 'ticker': row[2], 'shares': row[3], 'price': row[4], 'action': row[5], 'status': row[6], 'act_status': row[7], 'notes': row[8]} for row in c.fetchall()]
        socketio.emit('update_sell_market', {'sell_market': sell_market})
    return jsonify({'sell_market': sell_market})

@app.route('/get_stop_market', methods=['GET'])
def get_stop_market():
    today = datetime.now().strftime('%Y-%m-%d')
    with sqlite3.connect('tms_data.db') as conn:
        c = conn.cursor()
        c.execute('SELECT tradeID, time, ticker, shares, price, action, status, act_status, notes FROM StopMarket WHERE date = ?', (today,))
        stop_market = [{'tradeID': row[0], 'time': row[1], 'ticker': row[2], 'shares': row[3], 'price': row[4], 'action': row[5], 'status': row[6], 'act_status': row[7], 'notes': row[8]} for row in c.fetchall()]
        socketio.emit('update_stop_market', {'stop_market': stop_market})
    return jsonify({'stop_market': stop_market})

@app.route('/get_replace_stop', methods=['GET'])
def get_replace_stop():
    today = datetime.now().strftime('%Y-%m-%d')
    with sqlite3.connect('tms_data.db') as conn:
        c = conn.cursor()
        c.execute('SELECT tradeID, time, ticker, shares, price, action, status, act_status, notes FROM ReplaceStop WHERE date = ?', (today,))
        replace_stop = [{'tradeID': row[0], 'time': row[1], 'ticker': row[2], 'shares': row[3], 'price': row[4], 'action': row[5], 'status': row[6], 'act_status': row[7], 'notes': row[8]} for row in c.fetchall()]
        socketio.emit('update_replace_stop', {'replace_stop': replace_stop})
    return jsonify({'replace_stop': replace_stop})

@app.route('/get_buy_market', methods=['GET'])
def get_buy_market():
    today = datetime.now().strftime('%Y-%m-%d')
    with sqlite3.connect('tms_data.db') as conn:
        c = conn.cursor()
        c.execute('SELECT tradeID, time, ticker, shares, price, action, status, act_status, notes FROM BuyMarket WHERE date = ?', (today,))
        buy_market = [{'tradeID': row[0], 'time': row[1], 'ticker': row[2], 'shares': row[3], 'price': row[4], 'action': row[5], 'status': row[6], 'act_status': row[7], 'notes': row[8]} for row in c.fetchall()]
        socketio.emit('update_buy_market', {'buy_market': buy_market})
    return jsonify({'buy_market': buy_market})

@app.route('/get_active_trades', methods=['GET'])
def get_active_trades():
    today = datetime.now().strftime('%Y-%m-%d')
    with sqlite3.connect('tms_data.db') as conn:
        c = conn.cursor()
        c.execute('SELECT tradeID, time, strategy, ticker, shares, entry_price, stop_loss, lu_price, unrealized, date, sellOrderID, stopOrderID, last_price FROM ActiveTrades WHERE date = ?', (today,))
        active_trades = [{'tradeID': row[0], 'time': row[1], 'strategy': row[2], 'ticker': row[3], 'shares': row[4], 'entry_price': row[5], 'stop_loss': row[6], 'lu_price': row[7], 'unrealized': row[8], 'date': row[9], 'sellOrderID': row[10], 'stopOrderID': row[11], 'last_price': row[12]} for row in c.fetchall()]
        socketio.emit('update_active_trades', {'active_trades': active_trades})
    return jsonify({'active_trades': active_trades})

@app.route('/get_closed_trades', methods=['GET'])
def get_closed_trades():
    today = datetime.now().strftime('%Y-%m-%d')
    with sqlite3.connect('tms_data.db') as conn:
        c = conn.cursor()
        c.execute('''
            SELECT tradeID, strategy, ticker, shares, entry_price, entry_time, 
                   stop_loss, sl_time, exit_price, exit_time, date, reason, realized
            FROM ClosedTrades
            WHERE DATE(date) = ?
        ''', (today,))
        closed_trades = []
        for row in c.fetchall():
            if row[7] is None:  # If sl_time is None, it means the trade was closed by take profit
                reason = "Take Profit"
                exit_price = row[8]
                exit_time = row[9]
            else:  # If sl_time is not None, it means the trade was closed by stop loss
                reason = "Stop Loss"
                exit_price = row[6]
                exit_time = row[7]
            
            closed_trades.append({
                'tradeID': row[0],
                'strategy': row[1],
                'ticker': row[2],
                'shares': row[3],
                'entry_price': row[4],
                'entry_time': row[5],
                'exit_price': exit_price,
                'exit_time': exit_time,
                'reason': reason,
                'realized': row[12]
            })
    socketio.emit('update_closed_trades', {'closed_trades': closed_trades})        
    return jsonify({'closed_trades': closed_trades})

# Function to start the candle fetch process
def start_candle_fetch():
    def run_candle_fetch():
        candle_fetch.main()   

    thread = threading.Thread(target=run_candle_fetch)
    thread.start()

# Function to start the get_hi process
def start_get_hi():
    def run_get_hi():
        logging.debug("Starting get_hi process.")
        run_update_trade_parameters()
        
    logging.debug("Initializing get_hi thread.")
    thread = threading.Thread(target=run_get_hi)
    thread.start()
    logging.debug("get_hi thread started.")

# Function to start the StrategyLogic at 9:30 AM EST
def start_strategy_logic():
    def run_strategy_logic():
        eastern = pytz.timezone('US/Eastern')
        now = datetime.now(eastern)
        start_time = now.replace(hour=9, minute=30, second=0, microsecond=0)
        
        if now.time() > start_time.time():
            logging.info("Starting StrategyLogic immediately as the current time is past 9:30 AM EST.")
            strategy_logic.fetch_tickers_from_db()  # Fetch initial tickers
        else:
            wait_time = (start_time - now).total_seconds()    
            logging.info(f"Waiting {wait_time} seconds to start StrategyLogic.")
            time.sleep(wait_time)
            strategy_logic.fetch_tickers_from_db()  # Fetch initial tickers

    logging.debug("Initializing StrategyLogic thread.")
    thread = threading.Thread(target=run_strategy_logic)
    thread.start()
    logging.debug("StrategyLogic thread started.")

# Function to start the API connection
def start_api_connection():
    def run_api_connection():
        api_connection.login()
        if api_connection.connected:
            threading.Thread(target=api_connection.keep_alive, daemon=True).start()
            threading.Thread(target=api_connection.start_local_server, daemon=True).start()
            # Set the event to signal that the API connection is ready
            api_connection.connection_ready_event.set()
            start_ticker_monitor()
            # Keep the main thread running to ensure daemon threads stay alive
            while True:
                time.sleep(1)

    thread = threading.Thread(target=run_api_connection)
    thread.start()

# Start the ticker monitor process
def start_ticker_monitor():    
    def run_ticker_monitor():
        ticker_monitor = TickerMonitor(db_path='tms_data.db', api_client=api_connection)
        ticker_monitor.run()

    # Wait for the API connection to be ready before starting the ticker monitor
    api_connection.connection_ready_event.wait()
    thread = threading.Thread(target=run_ticker_monitor)
    thread.start()
    
# Function to start all modules
def start_modules():
    start_candle_fetch()
    start_get_hi()
    start_strategy_logic()
    start_api_connection()
    
def is_connected():
    try:
        # Ping Google's public DNS server
        response = os.system("ping -c 1 8.8.8.8")
        return response == 0
    except:
        return False

def restart_processes():
    start_candle_fetch()
    start_get_hi()
    start_strategy_logic()
    start_api_connection()
    logging.info("All processes restarted successfully.")

def monitor_network():
    while True:
        if is_connected():
            logging.info("Network is up. Restarting processes...")
            restart_processes()
            break
        else:
            logging.warning("Network is down. Retrying in 30 seconds...")
            time.sleep(30)

def main():
    try:
        logging.debug("Starting the application.")
        start_modules()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        logging.info("Starting network monitoring...")
        monitor_network()

if __name__ == "__main__":
    logging.debug("Starting the application.")
    port = int(os.environ.get('PORT', 5000))
    socketio.run(app, host='0.0.0.0', port=port, debug=True)
