import logging

logging.basicConfig(level=logging.DEBUG)
logging.debug("Starting app.py")

import os
from flask import Flask, request, jsonify, render_template
from flask_socketio import SocketIO, emit
import sqlite3
from datetime import datetime, timedelta
import threading
import logging
import time
import tms_sale 
import minchart_PM
import min_chart5
import ticker_range
from get_hi_module import run_update_trade_parameters
from api_connection import APIConnection
from position_add import PositionAdd
from strategy_logic import StrategyLogic
from strategy_logic5min import StrategyLogic as strategyLogic5min
from strategy_logicco import StrategyLogic as strategyLogicoc
from strategy_logicde import StrategyLogic as strategyLogicde
from risk_managementauto import RiskManagementauto
from risk_management import RiskManagement
from order_execution import OrderExecution
from trade_monitor import TradeMonitor
from end_of_day import EndOfDay
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


# Initialize the API connection
api_connection = APIConnection()

end_of_day = EndOfDay()

# Initialize the TradeMonitor instance
logging.info("Initializing TradeMonitor in app.py")
trade_monitor = TradeMonitor()

# Initialize the OrderExecution instance with TradeMonitor
logging.info("Initializing OrderExecution with TradeMonitor in app.py")
order_execution = OrderExecution(trade_monitor)

position_add = PositionAdd(order_execution, trade_monitor) 


# Initialize the RiskManagement module
risk_management = RiskManagement(order_execution)
risk_managementauto = RiskManagementauto(order_execution)

# Initialize the StrategyLogic module with risk management
strategy_logic = StrategyLogic(position_add=position_add, risk_management=risk_management)
strategy_logic_5min = strategyLogic5min(risk_managementauto)
strategy_logic_oc = strategyLogicoc(risk_managementauto)
strategy_logic_de = strategyLogicde(risk_managementauto)








# Root route
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get_position', methods=['GET'])
def get_position():
    ticker = request.args.get('ticker')
    

    try:
        with sqlite3.connect('EOD_data.db') as conn:
            cursor = conn.cursor()
            
            # Fetch total shares from ActiveTrades
            cursor.execute("SELECT shares FROM ActiveTrades WHERE ticker = ?", (ticker,))
            shares = cursor.fetchall()
            
            
            
            

            if  shares:
                total_shares = sum(row[0] for row in shares)
                
            else: 
                #if no positions found, set total shares to 0
                total_shares = 0    
                
                
                
                

            return jsonify({
                'status': 'success',
                'total_shares': total_shares
                    
            })
           

    except Exception as e:
        logging.error(f"Error fetching ticker details for {ticker}: {e}")
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/get_ticker_details', methods=['GET'])
def get_ticker_details():
    ticker = request.args.get('ticker')
    today_date = datetime.now().strftime('%Y-%m-%d')
    today_date_ohlc = datetime.now().strftime('%Y/%m/%d')  # Custom format for ohlc_1min

    try:
        with sqlite3.connect('EOD_data.db') as conn:
            cursor = conn.cursor()
            
            # Fetch PMH and Range% from TickerRange
            cursor.execute("SELECT pmh, pml, range FROM TickerRange WHERE ticker = ? AND date = ?", (ticker, today_date))
            pmh_result = cursor.fetchone()
            
            # Fetch the open price from ohlc_1min at timestamp "yyyy/mm/dd-09:30"
            open_timestamp = f"{today_date_ohlc}-09:30"
            cursor.execute("SELECT open FROM ohlc_1min WHERE ticker = ? AND timestamp = ?", (ticker, open_timestamp))
            open_result = cursor.fetchone()
            open_price = open_result if open_result else 0  # Display 0 if open price is not available yet
            
            # Fetch Gap% from TradeParameters
            cursor.execute("SELECT GAP, LAST, PCL FROM TradeParameters WHERE ticker = ? AND date = ?", (ticker, today_date))
            gap_result = cursor.fetchone()

            if pmh_result and gap_result:
                pmh, pml, range_percent = pmh_result
                gap_percent, last_price, prclose = gap_result
                
                # Convert last_price and pml to float if they are not None
                last_price = float(last_price) if last_price is not None else None
                prclose = float(prclose) if last_price is not None else None
                pml = float(pml) if pml is not None else None

                # Calculate 10% stop
                stop_price = round(pmh * 1.1, 2) if pmh else None
                
                # Calculate range_left
                # Calculate range_left as the percentage difference between last_price and pml
                range_left = round(((last_price - prclose) / prclose) * 100, 2) if prclose and last_price else None

                

                return jsonify({
                    'status': 'success',
                    'pmh': pmh,
                    'stop': stop_price,
                    'range': f"{range_percent}",
                    'gap': f"{gap_percent}%",
                    'open': open_price,  # Include open price in response
                    'last_price': last_price,  # Added for debugging
                    'pml': pml,                # Added for debugging
                    'rangeleft': f"{range_left}%" if range_left is not None else None
                    
                })
            else:
                return jsonify({'status': 'error', 'error': 'Ticker details not found'}), 404

    except Exception as e:
        logging.error(f"Error fetching ticker details for {ticker}: {e}")
        return jsonify({'status': 'error', 'error': str(e)}), 500


@app.route('/connect', methods=['POST'])
def connect():
    try:
        # Start the API connection
        start_api_connection()
        
        # Start all modules
        start_modules()
        
        return jsonify({'status': 'success'}), 200
    except Exception as e:
        logging.error(f"Error starting API connection: {e}")
        return jsonify({'status': 'failure', 'error': str(e)}), 500
    
@app.route('/status', methods=['GET'])
def get_status():
    # Check the connection status from the APIConnection class
    is_connected = api_connection.is_connected()

    # Return the status as 'connected' or 'disconnected'
    return jsonify({'status': 'connected' if is_connected else 'disconnected'})

@app.route('/close_trade', methods=['POST'])
def close_trade():
    data = request.get_json()
    ticker = data.get('ticker')  # Retrieve the ticker from the request body
    if not ticker:
        return jsonify({'status': 'error', 'error': 'Ticker is required'}), 400

    try:
        # Call close_position for the specified ticker
        end_of_day.close_position(ticker)
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500

    


# Route to get tickers
@app.route('/get_tickers', methods=['GET'])
def get_tickers():
    with sqlite3.connect('EOD_data.db') as conn:
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
            with sqlite3.connect('EOD_data.db') as conn:
                c = conn.cursor()
                date = datetime.now().strftime('%Y-%m-%d')
                time = datetime.now().strftime('%H:%M:%S')
                for ticker, r1m, r5m, r1r, r5r in zip(tickers, risk_1m, risk_5m, rsi_1m, rsi_5m):
                    c.execute('''
                        INSERT INTO TradeParameters (TICKER, DATE, TIME, RISK_1MIN, RISK_5MIN, RSI_1MIN, RSI_5MIN)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (ticker, date, time, r1m, r5m, r1r, r5r))
                conn.commit()
                logger.info(f"Successfully inserted tickers into TradeParameters. Attempt {attempt + 1}")
                break
        except sqlite3.OperationalError as e:
            logger.error(f"Attempt {attempt + 1} - sqlite3 OperationalError: {e}")
            if 'database is locked' in str(e):
                time.sleep(1)
            else:
                
                return jsonify({'status': 'failure', 'message': str(e)}), 500
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} - Unexpected error: {e}")
            return jsonify({'status': 'failure', 'message': str(e)}), 500

    if attempt == attempts - 1 and 'database is locked' in str(e):
        logger.error(f"All attempts exhausted. Database remains locked.")
        return jsonify({'status': 'failure', 'message': 'Database is locked, could not add tickers'}), 500
    
    # Emit an event to update the frontend
    socketio.emit('update_tickers')
    logger.debug("Emitting update_tickers event to frontend.")

    

    return jsonify({'status': 'success'})

# Route to remove a ticker
@app.route('/remove_ticker', methods=['POST'])
def remove_ticker():
    ticker = request.form.get('ticker')

    with sqlite3.connect('EOD_data.db') as conn:
        c = conn.cursor()
        c.execute('DELETE FROM TradeParameters WHERE TICKER = ?', (ticker,))
        c.execute('UPDATE TradeSignal SET status = ? WHERE ticker = ?', ('Canceled', ticker))
        conn.commit()

    # Emit an event to update the frontend and notify modules
    socketio.emit('ticker_removed', {'ticker': ticker})

    return jsonify({'status': 'success'})

# Routes for other tables
# Route to update Live Trading table with HOD and Last Price for a specific ticker and date
@app.route('/fetch_ticker_data', methods=['GET'])
def fetch_ticker_data():
    ticker = request.args.get('ticker')
    date = request.args.get('date')  # Assuming date is sent in the request

    if not ticker or not date:
        return jsonify({'status': 'failure', 'error': 'Missing ticker or date'}), 400

    try:
        with sqlite3.connect('EOD_data.db') as conn:
            c = conn.cursor()
            query = '''
                SELECT HIGH, LAST 
                FROM TradeParameters 
                WHERE TICKER = ? AND DATE = ?
            '''
            c.execute(query, (ticker, date))
            result = c.fetchone()

            if result:
                hod = result[0]
                last_price = result[1]
                return jsonify({
                    'status': 'success',
                    'hod': hod,
                    'last_price': last_price
                })
            else:
                return jsonify({'status': 'failure', 'error': 'Ticker data not found'}), 404

    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {e}")
        return jsonify({'status': 'failure', 'error': str(e)}), 500
    

    
    
@app.route('/send_order', methods=['POST'])
def send_order():
    data = request.json

    ticker = data.get('ticker')
    entry_price = data.get('entry_price')
    strategy = data.get('strategy')
    time = data.get('time')  # Pass as string and convert if needed
    hod = data.get('hod')
    shares = data.get('shares')
    risk = data.get('risk')
    s_loss = data.get('s_loss')

    if not all([ticker, entry_price, strategy, time, hod, shares, risk, s_loss]):
        return jsonify({'status': 'failure', 'error': 'Missing required parameters'}), 400

    try:
        # Call the fire_signals method from the StrategyLogic module
        strategy_logic.fire_signals(
            ticker=ticker,
            entry_price=entry_price,
            hod=hod,
            shares=shares,
            strategy_type=strategy,
            risk=risk,
            s_loss=s_loss
        )
        return jsonify({'status': 'success'})
    except Exception as e:
        logger.error(f"Error firing signals for {ticker}: {e}")
        return jsonify({'status': 'failure', 'error': str(e)}), 500
    



@app.route('/get_trade_signals', methods=['GET'])
def get_trade_signals():
    today = datetime.now().strftime('%Y-%m-%d')
    with sqlite3.connect('EOD_data.db') as conn:
        c = conn.cursor()
        c.execute('SELECT tradeID, time, strategy, ticker, price, vwap, shares, hi, status FROM TradeSignal WHERE DATE(time) = ?', (today,))
        trade_signals = [{'tradeID': row[0], 'time': row[1], 'strategy': row[2], 'ticker': row[3], 'price': row[4], 'vwap': row[5], 'shares': row[6], 'hi': row[7], 'status': row[8]} for row in c.fetchall()]
        socketio.emit('update_trade_signals', {'trade_signals': trade_signals})
    return jsonify({'trade_signals': trade_signals})

@app.route('/get_sell_market', methods=['GET'])
def get_sell_market():
    today = datetime.now().strftime('%Y-%m-%d')
    with sqlite3.connect('EOD_data.db') as conn:
        c = conn.cursor()
        c.execute('SELECT tradeID, strategy, time, ticker, shares, price, action, status, act_status, notes FROM SellMarket WHERE date = ?', (today,))
        sell_market = [{'tradeID': row[0], 'strategy': row[1], 'time': row[2], 'ticker': row[3], 'shares': row[4], 'price': row[5], 'action': row[6], 'status': row[7], 'act_status': row[8], 'notes': row[9]} for row in c.fetchall()]
        socketio.emit('update_sell_market', {'sell_market': sell_market})
    return jsonify({'sell_market': sell_market})

@app.route('/get_stop_market', methods=['GET'])
def get_stop_market():
    today = datetime.now().strftime('%Y-%m-%d')
    with sqlite3.connect('EOD_data.db') as conn:
        c = conn.cursor()
        c.execute('SELECT tradeID, strategy, time, ticker, shares, price, action, status, act_status, notes FROM StopMarket WHERE date = ?', (today,))
        stop_market = [{'tradeID': row[0], 'strategy': row[1], 'time': row[2], 'ticker': row[3], 'shares': row[4], 'price': row[5], 'action': row[6], 'status': row[7], 'act_status': row[8], 'notes': row[9]} for row in c.fetchall()]
        socketio.emit('update_stop_market', {'stop_market': stop_market})
    return jsonify({'stop_market': stop_market})

@app.route('/get_replace_stop', methods=['GET'])
def get_replace_stop():
    today = datetime.now().strftime('%Y-%m-%d')
    with sqlite3.connect('EOD_data.db') as conn:
        c = conn.cursor()
        c.execute('SELECT tradeID, strategy, time, ticker, shares, price, action, status, act_status, notes FROM ReplaceStop WHERE date = ?', (today,))
        replace_stop = [{'tradeID': row[0], 'strategy': row[1], 'time': row[2], 'ticker': row[3], 'shares': row[4], 'price': row[5], 'action': row[6], 'status': row[7], 'act_status': row[8], 'notes': row[9]} for row in c.fetchall()]
        socketio.emit('update_replace_stop', {'replace_stop': replace_stop})
    return jsonify({'replace_stop': replace_stop})

@app.route('/get_buy_market', methods=['GET'])
def get_buy_market():
    today = datetime.now().strftime('%Y-%m-%d')
    with sqlite3.connect('EOD_data.db') as conn:
        c = conn.cursor()
        c.execute('SELECT tradeID, strategy, time, ticker, shares, price, action, status, act_status, notes FROM BuyMarket WHERE date = ?', (today,))
        buy_market = [{'tradeID': row[0], 'strategy': row[1], 'time': row[2], 'ticker': row[3], 'shares': row[4], 'price': row[5], 'action': row[6], 'status': row[7], 'act_status': row[8], 'notes': row[9]} for row in c.fetchall()]
        socketio.emit('update_buy_market', {'buy_market': buy_market})
    return jsonify({'buy_market': buy_market})

@app.route('/get_active_trades', methods=['GET'])
def get_active_trades():
    today = datetime.now().strftime('%Y-%m-%d')
    with sqlite3.connect('EOD_data.db') as conn:
        c = conn.cursor()
        c.execute('SELECT tradeID, time, strategy, ticker, shares, entry_price, stop_loss, lu_price, unrealized, date, sellOrderID, stopOrderID, last_price FROM ActiveTrades WHERE date = ?', (today,))
        active_trades = [{'tradeID': row[0], 'time': row[1], 'strategy': row[2], 'ticker': row[3], 'shares': row[4], 'entry_price': row[5], 'stop_loss': row[6], 'lu_price': row[7], 'unrealized': row[8], 'date': row[9], 'sellOrderID': row[10], 'stopOrderID': row[11], 'last_price': row[12]} for row in c.fetchall()]
        socketio.emit('update_active_trades', {'active_trades': active_trades})
    return jsonify({'active_trades': active_trades})


@app.route('/get_closed_trades', methods=['GET'])
def get_closed_trades():
    today = datetime.now().strftime('%Y-%m-%d')
    with sqlite3.connect('EOD_data.db') as conn:
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

@app.route('/get_trade_summary', methods=['GET'])
def get_trade_summary():
    today = datetime.now().strftime('%Y-%m-%d')  # Get today's date in 'YYYY-MM-DD' format
    
    with sqlite3.connect('EOD_data.db') as conn:
        c = conn.cursor()
        
        # Initialize summary data
        summary = {
            '1Min': {'total_trades': 0, 'gain': 0, 'loss': 0, 'total_gain_loss': 0},
            '5Min': {'total_trades': 0, 'gain': 0, 'loss': 0, 'total_gain_loss': 0},
            'GrandTotal': {'total_trades': 0, 'gain': 0, 'loss': 0, 'total_gain_loss': 0}
        }
        
        # Query for today's closed trades
        c.execute('''
            SELECT strategy, SUM(realized) AS total_realized, COUNT(tradeID) AS trade_count 
            FROM ClosedTrades 
            WHERE DATE(date) = ? 
            GROUP BY strategy
        ''', (today,))
        
        for row in c.fetchall():
            strategy = row[0]
            total_realized = row[1]
            trade_count = row[2]
            
            # Update the summary data based on the strategy
            if strategy in summary:
                summary[strategy]['total_trades'] += trade_count
                if total_realized > 0:
                    summary[strategy]['gain'] += total_realized
                else:
                    summary[strategy]['loss'] += abs(total_realized)
                summary[strategy]['total_gain_loss'] += total_realized

            # Update GrandTotal
            summary['GrandTotal']['total_trades'] += trade_count
            summary['GrandTotal']['total_gain_loss'] += total_realized
            if total_realized > 0:
                summary['GrandTotal']['gain'] += total_realized
            else:
                summary['GrandTotal']['loss'] += abs(total_realized)

    return jsonify(summary)

def start_tms_sale():
    logging.info("Starting tms_sale process")
    
    def run_tms_sale_continuously():
        while True:
            tms_sale.main()
            time.sleep(60)  # Sleep to avoid overloading the system

    # Start tms_sale in a new thread
    thread = threading.Thread(target=run_tms_sale_continuously)
    thread.daemon = True  # Ensure the thread keeps running in the background
    thread.start()
    logging.info("tms_sale thread started")
    
    



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
    
def start_strategy_logic_5min():
    def run_strategy_logic_5min():
        logging.info("Starting StrategyLogic5min immediately as app.py starts.")
        strategy_logic_5min.fetch_tickers_from_db()  # Fetch initial tickers
        
    logging.debug("Initializing StrategyLogic5min thread.")
    thread = threading.Thread(target=run_strategy_logic_5min, daemon=True)  # Set daemon to True to close thread with the app
    thread.start()
    logging.debug("StrategyLogic5min thread started.")
    
def start_strategy_logic_oc():
    def run_strategy_logic_oc():
        logging.info("Starting StrategyLogicOC immediately as app.py starts.")
        strategy_logic_oc.fetch_tickers_from_db()  # Fetch initial tickers
        
    logging.debug("Initializing StrategyLogicOC thread.")
    thread = threading.Thread(target=run_strategy_logic_oc, daemon=True)  # Set daemon to True to close thread with the app
    thread.start()
    logging.debug("StrategyLogicOC thread started.")   
    
def start_strategy_logic_de():
    def run_strategy_logic_de():
        logging.info("Starting StrategyLogicDE immediately as app.py starts.")
        strategy_logic_de.fetch_tickers_from_db()  # Fetch initial tickers
        
    logging.debug("Initializing StrategyLogicDE thread.")
    thread = threading.Thread(target=run_strategy_logic_de, daemon=True)  # Set daemon to True to close thread with the app
    thread.start()
    logging.debug("StrategyLogicDE thread started.")           


# Function to start the API connection
def start_api_connection():
    def run_api_connection():
        api_connection.login()
        if api_connection.connected:
            threading.Thread(target=api_connection.keep_alive, daemon=True).start()
            threading.Thread(target=api_connection.start_local_server, daemon=True).start()
            # Set the event to signal that the API connection is ready
            api_connection.connection_ready_event.set()
            
            # Initialize the EndOfDay module and start its tasks
            
            end_of_day.connect_to_server()
            threading.Thread(target=end_of_day.listen_to_server, daemon=True).start()
            threading.Thread(target=end_of_day.end_of_day_tasks, daemon=True).start()
            
            

    thread = threading.Thread(target=run_api_connection)
    thread.start()


    
# Function to start TradeMonitor monitoring
def start_trade_monitor():
    logging.info("Starting TradeMonitor monitoring")
    
    # Start listening to the server
    threading.Thread(target=trade_monitor.listen_to_server, daemon=True).start()
    logging.info("TradeMonitor threads started")
    

# Function to start all modules
def start_modules():
   
    start_get_hi()
    start_strategy_logic()
    start_strategy_logic_5min()
    start_strategy_logic_oc()
    start_strategy_logic_de()
    start_tms_sale()
    start_trade_monitor()
    
    # Start the new modules
    logging.info("Starting 1min_chart10s and min_chart5 modules.")
    threading.Thread(target=minchart_PM.main, daemon=True).start()
    threading.Thread(target=min_chart5.main, daemon=True).start()
    threading.Thread(target=ticker_range.main, daemon=True).start()
    logging.info("Started all modules")
    
def is_connected():
    try:
        # Ping Google's public DNS server
        response = os.system("ping -c 1 8.8.8.8")
        return response == 0
    except:
        return False

def restart_processes():
    start_tms_sale()
    start_get_hi()
    start_strategy_logic()
    start_api_connection()
    start_trade_monitor()
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
    


