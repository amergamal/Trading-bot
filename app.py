import logging
import sqlite3
import os
from flask import Flask, request, jsonify, render_template
from flask_socketio import SocketIO, emit
import sqlite3
from datetime import datetime, timedelta
import threading
import time
import pytz


#import min_chart5
from api_connection import APIConnection

from sl_monitor import SLMonitor

from order_execution import OrderExecution
from trade_monitor import TradeMonitor
from end_of_day import EndOfDay

# Shared database lock (compatible with EndOfDay, TradeMonitor, SLMonitor, APIConnection)
db_lock = threading.Lock()

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
c_handler = logging.StreamHandler()
c_handler.setLevel(logging.ERROR)
c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
c_handler.setFormatter(c_format)
logger.addHandler(c_handler)

app = Flask(__name__)
socketio = SocketIO(app)
# Simple counter for trade_id (in-memory, reset on restart)
trade_id_counter = 0
# Initialize database connection
db_conn = sqlite3.connect('EOD_data.db', timeout=10)
db_conn.row_factory = sqlite3.Row

# Initialize modules
api_connection = APIConnection()
end_of_day = EndOfDay(socketio=socketio)
trade_monitor = TradeMonitor(socketio=socketio)

sl_monitor = SLMonitor(socketio=socketio)
order_execution = OrderExecution(trade_monitor=trade_monitor, sl_monitor=sl_monitor, end_of_day=end_of_day, socketio=socketio)




# Synchronization event for server readiness
server_ready_event = threading.Event()

# Flask Routes
@app.route('/')
def index():
    return render_template('index.html')

def get_next_trade_id():
    global trade_id_counter
    trade_id_counter += 1
    return trade_id_counter

@app.route('/get_order_data', methods=['GET'])
def get_order_data():
    ticker = request.args.get('ticker')
    today_date = datetime.now().strftime('%Y-%m-%d')
    try:
        with db_lock:
            cursor = db_conn.cursor()
            cursor.execute("SELECT HIGH, LAST FROM TradeParameters WHERE ticker = ? AND date = ?", (ticker, today_date))
            result = cursor.fetchone()
            if not result:
                return jsonify({'status': 'error', 'error': 'Ticker data not found'}), 404
            hod, last_price = result
            stop_loss = float(last_price) * 1.2 if last_price else None  # 20% above last_price
            return jsonify({
                'status': 'success',
                'hod': float(hod) if hod else None,
                'last_price': float(last_price) if last_price else None,
                'stop_loss': round(stop_loss, 2) if stop_loss else None
            })
    except Exception as e:
        logger.error(f"Error fetching order data for {ticker}: {e}")
        return jsonify({'status': 'error', 'error': str(e)}), 500



@app.route('/connect', methods=['POST'])
def connect():
    try:
        if not server_ready_event.is_set():
            logger.info("Server not ready, starting API connection")
            start_api_connection()
            logger.info("Waiting for server to start (timeout=30s)")
            server_ready_event.wait(timeout=30)
            if server_ready_event.is_set():
                logger.info("Server started successfully")
                return jsonify({'status': 'success'}), 200
            else:
                logger.error("Server failed to start within timeout")
                return jsonify({'status': 'failure', 'error': 'Server failed to start'}), 500
        else:
            logger.info("Server already running")
            return jsonify({'status': 'success'}), 200
    except Exception as e:
        logger.error(f"Error starting API connection: {e}")
        return jsonify({'status': 'failure', 'error': str(e)}), 500

@app.route('/status', methods=['GET'])
def get_status():
    is_connected = api_connection.is_connected()
    return jsonify({'status': 'connected' if is_connected else 'disconnected'})

@app.route('/close_trade', methods=['POST'])
def close_trade():
    data = request.get_json()
    ticker = data.get('ticker')
    trade_id = data.get('tradeID')
    if not ticker or not trade_id:
        return jsonify({'status': 'error', 'error': 'Ticker is required'}), 400
    try:
        end_of_day.close_position(ticker, trade_id)
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500

@app.route('/get_tickers', methods=['GET'])
def get_tickers():
    with db_lock:
        cursor = db_conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute('SELECT TICKER, DATE, TIME, RISK_1MIN, RISK_5MIN, RSI_1MIN, RSI_5MIN FROM TradeParameters WHERE DATE = ?', (today,))
        tickers = [{'ticker': row[0], 'date': row[1], 'time': row[2], 'risk_1m': row[3], 'risk_5m': row[4], 'rsi_1m': row[5], 'rsi_5m': row[6]} for row in cursor.fetchall()]
        
    return jsonify({'tickers': tickers})

@app.route('/add_ticker', methods=['POST'])
def add_ticker():
    data = request.form
    tickers = data.getlist('tickers[]')
    risk_1m = data.getlist('risk_1m[]')
    risk_5m = data.getlist('risk_5m[]')
    rsi_1m = data.getlist('rsi_1m[]')
    rsi_5m = data.getlist('rsi_5m[]')
    
    try:
        with db_lock:
            cursor = db_conn.cursor()
            date = datetime.now().strftime('%Y-%m-%d')
            time = datetime.now().strftime('%H:%M:%S')
            # Batch insert all tickers in a single transaction
            for ticker, r1m, r5m, r1r, r5r in zip(tickers, risk_1m, risk_5m, rsi_1m, rsi_5m):
                cursor.execute('''
                    INSERT INTO TradeParameters (TICKER, DATE, TIME, RISK_1MIN, RISK_5MIN, RSI_1MIN, RSI_5MIN)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (ticker, date, time, r1m, r5m, r1r, r5r))
            db_conn.commit()
            logger.info(f"Successfully inserted {len(tickers)} tickers into TradeParameters")
    except sqlite3.OperationalError as e:
        logger.error(f"Database error: {e}")
        return jsonify({'status': 'failure', 'message': str(e)}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({'status': 'failure', 'message': str(e)}), 500
    
    # Fetch updated tickers and emit
    with db_lock:
        cursor = db_conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute('SELECT TICKER, DATE, TIME, RISK_1MIN, RISK_5MIN, RSI_1MIN, RSI_5MIN FROM TradeParameters WHERE DATE = ?', (today,))
        tickers = [{'ticker': row[0], 'date': row[1], 'time': row[2], 'risk_1m': row[3], 'risk_5m': row[4], 'rsi_1m': row[5], 'rsi_5m': row[6]} for row in cursor.fetchall()]
    socketio.emit('update_tickers', {'tickers': tickers})
    logger.debug("Emitted update_tickers event to frontend")
    return jsonify({'status': 'success'})

@app.route('/remove_ticker', methods=['POST'])
def remove_ticker():
    ticker = request.form.get('ticker')
    with db_lock:
        cursor = db_conn.cursor()
        cursor.execute('DELETE FROM TradeParameters WHERE TICKER = ?', (ticker,))
        cursor.execute('UPDATE TradeSignal SET status = ? WHERE ticker = ?', ('Canceled', ticker))
        db_conn.commit()
        # Fetch updated tickers and emit
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute('SELECT TICKER, DATE, TIME, RISK_1MIN, RISK_5MIN, RSI_1MIN, RSI_5MIN FROM TradeParameters WHERE DATE = ?', (today,))
        tickers = [{'ticker': row[0], 'date': row[1], 'time': row[2], 'risk_1m': row[3], 'risk_5m': row[4], 'rsi_1m': row[5], 'rsi_5m': row[6]} for row in cursor.fetchall()]
    socketio.emit('update_tickers', {'tickers': tickers})
    return jsonify({'status': 'success'})



@app.route('/send_order', methods=['POST'])
def send_order():
    try:
        data = request.json
        ticker = data.get('ticker')
        entry_price = data.get('entry_price')
        strategy = data.get('strategy')
        time = data.get('time')
        hod = data.get('hod')
        shares = data.get('shares')
        risk = data.get('risk')
        s_loss = data.get('s_loss')

        # Validate all required fields
        if not all([ticker, entry_price, strategy, time, hod, shares, risk, s_loss]):
            return jsonify({'status': 'failure', 'error': 'Missing required parameters'}), 400

        # Convert numeric fields and validate
        try:
            entry_price = float(entry_price)
            hod = float(hod)
            shares = int(shares)
            risk = float(risk)
            s_loss = float(s_loss)
        except (ValueError, TypeError):
            return jsonify({'status': 'failure', 'error': 'Invalid numeric parameters'}), 400

        if entry_price <= 0 or hod <= 0 or shares <= 0 or risk <= 0 or s_loss <= 0:
            return jsonify({'status': 'failure', 'error': 'Numeric parameters must be positive'}), 400
        if s_loss <= entry_price:
            return jsonify({'status': 'failure', 'error': 'Stop loss must be above entry price for short selling'}), 400

        # 1. Generate trade_id
        trade_id = get_next_trade_id()
        
        

        # 2. Insert into TradeSignal table
        signal = {
            'trade_id': trade_id,
            'strategy': strategy,
            'ticker': ticker,
            'entry_price': round(entry_price, 2),
            's_loss': s_loss,  # Not inserted, used in command_sell
            'time': time,
            'hod': round(hod, 2),
            'shares': shares,
            'status': 'Fired',
        }

        conn = sqlite3.connect('EOD_data.db')
        try:
            c = conn.cursor()
            query_insert = """
                INSERT INTO TradeSignal (tradeID, time, strategy, ticker, price, shares, hi, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            c.execute(query_insert, (
                signal['trade_id'],
                signal['time'],
                signal['strategy'],
                signal['ticker'],
                signal['entry_price'],
                signal['shares'],
                signal['hod'],
                signal['status']
            ))
            conn.commit()
            logger.info(f"Trade signal inserted successfully: {signal}")
        except Exception as e:
            conn.close()
            logger.error(f"Error inserting trade signal: {e}")
            return jsonify({'status': 'failure', 'error': f"Failed to insert trade signal: {str(e)}"}), 500
        finally:
            conn.close()
        # 4. Set order_type and strategy for order execution
        order_type = 'target' if strategy.lower() == 'target' else strategy
        command_strategy = 'market' if strategy.lower() == 'target' else strategy    

        # 5. Call order_execution.execute_command
        command_sell = {
            'trade_id': trade_id,
            'ticker': ticker,
            'shares': shares,
            'order_type': order_type,  # Use strategy as order_type
            'stop_price': s_loss,    # Use s_loss as stop_price
            'price': entry_price,
            'strategy': command_strategy
        }

        try:
            order_execution.execute_command(command_sell)
            logger.info(f"Order executed successfully for trade_id {trade_id}: {command_sell}")
            return jsonify({'status': 'success'})
        except Exception as e:
            logger.error(f"Error executing order for {ticker}: {e}")
            return jsonify({'status': 'failure', 'error': str(e)}), 500
    except Exception as e:
        logger.error(f"Unexpected error in send_order: {e}")
        return jsonify({'status': 'failure', 'error': str(e)}), 500
    
@app.route('/get_trade_signals', methods=['GET'])
def get_trade_signals():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        cursor = db_conn.cursor()
        cursor.execute('SELECT tradeID, time, strategy, ticker, price, shares, hi, status FROM TradeSignal WHERE DATE(time) = ?', (today,))
        trade_signals = [{'tradeID': row[0], 'time': row[1], 'strategy': row[2], 'ticker': row[3], 'price': row[4], 'shares': row[5], 'hi': row[6], 'status': row[7]} for row in cursor.fetchall()]
        socketio.emit('update_trade_signals', {'trade_signals': trade_signals})
    return jsonify({'trade_signals': trade_signals})

@app.route('/get_sell_market', methods=['GET'])
def get_sell_market():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        cursor = db_conn.cursor()
        cursor.execute('SELECT tradeID, strategy, time, ticker, shares, price, action, status, act_status, notes FROM SellMarket WHERE date = ?', (today,))
        sell_market = [{'tradeID': row[0], 'strategy': row[1], 'time': row[2], 'ticker': row[3], 'shares': row[4], 'price': row[5], 'action': row[6], 'status': row[7], 'act_status': row[8], 'notes': row[9]} for row in cursor.fetchall()]
        socketio.emit('update_sell_market', {'sell_market': sell_market})
    return jsonify({'sell_market': sell_market})

@app.route('/get_stop_market', methods=['GET'])
def get_stop_market():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        cursor = db_conn.cursor()
        cursor.execute('SELECT tradeID, strategy, time, ticker, shares, price, action, status, act_status, notes FROM StopMarket WHERE date = ?', (today,))
        stop_market = [{'tradeID': row[0], 'strategy': row[1], 'time': row[2], 'ticker': row[3], 'shares': row[4], 'price': row[5], 'action': row[6], 'status': row[7], 'act_status': row[8], 'notes': row[9]} for row in cursor.fetchall()]
        socketio.emit('update_stop_market', {'stop_market': stop_market})
    return jsonify({'stop_market': stop_market})

@app.route('/get_executed_stop', methods=['GET'])
def get_executed_stop():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        cursor = db_conn.cursor()
        cursor.execute('SELECT tradeID, strategy, executed_time, ticker, shares, stop_loss FROM ExecutedStop WHERE date = ?', (today,))
        executed_stop = [{'tradeID': row[0], 'strategy': row[1], 'time': row[2], 'ticker': row[3], 'shares': row[4], 'price': row[5]} for row in cursor.fetchall()]
        socketio.emit('update_executed_stop', {'executed_stop': executed_stop})
    return jsonify({'executed_stop': executed_stop})

@app.route('/get_replace_stop', methods=['GET'])
def get_replace_stop():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        cursor = db_conn.cursor()
        cursor.execute('SELECT tradeID, strategy, time, ticker, shares, price, action, status, act_status, notes FROM ReplaceStop WHERE date = ?', (today,))
        replace_stop = [{'tradeID': row[0], 'strategy': row[1], 'time': row[2], 'ticker': row[3], 'shares': row[4], 'price': row[5], 'action': row[6], 'status': row[7], 'act_status': row[8], 'notes': row[9]} for row in cursor.fetchall()]
        socketio.emit('update_replace_stop', {'replace_stop': replace_stop})
    return jsonify({'replace_stop': replace_stop})

@app.route('/get_buy_market', methods=['GET'])
def get_buy_market():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        cursor = db_conn.cursor()
        cursor.execute('SELECT tradeID, strategy, time, ticker, shares, price, action, status, act_status, notes FROM BuyMarket WHERE date = ?', (today,))
        buy_market = [{'tradeID': row[0], 'strategy': row[1], 'time': row[2], 'ticker': row[3], 'shares': row[4], 'price': row[5], 'action': row[6], 'status': row[7], 'act_status': row[8], 'notes': row[9]} for row in cursor.fetchall()]
        socketio.emit('update_buy_market', {'buy_market': buy_market})
    return jsonify({'buy_market': buy_market})

@app.route('/get_active_trades', methods=['GET'])
def get_active_trades():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        cursor = db_conn.cursor()
        cursor.execute('SELECT tradeID, time, strategy, ticker, shares, entry_price, stop_loss, lu_price, unrealized, date, sellOrderID, stopOrderID, last_price FROM ActiveTrades WHERE date = ?', (today,))
        active_trades = [{'tradeID': row[0], 'time': row[1], 'strategy': row[2], 'ticker': row[3], 'shares': row[4], 'entry_price': row[5], 'stop_loss': row[6], 'lu_price': row[7], 'unrealized': row[8], 'date': row[9], 'sellOrderID': row[10], 'stopOrderID': row[11], 'last_price': row[12]} for row in cursor.fetchall()]
        socketio.emit('update_active_trades', {'active_trades': active_trades})
    return jsonify({'active_trades': active_trades})

@app.route('/get_closed_trades', methods=['GET'])
def get_closed_trades():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        cursor = db_conn.cursor()
        cursor.execute('''
            SELECT tradeID, strategy, ticker, shares, entry_price, entry_time, 
                   original_stop_loss, stop_loss, sl_time, exit_price, exit_time, 
                   date, reason, realized, r_gain_loss
            FROM ClosedTrades
            WHERE DATE(date) = ?
        ''', (today,))
        closed_trades = []
        for row in cursor.fetchall():
            closed_trade = {
                'tradeID': row[0],
                'strategy': row[1],
                'ticker': row[2],
                'shares': row[3],
                'entry_price': row[4],
                'entry_time': row[5],
                'SLoss': row[6],  # Alias original_stop_loss
                'stop_loss': row[7],
                'sl_time': row[8],
                'exit_price': row[9],
                'exit_time': row[10],
                'date': row[11],
                'reason': row[12],
                'realized': row[13],
                'RR': row[14]  # Alias r_gain_loss
            }
            closed_trades.append(closed_trade)
        socketio.emit('update_closed_trades', {'closed_trades': closed_trades})
    return jsonify({'closed_trades': closed_trades})

@app.route('/get_trade_summary', methods=['GET'])
def get_trade_summary():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        cursor = db_conn.cursor()
        summary = {
            'market': {'total_trades': 0, 'gain': 0, 'loss': 0, 'total_gain_loss': 0},
            'limit': {'total_trades': 0, 'gain': 0, 'loss': 0, 'total_gain_loss': 0},
            'GrandTotal': {'total_trades': 0, 'gain': 0, 'loss': 0, 'total_gain_loss': 0}
        }
        cursor.execute('''
            SELECT strategy, SUM(realized) AS total_realized, COUNT(tradeID) AS trade_count 
            FROM ClosedTrades 
            WHERE DATE(date) = ? 
            GROUP BY strategy
        ''', (today,))
        for row in cursor.fetchall():
            strategy = row[0]
            total_realized = row[1]
            trade_count = row[2]
            if strategy in summary:
                summary[strategy]['total_trades'] += trade_count
                if total_realized > 0:
                    summary[strategy]['gain'] += total_realized
                else:
                    summary[strategy]['loss'] += abs(total_realized)
                summary[strategy]['total_gain_loss'] += total_realized
            summary['GrandTotal']['total_trades'] += trade_count
            summary['GrandTotal']['total_gain_loss'] += total_realized
            if total_realized > 0:
                summary['GrandTotal']['gain'] += total_realized
            else:
                summary['GrandTotal']['loss'] += abs(total_realized)
    return jsonify(summary)

@app.route('/get_public_url', methods=['GET'])
def get_public_url():
    public_url = app.config.get('PUBLIC_URL', 'Not available')
    return jsonify({"public_url": public_url})





def start_api_connection():
    def run_api_connection():
        try:
            logger.info("Attempting API login")
            api_connection.login()
            logger.info(f"API login completed, connected: {api_connection.connected}")
            if api_connection.connected:
                logger.info("Starting local server in a separate thread")
                server_thread = threading.Thread(target=api_connection.start_local_server, daemon=True)
                server_thread.start()
                api_connection.connection_ready_event.wait(timeout=30)
                if api_connection.connection_ready_event.is_set():
                    logger.info("Local server started and connection_ready_event set")
                    server_ready_event.set()
                    logger.info("server_ready_event set")
                    end_of_day.connect_to_server()
                    # Start a single thread for both EndOfDay tasks
                    threading.Thread(target=end_of_day.run_combined_tasks, daemon=True).start()
                    sl_monitor._listen_to_server()  # Start SLMonitor only
                else:
                    logger.error("Local server failed to start within 30 seconds")
            else:
                logger.error("API login failed, cannot start server")
        except Exception as e:
            logger.error(f"Error in start_api_connection: {e}")
        finally:
            if api_connection.is_connected():
                server_ready_event.set()
                logger.info("server_ready_event set in finally block")
    logger.info("Starting API connection thread")
    threading.Thread(target=run_api_connection, daemon=True).start()
    
# Function to start TradeMonitor monitoring
def start_trade_monitor():
    logger.info("Waiting for server readiness before starting TradeMonitor")
    server_ready_event.wait(timeout=30)  # Wait up to 30 seconds for server readiness
    if server_ready_event.is_set():
        logger.info("Starting TradeMonitor monitoring")
        # Start listening to the server
        threading.Thread(target=trade_monitor.listen_to_server, daemon=True).start()
        logger.info("TradeMonitor threads started")
    else:
        logger.error("Server not ready after timeout, skipping TradeMonitor start")    

def start_modules():
    logger.info("Starting all modules")
    start_trade_monitor()
    #start_strategy_logic()
    #threading.Thread(target=min_chart5.main, args=(strategy_logic,), daemon=True).start()
    logger.info("Started all modules")

def is_connected():
    try:
        response = os.system("ping -c 1 8.8.8.8")
        return response == 0
    except:
        return False

def restart_processes():
    #start_strategy_logic()
    logger.info("All processes restarted successfully.")

def monitor_network():
    while True:
        if is_connected():
            logger.info("Network is up. Restarting processes...")
            restart_processes()
            break
        else:
            logger.warning("Network is down. Retrying in 30 seconds...")
            time.sleep(30)

def main():
    try:
        logger.info("Starting the application")
        public_url = os.environ.get('PUBLIC_URL', 'Not set')
        if public_url == 'Not set':
            logger.warning("PUBLIC_URL environment variable not set. Run Pinggy to get a public URL.")
        else:
            app.config['PUBLIC_URL'] = public_url
            logger.info(f"Public URL: {public_url}")
        start_api_connection()  # Start API connection first
        start_modules()
    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    logger.info("Starting the application")
    main()
    port = int(os.environ.get('PORT', 5000))
    socketio.run(app, host='0.0.0.0', port=port, debug=False)