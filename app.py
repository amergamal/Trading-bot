
import logging
from logging.handlers import RotatingFileHandler
import psycopg2
from psycopg2 import pool
from psycopg2.extras import Json
import os
from flask import Flask, request, jsonify, render_template
from flask_socketio import SocketIO, emit
import config
from datetime import datetime, timedelta, time as dt_time
import threading
from decimal import Decimal  # For handling Decimal to float conversion
import time
import pytz
from threading import Thread
#from PM_volume import PMVolume, start_pm_volume  # Add PMVolume import
from alert_manager import AlertManager  # NEW
from quote_update import VwapFetch
from api_connection import APIConnection
from strategy_logic import StrategyLogic
from sl_monitor import SLMonitor
from stopsell_monitor import SSMonitor
from risk_management import RiskManagement
from order_execution import OrderExecution
from trade_monitor import TradeMonitor
from end_of_day import EndOfDay
from minichart import Minichart  # Import Minichart
import traceback   
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature
import os
from dotenv import load_dotenv

load_dotenv()  # Loads your .env file

# Create serializer with your secret key
serializer = URLSafeTimedSerializer(os.getenv('SECRET_KEY'))


# Shared database lock (compatible with EndOfDay, TradeMonitor, SLMonitor, APIConnection)
db_lock = threading.Lock()

# Synchronization event for server readiness
server_ready_event = threading.Event()

# Clear root logger handlers
logging.getLogger('').handlers.clear()
logging.getLogger('werkzeug').disabled = True

_LOG_FMT = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def _setup_logger(name, log_file=None, console_level=logging.WARNING):
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)
    log.propagate = False
    if not log.handlers:
        if log_file:
            fh = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=2, encoding='utf-8')
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(_LOG_FMT)
            log.addHandler(fh)
        ch = logging.StreamHandler()
        ch.setLevel(console_level)
        ch.setFormatter(_LOG_FMT)
        log.addHandler(ch)
    return log

logger    = _setup_logger(__name__,        console_level=logging.INFO)
sl_logger = _setup_logger('StrategyLogic', 'strategy_logic.log')
_setup_logger('AlertManager',  'alertmanager.log')
_setup_logger('OrderExecution','orderexecution.log')
_setup_logger('TradeMonitor',  'trademonitor.log')
_setup_logger('EndOfDay',      'End_of_day.log')
_setup_logger('VwapFetch',     'quoteupdate.log')
_setup_logger('Server',        console_level=logging.WARNING)
_setup_logger('Minichart',     'minichart.log')
_setup_logger('SLMonitor',     'slmonitor.log')
_setup_logger('SSMonitor',     'ssmonitor.log')
_setup_logger('MultiStrategy', 'multi_strategy.log')     
    
  

app = Flask(__name__)
socketio = SocketIO(
    app,
    
    ping_timeout=60,                # client waits 60 s for ping
    ping_interval=25,               # server sends ping every 25 s
    #logger=True,                    # optional – shows engine.io logs
    #engineio_logger=True,
    cors_allowed_origins="*",
    async_mode='threading',
    path='/socket.io' 
)
#print("\n" + "="*60)
#print("SOCKETIO INSTANCE CREATED IN app.py")
#print(f"socketio object: {socketio}")
#print(f"socketio id: {id(socketio)}")
#print(f"socketio.server: {socketio.server}")
#print(f"socketio.server id: {id(socketio.server) if socketio.server else None}")
#print("="*60 + "\n")
# Simple counter for trade_id (in-memory, reset on restart)
trade_id_counter = 0

# Initialize PostgreSQL connection pool
try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=20,
        **config.DB_CONFIG
    )
    logger.info("PostgreSQL connection pool initialized")
except psycopg2.OperationalError as e:
    logger.error(f"Failed to initialize PostgreSQL connection pool: {e}")
    raise

# Module instances (will be initialized later)
api_connection = None
end_of_day = None
trade_monitor = None
vwap_fetch = None
sl_monitor = None
stopsell_monitor = None
order_execution = None
risk_management = None
strategy_logic = None
minichart = None  # Add Minichart instance
alert_manager = None  # NEW
#pm_volume = None  # Add PMVolume instance


def initialize_api_connection():
    """Initialize server connection."""
    global api_connection 
    logger.info("Server initialized")
    api_connection = APIConnection()

def initialize_modules():
    """Initialize all modules after server is ready."""
    global alert_manager, api_connection, end_of_day, trade_monitor, vwap_fetch, sl_monitor, stopsell_monitor, order_execution, risk_management, strategy_logic, minichart
    logger.info("Initializing modules")
    
    end_of_day = EndOfDay(socketio=socketio)
    trade_monitor = TradeMonitor(db_pool=db_pool, socketio=socketio, order_execution = order_execution)
    vwap_fetch = VwapFetch(db_pool=db_pool, trade_monitor=trade_monitor, socketio=socketio)
    sl_monitor = SLMonitor(socketio=socketio)
    # Create order_execution FIRST without stopsell_monitor (pass None or omit if default=None)
    order_execution = OrderExecution(trade_monitor=trade_monitor, sl_monitor=sl_monitor, stopsell_monitor=None, end_of_day=end_of_day, socketio=socketio)
    
    # Now create stopsell_monitor with the existing order_execution
    stopsell_monitor = SSMonitor(order_execution=order_execution, socketio=socketio)
    
    # Wire back-references now that both modules exist
    order_execution.stopsell_monitor = stopsell_monitor
    trade_monitor.order_execution = order_execution
    sl_monitor.trade_monitor = trade_monitor
    risk_management = RiskManagement(order_execution)
    strategy_logic = StrategyLogic(risk_management=risk_management, socketio=socketio)
    minichart = Minichart(db_pool=db_pool)  # Initialize Minichart with db_pool
    #pm_volume = PMVolume(db_pool=db_pool)  # Initialize PMVolume
    alert_manager = AlertManager(db_pool=db_pool, socketio=socketio)
    logger.info("AlertManager initialized")
    # Link AlertManager to VwapFetch for real-time prices
    vwap_fetch.alert_manager = alert_manager
    logger.info("AlertManager linked to VwapFetch")
    vwap_fetch.stopsell_monitor = stopsell_monitor
    logger.info("SSMonitor linked to VwapFetch for missed-entry detection")
    alert_manager.start_alert_loop()
    logger.info("5-minute alert loop started")
    
    logger.info("All modules initialized")

def initialize_trade_id_counter():
    """Initialize tradeidcounter with the highest tradeid."""
    try:
        with db_lock:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tradeidcounter (
                    id INTEGER PRIMARY KEY,
                    last_trade_id INTEGER NOT NULL
                )
            """)
            # Filter for numeric tradeid to avoid errors with non-numeric values (e.g., 'SL_1')
            cursor.execute("""
                SELECT MAX(CAST(tradeid AS INTEGER)) FROM (
                    SELECT tradeid FROM tradesignal WHERE tradeid ~ '^[0-9]+$'
                    UNION
                    SELECT tradeid FROM tradedetails WHERE tradeid ~ '^[0-9]+$'
                    UNION
                    SELECT tradeid FROM activetrades WHERE tradeid ~ '^[0-9]+$'
                    UNION
                    SELECT tradeid FROM closedtrades WHERE tradeid ~ '^[0-9]+$'
                    UNION
                    SELECT tradeid FROM buymarket WHERE tradeid ~ '^[0-9]+$'
                )
            """)
            max_trade_id = cursor.fetchone()[0]
            max_trade_id = int(max_trade_id) if max_trade_id else 0
            cursor.execute("SELECT last_trade_id FROM tradeidcounter WHERE id = 1")
            if cursor.fetchone():
                cursor.execute("UPDATE tradeidcounter SET last_trade_id = %s WHERE id = 1", (max_trade_id,))
            else:
                cursor.execute("INSERT INTO tradeidcounter (id, last_trade_id) VALUES (1, %s)", (max_trade_id,))
            conn.commit()
            logger.info(f"Initialized tradeID counter to {max_trade_id}")
            db_pool.putconn(conn)
    except psycopg2.Error as e:
        logger.error(f"Error initializing tradeID counter: {e}")
        if cursor:
                cursor.close()
        if conn:
            db_pool.putconn(conn)
            

@socketio.on('toggle_bot')
def handle_toggle_bot():
    strategy_logic.bot_enabled = not strategy_logic.bot_enabled
    emit('bot_state_update', {'enabled': strategy_logic.bot_enabled})
    
# Server
@socketio.on('request_bot_state')
def handle_request_bot_state():
    emit('bot_state_update', {'enabled': strategy_logic.bot_enabled})    

def update_hod_periodically():
    """Run every ~64 seconds: update PMH (once available) and HOD (continuously after open)"""
    while True:
        now = datetime.now()
        today_str = now.strftime('%Y-%m-%d')
        today_timestamp_prefix = today_str.replace('-', '/') + '%'
        market_open_str = f"{today_str} 09:30:00"
        is_pre_market = now.time() < dt_time(9, 30)

        # Sleep logic: wait until next cycle (~4 seconds past the minute)
        if is_pre_market:
            # Before 9:30: wake up ~every minute to check for incoming pre-market data
            seconds_to_wait = 64 - (now.second + now.microsecond / 1e6)
        else:
            # After 9:30: normal cycle
            seconds_to_wait = 64 - (now.second + now.microsecond / 1e6)

        if seconds_to_wait < 0:
            seconds_to_wait += 60
        time.sleep(seconds_to_wait)

        try:
            with app.app_context():
                conn = db_pool.getconn()
                cursor = conn.cursor()

                cursor.execute("SELECT ticker FROM tradeparameters WHERE date = %s", (today_str,))
                tickers = [row[0].upper() for row in cursor.fetchall()]
                if not tickers:
                    db_pool.putconn(conn)
                    continue

                updates_made = False

                for ticker in tickers:
                    updated = False

                    # Before 9:30: always update PMH to capture new higher highs
                    if is_pre_market:
                        cursor.execute("""
                            SELECT MAX(high)
                            FROM ohlc_1min
                            WHERE ticker = %s
                              AND "timestamp" LIKE %s
                              AND TO_TIMESTAMP(timestamp, 'YYYY/MM/DD-HH24:MI') < %s
                        """, (ticker, today_timestamp_prefix, market_open_str))
                        pmh_row = cursor.fetchone()
                        new_pmh = float(pmh_row[0]) if pmh_row and pmh_row[0] is not None else None
                        if new_pmh is not None:
                            cursor.execute("""
                                UPDATE tradeparameters
                                SET pmh = %s
                                WHERE ticker = %s AND date = %s
                            """, (new_pmh, ticker, today_str))
                            updated = True
                            updates_made = True

                    # After 9:30: only set PMH if still NULL (final check)
                    else:
                        cursor.execute("""
                            SELECT pmh FROM tradeparameters
                            WHERE ticker = %s AND date = %s
                        """, (ticker, today_str))
                        current_pmh = cursor.fetchone()[0]

                        if current_pmh is None:
                            cursor.execute("""
                                SELECT MAX(high)
                                FROM ohlc_1min
                                WHERE ticker = %s
                                  AND "timestamp" LIKE %s
                                  AND TO_TIMESTAMP(timestamp, 'YYYY/MM/DD-HH24:MI') < %s
                            """, (ticker, today_timestamp_prefix, market_open_str))
                            pmh_row = cursor.fetchone()
                            new_pmh = float(pmh_row[0]) if pmh_row and pmh_row[0] is not None else None
                            if new_pmh is not None:
                                cursor.execute("""
                                    UPDATE tradeparameters
                                    SET pmh = %s
                                    WHERE ticker = %s AND date = %s
                                """, (new_pmh, ticker, today_str))
                                updated = True
                                updates_made = True

                    # After market open: update HOD continuously
                    if not is_pre_market:
                        cursor.execute("""
                            SELECT MAX(high)
                            FROM ohlc_1min
                            WHERE ticker = %s
                              AND "timestamp" LIKE %s
                              AND TO_TIMESTAMP(timestamp, 'YYYY/MM/DD-HH24:MI') >= %s
                        """, (ticker, today_timestamp_prefix, market_open_str))
                        hod_row = cursor.fetchone()
                        new_hod = float(hod_row[0]) if hod_row and hod_row[0] is not None else None

                        if new_hod is not None:
                            cursor.execute("""
                                UPDATE tradeparameters
                                SET high = %s
                                WHERE ticker = %s AND date = %s
                            """, (new_hod, ticker, today_str))
                            updated = True
                            updates_made = True

                if updates_made:
                    conn.commit()
                    logger.debug(f"Updated PMH/HOD for one or more tickers")

                db_pool.putconn(conn)

        except Exception as e:
            logger.error(f"Error in periodic PMH/HOD update: {e}")
            if conn:
                db_pool.putconn(conn)


# Add this route
@app.route('/multi_monitor')
def multi_monitor_page():
    return render_template('multi_monitor.html')

@socketio.on('request_account_mode')
def handle_request_account_mode():
    emit('account_mode_update', {
        'mode': config.get_account_mode(),
        'account': config.get_active_account()
    })

@socketio.on('set_account_mode')
def handle_set_account_mode(data):
    mode = data.get('mode', 'live')
    config.set_account_mode(mode)
    account = config.get_active_account()
    logger.info(f"Account mode switched to: {mode} ({account}) — reconnecting DAS")
    socketio.emit('account_mode_update', {'mode': mode, 'account': account})

    # Cycle both DAS connections so they re-login with the new account.
    # Done in a thread so the SocketIO handler returns immediately.
    def _reconnect_das():
        if api_connection and api_connection.connected:
            logger.info(f"Cycling api_connection for account switch → {account}")
            api_connection.handle_disconnection()
        if vwap_fetch and vwap_fetch.connected:
            logger.info(f"Cycling vwap_fetch for account switch → {account}")
            vwap_fetch.reconnect()

    threading.Thread(target=_reconnect_das, daemon=True).start()

@socketio.on('request_multi_status')
def send_multi_status():
    if hasattr(trade_monitor, 'multi_pivots'):
        import pytz
        now_est = datetime.now(pytz.timezone('US/Eastern'))
        cutoff_time = getattr(trade_monitor, 'multi_addon_cutoff', dt_time(10, 30))
        past_cutoff = now_est.time() > cutoff_time
        cutoff_str = cutoff_time.strftime('%I:%M %p')
        status = {}
        for ticker, data in trade_monitor.multi_pivots.items():
            status[ticker] = {
                'add_count': data.get('add_count', 0),
                'pivot_high': float(data.get('pivot_high', 0)),
                'broken': data.get('broken', False),
                'current_stop': float(trade_monitor.multi_current_stop_level.get(ticker, 0)),
                'original_risk': float(trade_monitor.multi_original_risk.get(ticker, 0)),
                'last_trigger': str(data.get('last_trigger_ts')) if data.get('last_trigger_ts') else '—',
                'cutoff': past_cutoff or (ticker in getattr(trade_monitor, '_multi_cutoff_logged', set())),
                'cutoff_time': cutoff_str
            }
        emit('multi_status_update', status)
        
  # === MULTI PYRAMID CONTROL ===
@app.route('/cancel_multi_pyramid', methods=['POST'])
def cancel_multi_pyramid():
    data = request.get_json()
    ticker = data.get('ticker')
    if not ticker:
        return jsonify({'status': 'error', 'message': 'No ticker provided'})

    if hasattr(trade_monitor, 'cancel_multi_pyramid'):
        success = trade_monitor.cancel_multi_pyramid(ticker)
        return jsonify({'status': 'success' if success else 'error'})
    else:
        return jsonify({'status': 'error', 'message': 'TradeMonitor not ready'})
    
    
    
    
@socketio.on('cancel_multi_pyramid')
def handle_cancel_multi(data):
    ticker = data.get('ticker')
    if ticker and hasattr(trade_monitor, 'cancel_multi_pyramid'):
        trade_monitor.cancel_multi_pyramid(ticker)          
            
@socketio.on('request_enabled_strategies')
def send_enabled_strategies():
    enabled = list(strategy_logic.enabled_strategies) if strategy_logic else []
    emit('enabled_strategies_update', {'enabled_strategies': enabled})            
            
# ------------------------------------------------------------------
# NEW: Candle Conditions page and real-time updates
# ------------------------------------------------------------------

@app.route('/candle_conditions')
def candle_conditions_page():
    """Render the new Candle Conditions page (opens in new tab)"""
    return render_template('candle_conditions.html')

# Replace the entire @socketio.on('request_candle_conditions') function with this

@socketio.on('request_candle_conditions')
def send_candle_conditions():
    conn = None
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()

        # 1. Get tickers + params
        cursor.execute("""
            SELECT DISTINCT ON (ticker) ticker, open, pmh, vwap, last
            FROM tradeparameters
            WHERE date::date = CURRENT_DATE
            ORDER BY ticker, date DESC
        """)
        param_rows = cursor.fetchall()
        tickers = [row[0].upper() for row in param_rows]

        if not tickers:
            emit('candle_conditions_update', {'data': {}, 'ticker_strategies': {}})
            return

        param_map = {row[0].upper(): {
            'open': float(row[1]) if row[1] else None,
            'pmh': float(row[2]) if row[2] else None,
            'vwap': float(row[3]) if row[3] else None,
            'last_price': float(row[4]) if len(row) > 4 and row[4] is not None else None  # ← NEW: last price
        } for row in param_rows}

        # 2. Fetch per-ticker selected strategies
        cursor.execute("""
            SELECT ticker, strategy_risks
            FROM tradeparameters
            WHERE date::date = CURRENT_DATE
        """)
        ticker_strategies = {}
        for row in cursor.fetchall():
            ticker = row[0].upper()
            risks = row[1] or {}
            ticker_strategies[ticker] = list(risks.keys())

        # 3. Compute HOD from ohlc_1min (session high since 9:30) with time (latest timestamp for max high)
        cursor.execute("""
            SELECT ticker, hod, hod_time
            FROM (
                SELECT ticker, high AS hod, timestamp AS hod_time,
                       ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY high DESC, timestamp DESC) AS rn
                FROM ohlc_1min
                WHERE ticker = ANY(%s)
                  AND TO_TIMESTAMP(timestamp, 'YYYY/MM/DD-HH24:MI')::date = CURRENT_DATE
                  AND TO_TIMESTAMP(timestamp, 'YYYY/MM/DD-HH24:MI')::time >= '09:30'::time
            ) sub
            WHERE rn = 1
        """, (tickers,))
        hod_map = {row[0].upper(): {'hod': float(row[1]) if row[1] is not None else None, 
                                    'hod_time': row[2]} for row in cursor.fetchall()}

        # 4. Compute true Pre-Market High (before 9:30)
        cursor.execute("""
            SELECT ticker, MAX(high) AS pmh
            FROM ohlc_1min
            WHERE ticker = ANY(%s)
              AND TO_TIMESTAMP(timestamp, 'YYYY/MM/DD-HH24:MI')::date = CURRENT_DATE
              AND TO_TIMESTAMP(timestamp, 'YYYY/MM/DD-HH24:MI')::time < '09:30'::time
            GROUP BY ticker
        """, (tickers,))
        pmh_map = {row[0].upper(): float(row[1]) if row[1] is not None else None for row in cursor.fetchall()}

        # 5. Build data — query correct table per strategy + fired signal info
        data = {}
        def to_float(val):
            return float(val) if val is not None else None

        for ticker in tickers:
            selected = ticker_strategies.get(ticker, [])
            if not selected:
                continue

            param = param_map.get(ticker, {})
            data[ticker] = {}

            for strategy in selected:
                # Choose correct table and state column
                table = 'candleconditions_5min' if strategy.startswith('5Min') else 'candleconditions_1min'
                state_col_map = {
                    'below_pmh': 'state_below_pmh',
                    'vwap_crossover': 'state_vwap_crossover',
                    'below_sma': 'state_sma',
                    '2g2r': 'state_2g2r',
                    'vwap_dev': 'state_vwap_dev' # add more if needed
                }
                suffix = strategy.split('-')[-1] if '-' in strategy else ''
                state_col = state_col_map.get(suffix)

                # Get latest candle
                cursor.execute(f"""
                    SELECT timestamp, open, high, close, vwap, sma_10
                    FROM {table}
                    WHERE ticker = %s
                      AND timestamp::date = CURRENT_DATE
                    ORDER BY timestamp DESC
                    LIMIT 1
                """, (ticker,))
                row = cursor.fetchone()

                if row:
                    ts_str = row[0].strftime('%H:%M') if row[0] else 'N/A'
                    open_val = to_float(row[1])
                    high_val = to_float(row[2])
                    close_val = to_float(row[3])
                    vwap_val = to_float(row[4])
                    sma10_val = to_float(row[5])
                else:
                    ts_str = 'N/A'
                    open_val = high_val = close_val = vwap_val = sma10_val = None
                
                hod_info = hod_map.get(ticker, {'hod': None, 'hod_time': None})
                hod_val = hod_info['hod']
                hod_time_raw = hod_info.get('hod_time')
                hod_time_str = 'N/A'
                if hod_time_raw and isinstance(hod_time_raw, str) and len(hod_time_raw) >= 16:
                    hod_time_str = hod_time_raw[-5:]                 # last 5 chars = 'HH:MM'    

                common = {
                    'timestamp': ts_str,
                    'open': round(open_val, 2) if open_val is not None else (round(to_float(param.get('open')), 2) if param.get('open') else 'N/A'),
                    'pmh': round(pmh_map.get(ticker), 2) if pmh_map.get(ticker) is not None else 'N/A',
                    'hod': round(hod_val, 2) if hod_val is not None else 'N/A',
                    'hod_time': hod_time_str,
                    'vwap': round(vwap_val, 2) if vwap_val is not None else (round(to_float(param.get('vwap')), 2) if param.get('vwap') else 'N/A'),
                    'sma10': round(sma10_val, 2) if sma10_val is not None else 'N/A',
                    'vwap_dev_pct': 'N/A',  # placeholder
                    'sma_dev_pct': 'N/A'  # NEW: placeholder for SMA deviation
                }
                
                # Calculate VWAP deviation % using live last price
                last_price = param.get('last_price')
                vwap_final = vwap_val or param.get('vwap')
                if last_price is not None and vwap_final is not None and vwap_final != 0:
                    dev_pct = ((last_price - vwap_final) / vwap_final) * 100
                    common['vwap_dev_pct'] = round(dev_pct, 2)
                else:
                    common['vwap_dev_pct'] = 'N/A'
                    
                # NEW: Calculate SMA deviation % using live last price
                sma_final = sma10_val
                if last_price is not None and sma_final is not None and sma_final != 0:
                    sma_dev_pct = ((last_price - sma_final) / sma_final) * 100
                    common['sma_dev_pct'] = round(sma_dev_pct, 2)
                else:
                    common['sma_dev_pct'] = 'N/A'    

                # Get current state (latest candle)
                state_value = 'N/A'
                if row and state_col:
                    cursor.execute(f"""
                        SELECT {state_col}
                        FROM {table}
                        WHERE ticker = %s
                          AND timestamp::date = CURRENT_DATE
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """, (ticker,))
                    state_row = cursor.fetchone()
                    state_value = state_row[0] or 'N/A' if state_row else 'N/A'

                # Compute Fired Signal: (count) first_time
                fired_signal = '-'
                signal_price = '-'
                if state_col:
                    cursor.execute(f"""
                        SELECT COUNT(*), MIN(timestamp)
                        FROM {table}
                        WHERE ticker = %s
                          AND timestamp::date = CURRENT_DATE
                          AND {state_col} = 'SIGNAL_READY'
                    """, (ticker,))
                    sig_row = cursor.fetchone()
                    count = sig_row[0] if sig_row else 0
                    first_time = sig_row[1]
                    if count > 0 and first_time:
                        time_str = first_time.strftime('%H:%M:%S')
                        fired_signal = f"({count}) {time_str}"
                        
                        # Get the close price of the first signal candle
                        cursor.execute(f"""
                            SELECT close
                            FROM {table}
                            WHERE ticker = %s
                              AND timestamp = %s
                        """, (ticker, first_time))
                        close_row = cursor.fetchone()
                        if close_row and close_row[0] is not None:
                            signal_price = round(float(close_row[0]), 2)

                data[ticker][strategy] = {**common, 'state': state_value, 'fired_signal': fired_signal, 'signal_price': signal_price}

                
        # NEW: Insert into candle_conditions_log before emitting
        # We split timestamp into date and time for the table
        today_str = datetime.now().strftime('%Y-%m-%d')
        for ticker in data:
            for strategy, details in data[ticker].items():
                ts_str = details.get('timestamp', 'N/A')
                if ts_str == 'N/A':
                    continue  # Skip invalid timestamps

                # Parse date/time from ts_str if needed (assuming ts_str is 'HH:MM')
                log_time = ts_str  # Use as-is if it's already 'HH:MM'
                log_date = today_str  # Or derive from full timestamp if available

                cursor.execute("""
                    INSERT INTO candle_conditions_log 
                    (date, time, ticker, strategy, timestamp_str, open_price, pmh, hod, vwap, 
                     sma10, vwap_dev_pct, sma_dev_pct, state, fired_signal, last_price, hod_time, signal_price)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date, time, ticker, strategy) DO UPDATE SET
                        timestamp_str = EXCLUDED.timestamp_str,
                        open_price = EXCLUDED.open_price,
                        pmh = EXCLUDED.pmh,
                        hod = EXCLUDED.hod,
                        vwap = EXCLUDED.vwap,
                        sma10 = EXCLUDED.sma10,
                        vwap_dev_pct = EXCLUDED.vwap_dev_pct,
                        sma_dev_pct = EXCLUDED.sma_dev_pct,
                        state = EXCLUDED.state,
                        fired_signal = EXCLUDED.fired_signal,
                        last_price = EXCLUDED.last_price,
                        hod_time = EXCLUDED.hod_time,
                        signal_price = EXCLUDED.signal_price,
                        logged_at = CURRENT_TIMESTAMP
                """, (
                    log_date,
                    log_time,
                    ticker,
                    strategy,
                    ts_str,
                    details.get('open') if details.get('open') != 'N/A' else None,
                    details.get('pmh') if details.get('pmh') != 'N/A' else None,
                    details.get('hod') if details.get('hod') != 'N/A' else None,
                    details.get('vwap') if details.get('vwap') != 'N/A' else None,
                    details.get('sma10') if details.get('sma10') != 'N/A' else None,
                    details.get('vwap_dev_pct') if details.get('vwap_dev_pct') != 'N/A' else None,
                    details.get('sma_dev_pct') if details.get('sma_dev_pct') != 'N/A' else None,
                    details.get('state') if details.get('state') != 'N/A' else None,
                    details.get('fired_signal') if details.get('fired_signal') != '-' else None,
                    param_map.get(ticker, {}).get('last_price'),  # Last price from param_map
                    details.get('hod_time') if details.get('hod_time') != 'N/A' else None,
                    details.get('signal_price') if details.get('signal_price') != '-' else None
                ))

        conn.commit()


        # 6. Emit
        emit('candle_conditions_update', {
            'data': data,
            'ticker_strategies': ticker_strategies
        })

    except Exception as e:
        logger.error(f"Error in send_candle_conditions: {e}")
        emit('candle_conditions_update', {'data': {}, 'ticker_strategies': {}})
    finally:
        if conn:
            db_pool.putconn(conn)   

@app.route('/toggle_alerts', methods=['POST'])
def toggle_alerts():
    data = request.get_json()
    ticker = data.get('ticker')
    if not ticker:
        return jsonify({'status': 'error', 'error': 'ticker required'}), 400

    ticker = ticker.upper()
    with db_lock:
        conn = None
        cursor = None
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE ticker_alert_states 
                SET alert_active = NOT alert_active, last_updated = CURRENT_TIMESTAMP
                WHERE ticker = %s
                RETURNING alert_active
            """, (ticker,))
            result = cursor.fetchone()
            if result is None:
                return jsonify({'status': 'error', 'error': 'ticker not found'}), 404

            new_state = result[0]
            conn.commit()
            logger.info(f"Alerts toggled for {ticker}: {'ON' if new_state else 'OFF'}")

            # Emit update to all clients
            socketio.emit('alert_state_update', {'ticker': ticker, 'active': new_state})

            return jsonify({'status': 'success', 'active': new_state})
        except psycopg2.Error as e:
            logger.error(f"Error toggling alerts for {ticker}: {e}")
            if conn:
                conn.rollback()
            return jsonify({'status': 'error', 'error': str(e)}), 500
        finally:
            if cursor:
                cursor.close()
            if conn:
                db_pool.putconn(conn)

@app.route('/reset_alerts', methods=['POST'])
def reset_alerts():
    data = request.get_json()
    ticker = data.get('ticker')
    if not ticker:
        return jsonify({'status': 'error', 'error': 'ticker required'}), 400

    ticker = ticker.upper()
    with db_lock:
        conn = None
        cursor = None
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE ticker_alert_states 
                SET current_state = 'IDLE',
                    streak_count = 0,
                    last_green_high = NULL,
                    first_red_type = NULL,
                    last_sma_approach_alert = FALSE,
                    approaching_hod_spoken = FALSE,
                    approaching_pmh_spoken = FALSE,
                    last_hod_alert_price = NULL,
                    last_pmh_alert_price = NULL,
                    last_updated = CURRENT_TIMESTAMP
                WHERE ticker = %s
            """, (ticker,))
            if cursor.rowcount == 0:
                return jsonify({'status': 'error', 'error': 'ticker not found'}), 404

            conn.commit()
            logger.info(f"Alerts reset for {ticker}")

            # Emit update (state remains active, just reset)
            cursor.execute("SELECT alert_active FROM ticker_alert_states WHERE ticker = %s", (ticker,))
            active = cursor.fetchone()[0]
            socketio.emit('alert_state_update', {'ticker': ticker, 'active': active})

            return jsonify({'status': 'success'})
        except psycopg2.Error as e:
            logger.error(f"Error resetting alerts for {ticker}: {e}")
            if conn:
                conn.rollback()
            return jsonify({'status': 'error', 'error': str(e)}), 500
        finally:
            if cursor:
                cursor.close()
            if conn:
                db_pool.putconn(conn)
                
@app.route('/get_alert_state', methods=['GET'])
def get_alert_state():
    ticker = request.args.get('ticker')
    if not ticker:
        return jsonify({'status': 'error', 'error': 'ticker required'}), 400

    ticker = ticker.upper()
    with db_lock:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT alert_active FROM ticker_alert_states 
                WHERE ticker = %s
            """, (ticker,))
            row = cursor.fetchone()
            db_pool.putconn(conn)
            active = row[0] if row else True  # default to True if not found
            return jsonify({'status': 'success', 'active': active})
        except Exception as e:
            logger.error(f"Error getting alert state for {ticker}: {e}")
            return jsonify({'status': 'error', 'error': str(e)}), 500                

# Flask Routes
@app.route('/')
def index():
    return render_template('index.html')

def get_next_trade_id():
    """Generate the next unique tradeid."""
    with db_lock:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("SELECT last_trade_id FROM tradeidcounter WHERE id = 1")
            last_trade_id = cursor.fetchone()[0]
            next_trade_id = last_trade_id + 1
            cursor.execute("UPDATE tradeidcounter SET last_trade_id = %s WHERE id = 1", (next_trade_id,))
            conn.commit()
            logger.debug(f"Generated tradeid: {next_trade_id}")
            db_pool.putconn(conn)
            return str(next_trade_id)
        except psycopg2.Error as e:
            logger.error(f"Error generating tradeid: {e}")
            if cursor:
                cursor.close()
            if conn:
                db_pool.putconn(conn)
            raise
        
@app.route('/get_active_trade_for_ticker', methods=['GET'])
def get_active_trade_for_ticker():
    ticker = request.args.get('ticker')
    if not ticker:
        return jsonify({'status': 'error', 'error': 'ticker required'}), 400

    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        conn = db_pool.getconn()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT shares, entry_price
                FROM activetrades
                WHERE ticker = %s AND date = %s
            """, (ticker.upper(), today))
            rows = cur.fetchall()
            db_pool.putconn(conn)
        except Exception as e:
            db_pool.putconn(conn)
            return jsonify({'status': 'error', 'error': str(e)}), 500

    if not rows:
        return jsonify({'status': 'success', 'active': False})

    total_shares = 0
    weighted_price = 0.0
    for shares, entry_price in rows:
        if entry_price is None or shares is None:
            continue
        s = int(shares)
        p = float(entry_price)
        total_shares += s
        weighted_price += s * p

    if total_shares == 0:
        return jsonify({'status': 'success', 'active': False})

    avg_entry = weighted_price / total_shares

    return jsonify({
        'status': 'success',
        'active': True,
        'total_shares': total_shares,
        'avg_entry_price': round(avg_entry, 4),
    })        

@app.route('/get_order_data', methods=['GET'])
def get_order_data():
    ticker = request.args.get('ticker')
    sl_method = request.args.get('sl_method', '20%')
    today_date = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d')
    eastern = pytz.timezone('America/New_York')
    current_time = datetime.now(eastern).strftime('%Y-%m-%d %H:%M:%S')
    
    def find_last_highest_high(ticker, date):
        try:
            #logger.info(f"🔍 SEARCHING LAST HIGHEST HIGH for {ticker} on {date}")
            with db_lock:
                conn = db_pool.getconn()
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT prev_high
                    FROM (
                        SELECT high, timestamp,
                               LAG(high) OVER (ORDER BY timestamp DESC) as prev_high
                        FROM ohlc_5min 
                        WHERE ticker = %s AND timestamp LIKE %s
                    ) t
                    WHERE high < prev_high
                    ORDER BY timestamp DESC
                    LIMIT 1
                """, (ticker, f"{date.replace('-', '/')}%"))
                result = cursor.fetchone()
                db_pool.putconn(conn)
                if result:
                    #logger.info(f"✅ FOUND LAST HIGHEST HIGH for {ticker}: {result[0]}")
                    return result[0]
                else:
                    #logger.warning(f"⚠️  NO LAST HIGHEST HIGH found for {ticker}")
                    return None
        except Exception as e:
            logger.error(f"Error finding last highest high for {ticker}: {e}")
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'conn' in locals() and conn:
                db_pool.putconn(conn)
            return None
    
    try:
        #logger.info(f"📊 GET ORDER DATA for {ticker}")
        with db_lock:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("SELECT high, last, open, account_equity FROM tradeparameters WHERE ticker = %s AND date = %s", (ticker, today_date))
            result = cursor.fetchone()
            db_pool.putconn(conn)
            if not result:
                return jsonify({'status': 'error', 'error': 'Ticker data not found'}), 404
            hod, last_price, open_price, account_equity = result
        
        last_hi = find_last_highest_high(ticker, today_date)
        #logger.info(f"📈 FINAL last_hi for {ticker}: {last_hi}")
        
        spike = None
        if open_price and hod and open_price != '0':
            spike = ((float(hod) - float(open_price)) / float(open_price)) * 100
            spike = round(spike, 2)
        account = round(float(account_equity)) if account_equity is not None else None 
        
        stop_loss = None
        target = None 
        if last_price and last_hi:
            last_price_float = float(last_price)
            last_hi_float = float(last_hi)
            if sl_method == 'hod' and hod:
                stop_loss = float(hod) + 0.10
            elif sl_method == '10%':
                stop_loss = last_price_float * 1.1
            elif sl_method == '20%':
                stop_loss = last_price_float * 1.2
            elif sl_method == 'last_hi':
                stop_loss = last_hi_float   
            else:
                stop_loss = last_price_float * 1.2
                
            if stop_loss is not None:
                risk = stop_loss - last_price_float
                target = last_price_float - (2 * risk)    
        
        response = {
            'status': 'success',
            'hod': float(hod) if hod else None,
            'last_price': float(last_price) if last_price else None,
            'open_price': float(open_price) if open_price else None,
            #'hrange': float(hi_range) if hi_range else None,
            #'arange': float(avg_range) if avg_range else None,
            'account': account,
            'stop_loss': round(stop_loss, 2) if stop_loss else None,
            'target': round(target, 2) if target else None,
            'last_hi': float(last_hi) if last_hi else None,
            'time': current_time
        }
        #logger.info(f"✅ RESPONSE for {ticker}: last_hi={response['last_hi']}")
        return jsonify(response)
        
    except psycopg2.Error as e:
        logger.error(f"DB Error for {ticker}: {e}")
        return jsonify({'status': 'error', 'error': str(e)}), 500
    except ValueError as e:
        logger.error(f"Value Error for {ticker}: {e}")
        return jsonify({'status': 'error', 'error': 'Invalid numeric data in database'}), 500

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

@app.route('/close_trade')
def email_close_trade():
    token = request.args.get('token')
    if not token:
        return "<h3 style='color:red;'>Invalid link</h3>"

    try:
        data = serializer.loads(token, salt='close-trade', max_age=3600)  # Expires in 1 hour
        trade_id = data['trade_id']
        ticker = data['ticker']
        
        # ADD THIS CHECK FOR MANUAL TRADES
        if 'TEST124' in trade_id or trade_id.startswith('MANUAL'):
            # For test/manual trades, just delete from activetrades
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM activetrades WHERE tradeid = %s", (trade_id,))
            conn.commit()
            db_pool.putconn(conn)
            # Emit update
            socketio.emit('active_trade_remove', {'tradeID': trade_id})
            return f"<h2 style='color:green;'>Test Trade {trade_id} Closed</h2>"

        # Try to close — catch any error
        try:
            end_of_day.close_position(ticker, trade_id, reason='EmailClose')
            return f"""
            <div style="text-align:center;padding:50px;background:#f0fff0;">
                <h2 style="color:green;">Trade Closed Successfully</h2>
                <p>{ticker} - ID: {trade_id}</p>
            </div>
            """
        except Exception as e:
            logger.error(f"Failed to close trade {trade_id} via email: {e}")
            return f"""
            <div style="text-align:center;padding:50px;background:#ffebee;">
                <h2 style="color:red;">Failed to Close Trade</h2>
                <p>{ticker} - ID: {trade_id}</p>
                <p>Error: Trade not found or already closed</p>
                <p>Check dashboard.</p>
            </div>
            """
    except SignatureExpired:
        return "<h3 style='color:red;'>Link expired (valid 1 hour)</h3>"
    except BadSignature:
        return "<h3 style='color:red;'>Invalid link</h3>"

@app.route('/close_trade', methods=['POST'])
def close_trade():
    data = request.get_json()
    ticker = data.get('ticker')
    trade_id = data.get('tradeID')
    reason = data.get('reason')
    if not ticker or not trade_id:
        return jsonify({'status': 'error', 'error': 'Ticker and tradeID are required'}), 400
    try:
        end_of_day.close_position(ticker, trade_id, reason=reason)
        logger.info(f"Closed position for ticker {ticker}, tradeID {trade_id}")
        return jsonify({'status': 'success'})
    except psycopg2.Error as e:
        logger.error(f"Database error closing position for {ticker}, tradeID {trade_id}: {e}")
        return jsonify({'status': 'error', 'error': f"Database error: {str(e)}"}), 500
    except Exception as e:
        logger.error(f"Error closing position for {ticker}, tradeID {trade_id}: {e}")
        return jsonify({'status': 'error', 'error': str(e)}), 500

@app.route('/add_ticker_page')
def add_ticker_page():
    return render_template('add_ticker.html')
    
@app.route('/get_ticker_details', methods=['GET'])
def get_ticker_details():
    ticker = request.args.get('ticker').upper()
    today = datetime.now().strftime('%Y-%m-%d')
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT rsi_1min, rsi_5min, strategy_risks
            FROM tradeparameters
            WHERE ticker = %s AND date = %s
        """, (ticker, today))
        row = cursor.fetchone()
        db_pool.putconn(conn)
        if row:
            return jsonify({
                'rsi_1min': row[0],
                'rsi_5min': row[1],
                'strategy_risks': row[2] or {}
            })
        return jsonify({'error': 'Not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/update_ticker', methods=['POST'])
def update_ticker():
    data = request.get_json()
    ticker = data['ticker'].upper()
    today = datetime.now().strftime('%Y-%m-%d')
    try:
        with db_lock:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE tradeparameters
                SET rsi_1min = %s, rsi_5min = %s, strategy_risks = %s
                WHERE ticker = %s AND date = %s
            """, (data['rsi_1min'], data['rsi_5min'], Json(data['strategy_risks']), ticker, today))
            conn.commit()
            db_pool.putconn(conn)

        # Restart threads with new selection
        if strategy_logic:
            strategy_logic.remove_ticker(ticker)
            strategy_logic.add_ticker(
                ticker=ticker,
                rsi_1m=data['rsi_1min'],
                rsi_5m=data['rsi_5min'],
                selected_strategies=data['selected_strategies']
            )
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500

@app.route('/delete_ticker', methods=['POST'])
def delete_ticker():
    ticker = request.form.get('ticker').upper()
    today = datetime.now().strftime('%Y-%m-%d')
    try:
        with db_lock:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM tradeparameters WHERE ticker = %s AND date = %s", (ticker, today))
            conn.commit()
            db_pool.putconn(conn)

        if strategy_logic:
            strategy_logic.remove_ticker(ticker)
        return jsonify({'status': 'success'})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500    

@app.route('/get_tickers', methods=['GET'])
def get_tickers():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            # Only select existing columns + strategy_risks
            cursor.execute("""
                SELECT ticker, date, time, rsi_1min, rsi_5min, strategy_risks
                FROM tradeparameters 
                WHERE date = %s
            """, (today,))
            tickers = []
            for row in cursor.fetchall():
                tickers.append({
                    'ticker': row[0],
                    'date': row[1],
                    'time': row[2],
                    'rsi_1m': row[3],
                    'rsi_5m': row[4],
                    'strategy_risks': row[5] or {}  # Send the per-strategy risks
                })
            db_pool.putconn(conn)
            return jsonify({'tickers': tickers})
        except psycopg2.Error as e:
            logger.error(f"Error fetching tickers: {e}")
            if cursor:
                cursor.close()
            if conn:
                db_pool.putconn(conn)
            return jsonify({'status': 'error', 'error': str(e)}), 500

@app.route('/add_ticker', methods=['POST'])
def add_ticker():
    data = request.get_json()
    ticker = data.get('ticker')
    rsi_1min = data.get('rsi_1min', '1')
    rsi_5min = data.get('rsi_5min', '50')
    selected_strategies = data.get('selected_strategies', [])
    strategy_risks = data.get('strategy_risks', {})

    if not ticker or not selected_strategies:
        return jsonify({'status': 'error', 'error': 'Ticker and at least one strategy required'}), 400

    ticker = ticker.upper()

    today = datetime.now().strftime('%Y-%m-%d')
    current_time_str = datetime.now().strftime('%H:%M:%S')  # ← NEW

    try:
        with db_lock:
            conn = db_pool.getconn()
            cursor = conn.cursor()

            # Check if ticker already exists today
            cursor.execute("""
                SELECT 1 FROM tradeparameters
                WHERE ticker = %s AND date = %s
            """, (ticker, today))
            exists = cursor.fetchone()

            if exists:
                return jsonify({'status': 'error', 'error': f'Ticker {ticker} already added today'}), 400

            # Insert with current time
            cursor.execute("""
                INSERT INTO tradeparameters (
                    ticker, date, time, rsi_1min, rsi_5min, strategy_risks
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (ticker, today, current_time_str, rsi_1min, rsi_5min, Json(strategy_risks)))

            conn.commit()
            db_pool.putconn(conn)

        # Add to StrategyLogic
        if strategy_logic:
            strategy_logic.add_ticker(
                ticker=ticker,
                rsi_1m=rsi_1min,
                rsi_5m=rsi_5min,
                selected_strategies=selected_strategies
            )
            
        # <<< ADD THIS >>>
        socketio.emit('ticker_added')
        logger.info(f"Emitted 'ticker_added' event for {ticker}")
        # <<< END >>>    

        logger.info(f"Added ticker {ticker} at {current_time_str} with strategies {selected_strategies}")
        return jsonify({'status': 'success'})

    except Exception as e:
        logger.error(f"Error adding ticker {ticker}: {e}")
        return jsonify({'status': 'error', 'error': str(e)}), 500


        
@app.route('/manual_trade')
def manual_trade_page():
    return render_template('manual_trade.html')


@app.route('/add_manual_trade', methods=['POST'])
def add_manual_trade():
    data = request.get_json()
    
    ticker        = data.get('ticker', '').upper().strip()
    shares        = int(data.get('shares', 0))
    entry_price   = float(data.get('entry_price', 0))
    stop_loss     = float(data.get('stop_loss', 0))
    strategy      = data.get('strategy', 'Manual')
    sell_order_id = data.get('sellOrderID', 'MANUAL')
    stop_order_id = data.get('stopOrderID')  # Real DAS stop order ID from you

    if not all([ticker, shares > 0, entry_price > 0, stop_loss > 0, stop_order_id]):
        return jsonify({'status': 'error', 'message': 'Missing or invalid required fields'}), 400

    # --- SHORT CALCULATION: 1:3 RR, 2 decimals only ---
    risk_distance_raw = stop_loss - entry_price
    if risk_distance_raw < 0:
        return jsonify({'status': 'error', 'message': 'Stop loss must be ABOVE entry price for shorts'}), 400

    risk_distance = round(risk_distance_raw, 2)
    total_risk    = round(shares * risk_distance, 2)
    target_price  = round(entry_price - (3 * risk_distance), 2)

    trade_id   = get_next_trade_id()
    now_time   = datetime.now().strftime('%H:%M:%S')
    today_date = datetime.now().strftime('%Y-%m-%d')

    # --- NUMERIC-ONLY TOKEN (pure integer, high range to avoid collisions) ---
    token = 900000000 + int(trade_id)  # e.g., trade_id 123 → token 900000123

    trade_details = {
        'tradeID'     : trade_id,
        'time'        : now_time,
        'strategy'    : strategy,
        'ticker'      : ticker,
        'shares'      : shares,
        'entry_price' : entry_price,
        'stop_loss'   : stop_loss,
        'target_price': target_price,
        'risk'        : total_risk,
        'sellOrderID' : sell_order_id,
        'stopOrderID' : stop_order_id,
    }

    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()

        # 1. tradedetails insert (no target/risk)
        cursor.execute("""
            INSERT INTO tradedetails 
            (tradeid, time, ticker, strategy, shares, entry_price, stop_loss, 
             sellorderid, stoporderid, date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tradeid) DO NOTHING
        """, (trade_id, now_time, ticker, strategy, shares, entry_price, stop_loss,
              sell_order_id, stop_order_id, today_date))

        # 2. stopmarket insert — numeric token only
        cursor.execute("""
            INSERT INTO stopmarket 
            (tradeid, strategy, time, ticker, shares, price, token, orderid, 
             action, status, act_status, date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'SELL', 'Pending', 'Pending', %s)
            ON CONFLICT (token) DO NOTHING
        """, (trade_id, strategy, now_time, ticker, shares, stop_loss, token, 
              stop_order_id, today_date))

        conn.commit()
        logger.info(f"Manual short trade {trade_id} added | Token: {token} | "
                    f"{ticker} Entry {entry_price:.2f} Stop {stop_loss:.2f} "
                    f"Target {target_price:.2f} Risk ${total_risk:.2f}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Manual trade DB error: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500
    finally:
        db_pool.putconn(conn)

    # Notify monitors — puts it in activetrades with target & risk
    trade_monitor.receive_order_details(trade_details)
    sl_monitor.receive_order_details(trade_details)
    end_of_day.receive_order_details(trade_details)

    return jsonify({
        'status': 'success',
        'tradeID': trade_id,
        'target': target_price,
        'risk': total_risk
    })  
    
# ------------------------------------------------------------------
# PENDING ORDERS ENDPOINTS
# ------------------------------------------------------------------

@app.route('/get_pending_orders', methods=['GET'])
def get_pending_orders():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, ticker, risk, price, shares, target, stop_loss, hod, strategy, time
                FROM pending_orders
                WHERE date = %s
                ORDER BY id
            ''', (today,))
            rows = cursor.fetchall()
            pending = []
            for row in rows:
                pending.append({
                    'id': row[0],
                    'ticker': row[1],
                    'risk': float(row[2]) if row[2] is not None else None,
                    'price': float(row[3]) if row[3] is not None else None,
                    'shares': int(row[4]) if row[4] is not None else None,
                    'target': float(row[5]) if row[5] is not None else None,
                    'stop_loss': float(row[6]) if row[6] is not None else None,
                    'hod': float(row[7]) if row[7] is not None else None,
                    'strategy': row[8],
                    'time': row[9]
                })
            db_pool.putconn(conn)
            return jsonify({'pending_orders': pending})
        except Exception as e:
            logger.error(f"Error fetching pending orders: {e}")
            return jsonify({'status': 'error', 'error': str(e)}), 500

@app.route('/add_pending_order', methods=['POST'])
def add_pending_order():
    data = request.get_json()
    required = ['ticker', 'risk', 'price', 'shares', 'target', 'stop_loss', 'hod', 'strategy', 'time']
    if not all(k in data for k in required):
        return jsonify({'status': 'error', 'error': 'Missing fields'}), 400

    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO pending_orders
                (ticker, risk, price, shares, target, stop_loss, hod, strategy, time, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            ''', (
                data['ticker'], data['risk'], data['price'], data['shares'],
                data['target'], data['stop_loss'], data['hod'], data['strategy'],
                data['time'], today
            ))
            new_id = cursor.fetchone()[0]
            conn.commit()
            db_pool.putconn(conn)
            socketio.emit('pending_order_added', data | {'id': new_id})
            return jsonify({'status': 'success', 'id': new_id})
        except Exception as e:
            logger.error(f"Error adding pending order: {e}")
            if conn: conn.rollback()
            return jsonify({'status': 'error', 'error': str(e)}), 500

@app.route('/update_pending_order', methods=['POST'])
def update_pending_order():
    data = request.get_json()
    if 'id' not in data:
        return jsonify({'status': 'error', 'error': 'ID required'}), 400

    with db_lock:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE pending_orders
                SET ticker=%s, risk=%s, price=%s, shares=%s, target=%s,
                    stop_loss=%s, hod=%s, strategy=%s, time=%s
                WHERE id=%s
            ''', (
                data['ticker'], data['risk'], data['price'], data['shares'],
                data['target'], data['stop_loss'], data['hod'], data['strategy'],
                data['time'], data['id']
            ))
            conn.commit()
            db_pool.putconn(conn)
            socketio.emit('pending_order_updated', data)
            return jsonify({'status': 'success'})
        except Exception as e:
            logger.error(f"Error updating pending order: {e}")
            if conn: conn.rollback()
            return jsonify({'status': 'error', 'error': str(e)}), 500

@app.route('/delete_pending_order', methods=['POST'])
def delete_pending_order():
    data = request.get_json()
    if 'id' not in data:
        return jsonify({'status': 'error', 'error': 'ID required'}), 400

    with db_lock:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute('DELETE FROM pending_orders WHERE id=%s', (data['id'],))
            conn.commit()
            db_pool.putconn(conn)
            socketio.emit('pending_order_deleted', {'id': data['id']})
            return jsonify({'status': 'success'})
        except Exception as e:
            logger.error(f"Error deleting pending order: {e}")
            if conn: conn.rollback()
            return jsonify({'status': 'error', 'error': str(e)}), 500            

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
        target = data.get('target')

        if not all([ticker, entry_price, strategy, time, hod, shares, risk, s_loss, target]):
            return jsonify({'status': 'failure', 'error': 'Missing required parameters'}), 400

        try:
            entry_price = float(entry_price)
            hod = float(hod)
            shares = int(shares)
            risk = float(risk)
            s_loss = float(s_loss)
            target = float(target)
        except (ValueError, TypeError):
            return jsonify({'status': 'failure', 'error': 'Invalid numeric parameters'}), 400

        if entry_price <= 0 or hod <= 0 or shares <= 0 or risk <= 0 or s_loss <= 0:
            return jsonify({'status': 'failure', 'error': 'Numeric parameters must be positive'}), 400
        if s_loss <= entry_price:
            return jsonify({'status': 'failure', 'error': 'Stop loss must be above entry price for short selling'}), 400
        if target >= entry_price:
            return jsonify({'status': 'failure', 'error': 'Target must be below entry price for short selling'}), 400

        trade_id = get_next_trade_id()
        risk_dollar = float(data['risk'])  # from frontend
        
        # Set status to "Stop" if strategy is "stop"
        signal_status = "Stop" if strategy == "stop" else "Fired"
        
        signal = {
            'tradeID': trade_id,
            'strategy': strategy,
            'ticker': ticker,
            'price': round(entry_price, 2),
            's_loss': s_loss,
            'target': round(target, 2),
            'time': time,
            'hi': round(hod, 2),
            'shares': shares,
            'risk': risk_dollar,
            'status': signal_status,
        }

        conn = db_pool.getconn()
        try:
            cursor = conn.cursor()
            query_insert = """
                INSERT INTO tradesignal (tradeid, time, strategy, ticker, price, shares, target, hi, status, risk)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query_insert, (
                signal['tradeID'],
                signal['time'],
                signal['strategy'],
                signal['ticker'],
                str(signal['price']),
                signal['shares'],
                str(signal['target']),
                str(signal['hi']),
                signal['status'],
                signal['risk']
            ))
            conn.commit()
            logger.info(f"Trade signal inserted successfully: {signal}")
            
            # ADD THESE TWO LINES
            if 'socketio' in globals():                                   # safe check
                socketio.emit('trade_signal_update', [signal])             # array works with your frontend!
            # END OF ADDITION
        except psycopg2.Error as e:
            logger.error(f"Error inserting trade signal: {e}")
            return jsonify({'status': 'failure', 'error': f"Failed to insert trade signal: {str(e)}"}), 500
        finally:
            db_pool.putconn(conn)
            
        order_type = 'target' if strategy == 'Multi' else strategy    
        
        command_sell = {
            'trade_id': trade_id,
            'ticker': ticker,
            'shares': shares,
            'order_type': order_type,
            'stop_price': s_loss,
            'target_price': target,
            'price': entry_price,
            'risk': risk_dollar,
            'strategy': strategy
        }
        
        def execute_order_async(command):
            try:
                order_execution.execute_command(command)
                logger.info(f"Order executed successfully for trade_id {command['trade_id']}: {command}")
            except Exception as e:
                logger.error(f"Error executing order for {command['ticker']}: {e}")

        threading.Thread(target=execute_order_async, args=(command_sell,), daemon=True).start()
        logger.info(f"Order submitted asynchronously for trade_id {trade_id}: {command_sell}")

        return jsonify({'status': 'success'})

    except Exception as e:
        logger.error(f"Unexpected error in send_order: {e}")
        return jsonify({'status': 'failure', 'error': str(e)}), 500
    
@app.route('/get_trade_signals', methods=['GET'])
def get_trade_signals():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute('SELECT tradeid, time, strategy, ticker, price, shares, target, hi, risk, status FROM tradesignal WHERE time::date = %s', (today,))
            trade_signals = [{'tradeID': row[0], 'time': row[1], 'strategy': row[2], 'ticker': row[3], 'price': row[4], 'shares': row[5], 'target': row[6], 'hi': row[7], 'risk': float(row[8]) if row[8] is not None else 0.0, 'status': row[9]} for row in cursor.fetchall()]
            db_pool.putconn(conn)
            socketio.emit('trade_signal_update', trade_signals)
            return jsonify({'trade_signals': trade_signals})
        except psycopg2.Error as e:
            logger.error(f"Error fetching trade signals: {e}")
            if cursor:
                cursor.close()
            if conn:
                db_pool.putconn(conn)
            return jsonify({'status': 'error', 'error': str(e)}), 500
        
@app.route('/update_trade_target', methods=['POST'])
def update_trade_target():
    data = request.get_json()
    trade_id = data.get('tradeID')
    ticker = data.get('ticker')
    target = data.get('target')
    
    if not trade_id or not ticker or target is None:
        logger.error(f"Missing required parameters: tradeID={trade_id}, ticker={ticker}, target={target}")
        return jsonify({'status': 'error', 'error': 'tradeID, ticker, and target are required'}), 400
    
    if target <= 0:
        logger.error(f"Invalid target price: {target}")
        return jsonify({'status': 'error', 'error': 'Target price must be positive'}), 400
    
    with db_lock:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            today = datetime.now().strftime('%Y-%m-%d')
            
            # Validate target price against entry price for short selling
            cursor.execute("""
                SELECT entry_price, strategy FROM activetrades 
                WHERE tradeid = %s AND ticker = %s AND date = %s
            """, (trade_id, ticker, today))
            row = cursor.fetchone()
            if not row:
                logger.warning(f"No active trade found for tradeID {trade_id}, ticker {ticker} on date {today}")
                db_pool.putconn(conn)
                return jsonify({'status': 'error', 'error': 'No active trade found'}), 404
            
            entry_price, strategy = row
            if strategy == 'target' and float(entry_price) <= target:
                logger.error(f"Target price {target} must be below entry price {entry_price} for short selling")
                db_pool.putconn(conn)
                return jsonify({'status': 'error', 'error': 'Target price must be below entry price for short selling'}), 400
            
            # Update target price in activetrades
            cursor.execute("""
                UPDATE activetrades 
                SET target = %s 
                WHERE tradeid = %s AND ticker = %s AND date = %s
            """, (target, trade_id, ticker, today))
            
            if cursor.rowcount == 0:
                logger.warning(f"No active trade found for tradeID {trade_id}, ticker {ticker} on date {today}")
                db_pool.putconn(conn)
                return jsonify({'status': 'error', 'error': 'No active trade found'}), 404
            
            conn.commit()
            logger.info(f"Updated target price to {target} for tradeID {trade_id}, ticker {ticker}")
            
            # Fetch the updated trade to emit via SocketIO
            cursor.execute("""
                SELECT tradeid, time, strategy, ticker, shares, entry_price, target, stop_loss, 
                       lu_price, unrealized, date, sellorderid, stoporderid, last_price 
                FROM activetrades 
                WHERE tradeid = %s AND ticker = %s AND date = %s
            """, (trade_id, ticker, today))
            row = cursor.fetchone()
            if row:
                active_trade = {
                    'tradeID': row[0],
                    'time': row[1],
                    'strategy': row[2],
                    'ticker': row[3],
                    'shares': row[4],
                    'entry_price': float(row[5]) if row[5] is not None else None,
                    'target': float(row[6]) if row[6] is not None else None,
                    'stop_loss': float(row[7]) if row[7] is not None else None,
                    'lu_price': float(row[8]) if row[8] is not None else None,
                    'unrealized': float(row[9]) if row[9] is not None else None,
                    'date': row[10],
                    'sellOrderID': row[11],
                    'stopOrderID': row[12],
                    'last_price': float(row[13]) if row[13] is not None else None
                }
                socketio.emit('active_trade_update', active_trade)
                logger.info(f"Emitted active_trade_update for tradeID {trade_id}")
            
            db_pool.putconn(conn)
            return jsonify({'status': 'success'})
        
        except psycopg2.Error as e:
            logger.error(f"Database error updating target price for tradeID {trade_id}: {e}")
            if cursor:
                cursor.close()
            if conn:
                db_pool.putconn(conn)
            return jsonify({'status': 'error', 'error': str(e)}), 500
        except Exception as e:
            logger.error(f"Unexpected error updating target price for tradeID {trade_id}: {e}")
            if cursor:
                cursor.close()
            if conn:
                db_pool.putconn(conn)
            return jsonify({'status': 'error', 'error': str(e)}), 500   

@app.route('/close_partial')
def email_close_partial():
    token = request.args.get('token')
    if not token:
        return "<h3 style='color:red;'>Invalid link</h3>"

    try:
        data = serializer.loads(token, salt='close-partial', max_age=3600)  # 1 hour expiry
        trade_id = data['trade_id']
        ticker = data['ticker']

        # For test/manual trades – just remove from active
        if 'TEST124' in trade_id or trade_id.startswith('MANUAL'):
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM activetrades WHERE tradeid = %s", (trade_id,))
            conn.commit()
            db_pool.putconn(conn)
            socketio.emit('active_trade_remove', {'tradeID': trade_id})
            return f"<h2 style='color:green;'>Test Trade {trade_id} Partially Closed</h2>"

        # Perform partial close – using 50% by default (you can adjust later)
        conn = db_pool.getconn()
        cursor = conn.cursor()
        cursor.execute("SELECT shares FROM activetrades WHERE tradeid = %s AND ticker = %s", (trade_id, ticker))
        row = cursor.fetchone()
        db_pool.putconn(conn)

        if not row:
            return "<h3 style='color:red;'>Trade not found or already closed</h3>"

        total_shares = int(row[0])
        shares_to_close = total_shares // 2
        leftover = total_shares - shares_to_close

        if shares_to_close == 0:
            return "<h3 style='color:orange;'>Not enough shares to partially close</h3>"

        result = end_of_day.partial_close_trade(
            trade_id=trade_id,
            shares_to_close=shares_to_close,
            ticker=ticker,
            leftover_shares=leftover
        )

        if result.get('status') == 'success':
            return f"""
            <div style="text-align:center;padding:50px;background:#f0fff0;">
                <h2 style="color:green;">Partial Close Successful ({shares_to_close} shares)</h2>
                <p>{ticker} - ID: {trade_id}</p>
                <p>Remaining: {leftover} shares</p>
            </div>
            """
        else:
            return f"""
            <div style="text-align:center;padding:50px;background:#ffebee;">
                <h2 style="color:red;">Partial Close Failed</h2>
                <p>{ticker} - ID: {trade_id}</p>
                <p>Error: {result.get('error', 'Unknown')}</p>
            </div>
            """

    except SignatureExpired:
        return "<h3 style='color:red;'>Link expired (valid 1 hour)</h3>"
    except BadSignature:
        return "<h3 style='color:red;'>Invalid link</h3>"        
        
@app.route('/partial_close', methods=['POST'])
def partial_close():
    data = request.get_json()
    trade_id = data.get('tradeID')
    ticker = data.get('ticker')
    close_percent = data.get('percent', 50)  # default half
    last_price = data.get('last_price')  # optional, forwarded by trade_monitor

    if not trade_id or not ticker:
        return jsonify({'status': 'error', 'error': 'Missing tradeID or ticker'}), 400

    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT shares FROM activetrades
            WHERE tradeid = %s AND ticker = %s
        """, (trade_id, ticker))
        row = cursor.fetchone()
        if not row:
            return jsonify({'status': 'error', 'error': 'Trade not active'}), 404

        total_shares = int(row[0])
        shares_to_close = int(total_shares * (close_percent / 100))
        leftover_shares = total_shares - shares_to_close

        if shares_to_close == 0:
            return jsonify({'status': 'error', 'error': 'No shares to close'}), 400

        # Call EOD function
        result = end_of_day.partial_close_trade(
            trade_id=trade_id,
            shares_to_close=shares_to_close,
            ticker=ticker,
            leftover_shares=leftover_shares,
            last_price=last_price
        )

        return jsonify(result)

    except Exception as e:
        logger.error(f"/partial_close error: {e}")
        return jsonify({'status': 'error', 'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            db_pool.putconn(conn)             

@app.route('/replace_order', methods=['POST'])
def replace_order():
    try:
        data = request.get_json()
        ticker = data.get('ticker')
        shares = data.get('shares')
        price = data.get('price')
        order_id = data.get('order_id')
        trade_id = data.get('trade_id')

        if not all([ticker, shares, price, order_id, trade_id]):
            logger.error(f"Missing required fields in replace_order: {data}")
            return jsonify({'status': 'error', 'error': 'Missing required fields'}), 400

        try:
            shares = int(shares)
            price = float(price)
        except (ValueError, TypeError):
            logger.error(f"Invalid numeric parameters in replace_order: shares={shares}, price={price}")
            return jsonify({'status': 'error', 'error': 'Invalid numeric parameters'}), 400

        if shares <= 0 or price <= 0:
            logger.error(f"Non-positive numeric parameters in replace_order: shares={shares}, price={price}")
            return jsonify({'status': 'error', 'error': 'Shares and price must be positive'}), 400
        
        

        # Call send_replace_order from TradeMonitor
        trade_monitor.send_replace_order(
            stop_order_id=order_id,
            ticker=ticker,
            shares=shares,
            new_stop_price=price,
            trade_id=trade_id,
            source='user'
        )

        

        return jsonify({'status': 'success', 'message': 'Replace order sent successfully'})

    except Exception as e:
        logger.error(f"Error in replace_order: {e}")
        return jsonify({'status': 'error', 'error': str(e)}), 500        

@app.route('/get_sell_market', methods=['GET'])
def get_sell_market():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute('SELECT tradeid, strategy, time, ticker, shares, price, action, status, act_status, notes FROM sellmarket WHERE date = %s', (today,))
            sell_market = [{'tradeID': row[0], 'strategy': row[1], 'time': row[2], 'ticker': row[3], 'shares': row[4], 'price': row[5], 'action': row[6], 'status': row[7], 'act_status': row[8], 'notes': row[9]} for row in cursor.fetchall()]
            db_pool.putconn(conn)
            socketio.emit('update_sell_market', {'sell_market': sell_market})
            return jsonify({'sell_market': sell_market})
        except psycopg2.Error as e:
            logger.error(f"Error fetching sell market data: {e}")
            if cursor:
                cursor.close()
            if conn:
                db_pool.putconn(conn)
            return jsonify({'status': 'error', 'error': str(e)}), 500

@app.route('/get_stop_market', methods=['GET'])
def get_stop_market():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute('SELECT tradeid, strategy, time, ticker, shares, price, action, status, act_status, notes, orderid FROM stopmarket WHERE date = %s', (today,))
            stop_market = [{'tradeID': row[0], 'strategy': row[1], 'time': row[2], 'ticker': row[3], 'shares': row[4], 'price': row[5], 'action': row[6], 'status': row[7], 'act_status': row[8], 'notes': row[9], 'orderID': row[10]} for row in cursor.fetchall()]
            db_pool.putconn(conn)
            socketio.emit('update_stop_market', {'stop_market': stop_market})
            return jsonify({'stop_market': stop_market})
        except psycopg2.Error as e:
            logger.error(f"Error fetching stop market data: {e}")
            if cursor:
                cursor.close()
            if conn:
                db_pool.putconn(conn)
            return jsonify({'status': 'error', 'error': str(e)}), 500

@app.route('/get_executed_stop', methods=['GET'])
def get_executed_stop():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute('SELECT tradeid, strategy, executed_time, ticker, shares, stop_loss FROM executedstop WHERE date = %s', (today,))
            executed_stop = [{'tradeID': row[0], 'strategy': row[1], 'time': row[2], 'ticker': row[3], 'shares': row[4], 'price': row[5]} for row in cursor.fetchall()]
            db_pool.putconn(conn)
            socketio.emit('update_executed_stop', {'executed_stop': executed_stop})
            return jsonify({'executed_stop': executed_stop})
        except psycopg2.Error as e:
            logger.error(f"Error fetching executed stop data: {e}")
            if cursor:
                cursor.close()
            if conn:
                db_pool.putconn(conn)
            return jsonify({'status': 'error', 'error': str(e)}), 500

@app.route('/get_replace_stop', methods=['GET'])
def get_replace_stop():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute('SELECT tradeid, strategy, time, ticker, shares, price, action, status, act_status, notes FROM replacestop WHERE date = %s', (today,))
            replace_stop = [
                {
                    'tradeID': row[0],
                    'strategy': row[1],
                    'time': row[2],
                    'ticker': row[3],
                    'shares': row[4],
                    'price': float(row[5]) if row[5] is not None else None,
                    'action': row[6],
                    'status': row[7],
                    'act_status': row[8],
                    'notes': row[9]
                }
                for row in cursor.fetchall()
            ]
            db_pool.putconn(conn)
            socketio.emit('update_replace_stop', {'replace_stop': replace_stop})
            return jsonify({'replace_stop': replace_stop})
        except psycopg2.Error as e:
            logger.error(f"Error fetching replace stop data: {e}")
            if conn:
                db_pool.putconn(conn)
            return jsonify({'status': 'error', 'error': str(e)}), 500
        
@app.route('/get_canceled_stop', methods=['GET'])
def get_canceled_stop():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute('SELECT tradeid, strategy, time, ticker, shares, price, action, status, act_status, notes FROM canceledstop WHERE date = %s', (today,))
            canceled_stop = [
                {
                    'tradeID': row[0],
                    'strategy': row[1],
                    'time': row[2],
                    'ticker': row[3],
                    'shares': row[4],
                    'price': float(row[5]) if row[5] is not None else None,
                    'action': row[6],
                    'status': row[7],
                    'act_status': row[8],
                    'notes': row[9]
                }
                for row in cursor.fetchall()
            ]
            db_pool.putconn(conn)
            
            return jsonify({'canceled_stop': canceled_stop})
        except psycopg2.Error as e:
            logger.error(f"Error fetching replace stop data: {e}")
            if conn:
                db_pool.putconn(conn)
            return jsonify({'status': 'error', 'error': str(e)}), 500        

@app.route('/get_buy_market', methods=['GET'])
def get_buy_market():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute('SELECT tradeid, strategy, time, ticker, shares, price, action, status, act_status, notes FROM buymarket WHERE date = %s', (today,))
            buy_market = [
                {
                    'tradeID': row[0],
                    'strategy': row[1],
                    'time': row[2],
                    'ticker': row[3],
                    'shares': row[4],
                    'price': row[5],
                    'action': row[6],
                    'status': row[7],
                    'act_status': row[8],
                    'notes': row[9]
                }
                for row in cursor.fetchall()
            ]
            db_pool.putconn(conn)
            socketio.emit('update_buy_market', {'buy_market': buy_market})
            return jsonify({'buy_market': buy_market})
        except psycopg2.Error as e:
            logger.error(f"Error fetching buy market data: {e}, SQL: {cursor.query.decode()}")
            if cursor:
                cursor.close()
            if conn:
                db_pool.putconn(conn)
            return jsonify({'status': 'error', 'error': str(e)}), 500

@app.route('/get_active_trades', methods=['GET'])
def get_active_trades():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute('SELECT tradeid, time, strategy, ticker, shares, entry_price, target, stop_loss, lu_price, unrealized, realized, date, sellorderid, stoporderid, last_price, risk FROM activetrades WHERE date = %s', (today,))
            active_trades = [
                {
                    'tradeID': row[0],
                    'time': row[1],
                    'strategy': row[2],
                    'ticker': row[3],
                    'shares': row[4],
                    'entry_price': float(row[5]) if row[5] is not None else None,
                    'target': float(row[6]) if row[6] is not None else None,
                    'risk': float(row[15]) if row[15] else 0.0,   # NEW
                    'stop_loss': float(row[7]) if row[7] is not None else None,
                    'lu_price': float(row[8]) if row[8] is not None else None,
                    'unrealized': float(row[9]) if row[9] is not None else None,
                    'realized': float(row[10]) if row[10] is not None else 0.0,  # NEW
                    'date': row[11],
                    'sellOrderID': row[12],
                    'stopOrderID': row[13],
                    'last_price': float(row[14]) if row[14] is not None else None
                }
                for row in cursor.fetchall()
            ]
            db_pool.putconn(conn)
            print("EMITTING ACTIVE TRADES:", active_trades)  # ← ADD THIS
            socketio.emit('active_trade_update', active_trades)
            return jsonify({'active_trades': active_trades})
        except psycopg2.Error as e:
            logger.error(f"Error fetching active trades data: {e}")
            if cursor:
                cursor.close()
            if conn:
                db_pool.putconn(conn)
            return jsonify({'status': 'error', 'error': str(e)}), 500
        
        

@app.route('/get_closed_trades', methods=['GET'])
def get_closed_trades():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT tradeid, strategy, ticker, shares, entry_price, entry_time, 
                       target, original_stop_loss, stop_loss, sl_time, exit_price, exit_time, 
                       date, reason, realized, r_gain_loss as RR, risk
                FROM closedtrades
                WHERE date::date = %s
            ''', (today,))
            closed_trades = []
            for row in cursor.fetchall():
                closed_trade = {
                    'tradeID': row[0],
                    'strategy': row[1],
                    'ticker': row[2],
                    'shares': row[3],
                    'entry_price': float(row[4]) if row[4] and row[4] != '' else 0.0,
                    'entry_time': row[5] or '',
                    'target': str(row[6]) if row[6] is not None else '',
                    'risk': float(row[16]) if row[16] else 0.0,   # NEW 
                    'SLoss': float(row[7]) if row[7] and row[7] != '' else None,
                    'stop_loss': float(row[8]) if row[8] and row[8] != '' else None,
                    'sl_time': row[9] or '',
                    'exit_price': float(row[10]) if row[10] and row[10] != '' else None,
                    'exit_time': row[11] or '',
                    'Exit': float(row[10]) if row[10] and row[10] != '' else None,
                    'Exit Time': row[11] or '',
                    'date': row[12],
                    'reason': row[13] or '',
                    'realized': float(row[14]) if row[14] and row[14] != '' else 0.0,
                    'RR': float(row[15]) if row[15] and row[15] != '' else 0.0
                }
                closed_trades.append(closed_trade)
            db_pool.putconn(conn)
            
            socketio.emit('update_closed_trades', {'closed_trades': closed_trades})
            return jsonify({'closed_trades': closed_trades})
        except psycopg2.Error as e:
            logger.error(f"Error fetching closed trades data: {e}")
            if cursor:
                cursor.close()
            if conn:
                db_pool.putconn(conn)
            return jsonify({'status': 'error', 'error': str(e)}), 500
        except ValueError as e:
            logger.error(f"Value error in type conversion for closed trades: {e}")
            if cursor:
                cursor.close()
            if conn:
                db_pool.putconn(conn)
            return jsonify({'status': 'error', 'error': 'Invalid numeric data in database'}), 500

@app.route('/get_trade_summary', methods=['GET'])
def get_trade_summary():
    today = datetime.now().strftime('%Y-%m-%d')
    with db_lock:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()

            # ORDER: total_trades → gain → loss → total_risk → total_gain_loss
            summary = {
                'market':  {'total_trades': 0, 'gain': 0.0, 'loss': 0.0, 'total_risk': 0.0, 'total_gain_loss': 0.0},
                'limit':   {'total_trades': 0, 'gain': 0.0, 'loss': 0.0, 'total_risk': 0.0, 'total_gain_loss': 0.0},
                'target':  {'total_trades': 0, 'gain': 0.0, 'loss': 0.0, 'total_risk': 0.0, 'total_gain_loss': 0.0},
                'stop':  {'total_trades': 0, 'gain': 0.0, 'loss': 0.0, 'total_risk': 0.0, 'total_gain_loss': 0.0},
                'GrandTotal': {'total_trades': 0, 'gain': 0.0, 'loss': 0.0, 'total_risk': 0.0, 'total_gain_loss': 0.0}
            }

            cursor.execute('''
                SELECT 
                    lower(strategy),
                    COUNT(*) AS trades,
                    SUM(CASE WHEN realized >= 0 THEN realized ELSE 0 END) AS gain,
                    SUM(CASE WHEN realized < 0 THEN ABS(realized) ELSE 0 END) AS loss,
                    COALESCE(SUM(risk), 0) AS total_risk,
                    SUM(realized) AS total_gain_loss
                FROM closedtrades 
                WHERE date::date = %s 
                GROUP BY lower(strategy)
            ''', (today,))

            for row in cursor.fetchall():
                strat = row[0]
                if strat in summary:
                    summary[strat].update({
                        'total_trades': int(row[1]),
                        'gain': float(row[2] or 0),
                        'loss': float(row[3] or 0),
                        'total_risk': float(row[4]),
                        'total_gain_loss': float(row[5] or 0)
                    })

                # Grand Total
                summary['GrandTotal']['total_trades'] += int(row[1])
                summary['GrandTotal']['gain'] += float(row[2] or 0)
                summary['GrandTotal']['loss'] += float(row[3] or 0)
                summary['GrandTotal']['total_risk'] += float(row[4])
                summary['GrandTotal']['total_gain_loss'] += float(row[5] or 0)

            db_pool.putconn(conn)
            socketio.emit('update_trade_summary', summary)
            return jsonify(summary)

        except Exception as e:
            logger.error(f"Error in get_trade_summary: {e}")
            return jsonify({'status': 'error', 'error': str(e)}), 500

@app.route('/get_public_url', methods=['GET'])
def get_public_url():
    public_url = app.config.get('PUBLIC_URL', 'Not available')
    return jsonify({"public_url": public_url})

@app.route('/trade_report')
def trade_report_page():
    return render_template('trade_report.html')

@app.route('/get_trade_reports', methods=['GET'])
def get_trade_reports():
    tradeid = request.args.get('tradeid')
    ticker = request.args.get('ticker')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    with db_lock:
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()

            # Build WHERE clause dynamically
            where_clauses = []
            params = []

            # Date filter is optional; if not provided, fetch all historical data
            if start_date and end_date:
                where_clauses.append("time::date BETWEEN %s AND %s")
                params.extend([start_date, end_date])
            elif start_date:
                where_clauses.append("time::date >= %s")
                params.append(start_date)
            elif end_date:
                where_clauses.append("time::date <= %s")
                params.append(end_date)

            if tradeid:
                where_clauses.append("tradeid = %s")
                params.append(tradeid)
            if ticker:
                where_clauses.append("ticker ILIKE %s")  # Case-insensitive partial match
                params.append(f"%{ticker}%")

            # If no conditions are specified, fetch all trades
            where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
            query = f'''
                SELECT DISTINCT tradeid, time FROM tradesignal 
                WHERE {where_clause}
                ORDER BY time DESC
            '''
            cursor.execute(query, params)
            trade_ids = [row[0] for row in cursor.fetchall()]

            if not trade_ids:
                db_pool.putconn(conn)
                return jsonify({'status': 'error', 'error': 'No trades found'}), 404

            reports = []
            for tid in trade_ids:
                # Fetch signal
                cursor.execute('''
                    SELECT tradeid, time, strategy, ticker, price, shares, target, hi, status 
                    FROM tradesignal 
                    WHERE tradeid = %s
                ''', (tid,))
                signal_row = cursor.fetchone()
                if not signal_row:
                    continue

                signal = {
                    'tradeID': signal_row[0],
                    'time': signal_row[1],
                    'strategy': signal_row[2],
                    'ticker': signal_row[3],
                    'entry_price': float(signal_row[4]) if signal_row[4] else None,
                    'shares': int(signal_row[5]) if signal_row[5] else None,
                    'target': float(signal_row[6]) if signal_row[6] else None,
                    'hi': float(signal_row[7]) if signal_row[7] else None,
                    'status': signal_row[8]
                }

                # Active trade
                active_trade = None
                cursor.execute('''
                    SELECT tradeid, time, strategy, ticker, shares, entry_price, target, stop_loss, 
                           lu_price, unrealized, date, sellorderid, stoporderid, last_price 
                    FROM activetrades 
                    WHERE tradeid = %s
                ''', (tid,))
                active_row = cursor.fetchone()
                if active_row:
                    active_trade = {
                        'tradeID': active_row[0],
                        'time': active_row[1],
                        'strategy': active_row[2],
                        'ticker': active_row[3],
                        'shares': int(active_row[4]) if active_row[4] else None,
                        'entry_price': float(active_row[5]) if active_row[5] else None,
                        'target': float(active_row[6]) if active_row[6] else None,
                        'stop_loss': float(active_row[7]) if active_row[7] else None,
                        'lu_price': float(active_row[8]) if active_row[8] else None,
                        'unrealized': float(active_row[9]) if active_row[9] else None,
                        'date': active_row[10],
                        'sellOrderID': active_row[11],
                        'stopOrderID': active_row[12],
                        'last_price': float(active_row[13]) if active_row[13] else None
                    }

                # Closed trade
                closed_trade = None
                cursor.execute('''
                    SELECT tradeid, strategy, ticker, shares, entry_price, entry_time, 
                           target, original_stop_loss, stop_loss, sl_time, exit_price, exit_time, 
                           date, reason, realized, r_gain_loss
                    FROM closedtrades
                    WHERE tradeid = %s
                ''', (tid,))
                closed_row = cursor.fetchone()
                if closed_row:
                    closed_trade = {
                        'tradeID': closed_row[0],
                        'strategy': closed_row[1],
                        'ticker': closed_row[2],
                        'shares': int(closed_row[3]) if closed_row[3] else None,
                        'entry_price': float(closed_row[4]) if closed_row[4] else 0.0,
                        'entry_time': closed_row[5] or '',
                        'target': float(closed_row[6]) if closed_row[6] else None,
                        'original_stop_loss': float(closed_row[7]) if closed_row[7] else None,
                        'stop_loss': float(closed_row[8]) if closed_row[8] else None,
                        'sl_time': closed_row[9] or '',
                        'exit_price': float(closed_row[10]) if closed_row[10] else None,
                        'exit_time': closed_row[11] or '',
                        'date': closed_row[12],
                        'reason': closed_row[13] or '',
                        'realized': float(closed_row[14]) if closed_row[14] else 0.0,
                        'rr': float(closed_row[15]) if closed_row[15] else 0.0
                    }

                # Executions
                exec_where = "tradeid = %s"
                exec_params = [tid]
                if start_date and end_date:
                    exec_where += " AND date BETWEEN %s AND %s"
                    exec_params.extend([start_date, end_date])
                elif start_date:
                    exec_where += " AND date >= %s"
                    exec_params.append(start_date)
                elif end_date:
                    exec_where += " AND date <= %s"
                    exec_params.append(end_date)
                executions = []
                for table_query in [
                    ('buymarket', ['tradeid', 'strategy', 'time', 'ticker', 'shares', 'price', 'action', 'status', 'act_status', 'notes']),
                    ('sellmarket', ['tradeid', 'strategy', 'time', 'ticker', 'shares', 'price', 'action', 'status', 'act_status', 'notes']),
                    ('stopmarket', ['tradeid', 'strategy', 'time', 'ticker', 'shares', 'price', 'action', 'status', 'act_status', 'notes']),
                    ('executedstop', ['tradeid', 'strategy', 'executed_time', 'ticker', 'shares', 'stop_loss']),
                    ('replacestop', ['tradeid', 'strategy', 'time', 'ticker', 'shares', 'price', 'action', 'status', 'act_status', 'notes'])
                ]:
                    table_name, columns = table_query
                    date_col = 'executed_time' if table_name == 'executedstop' else 'date'
                    where_str = exec_where.replace('date', date_col)
                    cursor.execute(f'''
                        SELECT {', '.join(columns)} 
                        FROM {table_name} 
                        WHERE {where_str}
                    ''', exec_params)
                    for row in cursor.fetchall():
                        converted_row = list(row)
                        if table_name in ['buymarket', 'sellmarket', 'stopmarket', 'replacestop']:
                            converted_row[5] = float(converted_row[5]) if converted_row[5] else None  # price
                            converted_row[4] = int(converted_row[4]) if converted_row[4] else None  # shares
                        elif table_name == 'executedstop':
                            converted_row[4] = int(converted_row[4]) if converted_row[4] else None  # shares
                            converted_row[5] = float(converted_row[5]) if converted_row[5] else None  # stop_loss
                        exec_record = dict(zip(['table'] + columns, [table_name] + converted_row))
                        executions.append(exec_record)

                reports.append({
                    'tradeID': tid,
                    'signal': signal,
                    'active_trade': active_trade,
                    'closed_trade': closed_trade,
                    'executions': executions
                })

            db_pool.putconn(conn)

            socketio.emit('trade_reports_update', {'reports': reports})

            return jsonify({'status': 'success', 'reports': reports})

        except psycopg2.Error as e:
            logger.error(f"Error fetching trade reports: {e}")
            if 'conn' in locals() and conn:
                db_pool.putconn(conn)
            return jsonify({'status': 'error', 'error': str(e)}), 500
        except ValueError as e:
            logger.error(f"Value error in trade reports: {e}")
            if 'conn' in locals() and conn:
                db_pool.putconn(conn)
            return jsonify({'status': 'error', 'error': 'Invalid numeric data'}), 500
        except Exception as e:
            logger.error(f"Unexpected error in trade reports: {e}")
            if 'conn' in locals() and conn:
                db_pool.putconn(conn)
            return jsonify({'status': 'error', 'error': str(e)}), 500
        
@app.route('/get_ohlc_data', methods=['GET'])
def get_ohlc_data():
    tickers = request.args.getlist('ticker')  # Accept multiple tickers
    date = request.args.get('date')  # Expected format: YYYY-MM-DD
    if not tickers or not date:
        return jsonify({'status': 'error', 'error': 'Tickers and date are required'}), 400

    try:
        with db_lock:
            conn = db_pool.getconn()
            cursor = conn.cursor()
            # Query for multiple tickers
            cursor.execute('''
                SELECT ticker, timestamp, open, high, low, close, volume
                FROM ohlc_5min
                WHERE ticker = ANY(%s) AND timestamp LIKE %s
                ORDER BY ticker, timestamp
            ''', (tickers, f"{date.replace('-', '/')}%"))
            ohlc_data = {}
            for row in cursor.fetchall():
                ticker = row[0]
                if ticker not in ohlc_data:
                    ohlc_data[ticker] = []
                ohlc_data[ticker].append({
                    'time': row[1],  # e.g., "2024/10/25-11:15"
                    'open': float(row[2]) if row[2] is not None else None,
                    'high': float(row[3]) if row[3] is not None else None,
                    'low': float(row[4]) if row[4] is not None else None,
                    'close': float(row[5]) if row[5] is not None else None,
                    'volume': int(row[6]) if row[6] is not None else None
                })
            db_pool.putconn(conn)
            return jsonify({'status': 'success', 'ohlc': ohlc_data})
    except psycopg2.Error as e:
        logger.error(f"Error fetching OHLC data for {tickers} on {date}: {e}")
        if cursor:
            cursor.close()
        if conn:
            db_pool.putconn(conn)
        return jsonify({'status': 'error', 'error': str(e)}), 500            

def start_strategy_logic():
    def run_strategy_logic():
        try:
            sl_logger.info("Starting StrategyLogic immediately as the current time is past 9:30 AM EST.")
            if strategy_logic is None:
                sl_logger.error("StrategyLogic is not initialized in run_strategy_logic")
                logger.error("StrategyLogic is not initialized in run_strategy_logic")
                return
            strategy_logic.fetch_tickers_from_db()
        except Exception as e:
            sl_logger.error(f"Error in run_strategy_logic: {e}")
            logger.error(f"Error in run_strategy_logic: {e}")
    try:
        if strategy_logic is None:
            sl_logger.error("StrategyLogic is not initialized")
            logger.error("StrategyLogic is not initialized in start_strategy_logic")
            return
        sl_logger.debug("Initializing StrategyLogic thread.")
        thread = threading.Thread(target=run_strategy_logic, daemon=True)
        thread.start()
        sl_logger.debug("StrategyLogic thread started.")
    except Exception as e:
        sl_logger.error(f"Error in start_strategy_logic: {e}")
        logger.error(f"Error in start_strategy_logic: {e}")

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
                else:
                    logger.error("Local server failed to start within 30 seconds")
            else:
                logger.error("API login failed, cannot start server")
        except Exception as e:
            logger.error(f"Error in start_api_connection: {e}")
        finally:
            if api_connection and api_connection.is_connected():
                server_ready_event.set()
                logger.info("server_ready_event set in finally block")
    logger.info("Starting API connection thread")
    threading.Thread(target=run_api_connection, daemon=True).start()

def start_trade_monitor():
    logger.info("Starting TradeMonitor monitoring")
    threading.Thread(target=trade_monitor.listen_to_server, daemon=True).start()
    logger.info("TradeMonitor threads started")

def start_vwap_fetch():
    logger.info("Starting VwapFetch")
    try:
        vwap_fetch.login()
        if vwap_fetch.connected:
            logger.info("VwapFetch connected to DAS API")
            threading.Thread(target=vwap_fetch.keep_alive, daemon=True).start()
            vwap_fetch.run()
            threading.Thread(target=vwap_fetch.update_account_equity, daemon=True).start()
        else:
            logger.error("VwapFetch failed to connect to DAS API")
    except Exception as e:
        logger.error(f"Error starting VwapFetch: {e}")

def start_minichart():
    logger.info("Starting Minichart")
    try:
        threading.Thread(target=minichart.monitor_new_tickers, daemon=True).start()
        logger.info("Minichart thread started")
    except Exception as e:
        logger.error(f"Error starting Minichart: {e}")
        
    

def start_modules():
    logger.info("Starting all modules")
    try:
        end_of_day.connect_to_server()
        threading.Thread(target=end_of_day.listen_to_server, daemon=True).start()
        threading.Thread(target=end_of_day.run_combined_tasks, daemon=True).start()
        logger.info("EndOfDay started")
    except Exception as e:
        logger.error(f"Error starting EndOfDay: {e}")
    try:
        sl_monitor.connect_to_server()
        threading.Thread(target=sl_monitor.listen_to_server, daemon=True).start()
        logger.info("SLMonitor started")
    except Exception as e:
        logger.error(f"Error starting SLMonitor: {e}")
    try:
        stopsell_monitor.connect_to_server()
        threading.Thread(target=stopsell_monitor.listen_to_server, daemon=True).start()
        logger.info("SSMonitor started")
    except Exception as e:
        logger.error(f"Error starting SSMonitor: {e}")    
    try:
        logger.debug("Calling start_trade_monitor")
        start_trade_monitor()
    except Exception as e:
        logger.error(f"Error in start_trade_monitor: {e}")
    try:
        logger.debug("Calling start_strategy_logic")
        start_strategy_logic()
    except Exception as e:
        logger.error(f"Error in start_strategy_logic: {e}")
    try:
        logger.debug("Calling start_vwap_fetch")
        start_vwap_fetch()
    except Exception as e:
        logger.error(f"Error in start_vwap_fetch: {e}")
    try:
        logger.debug("Calling start_minichart")
        start_minichart()
    except Exception as e:
        logger.error(f"Error in start_minichart: {e}")
        
    # Start the combined PMH + HOD periodic updater
    threading.Thread(target=update_hod_periodically, daemon=True).start()

    logger.info("Started periodic PMH/HOD updater")    
    
       
    logger.info("Started all modules")

def is_connected():
    try:
        response = os.system("ping -c 1 8.8.8.8")
        return response == 0
    except:
        return False

def restart_processes():
    start_strategy_logic()
    start_vwap_fetch()
    start_minichart()  # Add Minichart to restart
    
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
def run_socketio_forever():
    port = int(os.environ.get('PORT', 5001))
    logger.info(f"SocketIO server listening on 0.0.0.0:{port}")
    # use_reloader=False → prevents double-start in dev
    socketio.run(app, host='0.0.0.0', port=port,
                 debug=False, use_reloader=False, log_output=False)


def main():
    # ----- env / DB ------------------------------------------------
    public_url = os.environ.get('PUBLIC_URL', 'Not set')
    if public_url == 'Not set':
        logger.warning("PUBLIC_URL not set – run Pinggy to get one.")
    else:
        app.config['PUBLIC_URL'] = public_url
        logger.info(f"Public URL: {public_url}")

    initialize_trade_id_counter()
    initialize_api_connection()

    # ----- start the API connection in its own thread -------------
    start_api_connection()

    # ----- give the API a chance to become ready ------------------
    ready = server_ready_event.wait(timeout=30)
    if ready:
        logger.info("API ready initialise modules")
        initialize_modules()
        start_modules()
    else:
        logger.warning("API not ready yet – modules will start later when it connects")

def safe_start():
    while True:
        try:
            logger.info("Starting the application")
            main()                                   # <-- your original logic
            run_socketio_forever() 
            
        except Exception as e:
            logger.error(
                f"CRITICAL: Application crashed – restarting in 5 s\n"
                f"Error: {e}\n{traceback.format_exc()}"
            )
            time.sleep(5)                            # wait before retry

if __name__ == "__main__":
    safe_start()