import socket
import logging
import time
import sqlite3
from datetime import datetime, timedelta
from threading import Lock, Thread

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

DAS_API_BASE_URL = '127.0.0.1'
DAS_API_PORT = 9800
DAS_API_USERNAME = 'CD4832'
DAS_API_PASSWORD = 'Gamala123'
DAS_API_ACCOUNT = 'TRCD4832'

# Store the last processed timestamp globally
db_lock = Lock()
processed_tickers = set()

def create_socket():
    logger.debug('Creating socket...')
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    logger.debug(f'Socket created. Attempting to connect to {DAS_API_BASE_URL}:{DAS_API_PORT}...')
    s.connect((DAS_API_BASE_URL, DAS_API_PORT))
    logger.debug('Connection established')
    return s

def send_command(sock, command):
    try:
        full_command = f'{command}\r\n'
        logger.debug(f'Sending command to DAS: {full_command}')
        sock.sendall(full_command.encode())
    except (OSError, BrokenPipeError) as e:
        logger.error(f'Error sending command: {e}')
        sock.close()
        return None

def receive_response(sock, buffer_size=4096):
    try:
        response = sock.recv(buffer_size).decode()
        logger.debug(f'Received response: {response}')
        return response
    except (OSError, BrokenPipeError) as e:
        logger.error(f'Error receiving response: {e}')
        sock.close()
        return None

def login(sock):
    login_command = f'LOGIN {DAS_API_USERNAME} {DAS_API_PASSWORD} {DAS_API_ACCOUNT} 0'
    send_command(sock, login_command)
    while True:
        login_response = receive_response(sock)
        if login_response:
            if 'LOGIN SUCCESSED' in login_response:
                logger.info('Login successful')
                return True
            elif '#Please login to continue.' in login_response:
                logger.warning('Received prompt to login again, retrying...')
                send_command(sock, login_command)
            else:
                logger.error(f'Unexpected login response: {login_response}')
                return False

def request_minute_chart(sock, symbol, start_time, end_time='LATEST', min_type=5):
    minchart_command = f'SB {symbol} MINCHART {start_time} {end_time} {min_type}'
    send_command(sock, minchart_command)

def ensure_database_schema():
    with db_lock:
        conn = sqlite3.connect('EOD_data.db')
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS TickerRange (
                ticker TEXT,
                date TEXT,
                pmh REAL,
                pml REAL,
                range TEXT,
                UNIQUE(ticker, date)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ohlc_1min (
                ticker TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                timestamp TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ohlc_5min (
                ticker TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                timestamp TEXT
            )
        """)
        conn.commit()
        conn.close()
        logger.info("Verified/created TickerRange and ohlc table schemas.")

def insert_into_db(table, data):
    with db_lock:
        conn = sqlite3.connect('EOD_data.db', timeout=10)
        c = conn.cursor()
        try:
            c.executemany(f'INSERT OR IGNORE INTO {table} (ticker, open, high, low, close, volume, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)', data)
            conn.commit()
            logger.debug(f"Inserted {len(data)} rows into {table}")
        except sqlite3.Error as e:
            logger.error(f"Database error in insert_into_db for {table}: {e}")
        finally:
            conn.close()

def calculate_pmh_pml_and_insert(ticker):
    with db_lock:
        conn = sqlite3.connect('EOD_data.db', timeout=10)
        cursor = conn.cursor()
        try:
            today_date = datetime.now().strftime('%Y/%m/%d')
            today_date_sql = datetime.now().strftime('%Y-%m-%d')

            cursor.execute("""
                SELECT 1 FROM TickerRange WHERE ticker = ? AND date = ?
            """, (ticker, today_date_sql))
            exists = cursor.fetchone()

            if not exists:
                cursor.execute("""
                    INSERT INTO TickerRange (ticker, date, pmh, pml, range)
                    VALUES (?, ?, ?, ?, ?)
                """, (ticker, today_date_sql, 1, 1, '1%'))
                logger.info(f"Inserted default TickerRange record for {ticker} on {today_date_sql}: pmh=1, pml=1, range='1%'")

            cursor.execute("""
                SELECT timestamp, high, low
                FROM ohlc_5min
                WHERE ticker = ? 
                AND substr(timestamp, 1, 10) = ?
                AND time(substr(timestamp, 12, 5)) < '09:30'
            """, (ticker, today_date))
            debug_data = cursor.fetchall()
            logger.debug(f"ohlc_5min data for {ticker} on {today_date}: {debug_data}")

            cursor.execute("""
                SELECT MAX(high), MIN(low)
                FROM ohlc_5min
                WHERE ticker = ? 
                AND substr(timestamp, 1, 10) = ?
                AND time(substr(timestamp, 12, 5)) < '09:30'
            """, (ticker, today_date))
            pmh, pml = cursor.fetchone()
            logger.debug(f"Fetched PMH: {pmh}, PML: {pml} for {ticker} on {today_date}")

            if pmh and pml and pmh != 0 and pml != 0:
                range_percentage = ((pmh - pml) / pml) * 100
                formatted_percentage = f"{int(range_percentage)}%"

                cursor.execute("""
                    UPDATE TickerRange
                    SET pmh = ?, pml = ?, range = ?
                    WHERE ticker = ? AND date = ?
                """, (pmh, pml, formatted_percentage, ticker, today_date_sql))
                logger.info(f"Updated PMH: {pmh}, PML: {pml}, Range: {formatted_percentage} for {ticker} on {today_date_sql}")
            else:
                logger.warning(f"No valid pre-market data available for {ticker}: PMH={pmh}, PML={pml}, retaining default values")
        except sqlite3.Error as e:
            logger.error(f"Database error in calculate_pmh_pml_and_insert for {ticker}: {e}")
        finally:
            conn.commit()
            conn.close()

def parse_and_store_data(response, symbol):
    logger.debug(f'Parsing response: {response}')
    lines = response.strip().split('\n')
    data_1min = []
    data_5min = []
    for line in lines:
        logger.debug(f'Processing line: {line}')
        if line.startswith('$Bar'):
            parts = line.split()
            logger.debug(f'Line parts: {parts}')
            
            if len(parts) < 8 or parts[-1] not in ['1', '5']:
                logger.warning(f"Ignoring invalid or non-1/5-minute data for {symbol}: {line}")
                continue
            
            try:
                date_time = parts[2]
                open_price = float(parts[5])
                high_price = float(parts[3])
                low_price = float(parts[4])
                close_price = float(parts[6])
                volume = int(parts[7]) if len(parts) > 7 else 0
                min_type = parts[-1]
                
                if min_type == '1':
                    data_1min.append((symbol, open_price, high_price, low_price, close_price, volume, date_time))
                elif min_type == '5':
                    data_5min.append((symbol, open_price, high_price, low_price, close_price, volume, date_time))
                
                logger.info(f'{symbol} | MinType: {min_type} | Timestamp: {date_time} | Open: {open_price} | High: {high_price} | Low: {low_price} | Close: {close_price} | Volume: {volume}')
                
            except (IndexError, ValueError) as e:
                logger.error(f'Error parsing line: {line}, error: {e}')
    
    if data_1min:
        insert_into_db('ohlc_1min', data_1min)
        logger.info(f'1-minute data for {symbol} successfully inserted into ohlc_1min table.')
    if data_5min:
        insert_into_db('ohlc_5min', data_5min)
        logger.info(f'5-minute data for {symbol} successfully inserted into ohlc_5min table.')
        calculate_pmh_pml_and_insert(symbol)

def get_next_1min_run_time():
    """Calculate the next 1-minute interval time, 2 seconds after the minute mark."""
    now = datetime.now()
    next_run_time = (now + timedelta(minutes=1)).replace(second=2, microsecond=0)
    return next_run_time

def get_next_5min_run_time():
    """Calculate the next 5-minute interval time, 2 seconds after the 5-minute mark."""
    now = datetime.now()
    next_minute = (now.minute // 5 + 1) * 5
    if next_minute == 60:
        next_run_time = (now + timedelta(hours=1)).replace(minute=0, second=2, microsecond=0)
    else:
        next_run_time = now.replace(minute=next_minute, second=2, microsecond=0)
    return next_run_time

def process_ticker(symbol, start_time_str):
    try:
        start_time = datetime.strptime(start_time_str, '%Y/%m/%d-%H:%M')
        sock = create_socket()
        welcome_message = receive_response(sock)
        logger.debug(f'Received welcome message: {welcome_message}')
        
        if not login(sock):
            sock.close()
            return
        
        calculate_pmh_pml_and_insert(symbol)
        
        logger.info(f'First data request for {symbol} after initial 5-second startup.')
        initial_fetch_end_time = datetime.now() + timedelta(seconds=5)
        
        while datetime.now() < initial_fetch_end_time:
            request_minute_chart(sock, symbol, start_time_str, min_type=1)
            request_minute_chart(sock, symbol, start_time_str, min_type=5)
            
            response = receive_response(sock)
            if response:
                logger.debug(f'Received initial minute chart response for {symbol}: {response}')
                parse_and_store_data(response, symbol)
            else:
                logger.warning(f"No data received for {symbol} during the initial 5 seconds.")
            time.sleep(0.1)
        
        # Initialize next run times
        next_1min_run = get_next_1min_run_time()
        next_5min_run = get_next_5min_run_time()
        
        while True:
            if not ticker_exists_in_db(symbol):
                logger.info(f'{symbol} has been removed from the database. Stopping data collection.')
                break
            
            now = datetime.now()
            
            # Check if it's time to fetch 1-minute candles
            if now >= next_1min_run:
                request_minute_chart(sock, symbol, start_time_str, min_type=1)
                response = receive_response(sock)
                if response:
                    logger.debug(f'Received 1-minute chart response for {symbol}: {response}')
                    parse_and_store_data(response, symbol)
                else:
                    logger.debug(f'No 1-minute data received for {symbol}, retrying...')
                next_1min_run = get_next_1min_run_time()
            
            # Check if it's time to fetch 5-minute candles
            if now >= next_5min_run:
                request_minute_chart(sock, symbol, start_time_str, min_type=5)
                response = receive_response(sock)
                if response:
                    logger.debug(f'Received 5-minute chart response for {symbol}: {response}')
                    parse_and_store_data(response, symbol)
                else:
                    logger.debug(f'No 5-minute data received for {symbol}, retrying...')
                next_5min_run = get_next_5min_run_time()
            
            # Sleep for a short interval to check times again
            time.sleep(1)
    
        send_command(sock, f'UNSB {symbol} MINCHART 1')
        send_command(sock, f'UNSB {symbol} MINCHART 5')
        sock.close()
        logger.info(f'Finished collecting data for {symbol}.')
    
    except Exception as e:
        logger.error(f'Error processing ticker {symbol}: {e}')

def ticker_exists_in_db(symbol):
    with db_lock:
        conn = sqlite3.connect('EOD_data.db', timeout=10)
        c = conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        c.execute("SELECT 1 FROM TradeParameters WHERE Date=? AND TICKER=?", (today, symbol))
        result = c.fetchone()
        conn.close()
        return result is not None

def get_todays_tickers():
    with db_lock:
        conn = sqlite3.connect('EOD_data.db', timeout=10)
        c = conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        c.execute("SELECT TICKER, RSI_1MIN, RSI_5MIN FROM TradeParameters WHERE Date=?", (today,))
        tickers = [(row[0], row[1], row[2]) for row in c.fetchall()]
        conn.close()
        logger.debug(f"Fetched tickers: {tickers}")
        return tickers

def monitor_new_tickers(start_time_str, strategy_logic=None):
    global processed_tickers
    if strategy_logic is None:
        logger.error("No strategy_logic provided, cannot fetch tickers")
        return
    while True:
        ticker_data = strategy_logic.fetch_tickers_from_db()
        tickers = [ticker for ticker, rsi_1m, rsi_5m in ticker_data]
        if not tickers:
            logger.info("No tickers found in the database. Waiting for new entries...")
        for ticker in tickers:
            if ticker not in processed_tickers:
                logger.info(f"New ticker found: {ticker}. Starting data collection.")
                processed_tickers.add(ticker)
                ticker_thread = Thread(target=process_ticker, args=(ticker, start_time_str), daemon=True)
                ticker_thread.start()
        for ticker in list(processed_tickers):
            if ticker not in tickers:
                logger.info(f'{ticker} has been removed from the database. Stopping its data collection.')
                processed_tickers.remove(ticker)
        time.sleep(60)

def main(strategy_logic=None):
    ensure_database_schema()
    
    if strategy_logic is None:
        logger.error("No strategy_logic provided, cannot fetch tickers")
        return
    
    current_date = datetime.now()
    start_time_str = current_date.replace(hour=4, minute=0, second=0, microsecond=0).strftime('%Y/%m/%d-%H:%M')

    logger.info(f"Starting ticker monitoring and data collection from {start_time_str} for 1-minute and 5-minute charts.")
    
    try:
        monitor_thread = Thread(target=monitor_new_tickers, args=(start_time_str, strategy_logic), daemon=True)
        monitor_thread.start()
        monitor_thread.join()
    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    class MockStrategyLogic:
        def fetch_tickers_from_db(self):
            return get_todays_tickers()
    
    main(strategy_logic=MockStrategyLogic())