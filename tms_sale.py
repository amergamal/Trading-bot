import socket
import logging
import psycopg2
from psycopg2 import pool
import config  # Import config.py for DB_CONFIG
import time
import pandas as pd
from datetime import datetime, timedelta
from threading import Thread

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('tms_sale')

DAS_API_BASE_URL = '127.0.0.1'
DAS_API_PORT = 9800
DAS_API_USERNAME = 'CD4832'
DAS_API_PASSWORD = 'Gamal_123'
DAS_API_ACCOUNT = 'TRCD4832'

# Initialize PostgreSQL connection pool
try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=20,
        **config.DB_CONFIG
    )
    logging.info("PostgreSQL connection pool initialized")
except psycopg2.OperationalError as e:
    logging.error(f"Failed to initialize PostgreSQL connection pool: {e}")
    raise

def create_socket():
    logging.debug('Creating socket...')
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    logging.debug(f'Socket created. Attempting to connect to {DAS_API_BASE_URL}:{DAS_API_PORT}...')
    s.connect((DAS_API_BASE_URL, DAS_API_PORT))
    logging.debug('Connection established')
    return s

def send_command(sock, command):
    try:
        full_command = f'{command}\r\n'
        logging.debug(f'Sending command to DAS: {full_command}')
        sock.sendall(full_command.encode())
    except (OSError, BrokenPipeError) as e:
        logging.error(f'Error sending command: {e}')
        sock.close()
        return None

def receive_response(sock, buffer_size=4096):
    try:
        response = sock.recv(buffer_size).decode()
        
        return response
    except (OSError, BrokenPipeError) as e:
        logging.error(f'Error receiving response: {e}')
        sock.close()
        return None

def login(sock):
    login_command = f'LOGIN {DAS_API_USERNAME} {DAS_API_PASSWORD} {DAS_API_ACCOUNT} 0'
    send_command(sock, login_command)
    while True:
        login_response = receive_response(sock)
        if login_response:
            if 'LOGIN SUCCESSED' in login_response:
                logging.info('Login successful')
                return True
            elif '#Please login to continue.' in login_response:
                logging.warning('Received prompt to login again, retrying...')
                send_command(sock, login_command)
            else:
                logging.error(f'Unexpected login response: {login_response}')
                return False

def request_time_and_sales(sock, symbol):
    tms_command = f'SB {symbol} tms'
    send_command(sock, tms_command)
    

    
    
def get_tickers_from_db(date):
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT ticker FROM tradeparameters WHERE date = %s", (date,))
        tickers = [row[0] for row in cursor.fetchall()]
        return tickers
    except psycopg2.Error as e:
        logging.error(f"Error fetching tickers: {e}")
        return []
    finally:
        if conn:
            db_pool.putconn(conn)
    
    
def insert_minute_data(symbol, ohlc, volume, minute):
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO ohlc_1min (ticker, open, high, low, close, volume, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (symbol, ohlc['open'], ohlc['high'], ohlc['low'], ohlc['close'], volume, minute))
        conn.commit()
        logging.info(f"Inserted 1-min data for {symbol} at {minute}: Open={ohlc['open']}, High={ohlc['high']}, Low={ohlc['low']}, Close={ohlc['close']}, Volume={volume}")
    except psycopg2.Error as e:
        logging.error(f"Error inserting data into ohlc_1min: {e}")
    finally:
        if conn:
            db_pool.putconn(conn)     
            
def insert_five_minute_data(symbol, ohlc, volume, timestamp):
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO ohlc_5min (ticker, open, high, low, close, volume, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (symbol, ohlc['open'], ohlc['high'], ohlc['low'], ohlc['close'], volume, timestamp))
        conn.commit()
        logging.info(f"Inserted 5-min data for {symbol} at {timestamp}: Open={ohlc['open']}, High={ohlc['high']}, Low={ohlc['low']}, Close={ohlc['close']}, Volume={volume}")
    except psycopg2.Error as e:
        logging.error(f"Error inserting data into ohlc_5min: {e}")
    finally:
        if conn:
            db_pool.putconn(conn)          

def parse_and_aggregate(response, symbol, current_minute_data, current_five_minute_data):
    """
    Parse T&S response and aggregate into 1-minute and 5-minute OHLC bars.
    """
    lines = response.strip().split('\n')
    today_date = datetime.now().strftime('%Y-%m-%d')  # Get today's date

    for line in lines:
        if line.startswith('$T&S'):
            parts = line.split()
            try:
                price = float(parts[2])
                quantity = int(parts[3])
                timestamp_str = parts[5]
                condition = int(parts[8])

                # Convert timestamp to datetime
                timestamp = datetime.strptime(timestamp_str, "%H:%M:%S")
                full_timestamp = datetime.strptime(f"{today_date} {timestamp_str}", "%Y-%m-%d %H:%M:%S")
                
                # 1-minute aggregation
                minute = full_timestamp.replace(second=0)  # Strip seconds for minute aggregation
                
                # 5-minute aggregation
                # Round down to the nearest 5-minute boundary and label with end time (e.g., 10:03 -> 10:00, bar at 10:05)
                minute_val = full_timestamp.minute
                five_minute_start = full_timestamp.replace(minute=(minute_val // 5) * 5, second=0)
                five_minute_end = five_minute_start + timedelta(minutes=5)
                
                # Validate trade based on condition (bit 5 must be 1 for valid last price)
                if condition & 0x20 == 0:
                    continue

                # 1-minute OHLC and volume update
                if current_minute_data['minute'] is None:
                    current_minute_data['minute'] = minute
                    current_minute_data['ohlc']['open'] = price
                    current_minute_data['ohlc']['high'] = price
                    current_minute_data['ohlc']['low'] = price
                    current_minute_data['ohlc']['close'] = price
                    current_minute_data['volume'] = quantity
                elif minute != current_minute_data['minute']:
                    # Insert completed 1-minute bar
                    formatted_minute = current_minute_data['minute'].strftime('%Y/%m/%d-%H:%M')
                    insert_minute_data(symbol, current_minute_data['ohlc'], current_minute_data['volume'], formatted_minute)
                    # Reset for new minute
                    current_minute_data['minute'] = minute
                    current_minute_data['ohlc']['open'] = price
                    current_minute_data['ohlc']['high'] = price
                    current_minute_data['ohlc']['low'] = price
                    current_minute_data['ohlc']['close'] = price
                    current_minute_data['volume'] = quantity
                else:
                    # Update current minute's OHLC and volume
                    current_minute_data['ohlc']['high'] = max(current_minute_data['ohlc']['high'], price)
                    current_minute_data['ohlc']['low'] = min(current_minute_data['ohlc']['low'], price)
                    current_minute_data['ohlc']['close'] = price
                    current_minute_data['volume'] += quantity

                # 5-minute OHLC and volume update
                if current_five_minute_data['start_time'] is None:
                    current_five_minute_data['start_time'] = five_minute_start
                    current_five_minute_data['ohlc']['open'] = price
                    current_five_minute_data['ohlc']['high'] = price
                    current_five_minute_data['ohlc']['low'] = price
                    current_five_minute_data['ohlc']['close'] = price
                    current_five_minute_data['volume'] = quantity
                elif five_minute_end != current_five_minute_data['start_time'] + timedelta(minutes=5):
                    # Insert completed 5-minute bar immediately
                    formatted_five_minute = current_five_minute_data['start_time'].strftime('%Y/%m/%d-%H:%M')
                    ohlc = current_five_minute_data['ohlc'].copy()
                    volume = current_five_minute_data['volume']
                    insert_five_minute_data(symbol, ohlc, volume, formatted_five_minute)
                    # Reset for new 5-minute interval
                    current_five_minute_data['start_time'] = five_minute_start
                    current_five_minute_data['ohlc']['open'] = price
                    current_five_minute_data['ohlc']['high'] = price
                    current_five_minute_data['ohlc']['low'] = price
                    current_five_minute_data['ohlc']['close'] = price
                    current_five_minute_data['volume'] = quantity
                else:
                    # Update current 5-minute OHLC and volume
                    current_five_minute_data['ohlc']['high'] = max(current_five_minute_data['ohlc']['high'], price)
                    current_five_minute_data['ohlc']['low'] = min(current_five_minute_data['ohlc']['low'], price)
                    current_five_minute_data['ohlc']['close'] = price
                    current_five_minute_data['volume'] += quantity

            except (IndexError, ValueError) as e:
                logging.error(f'Error parsing line: {e}')
            except Exception as e:
                logging.error(f'Unexpected error: {e}')

def process_ticker(symbol):
    """
    Process a ticker symbol, continuously receiving Time & Sales (T&S) data indefinitely.
    """
    try:
        # Create a socket connection to the server
        sock = create_socket()
    
        # Receive the welcome message
        welcome_message = receive_response(sock)
        logging.debug(f'Received welcome message: {welcome_message}')
        
        time.sleep(1)  # Ensure server readiness
    
        # Login to the server
        if not login(sock):
            logging.error('Login failed.')
            sock.close()
            return
        
        # Request Time and Sales data for the ticker symbol
        request_time_and_sales(sock, symbol)
    
        # Track current minute data
        current_minute_data = {
            'minute': None,
            'ohlc': {'open': None, 'high': float('-inf'), 'low': float('inf'), 'close': None},
            'volume': 0
        }
        
        current_five_minute_data = {
            'start_time': None,
            'ohlc': {'open': None, 'high': float('-inf'), 'low': float('inf'), 'close': None},
            'volume': 0
        }
    
        # Continuously monitor and append T&S data
        while True:
            response = receive_response(sock)
            if response:
               
                parse_and_aggregate(response, symbol, current_minute_data, current_five_minute_data)
            else:
                logging.debug('No response, waiting...')
                time.sleep(1)  # Adjust sleep time as needed to avoid busy-waiting.
    
    except Exception as e:
        logging.error(f'Error during DAS API test for {symbol}: {e}')
    finally:
        # Ensure the socket is closed to avoid resource leakage
        if sock:
            sock.close()
            logging.debug('Socket closed.')

def main():
    logger.debug("Starting tms_sale.main()")
    today_date = datetime.now().strftime('%Y-%m-%d')  # Use today's date to fetch tickers from the database
    processed_symbols = set()  # Track already processed symbols

    while True:
        
        # Fetch tickers from the database
        symbols = get_tickers_from_db(today_date)
        
        if symbols:
            logger.debug(f"Fetched tickers: {symbols}")
            
            for symbol in symbols:
                if symbol not in processed_symbols:
                    # Start a new thread for unprocessed symbols
                    logging.info(f'Starting process for new ticker: {symbol}')
                    thread = Thread(target=process_ticker, args=(symbol,))
                    thread.start()
                    
                    processed_symbols.add(symbol)

            
        else:
            logging.info('No tickers found for the provided date.')

        # Sleep for a minute before checking for new tickers again
        time.sleep(60)

if __name__ == "__main__":
    main()
