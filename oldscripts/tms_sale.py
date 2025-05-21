import socket
import logging
import sqlite3
import time
from datetime import datetime
from threading import Thread

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DAS_API_BASE_URL = '127.0.0.1'
DAS_API_PORT = 9800
DAS_API_USERNAME = 'CD4832'
DAS_API_PASSWORD = 'Gamala123'
DAS_API_ACCOUNT = 'TRCD4832'

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

def request_time_and_sales(sock, symbol):
    tms_command = f'SB {symbol} tms'
    send_command(sock, tms_command)

def get_db_connection():
    try:
        conn = sqlite3.connect('EOD_data.db')
        return conn
    except sqlite3.Error as e:
        logger.error(f'Error connecting to the database: {e}')
        return None

def insert_minute_data(symbol, ohlc, volume, minute):
    """
    Insert the OHLC and volume data into the ohlc_1min table.
    """
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO ohlc_1min (ticker, open, high, low, close, volume, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (symbol, ohlc['open'], ohlc['high'], ohlc['low'], ohlc['close'], volume, minute))
            conn.commit()
            logger.info(f"Inserted data for {symbol} at {minute}: Open={ohlc['open']}, High={ohlc['high']}, Low={ohlc['low']}, Close={ohlc['close']}, Volume={volume}")
        except sqlite3.Error as e:
            logger.error(f'Error inserting data into ohlc_1min: {e}')
        finally:
            conn.close()

def parse_and_aggregate(response, symbol, current_minute_data):
    if not response:
        logger.debug('Empty response received, skipping parsing')
        return

    lines = response.strip().split('\n')
    today_date = datetime.now().strftime('%Y-%m-%d')

    for line in lines:
        if not line.startswith('$T&S'):
            logger.debug(f'Skipping non-T&S line: {line}')
            continue

        parts = line.split()
        if len(parts) < 9:
            logger.warning(f'Invalid $T&S line for {symbol}, too few parts: {line}')
            continue

        try:
            price = float(parts[2])
            quantity = int(parts[3])
            timestamp_str = parts[5]
            condition = int(parts[8])

            # Convert timestamp to a datetime object for minute-based aggregation
            try:
                timestamp = datetime.strptime(timestamp_str, "%H:%M:%S")
                full_timestamp = datetime.strptime(f"{today_date} {timestamp_str}", "%Y-%m-%d %H:%M:%S")
                minute = full_timestamp.replace(second=0)  # Strip seconds for minute aggregation
            except ValueError as e:
                logger.error(f'Invalid timestamp format in line: {line}, error: {e}')
                continue

            # Validate trade based on condition
            if condition & 0x20 == 0:
                logger.debug(f'Skipping trade for {symbol} with invalid condition: {condition}')
                continue

            # Check if we need to start a new minute
            if current_minute_data['minute'] is None:
                current_minute_data['minute'] = minute
                current_minute_data['ohlc']['open'] = price
                current_minute_data['ohlc']['high'] = price
                current_minute_data['ohlc']['low'] = price
                current_minute_data['ohlc']['close'] = price
                current_minute_data['volume'] = quantity
            elif minute != current_minute_data['minute']:
                # Insert the completed minute
                formatted_minute = current_minute_data['minute'].strftime('%Y/%m/%d-%H:%M')
                insert_minute_data(symbol, current_minute_data['ohlc'], current_minute_data['volume'], formatted_minute)

                # Start a new minute
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

        except (IndexError, ValueError) as e:
            logger.error(f'Error parsing line: {line}, error: {e}')
        except Exception as e:
            logger.error(f'Unexpected error parsing line: {line}, error: {e}')

def process_ticker(symbol):
    """
    Process a ticker symbol, continuously receiving Time & Sales (T&S) data indefinitely.
    """
    sock = None
    try:
        sock = create_socket()
        welcome_message = receive_response(sock)
        logger.debug(f'Received welcome message: {welcome_message}')
        
        time.sleep(1)  # Ensure server readiness
    
        if not login(sock):
            logger.error('Login failed.')
            return
        
        request_time_and_sales(sock, symbol)
    
        current_minute_data = {
            'minute': None,
            'ohlc': {'open': None, 'high': float('-inf'), 'low': float('inf'), 'close': None},
            'volume': 0
        }
    
        while True:
            response = receive_response(sock)
            if response:
                parse_and_aggregate(response, symbol, current_minute_data)
            else:
                logger.debug('No response, waiting...')
                time.sleep(1)
    
    except Exception as e:
        logger.error(f'Error during DAS API test for {symbol}: {e}')
    finally:
        if sock:
            sock.close()
            logger.debug('Socket closed.')

def main(strategy_logic=None):
    logger.info("tms_sale main function is running.")
    if strategy_logic is None:
        logger.error("No strategy_logic provided, cannot fetch tickers")
        return
    processed_symbols = set()

    while True:
        ticker_data = strategy_logic.fetch_tickers_from_db()
        
        if ticker_data is None:
            logger.error("Received None from fetch_tickers_from_db, skipping iteration")
            time.sleep(60)
            continue
        symbols = [ticker for ticker, rsi_1m, rsi_5m in ticker_data]
        
        if symbols:
            threads = []
            for symbol in symbols:
                if symbol not in processed_symbols:
                    logger.info(f'Starting process for new ticker: {symbol}')
                    thread = Thread(target=process_ticker, args=(symbol,), daemon=True)
                    thread.start()
                    threads.append(thread)
                    processed_symbols.add(symbol)
        else:
            logger.info('No tickers found for the provided date.')

        time.sleep(60)

if __name__ == "__main__":
    main()