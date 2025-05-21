import socket
import logging
import time
import sqlite3
import os
import pytz
from datetime import datetime, timedelta
from threading import Lock, Thread

# Set logging level to DEBUG to ensure all log messages are visible
logging.basicConfig(level=logging.DEBUG)

DAS_API_BASE_URL = '127.0.0.1'
DAS_API_PORT = 9800
DAS_API_USERNAME = 'CD4832'
DAS_API_PASSWORD = 'Gamala123'
DAS_API_ACCOUNT = '104832'

# Store the last processed timestamp globally
db_lock = Lock()  # Lock to manage database access
processed_tickers = set()  # Keep track of already processed tickers

def create_socket():
    logging.debug('Creating socket...')
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((DAS_API_BASE_URL, DAS_API_PORT))
    logging.debug('Connection established')
    return s

def send_command(sock, command):
    try:
        full_command = f'{command}\r\n'
        sock.sendall(full_command.encode())
        logging.debug(f'Sending command to DAS: {full_command}')
    except (OSError, BrokenPipeError) as e:
        logging.error(f'Error sending command: {e}')
        sock.close()

def receive_response(sock, buffer_size=4096):
    try:
        response = sock.recv(buffer_size).decode()
        logging.debug(f'Received response: {response}')
        return response
    except (OSError, BrokenPipeError) as e:
        logging.error(f'Error receiving response: {e}')
        sock.close()

def login(sock):
    login_command = f'LOGIN {DAS_API_USERNAME} {DAS_API_PASSWORD} {DAS_API_ACCOUNT} 0'
    send_command(sock, login_command)
    while True:
        login_response = receive_response(sock)
        if login_response:
            if 'LOGIN SUCCESSED' in login_response:
                logging.info('Login successful')
                return True
            elif '#Welcome to DAS Command API' in login_response:
                logging.info('Welcome message received, waiting for login success...')
            elif '#Please login to continue.' in login_response:
                logging.warning('Received prompt to login again, retrying...')
                send_command(sock, login_command)
            else:
                logging.error(f'Unexpected login response: {login_response!r}')
                return False

def request_minute_chart(sock, symbol, start_time, end_time, min_type=1):
    minchart_command = f'SB {symbol} MINCHART {start_time} {end_time} {min_type}'
    send_command(sock, minchart_command)

def insert_into_db(table, data):
    with db_lock:
        conn = sqlite3.connect('EOD_data.db')
        c = conn.cursor()
        c.executemany(f'INSERT OR IGNORE INTO {table} (ticker, open, high, low, close, volume, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)', data)
        conn.commit()
        conn.close()

def parse_and_store_data(response, symbol):
    logging.debug(f'Parsing response: {response}')
    lines = response.strip().split('\n')
    data = []
    for line in lines:
        if line.startswith('$Bar'):
            parts = line.split()
            if len(parts) < 8:
                logging.error(f'Malformed line: {line}')
                continue
            try:
                date_time = parts[2]
                open_price = float(parts[5])
                high_price = float(parts[3])
                low_price = float(parts[4])
                close_price = float(parts[6])
                volume = int(parts[7]) if len(parts) > 7 else 0
                data.append((symbol, open_price, high_price, low_price, close_price, volume, date_time))
                logging.info(f'{symbol} | Timestamp: {date_time} | Open: {open_price} | High: {high_price} | Low: {low_price} | Close: {close_price} | Volume: {volume}')
            except (IndexError, ValueError) as e:
                logging.error(f'Error parsing line: {e}')
    
    if data:
        insert_into_db('ohlc_1min', data)
        logging.info(f'Data for {symbol} successfully inserted into ohlc_1min table.')

def process_ticker(symbol, start_time_str, run_duration):
    """
    Processes the ticker symbol starting from the provided start time (formatted as yyyy/mm/dd-hh:mm).
    """
    try:
        sock = create_socket()
        if not login(sock):
            logging.error(f'Failed to log in to DAS server for ticker {symbol}. Socket closed.')
            sock.close()
            return
        logging.info(f'Successfully logged in to DAS server for ticker {symbol}.')

        # Calculate the end time based on the run_duration (in seconds)
        end_time = datetime.now() + timedelta(seconds=run_duration)

        # Continuously request data in a loop
        while True:
            # Set the end_time to the current minute
            current_time = datetime.now()
            
            # Check if the current time has exceeded the end time
            if current_time >= end_time:
                logging.info(f'Duration of {run_duration} seconds reached for {symbol}, stopping data collection.')
                break
            
            # Request Minute Chart data from start_time to current_time
            request_minute_chart(sock, symbol, start_time_str, end_time=current_time.strftime('%Y/%m/%d-%H:%M'))
        
            # Monitor and append data to database
            response = receive_response(sock)
            if response:
                parse_and_store_data(response, symbol)
    
    except Exception as e:
        logging.error(f'Error processing ticker {symbol}: {e}')
    finally:
        sock.close()
        logging.debug(f'Socket closed for ticker {symbol}.')

def get_tickers_from_db():
    conn = sqlite3.connect('EOD_data.db')
    c = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')
    c.execute("SELECT DISTINCT TICKER FROM TradeParameters WHERE DATE=?", (today,))
    tickers = [row[0] for row in c.fetchall()]
    conn.close()
    return tickers

def monitor_new_tickers():
    """
    Continuously monitors the database for new tickers and starts new threads for them if found.
    """
    global processed_tickers

    while True:
        tickers = get_tickers_from_db()
        
        if not tickers:
            logging.info("No tickers found in the database. Waiting for new entries...")
        else:
            # Start new threads for any new tickers found in the database
            for symbol in tickers:
                if symbol not in processed_tickers:
                    logging.info(f'Found new ticker {symbol}, starting data collection.')
                    processed_tickers.add(symbol)
                    start_time = datetime.now().replace(hour=4, minute=0, second=0, microsecond=0).strftime('%Y/%m/%d-%H:%M')
                    ticker_thread = Thread(target=process_ticker, args=(symbol, start_time, 10))
                    ticker_thread.start()

            # Sleep for a while before checking the database again
            time.sleep(5)  # Check for new tickers every 5 seconds

def main():
    monitor_new_tickers()  # Start monitoring for new tickers

# Example usage
if __name__ == "__main__":
    main()