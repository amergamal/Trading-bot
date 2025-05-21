import socket
import logging
import time
import pandas as pd
import sqlite3
import os
import pytz
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

db_lock = Lock()  # Lock to manage database access
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

def insert_into_db(table, data):
    with db_lock:  # Use the lock to ensure thread-safe database access
        conn = sqlite3.connect('EOD_data.db')
        c = conn.cursor()
        if table == 'ohlc_5min':
            c.executemany('INSERT OR IGNORE INTO ohlc_5min (ticker, open, high, low, close, volume, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)', data)
        
        conn.commit()
        conn.close()



def parse_and_store_data(response, symbol):
    logger.debug(f'Parsing response: {response}')
    lines = response.strip().split('\n')
    data = []  # Prepare list to store multiple rows for bulk insert
    for line in lines:
        logger.debug(f'Processing line: {line}')
        if line.startswith('$Bar'):
            parts = line.split()
            logger.debug(f'Line parts: {parts}')
            
            # Ensure the time frame is 5-minute (parts[-1] should be '5' for 5-minute data)
            if len(parts) < 8 or parts[-1] != '5':  # Skip if it's not 5-minute data
                logger.warning(f"Ignoring non-5-minute data for {symbol}.")
                continue
            
            try:
                date_time = parts[2]
                open_price = float(parts[5])
                high_price = float(parts[3])
                low_price = float(parts[4])
                close_price = float(parts[6])
                volume = int(parts[7]) if len(parts) > 7 else 0
                
                # Add to data list for insertion
                data.append((symbol, open_price, high_price, low_price, close_price, volume, date_time))
                
                logger.info(f'{symbol} | Timestamp: {date_time} | Open: {open_price} | High: {high_price} | Low: {low_price} | Close: {close_price} | Volume: {volume}')
                
            except (IndexError, ValueError) as e:
                logger.error(f'Error parsing line: {e}')
    
    if data:
        # Insert all parsed data into the database
        insert_into_db('ohlc_5min', data)
        logger.info(f'Data for {symbol} successfully inserted into ohlc_5min table.')
        



def process_ticker(symbol, start_time_str):
    """
    Processes the ticker symbol starting from the provided start time (formatted as yyyy/mm/dd-hh:mm).
    """
    try:
        start_time = datetime.strptime(start_time_str, '%Y/%m/%d-%H:%M')
        
        sock = create_socket()
        welcome_message = receive_response(sock)
        logger.debug(f'Received welcome message: {welcome_message}')
        
        if not login(sock):
            sock.close()
            return
        
        # First data request immediately after logger in
        logger.info(f'First data request for {symbol} after initial 5-second startup.')
        initial_fetch_end_time = datetime.now() + timedelta(seconds=5)
        
        # Keep fetching data for 10 seconds
        while datetime.now() < initial_fetch_end_time:
            request_minute_chart(sock, symbol, start_time_str)
            
            # Check and process the response
            response = receive_response(sock)
            if response:
                logger.debug(f'Received initial minute chart response for {symbol}: {response}')
                parse_and_store_data(response, symbol)
            else:
                logger.warning(f"No data received for {symbol} during the initial 5 seconds.")
        
        # Now switch to regular 5-minute interval requests
        while True:
            # Check if the ticker still exists in the database before each interval
            if not ticker_exists_in_db(symbol):
                logger.info(f'{symbol} has been removed from the database. Stopping data collection.')
                break
            
            next_run_time = get_next_run_time()
            sleep_duration = (next_run_time - datetime.now()).total_seconds()
            logger.debug(f'Sleeping for {sleep_duration:.2f} seconds until the next run at {next_run_time}.')
            time.sleep(sleep_duration)

            request_minute_chart(sock, symbol, start_time_str)
            response = receive_response(sock)
            if response:
                logger.debug(f'Received minute chart response: {response}')
                parse_and_store_data(response, symbol)
            else:
                logger.debug('No data received, retrying...')
    
        sock.close()
        logger.info(f'Finished collecting data for {symbol}.')
    
    except Exception as e:
        logger.error(f'Error processing ticker {symbol}: {e}')

def ticker_exists_in_db(symbol):
    """
    Checks if the ticker still exists in the database.
    """
    conn = sqlite3.connect('EOD_data.db')
    c = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')
    c.execute("SELECT 1 FROM TradeParameters WHERE DATE=? AND TICKER=?", (today, symbol))
    result = c.fetchone()  # Fetch one result, if it exists
    conn.close()
    
    return result is not None  # Return True if the ticker is still in the database        
        
def get_next_run_time():
    """
    Calculate the next 5-minute interval time, 2 seconds after the 5-minute mark.
    Example: if current time is 10:00:00, it should return 10:05:02.
    """
    now = datetime.now()

    # Calculate the next 5-minute interval
    next_minute = (now.minute // 5 + 1) * 5
    if next_minute == 60:  # Handle the rollover case where next_minute would be 60
        next_run_time = (now + timedelta(hours=1)).replace(minute=0, second=2, microsecond=0)
    else:
        next_run_time = now.replace(minute=next_minute, second=2, microsecond=0)
    
    return next_run_time
      



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
                ticker_thread = Thread(target=process_ticker, args=(ticker, start_time_str))
                ticker_thread.start()
        for ticker in list(processed_tickers):
            if ticker not in tickers:
                logger.info(f'{ticker} has been removed from the database. Stopping its data collection.')
                processed_tickers.remove(ticker)
        time.sleep(60)

def main(strategy_logic=None):
    if strategy_logic is None:
        logger.error("No strategy_logic provided, cannot fetch tickers")
        return
    
    
    global processed_tickers

    # Define start time as 7:00 AM today
    current_date = datetime.now()
    start_time_str = current_date.replace(hour=7, minute=0, second=0, microsecond=0).strftime('%Y/%m/%d-%H:%M')

    logger.info(f"Starting ticker monitoring and data collection from {start_time_str}.")
    
    # Monitor for new tickers
    try:
        monitor_thread = Thread(target=monitor_new_tickers, args=(start_time_str, strategy_logic), daemon=True)
        monitor_thread.start()
        monitor_thread.join()  # Keep the main thread alive
    except Exception as e:
        logger.error(f"Error in min_chart5 main: {e}")

# Example usage
if __name__ == "__main__":
    main()
