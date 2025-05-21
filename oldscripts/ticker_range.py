import socket
import logging
import time
import yfinance as yf
import pandas as pd
import sqlite3
import os
import pandas_market_calendars as mcal
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
            
def get_yfinance_close(ticker):
    """Fetches the previous close using yfinance and rounds it to two decimal places."""
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="5d")
        if not hist.empty:
            today = datetime.now().date()
            previous_day = next((date for date in reversed(hist.index.date) if date < today), None)
            if previous_day:
                previous_close = hist.loc[str(previous_day), 'Close']
                previous_close = round(previous_close, 2)
                logger.info(f"yfinance: Previous close for {ticker} on {previous_day} is {previous_close}")
                return previous_close
            logger.warning(f"No valid previous trading day data found for {ticker}.")
    except Exception as e:
        logger.error(f"Failed to fetch data from yfinance for {ticker}: {e}")
    return None            
            
def request_previous_day_close(sock, ticker):
    """
    Requests the previous trading day's closing price for a given ticker.
    """
    nyse = mcal.get_calendar('NYSE')
    today = datetime.now()
    schedule = nyse.schedule(start_date=today - timedelta(days=7), end_date=today)
    previous_trading_day = schedule.iloc[-2].name  # Get last trading day
    start_date = previous_trading_day.strftime('%Y/%m/%d')
    end_date = start_date

    daychart_command = f"SB {ticker} DAYCHART {start_date} {end_date}"
    send_command(sock, daychart_command)
    
    time.sleep(0.5)
    
    response = receive_response(sock)
    previous_close = None
    if response:
        logger.debug(f'Parsing response: {response}')
        lines = response.strip().split('\n')
        for line in lines:
            if line.startswith("$Bar") and ticker in line:
                parts = line.split()
                previous_close = float(parts[6])  # Close price is the 6th element
                logger.info(f"Previous day close for {ticker} on {start_date}: {previous_close}")
                
                # Call insert_PCL to store the PCL in the database
                update_PCL(ticker, previous_close)
                break

    if previous_close is None:
        logger.warning(f"Previous day close for {ticker} could not be retrieved on {start_date}")
    
    return previous_close

def request_minute_chart(sock, symbol, start_time, end_time, min_type=5):
    minchart_command = f'SB {symbol} MINCHART {start_time} {end_time} {min_type}'
    send_command(sock, minchart_command)

def insert_into_db(table, data):
    with db_lock:  # Use the lock to ensure thread-safe database access
        conn = sqlite3.connect('EOD_data.db')
        c = conn.cursor()
        if table == 'ohlc_PM5min':
            c.executemany('INSERT OR IGNORE INTO ohlc_PM5min (ticker, open, high, low, close, volume, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)', data)
        
        conn.commit()
        conn.close()
def update_PCL(ticker, pcl):
    """
    Update the Previous Close (PCL) in TradeParameters for the given ticker and date.
    """
    conn = sqlite3.connect('EOD_data.db')
    cursor = conn.cursor()
    
    today_date = datetime.now().strftime('%Y-%m-%d')

    try:
        cursor.execute("""
            UPDATE TradeParameters 
            SET PCL = ?
            WHERE TICKER = ? AND DATE = ?
        """, (pcl, ticker, today_date))
        
        if cursor.rowcount == 0:
            logger.warning(f"No matching record found for {ticker} on {today_date} to update PCL.")
        else:
            logger.info(f"Updated PCL: {pcl} for {ticker} on {today_date}")
    except sqlite3.Error as e:
        logger.error(f"Error updating PCL for {ticker} on {today_date}: {e}")
    finally:
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
        insert_into_db('ohlc_PM5min', data)
        logger.info(f'Data for {symbol} successfully inserted into ohlc_PM5min table.')
        
def calculate_pmh_pml_and_insert(ticker):
    """
    Calculate Pre-Market High (PMH), Pre-Market Low (PML), and Range % and insert into the TickerRange table.
    """
    conn = sqlite3.connect('EOD_data.db')
    cursor = conn.cursor()

    # Fetch pre-market data for the ticker up to 09:30 AM today
    today_date = datetime.now().strftime('%Y/%m/%d')
    cursor.execute("""
        SELECT MAX(high), MIN(low)
        FROM ohlc_PM5min
        WHERE ticker = ? 
        AND substr(timestamp, 1, 10) = ?
        AND time(substr(timestamp, 12, 5)) < '09:30'
    """, (ticker, today_date))
    pmh, pml = cursor.fetchone()

    if pmh and pml:
        # Calculate the range percentage
        range_percentage = ((pmh - pml) / pml) * 100
        formatted_percentage = f"{int(range_percentage)}%"  # Format to no decimals and add '%'

        
        today_date = datetime.now().strftime('%Y-%m-%d')
        
        # Check if the record exists
        cursor.execute("""
            SELECT 1 FROM TickerRange WHERE ticker = ? AND date = ?
        """, (ticker, today_date))
        exists = cursor.fetchone()

        if exists:
            # Update the existing record
            cursor.execute("""
                UPDATE TickerRange
                SET pmh = ?, pml = ?, range = ?
                WHERE ticker = ? AND date = ?
            """, (pmh, pml, formatted_percentage, ticker, today_date))
            logger.info(f"Updated PMH: {pmh}, PML: {pml}, Range: {formatted_percentage} for {ticker} on {today_date}")
        else:
            # Insert a new record
            cursor.execute("""
                INSERT INTO TickerRange (ticker, date, pmh, pml, range)
                VALUES (?, ?, ?, ?, ?)
            """, (ticker, today_date, pmh, pml, formatted_percentage))
            logger.info(f"Inserted PMH: {pmh}, PML: {pml}, Range: {formatted_percentage} for {ticker} on {today_date}")
    else:
        logger.warning(f"No pre-market data available for {ticker} to calculate PMH and PML.")

    conn.commit()
    conn.close()

def update_gap_in_trade_parameters(ticker):
    """
    Fetch the previous close from TradeParameters, find the latest close price from ohlc_PM5min at or before 09:29, calculate the gap, and update TradeParameters.
    """
    conn = sqlite3.connect('EOD_data.db')
    cursor = conn.cursor()
    
    # Step 1: Fetch the previous close from TradeParameters
    today_date = datetime.now().strftime('%Y-%m-%d')
    cursor.execute("SELECT PCL FROM TradeParameters WHERE TICKER = ? AND DATE = ?", (ticker, today_date))
    result = cursor.fetchone()
    
    if result:
        try:
            previous_close = float(result[0])  # Ensure previous_close is a float
        
            # Step 2: Fetch the close price from ohlc_PM5min where timestamp is 09:30 or the most recent close before 09:30
            formatted_today_date = datetime.now().strftime('%Y/%m/%d')
            cursor.execute("""
                SELECT close
                FROM ohlc_PM5min
                WHERE ticker = ?
                AND timestamp <= ?
                ORDER BY timestamp DESC
                LIMIT 1
            """, (ticker, f"{formatted_today_date}-09:25"))
        
            result = cursor.fetchone()
            if result:
                current_close = float(result[0])  # Ensure current_close is a float
            
                # Step 3: Calculate the gap
                gap = round(((current_close - previous_close) / previous_close) * 100)
            
                # Step 4: Update the GAP in TradeParameters
                cursor.execute("""
                    UPDATE TradeParameters
                    SET GAP = ?
                    WHERE TICKER = ? AND DATE = ?
                """, (gap, ticker, today_date))
            
                logger.info(f"Updated GAP for {ticker}: {gap}%")
            else:
                logger.warning(f"No 09:30 or earlier close found in ohlc_PM5min for {ticker}.")
        except ValueError as e:
            logger.error(f"Error in calculating gap for {ticker}. Ensure numeric values: {e}")        
    else:
        logger.warning(f"Previous close not found in TradeParameters for {ticker}.")
    
    conn.commit()
    conn.close()



def process_ticker(symbol, start_time_str, end_time_str):
    """
    Processes the ticker symbol starting from the provided start time (formatted as yyyy/mm/dd-hh:mm).
    Fetch data for 5 seconds without sleeping, then stop and call calculate_pmh_pml_and_insert.
    """
    try:
        
        
        sock = create_socket()
        welcome_message = receive_response(sock)
        logger.debug(f'Received welcome message: {welcome_message}')
        
        if not login(sock):
            sock.close()
            return
        
        # Fetch previous day's close for the ticker
        previous_close = request_previous_day_close(sock, symbol)
        
        # Step 2: Fallback to yfinance if DAS API fails
        if previous_close is None:
            logger.info(f"Falling back to yfinance for {symbol}.")
            previous_close = get_yfinance_close(symbol)
            
        # Step 3: Update PCL if a valid close was found
        if previous_close is not None:
            update_PCL(symbol, previous_close)
        else:
            logger.error(f"Failed to capture previous close for {symbol} from both DAS API and yfinance.")    

        # Set the end time for the 5-second data fetching period
        initial_fetch_end_time = datetime.now() + timedelta(seconds=5)

        # Fetch data continuously for 5 seconds with no sleep
        while datetime.now() < initial_fetch_end_time:
            request_minute_chart(sock, symbol, start_time_str, end_time_str)
            
            response = receive_response(sock)
            if response:
                logger.debug(f'Received minute chart response: {response}')
                parse_and_store_data(response, symbol)
            else:
                logger.debug('No data received, retrying...')

        # After the 5-second fetch ends, call calculate_pmh_pml_and_insert
        logger.info(f'Initial data collection for {symbol} completed. Calling calculate_pmh_pml_and_insert.')
        calculate_pmh_pml_and_insert(symbol)  # Function to calculate PMH, PML, and range
        update_gap_in_trade_parameters(symbol)
        
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
        

      



def monitor_new_tickers(start_time_str, end_time_str, strategy_logic=None):
    """
    Continuously monitor the database for new tickers and start threads for them.
    Runs every 5 seconds to check for new tickers.
    """
    global processed_tickers
    eastern = pytz.timezone('US/Eastern')
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
                
                ticker_thread = Thread(target=process_ticker, args=(ticker, start_time_str, end_time_str))
                ticker_thread.start()
                
        # Remove tickers from the global list if they are no longer in the database
        for ticker in list(processed_tickers):  # Iterate over a copy to safely modify the set
            if ticker not in tickers:
                logger.info(f'{ticker} has been removed from the database. Stopping its data collection.')
                processed_tickers.remove(ticker)
                # No need to stop the thread explicitly; the process_ticker thread will stop itself        

        # Re-fetch all tickers at 9:30 AM
        now = datetime.now(eastern)
        if now.hour == 9 and now.minute == 30:
            logger.info("9:30 AM reached. Re-fetching and processing all tickers.")
            processed_tickers.clear()  # Clear processed tickers to allow re-processing
            
            for ticker in tickers:
                if ticker not in processed_tickers:
                    processed_tickers.add(ticker)
                    ticker_thread = Thread(target=process_ticker, args=(ticker, start_time_str, end_time_str))
                    ticker_thread.start()
            time.sleep(60)  # Wait for 1 minute to prevent re-triggering within the same minute

        # Wait for 30 seconds before checking again
        time.sleep(60)

def main(strategy_logic=None):
    
    if strategy_logic is None:
        logger.error("No strategy_logic provided, cannot fetch tickers")
        return
    
    global processed_tickers

    # Define start time as 4:00 AM today
    current_date = datetime.now()
    start_time_str = current_date.replace(hour=4, minute=0, second=0, microsecond=0).strftime('%Y/%m/%d-%H:%M')
    end_time_str = current_date.replace(hour=9, minute=30, second=0, microsecond=0).strftime('%Y/%m/%d-%H:%M')

    logger.info(f"Starting ticker monitoring and data collection from {start_time_str} until {end_time_str}.")
    
    try:
        monitor_thread = Thread(target=monitor_new_tickers, args=(start_time_str, end_time_str, strategy_logic), daemon=True)
        monitor_thread.start()
        monitor_thread.join()
    except Exception as e:
        logger.error(f"Error in ticker_range main: {e}")

# Example usage
if __name__ == "__main__":
    main()
