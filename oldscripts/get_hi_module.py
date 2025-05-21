import sqlite3
import time
import threading
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def format_to_two_decimals(value):
    return f"{value:.2f}"


def update_trade_parameters():
    logging.debug("Starting update_trade_parameters function.")
    
    today_date = datetime.now().strftime('%Y-%m-%d')
    
    # Connect to the database
    conn = sqlite3.connect('EOD_data.db')
    c = conn.cursor()

    # Select all tickers with today's dates from TradeParameters
    c.execute('SELECT TICKER, DATE FROM TradeParameters WHERE DATE = ?', (today_date,))
    trade_parameters = c.fetchall()
    logging.debug(f"Fetched trade parameters: {trade_parameters}")

    for ticker, date in trade_parameters:
        # Convert date to the format yyyy/mm/dd
        formatted_date = datetime.strptime(date, '%Y-%m-%d').strftime('%Y/%m/%d')

        # Check if there is a record with timestamp 9:29 AM
        c.execute('''
            SELECT COUNT(*)
            FROM ohlc_1min
            WHERE ticker = ? AND substr(timestamp, 1, 10) = ? AND time(substr(timestamp, 12, 5)) = time('09:29')
        ''', (ticker, formatted_date))
        record_929_exists = c.fetchone()[0] > 0

        if record_929_exists:
            # Query for the highest value (HOD) after 9:29 AM
            c.execute('''
                SELECT MAX(high), timestamp
                FROM ohlc_1min
                WHERE ticker = ? AND substr(timestamp, 1, 10) = ? AND time(substr(timestamp, 12, 5)) >= time('09:29')
            ''', (ticker, formatted_date))
            result = c.fetchone()
            logging.debug(f"HOD query result for ticker {ticker} after 9:29 AM: {result}")
        else:
            # Query for the highest value (PMH) before 9:29 AM
            c.execute('''
                SELECT MAX(high), timestamp
                FROM ohlc_1min
                WHERE ticker = ? AND substr(timestamp, 1, 10) = ? AND time(substr(timestamp, 12, 5)) < time('09:29')
            ''', (ticker, formatted_date))
            result = c.fetchone()
            logging.debug(f"PMH query result for ticker {ticker} before 9:29 AM: {result}")

        if result and result[0] is not None:
            high, hi_time = result
            high = format_to_two_decimals(float(high))
            
            # Validate `hi_time` and handle timestamp in `YYYY/MM/DD-HH:MM` format
            if hi_time and '-' in hi_time:
                hi_time_only = hi_time.split('-')[1]  # Extract the time portion (HH:MM)
            else:
                logging.warning(f"Invalid timestamp format for ticker {ticker} on {formatted_date}: {hi_time}")
                hi_time_only = None  # Skip this record if timestamp is invalid

            if hi_time_only:
                logging.info(f'High for {ticker} on {formatted_date} is {high} at {hi_time_only}')
                
                # Update the HIGH and HI_TIME columns in TradeParameters
                c.execute('''
                    UPDATE TradeParameters 
                    SET HIGH = ?, HI_TIME = ? 
                    WHERE TICKER = ? AND DATE = ?
                ''', (high, hi_time_only, ticker, date))
                logging.debug(f"Updated TradeParameters for ticker {ticker} with high {high} and hi_time {hi_time_only}.")
            else:
                logging.warning(f"Skipping update for ticker {ticker} due to invalid hi_time.")
        else:
            logging.warning(f'No data found for {ticker} on {formatted_date}.')
            
        # New logic: Update PMH in the TickerRange table with the max high before 9:29 AM
        c.execute('''
            SELECT MAX(high)
            FROM ohlc_1min
            WHERE ticker = ? AND substr(timestamp, 1, 10) = ? AND time(substr(timestamp, 12, 5)) < time('09:29')
        ''', (ticker, formatted_date))
        pmh_result = c.fetchone()

        if pmh_result and pmh_result[0] is not None:
            pmh = format_to_two_decimals(float(pmh_result[0]))
            c.execute('''
                UPDATE TickerRange
                SET PMH = ?
                WHERE ticker = ? AND date = ?
            ''', (pmh, ticker, today_date))
            logging.info(f"Updated TickerRange for ticker {ticker} with PMH: {pmh}")
        else:
            logging.warning(f"No PMH data found for {ticker} before 9:29 AM.")    

    # Commit the changes and close the connection
    conn.commit()
    conn.close()
    logging.info('Trade parameters updated successfully.')


def run_update_trade_parameters():
    logging.debug("Starting run_update_trade_parameters loop.")
    while True:
        update_trade_parameters()
        time.sleep(60)  # Wait for 1 minute before running the update again


if __name__ == "__main__":
    # Start the update loop in a separate thread
    threading.Thread(target=run_update_trade_parameters).start()
