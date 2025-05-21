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

        # Find the highest value and the time it occurred in ohlc_1min starting at or after 9:29 AM
        c.execute('''
            SELECT MAX(high), timestamp 
            FROM ohlc_1min 
            WHERE ticker = ? AND substr(timestamp, 1, 10) = ? AND time(substr(timestamp, 12, 5)) >= time('04:29')
        ''', (ticker, formatted_date))
        
        result = c.fetchone()
        logging.debug(f"Query result for ticker {ticker} on {formatted_date}: {result}")
        if result and result[0] is not None:
            high, hi_time = result
            high = format_to_two_decimals(float(high))
            hi_time_only = hi_time.split('-')[1]  # Extract the time portion
            logging.info(f'Highest value for {ticker} on {formatted_date} starting at or after 9:29 AM is {high} at {hi_time_only}')

            # Update the HIGH and HI_TIME columns in TradeParameters
            c.execute('''
                UPDATE TradeParameters 
                SET HIGH = ?, HI_TIME = ? 
                WHERE TICKER = ? AND DATE = ?
            ''', (high, hi_time_only, ticker, date))
            logging.debug(f"Updated TradeParameters for ticker {ticker} with high {high} and hi_time {hi_time_only}.")
        else:
            logging.warning(f'No data found for {ticker} on {formatted_date} starting at or after 9:29 AM')

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
    # Start both update loops in separate threads
    threading.Thread(target=run_update_trade_parameters).start()
   
