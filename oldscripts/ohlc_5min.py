import sqlite3
import logging
import time
import os
from datetime import datetime, timedelta
import threading

logging.basicConfig(level=logging.INFO)

DATABASE_PATH = os.path.abspath('C:/Users/a1031/clicksendLUAutoUpdate/ClickSend-LUAutoUpdate/EOD_data.db')

def aggregate_to_5min():
    """
    Aggregates data from ohlc_1min to ohlc_5min by 5-minute intervals.
    The 5-minute candle represents the open of the first minute (10:50),
    high and low from 10:50 to 10:54, and close of the last minute (10:54).
    """
    try:
        # Current time (we assume it's now 10:55 for this example)
        now = datetime.now()
        # The 5-minute window we're aggregating is from 10:50 to 10:54
        end_time = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
        start_time = end_time - timedelta(minutes=4)  # 5-minute period: 10:50 to 10:54
        
        # Timestamp for the new 5-minute candle (based on the start time of the window)
        five_min_timestamp = start_time.strftime('%Y/%m/%d-%H:%M')

        # Format time strings
        start_time_str = start_time.strftime('%Y/%m/%d-%H:%M')
        end_time_str = end_time.strftime('%Y/%m/%d-%H:%M')

        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()

            # Get all distinct tickers within the time range (10:50 to 10:54)
            cursor.execute('''
                SELECT DISTINCT ticker FROM ohlc_1min 
                WHERE timestamp >= ? AND timestamp <= ?
            ''', (start_time_str, end_time_str))

            tickers = cursor.fetchall()

            for ticker in tickers:
                ticker_name = ticker[0]

                # Fetch the open price from 10:50
                cursor.execute('''
                    SELECT open FROM ohlc_1min
                    WHERE ticker = ? AND timestamp = ?
                ''', (ticker_name, start_time_str))
                open_result = cursor.fetchone()
                open_value = open_result[0] if open_result else None

                # Fetch the close price from 10:54
                cursor.execute('''
                    SELECT close FROM ohlc_1min
                    WHERE ticker = ? AND timestamp = ?
                ''', (ticker_name, end_time_str))
                close_result = cursor.fetchone()
                close_value = close_result[0] if close_result else None

                if not (open_value and close_value):
                    logging.warning(f"Missing open or close for ticker {ticker_name} from {start_time_str} to {end_time_str}. Skipping aggregation.")
                    continue  # Skip this ticker if open or close is missing

                # Aggregate the high and low between 10:50 and 10:54
                cursor.execute('''
                    SELECT MAX(high), MIN(low), SUM(volume) FROM ohlc_1min
                    WHERE ticker = ? AND timestamp >= ? AND timestamp <= ?
                ''', (ticker_name, start_time_str, end_time_str))
                high_low_result = cursor.fetchone()
                high_value, low_value, volume = high_low_result if high_low_result else (None, None, 0)

                # Insert the aggregated data into ohlc_5min
                cursor.execute(f'''
                INSERT INTO ohlc_5min (ticker, open, high, low, close, volume, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (ticker_name, open_value, high_value, low_value, close_value, volume, five_min_timestamp))

                logging.info(f"Successfully aggregated 5-min candle for {ticker_name} from {start_time_str} to {end_time_str} (open: {open_value}, high: {high_value}, low: {low_value}, close: {close_value}).")

            conn.commit()

    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

def wait_for_next_five_minute_period():
    """
    Calculate and wait until the next 'XX:05:20', 'XX:10:20', 'XX:15:20', etc.
    """
    now = datetime.now()
    # Calculate the next 5-minute interval (XX:05:20, XX:10:20, etc.)
    next_minute = (now.minute // 5 + 1) * 5

    # Roll over the hour if next_minute exceeds 59
    if next_minute >= 60:
        next_minute = 0
        next_hour = now.hour + 1 if now.hour < 23 else 0  # Roll over to the next hour or midnight
        next_run_time = now.replace(hour=next_hour, minute=next_minute, second=20, microsecond=0)
    else:
        next_run_time = now.replace(minute=next_minute, second=20, microsecond=0)

    # Calculate how long to sleep until the next run time
    sleep_time = (next_run_time - now).total_seconds()
    
    logging.info(f"Next aggregation scheduled at {next_run_time}. Sleeping for {sleep_time} seconds.")
    
    time.sleep(sleep_time)  # Sleep until the exact next 5-minute interval (XX:05:20, XX:10:20, etc.)

def aggregate_ohlc_data_periodically():
    """
    Periodically aggregate data from 1-minute to 5-minute intervals at exact 5-minute marks.
    """
    while True:
        # Wait until the next XX:05:20, XX:10:20, etc.
        wait_for_next_five_minute_period()

        # Run the aggregation function
        aggregate_to_5min()

# Start the aggregation thread
def start_aggregation_thread():
    threading.Thread(target=aggregate_ohlc_data_periodically, daemon=True).start()

# Entry point for running the script
if __name__ == "__main__":
    logging.info("Starting aggregation thread for ohlc_1min to ohlc_5min.")
    start_aggregation_thread()

    # Keep the script running
    while True:
        time.sleep(60)
