import sqlite3
import pandas as pd
from datetime import datetime
import time as tm
import threading
import logging

class DataFetcher:
    def __init__(self):
        self.tickers = {}
        self.logger = logging.getLogger('DataFetcher')
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    def fetch_tickers_from_db(self):
        """Fetch tickers from the TradeParameters table for the current day."""
        today = datetime.now().strftime('%Y-%m-%d')
        conn = None
        try:
            conn = sqlite3.connect('tms_data.db')
            c = conn.cursor()
            query = """
                SELECT TICKER, RSI_1MIN, RSI_5MIN FROM TradeParameters
                WHERE DATE = ?
            """
            c.execute(query, (today,))
            rows = c.fetchall()
            self.logger.debug(f"Fetched tickers: {rows}")
        except Exception as e:
            self.logger.error(f"Error fetching tickers from db: {e}")
        finally:
            if conn:
                conn.close()

        current_tickers = set(self.tickers.keys())
        for row in rows:
            ticker, rsi_1m, rsi_5m = row
            if ticker not in current_tickers:
                self.add_ticker(ticker, rsi_1m, rsi_5m)

    def add_ticker(self, ticker, rsi_1m, rsi_5m):
        self.tickers[ticker] = {
            'rsi_1m': rsi_1m,
            'rsi_5m': rsi_5m,
        }
        self.logger.info(f"Added ticker {ticker} to self.tickers.")
        threading.Thread(target=self.run_ticker, args=(ticker,), daemon=True).start()

    def fetch_new_data(self, ticker, table, last_timestamp):
        """Fetch the latest 16 periods of data from the database."""
        # Connect to the SQLite database
        conn = sqlite3.connect('tms_data.db')
        cursor = conn.cursor()

        # Fetch the latest 16 periods for the given ticker
        query = f'''
        SELECT * FROM {table}
        WHERE ticker = ?
        ORDER BY timestamp DESC
        LIMIT 16
        '''
        print(f"Running query: {query}")
        cursor.execute(query, (ticker,))
        rows = cursor.fetchall()

        # Define the columns including 'id'
        columns = ['id', 'ticker', 'open', 'high', 'low', 'close', 'volume', 'timestamp']

        # Close the connection
        conn.close()

        # Convert to DataFrame for better visualization
        data = pd.DataFrame(rows, columns=columns)

        # Sort data in ascending order of timestamp
        data.sort_values(by='timestamp', ascending=True, inplace=True)

        return data

    def insert_data_into_latest(self, data, ticker, table):
        conn = sqlite3.connect('tms_data.db')
        cursor = conn.cursor()

        target_table = 'latest_ohlc_1min' if table == 'ohlc_1min' else 'latest_ohlc_5min'

        for index, row in data.iterrows():
            # Check if the row with all the same columns already exists
            cursor.execute(f'''
            SELECT 1 FROM {target_table} 
            WHERE ticker = ? AND timestamp = ? 
            ''', (row['ticker'], row['timestamp']))
            
            if cursor.fetchone() is None:      
                try:
                    cursor.execute(f'''
                    INSERT INTO {target_table} (ticker, timestamp, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (row['ticker'], row['timestamp'], row['open'], row['high'], row['low'], row['close'], row['volume']))
                except sqlite3.IntegrityError:
                    print(f"Integrity error for {row['timestamp']}. Skipping insertion.")
            else:
                print(f"Duplicate entry for {row['timestamp']} with ticker {row['ticker']}. Skipping insertion.")
       

        cursor.execute(f'''
        DELETE FROM {target_table} 
        WHERE ticker = ? AND timestamp NOT IN (
            SELECT timestamp FROM {target_table} 
            WHERE ticker = ? 
            ORDER BY timestamp DESC 
            LIMIT 16
        )
        ''', (ticker, ticker))

        conn.commit()
        conn.close()
        self.logger.info(f"Updated data for {ticker} in {target_table}")

    def get_last_timestamp(self, ticker, table):
        target_table = 'latest_ohlc_1min' if table == 'ohlc_1min' else 'latest_ohlc_5min'
        conn = sqlite3.connect('tms_data.db')
        cursor = conn.cursor()

        cursor.execute(f'''
        SELECT MAX(timestamp) FROM {target_table}
        WHERE ticker = ?
        ''', (ticker,))
        last_timestamp = cursor.fetchone()[0] or '1970-01-01 00:00:00'

        conn.close()
        return last_timestamp

    def scheduled_fetch_and_insert(self, ticker):
        # Initial fetch and insert
        last_timestamp_1min = self.get_last_timestamp(ticker, 'ohlc_1min')
        data_1min = self.fetch_new_data(ticker, 'ohlc_1min', last_timestamp_1min)
        self.insert_data_into_latest(data_1min, ticker, 'ohlc_1min')

        last_timestamp_5min = self.get_last_timestamp(ticker, 'ohlc_5min')
        data_5min = self.fetch_new_data(ticker, 'ohlc_5min', last_timestamp_5min)
        self.insert_data_into_latest(data_5min, ticker, 'ohlc_5min')

        while True:
            current_time = datetime.now()

            # Calculate next fetch times
            next_fetch_1min = current_time.replace(second=4, microsecond=0)
            next_fetch_5min = next_fetch_1min if next_fetch_1min.minute % 5 == 0 else next_fetch_1min.replace(minute=(next_fetch_1min.minute + (5 - next_fetch_1min.minute % 5) % 5))

            if next_fetch_1min < current_time:
                next_fetch_1min = next_fetch_1min.replace(minute=next_fetch_1min.minute + 1)
            if next_fetch_5min < current_time:
                next_fetch_5min = next_fetch_5min.replace(minute=next_fetch_5min.minute + 5)

            # Sleep until the next scheduled time
            sleep_time = min((next_fetch_1min - current_time).total_seconds(), (next_fetch_5min - current_time).total_seconds())
            tm.sleep(sleep_time)

            current_time = datetime.now()

            if current_time >= next_fetch_1min:
                last_timestamp_1min = self.get_last_timestamp(ticker, 'ohlc_1min')
                data_1min = self.fetch_new_data(ticker, 'ohlc_1min', last_timestamp_1min)
                self.insert_data_into_latest(data_1min, ticker, 'ohlc_1min')

            if current_time >= next_fetch_5min:
                last_timestamp_5min = self.get_last_timestamp(ticker, 'ohlc_5min')
                data_5min = self.fetch_new_data(ticker, 'ohlc_5min', last_timestamp_5min)
                self.insert_data_into_latest(data_5min, ticker, 'ohlc_5min')

    def run_ticker(self, ticker):
        self.scheduled_fetch_and_insert(ticker)

# Test script
if __name__ == "__main__":
    fetcher = DataFetcher()
    fetcher.fetch_tickers_from_db()
