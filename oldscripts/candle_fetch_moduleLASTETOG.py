import logging
import sqlite3
import pandas as pd
from alpaca_trade_api.rest import REST, TimeFrame, TimeFrameUnit
from datetime import datetime, timedelta
import pytz
import threading
import time

# Alpaca API credentials
API_KEY = 'AKOZDEBXV8QM03UO78B3'
API_SECRET = 'VeXdgXprgHFF61brUsoJCO8nPE0SPUitlgZEXcip'
BASE_URL = 'https://api.alpaca.markets'

logging.basicConfig(level=logging.DEBUG)

class CandleFetch:
    def __init__(self):
        self.api = REST(API_KEY, API_SECRET, BASE_URL, api_version='v2')
        self.local_tz = pytz.timezone('US/Eastern')
        self.lock = threading.Lock()  # Initialize a lock
        
        # Set up the logger for this class
        self.logger = logging.getLogger('CandleFetch')
        self.logger.setLevel(logging.DEBUG)

    def get_tickers_from_db(self, date):
        conn = sqlite3.connect('EOD_data.db')
        c = conn.cursor()
        c.execute("SELECT DISTINCT TICKER FROM TradeParameters WHERE DATE=?", (date,))
        tickers = [row[0] for row in c.fetchall()]
        conn.close()
        return tickers

    def fetch_bars(self, symbol, start_time, end_time, timeframe):
        self.logger.info(f'Fetching {timeframe} data for {symbol} from {start_time} to {end_time}')
        try:
            bars = self.api.get_bars(symbol, timeframe, start=start_time, end=end_time).df

            # Convert the timestamps to the desired timezone
            if not bars.empty:
                bars.index = pd.to_datetime(bars.index).tz_convert(self.local_tz)
                bars['timestamp'] = bars.index.strftime('%Y/%m/%d-%H:%M')
                bars['ticker'] = symbol
                bars = bars[['ticker', 'open', 'high', 'low', 'close', 'volume', 'timestamp']]
            return bars
        except Exception as e:
            self.logger.error(f'Error fetching {timeframe} data for {symbol}: {e}')
            return pd.DataFrame()

    def insert_into_db(self, table, data):
        conn = sqlite3.connect('EOD_data.db')
        c = conn.cursor()
        if table == 'ohlc_1min':
            self.logger.debug(f"Attempting to insert into ohlc_1min for data: {data}")
            c.executemany('INSERT OR IGNORE INTO ohlc_1min (ticker, open, high, low, close, volume, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)', data)
        elif table == 'ohlc_5min':
            self.logger.debug(f"Attempting to insert into ohlc_5min for data: {data}")
            c.executemany('INSERT OR IGNORE INTO ohlc_5min (ticker, open, high, low, close, volume, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)', data)
        conn.commit()
        conn.close()

    def insert_into_latest_1min(self, data):
        with self.lock:
            conn = sqlite3.connect('EOD_data.db')
            c = conn.cursor()

            # Insert into latest_ohlc_1min
            self.logger.debug(f"Attempting to insert into latest_ohlc_1min for data: {data}")
            c.executemany('INSERT OR IGNORE INTO latest_ohlc_1min (ticker, open, high, low, close, volume, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)', data)

            for ticker in set(row[0] for row in data):
                # Check current count
                c.execute('''
                SELECT COUNT(*) FROM latest_ohlc_1min
                WHERE ticker = ?
                ''', (ticker,))
                count = c.fetchone()[0]

                self.logger.debug(f"Current record count for {ticker} in latest_ohlc_1min: {count}")

                if count > 16:
                    self.logger.debug(f"Maintaining latest 16 records for ticker {ticker} in latest_ohlc_1min.")
                    c.execute('''
                    DELETE FROM latest_ohlc_1min 
                    WHERE timestamp NOT IN (
                        SELECT timestamp FROM latest_ohlc_1min 
                        WHERE ticker = ? 
                        ORDER BY timestamp DESC 
                        LIMIT 16
                    )
                    ''', (ticker,))

                # Re-check the number of records left
                c.execute('''
                SELECT COUNT(*) FROM latest_ohlc_1min
                WHERE ticker = ?
                ''', (ticker,))
                new_count = c.fetchone()[0]
                self.logger.debug(f"After maintenance, {new_count} records remain for {ticker} in latest_ohlc_1min.")

            conn.commit()
            conn.close()
            self.logger.debug("Updated latest_ohlc_1min successfully.")

    def insert_into_latest_5min(self, data):
        with self.lock:
            conn = sqlite3.connect('EOD_data.db')
            c = conn.cursor()

            # Insert into latest_ohlc_5min
            self.logger.debug(f"Attempting to insert into latest_ohlc_5min for data: {data}")
            c.executemany('INSERT OR IGNORE INTO latest_ohlc_5min (ticker, open, high, low, close, volume, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)', data)

            for ticker in set(row[0] for row in data):
                # Check current count
                c.execute('''
                SELECT COUNT(*) FROM latest_ohlc_5min
                WHERE ticker = ?
                ''', (ticker,))
                count = c.fetchone()[0]

                self.logger.debug(f"Current record count for {ticker} in latest_ohlc_5min: {count}")

                if count > 16:
                    self.logger.debug(f"Maintaining latest 16 records for ticker {ticker} in latest_ohlc_5min.")
                    c.execute('''
                    DELETE FROM latest_ohlc_5min 
                    WHERE timestamp NOT IN (
                        SELECT timestamp FROM latest_ohlc_5min 
                        WHERE ticker = ? 
                        ORDER BY timestamp DESC 
                        LIMIT 16
                    )
                    ''', (ticker,))

                # Re-check the number of records left
                c.execute('''
                SELECT COUNT(*) FROM latest_ohlc_5min
                WHERE ticker = ?
                ''', (ticker,))
                new_count = c.fetchone()[0]
                self.logger.debug(f"After maintenance, {new_count} records remain for {ticker} in latest_ohlc_5min.")

            conn.commit()
            conn.close()
            self.logger.debug("Updated latest_ohlc_5min successfully.")

    def get_next_minute_interval(self, interval_minutes):
        now = datetime.now(self.local_tz)
        next_interval = (now + timedelta(minutes=interval_minutes)).replace(second=2, microsecond=0)
        return next_interval

    def get_next_five_minute_interval(self):
        now = datetime.now(self.local_tz)
        minute = now.minute
        next_interval_minute = (minute // 5 + 1) * 5
        if next_interval_minute >= 60:
            next_interval_minute = 0
            now = now + timedelta(hours=1)
        next_interval = now.replace(minute=next_interval_minute, second=2, microsecond=0)
        return next_interval
    
    

    def main(self):
        try:
            
            while True:
                today = datetime.now(self.local_tz)
                tickers = self.get_tickers_from_db(today.strftime('%Y-%m-%d'))
                if not tickers:
                    self.logger.error('No tickers found for the specified date.')
                    time.sleep(60)  # Instead of exiting, wait and retry in 60 seconds
                    continue

                # Initial fetching from 4:30 AM to the current time
                start_time = today.replace(hour=4, minute=30, second=0, microsecond=0)
                end_time = datetime.now(self.local_tz)

                # Fetch historical data until the current time
                self.logger.info("Fetching historical data from 4:30 AM to current time")
                all_1min_data = []
                all_5min_data = []
                for ticker in tickers:
                    data_1min = self.fetch_bars(ticker, start_time.isoformat(timespec='seconds'), end_time.isoformat(timespec='seconds'), TimeFrame(1, TimeFrameUnit.Minute))
                    data_5min = self.fetch_bars(ticker, start_time.isoformat(timespec='seconds'), end_time.isoformat(timespec='seconds'), TimeFrame(5, TimeFrameUnit.Minute))
                    if not data_1min.empty:
                        all_1min_data.append(data_1min)
                    if not data_5min.empty:
                        all_5min_data.append(data_5min)

                if all_1min_data:
                    combined_1min_data = pd.concat(all_1min_data, ignore_index=True)
                    self.insert_into_db('ohlc_1min', combined_1min_data.values.tolist())
                    self.insert_into_latest_1min(combined_1min_data.values.tolist())

                if all_5min_data:
                    combined_5min_data = pd.concat(all_5min_data, ignore_index=True)
                    self.insert_into_db('ohlc_5min', combined_5min_data.values.tolist())
                    self.insert_into_latest_5min(combined_5min_data.values.tolist())

                # Start real-time fetching from the next interval onwards
                while True:
                    try:
                        # Refresh the current date to ensure you are always working with today's date
                        today = datetime.now(self.local_tz).strftime('%Y-%m-%d')
                
                        # Re-fetch tickers before every fetch cycle
                        tickers = self.get_tickers_from_db(today)
                        if not tickers:
                            self.logger.warning('No tickers found for the specified date. Skipping this cycle.')
                            time.sleep(10)
                            continue
                
                        next_run_time_1min = self.get_next_minute_interval(1)
                        next_run_time_5min = self.get_next_five_minute_interval()

                        # Wait until at least one of the next run times is reached
                        while datetime.now(self.local_tz) < next_run_time_1min and datetime.now(self.local_tz) < next_run_time_5min:
                            time.sleep(0.5)  # Use a short sleep to reduce CPU usage without excessive delay

                        # Fetch 1-minute data
                        if datetime.now(self.local_tz) >= next_run_time_1min:
                            self.logger.info(f"Fetching 1-min bars at {datetime.now(self.local_tz)}")
                            all_1min_data = []
                            for ticker in tickers:
                                # Adjust the start and end time for the most recent 1-minute interval
                                end_time = datetime.now(self.local_tz).replace(second=0, microsecond=0)
                                start_time = end_time - timedelta(minutes=1)

                                data_1min = self.fetch_bars(ticker, start_time.isoformat(timespec='seconds'), end_time.isoformat(timespec='seconds'), TimeFrame(1, TimeFrameUnit.Minute))
                                if not data_1min.empty:
                                    all_1min_data.append(data_1min)
                            if all_1min_data:
                                combined_1min_data = pd.concat(all_1min_data, ignore_index=True)
                                self.logger.info(f"1-min data fetched: {len(combined_1min_data)} records")
                                self.insert_into_db('ohlc_1min', combined_1min_data.values.tolist())
                                self.insert_into_latest_1min(combined_1min_data.values.tolist())
                            else:
                                self.logger.info("No 1-minute data fetched for any tickers.")
                            next_run_time_1min = self.get_next_minute_interval(1)

                        # Fetch 5-minute data
                        if datetime.now(self.local_tz) >= next_run_time_5min:
                            self.logger.info(f"Fetching 5-min bars at {datetime.now(self.local_tz)}")
                            all_5min_data = []
                            for ticker in tickers:
                                # Calculate the proper 5-minute interval
                                end_time = datetime.now(self.local_tz).replace(second=0, microsecond=0)
                                start_time = end_time - timedelta(minutes=5)

                                data_5min = self.fetch_bars(ticker, start_time.isoformat(timespec='seconds'), end_time.isoformat(timespec='seconds'), TimeFrame(5, TimeFrameUnit.Minute))
                                if not data_5min.empty:
                                    all_5min_data.append(data_5min)
                            if all_5min_data:
                                combined_5min_data = pd.concat(all_5min_data, ignore_index=True)
                                self.logger.info(f"5-min data fetched: {len(combined_5min_data)} records")
                                self.insert_into_db('ohlc_5min', combined_5min_data.values.tolist())
                                self.insert_into_latest_5min(combined_5min_data.values.tolist())
                            else:
                                self.logger.info("No 5-minute data fetched for any tickers.")
                            next_run_time_5min = self.get_next_five_minute_interval()

                        # Update start_time and end_time for the next fetch
                        start_time = end_time
                        end_time = start_time + timedelta(minutes=1)

                    except Exception as e:
                        self.logger.error(f"Error in fetching loop: {e}")
                        time.sleep(10)  # Retry after a minute if an error occurs
 
        except Exception as e:
            self.logger.error(f"Fatal error in CandleFetch: {e}")



# Create an instance of the CandleFetch class
candle_fetch = CandleFetch()

# Run the main function in a separate thread
thread = threading.Thread(target=candle_fetch.main, daemon=True)
thread.start()
