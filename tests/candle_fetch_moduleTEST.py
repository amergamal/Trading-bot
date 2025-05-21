import logging
import sqlite3
import pandas as pd
from alpaca_trade_api.rest import REST, TimeFrame, TimeFrameUnit
from datetime import datetime, timedelta
import pytz
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
        self.logger = logging.getLogger(__name__)
        self.logger.info("CandleFetch module initialized.")

    def get_tickers_from_db(self, date):
        conn = sqlite3.connect('tms_data.db')
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
            bars.index = bars.index.tz_convert(self.local_tz)
            bars['timestamp'] = bars.index.strftime('%Y/%m/%d-%H:%M')

            bars['ticker'] = symbol
            bars = bars[['ticker', 'open', 'high', 'low', 'close', 'volume', 'timestamp']]
            return bars
        except Exception as e:
            self.logger.error(f'Error fetching {timeframe} data for {symbol}: {e}')
            return pd.DataFrame()

    def insert_into_db(self, table, data):
        conn = sqlite3.connect('tms_data.db')
        c = conn.cursor()
        if table == 'ohlc_1min':
            c.executemany('INSERT OR IGNORE INTO ohlc_1min (ticker, open, high, low, close, volume, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)', data)
        elif table == 'ohlc_5min':
            c.executemany('INSERT OR IGNORE INTO ohlc_5min (ticker, open, high, low, close, volume, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)', data)
        conn.commit()
        conn.close()

    def get_next_minute_interval(self, interval_minutes):
        now = datetime.now()
        next_interval = (now + timedelta(minutes=interval_minutes)).replace(second=2, microsecond=0)
        return next_interval

    def get_next_five_minute_interval(self):
        now = datetime.now()
        minute = now.minute
        next_interval_minute = (minute // 5 + 1) * 5
        if next_interval_minute >= 60:
            next_interval_minute = 0
            now = now + timedelta(hours=1)
        next_interval = now.replace(minute=next_interval_minute, second=2, microsecond=0)
        return next_interval

    def main(self):
        self.logger.info("Starting CandleFetch main loop.")
        today = datetime.now().strftime('%Y-%m-%d')
        tickers = self.get_tickers_from_db(today)
        if not tickers:
            self.logger.error('No tickers found for the specified date.')
            return

        start_time = datetime.now(self.local_tz).replace(hour=4, minute=0, second=0, microsecond=0).isoformat()
        end_time = datetime.now(self.local_tz).isoformat()

        while True:
            next_run_time_1min = self.get_next_minute_interval(1)
            next_run_time_5min = self.get_next_five_minute_interval()
            self.logger.info(f"Next 1-min run time: {next_run_time_1min}, Next 5-min run time: {next_run_time_5min}")
            
            while datetime.now() < next_run_time_1min and datetime.now() < next_run_time_5min:
                time.sleep(1)

            if datetime.now() >= next_run_time_1min:
                self.logger.info(f"Fetching 1-min bars at {datetime.now()}")
                all_1min_data = []
                for ticker in tickers:
                    data_1min = self.fetch_bars(ticker, start_time, end_time, TimeFrame(1, TimeFrameUnit.Minute))
                    if not data_1min.empty:
                        all_1min_data.append(data_1min)
                if all_1min_data:
                    combined_1min_data = pd.concat(all_1min_data, ignore_index=True)
                    self.logger.debug("1-min data:")
                    self.logger.debug(combined_1min_data)
                    self.insert_into_db('ohlc_1min', combined_1min_data.values.tolist())
                else:
                    self.logger.info("No 1-minute data fetched for any tickers.")
                time.sleep(2)
                next_run_time_1min = self.get_next_minute_interval(1)

            if datetime.now() >= next_run_time_5min:
                self.logger.info(f"Fetching 5-min bars at {datetime.now()}")
                all_5min_data = []
                for ticker in tickers:
                    data_5min = self.fetch_bars(ticker, start_time, end_time, TimeFrame(5, TimeFrameUnit.Minute))
                    if not data_5min.empty:
                        all_5min_data.append(data_5min)
                if all_5min_data:
                    combined_5min_data = pd.concat(all_5min_data, ignore_index=True)
                    self.logger.debug("5-min data:")
                    self.logger.debug(combined_5min_data)
                    self.insert_into_db('ohlc_5min', combined_5min_data.values.tolist())
                else:
                    self.logger.info("No 5-minute data fetched for any tickers.")
                time.sleep(2)
                next_run_time_5min = self.get_next_five_minute_interval()

            # Update end_time to keep fetching new data
            end_time = datetime.now(self.local_tz).isoformat()

# Test the CandleFetch module
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    candle_fetch = CandleFetch()
    candle_fetch.main()
