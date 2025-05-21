import sqlite3
import datetime
import logging
import threading
import time
import socket
import os
from datetime import datetime, timedelta
from trade_monitor import TradeMonitor

DAS_API_BASE_URL = '127.0.0.1'
DAS_API_PORT = 9800
DAS_API_USERNAME = 'CD4832'
DAS_API_PASSWORD = 'Gamala123'
DAS_API_ACCOUNT = '104832'  # Confirm if '104832' is needed

class VwapFetch:
    def __init__(self, db_path, trade_monitor):
        self.sock = None
        self.connected = False
        self.response_buffer = []
        self.reconnecting = False
        self.trade_monitor = trade_monitor
        self.db_path = db_path
        self.db_lock = threading.Lock()
        self.subscribed_tickers = set()
        self.initial_quote_data = {}
        self.processed_tickers = set()
        self.logger = logging.getLogger('VwapFetch')
        logging.basicConfig(level=logging.DEBUG)

    def create_socket(self):
        self.logger.debug('Creating socket...')
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((DAS_API_BASE_URL, DAS_API_PORT))
        self.logger.debug('Socket connected to DAS API.')
        return s

    def send_command(self, command):
        if self.sock:
            try:
                full_command = f'{command}\r\n'
                self.logger.debug(f'Sending command to DAS: {full_command}')
                self.sock.sendall(full_command.encode())
            except (OSError, BrokenPipeError) as e:
                self.logger.error(f'Error sending command: {e}')
                self.connected = False
                self.reconnect()
        else:
            self.logger.warning('Socket is None, cannot send command')

    def receive_response(self):
        if self.sock:
            try:
                response = b""
                while True:
                    part = self.sock.recv(4096)
                    if not part:
                        raise OSError("Disconnected")
                    response += part
                    if len(part) < 4096:
                        break
                response = response.decode()
                self.logger.debug(f'Received response: {response}')
                return response
            except (OSError, BrokenPipeError) as e:
                self.logger.error(f'Error receiving response: {e}')
                self.connected = False
                self.reconnect()
                return None

    def continuously_receive(self):
        while self.connected:
            response = self.receive_response()
            if response:
                for line in response.splitlines():
                    if line.strip():
                        self.response_buffer.append(line)

    def login(self):
        self.sock = self.create_socket()
        login_command = f'LOGIN {DAS_API_USERNAME} {DAS_API_PASSWORD} {DAS_API_ACCOUNT} 0'
        self.send_command(login_command)
        while True:
            login_response = self.receive_response()
            if login_response:
                if 'LOGIN SUCCESSED' in login_response:
                    self.logger.info('Login successful')
                    self.connected = True
                    self.logger.info('API connection is now ready')
                    threading.Thread(target=self.continuously_receive, daemon=True).start()
                    self.update_account_equity()
                    break
                elif '#Welcome to DAS Command API' in login_response:
                    self.logger.info('Welcome message received, waiting for login success...')
                elif '#Please login to continue.' in login_response:
                    self.logger.warning('Received prompt to login again, retrying...')
                    self.send_command(login_command)
                else:
                    self.logger.error(f'Unexpected login response: {login_response}')
                    self.sock.close()
                    self.connected = False
                    break

    def keep_alive(self):
        while True:
            time.sleep(30)
            if self.sock:
                try:
                    self.send_command('ECHO')
                except AttributeError:
                    self.logger.warning('Socket is None, skipping keep alive')
            else:
                self.logger.warning('Socket is None, skipping keep alive')

    def reconnect(self):
        self.logger.warning('Reconnecting...')
        self.sock = self.create_socket()
        self.login()
        for ticker in self.subscribed_tickers:
            self.subscribe_to_level1(ticker)
            self.subscribe_to_minute_chart(ticker)

    def connect_to_db(self):
        abs_db_path = os.path.abspath(self.db_path)
        self.logger.debug(f"Connecting to database at: {abs_db_path}")
        return sqlite3.connect(abs_db_path)

    def ensure_database_schema(self):
        with self.db_lock:
            conn = self.connect_to_db()
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ohlc_1min (
                    ticker TEXT,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume INTEGER,
                    timestamp TEXT
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ohlc_5min (
                    ticker TEXT,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume INTEGER,
                    timestamp TEXT
                )
            """)
            conn.commit()
            conn.close()
            self.logger.info("Verified/created ohlc table schemas.")

    def insert_into_db(self, table, data):
        with self.db_lock:
            conn = self.connect_to_db()
            c = conn.cursor()
            try:
                c.executemany(f'INSERT OR IGNORE INTO {table} (ticker, open, high, low, close, volume, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)', data)
                conn.commit()
                self.logger.debug(f"Inserted {len(data)} rows into {table}")
            except sqlite3.Error as e:
                self.logger.error(f"Database error in insert_into_db for {table}: {e}")
            finally:
                conn.close()

    def parse_and_store_data(self, response, symbol):
        self.logger.debug(f'Parsing minute chart response: {response}')
        lines = response.strip().split('\n')
        data_1min = []
        data_5min = []
        for line in lines:
            self.logger.debug(f'Processing line: {line}')
            if line.startswith('$Bar'):
                parts = line.split()
                self.logger.debug(f'Line parts: {parts}')
                if len(parts) < 8 or parts[-1] not in ['1', '5']:
                    self.logger.warning(f"Ignoring invalid or non-1/5-minute data for {symbol}: {line}")
                    continue
                try:
                    date_time = parts[2]
                    open_price = float(parts[5])
                    high_price = float(parts[3])
                    low_price = float(parts[4])
                    close_price = float(parts[6])
                    volume = int(parts[7]) if len(parts) > 7 else 0
                    min_type = parts[-1]
                    if min_type == '1':
                        data_1min.append((symbol, open_price, high_price, low_price, close_price, volume, date_time))
                    elif min_type == '5':
                        data_5min.append((symbol, open_price, high_price, low_price, close_price, volume, date_time))
                    self.logger.info(f'{symbol} | MinType: {min_type} | Timestamp: {date_time} | Open: {open_price} | High: {high_price} | Low: {low_price} | Close: {close_price} | Volume: {volume}')
                except (IndexError, ValueError) as e:
                    self.logger.error(f'Error parsing line: {line}, error: {e}')
        if data_1min:
            self.insert_into_db('ohlc_1min', data_1min)
            self.logger.info(f'1-minute data for {symbol} successfully inserted into ohlc_1min table.')
        if data_5min:
            self.insert_into_db('ohlc_5min', data_5min)
            self.logger.info(f'5-minute data for {symbol} successfully inserted into ohlc_5min table.')

    def get_next_1min_run_time(self):
        now = datetime.now()
        next_run_time = (now + timedelta(minutes=1)).replace(second=2, microsecond=0)
        return next_run_time

    def get_next_5min_run_time(self):
        now = datetime.now()
        next_minute = (now.minute // 5 + 1) * 5
        if next_minute == 60:
            next_run_time = (now + timedelta(hours=1)).replace(minute=0, second=2, microsecond=0)
        else:
            next_run_time = now.replace(minute=next_minute, second=2, microsecond=0)
        return next_run_time

    def request_minute_chart(self, ticker, start_time, min_type=5):
        minchart_command = f'SB {ticker} MINCHART {start_time} LATEST {min_type}'
        self.send_command(minchart_command)
        self.logger.debug(f"Requested {min_type}-minute chart for {ticker}")

    def subscribe_to_minute_chart(self, ticker):
        if ticker not in self.subscribed_tickers:
            start_time_str = datetime.now().replace(hour=4, minute=0, second=0, microsecond=0).strftime('%Y/%m/%d-%H:%M')
            self.request_minute_chart(ticker, start_time_str, min_type=1)
            self.request_minute_chart(ticker, start_time_str, min_type=5)
            self.subscribed_tickers.add(ticker)
            self.logger.debug(f"Subscribed to 1-minute and 5-minute charts for {ticker}")

    def process_ticker_minute_charts(self, ticker, start_time_str):
        try:
            start_time = datetime.strptime(start_time_str, '%Y/%m/%d-%H:%M')
            self.logger.info(f'First minute chart data request for {ticker} after initial 5-second startup.')
            initial_fetch_end_time = datetime.now() + timedelta(seconds=5)
            while datetime.now() < initial_fetch_end_time:
                self.request_minute_chart(ticker, start_time_str, min_type=1)
                self.request_minute_chart(ticker, start_time_str, min_type=5)
                time.sleep(0.1)
            next_1min_run = self.get_next_1min_run_time()
            next_5min_run = self.get_next_5min_run_time()
            while True:
                if not self.ticker_exists_in_db(ticker):
                    self.logger.info(f'{ticker} has been removed from the database. Stopping minute chart collection.')
                    self.subscribed_tickers.discard(ticker)
                    self.send_command(f'UNSB {ticker} MINCHART 1')
                    self.send_command(f'UNSB {ticker} MINCHART 5')
                    break
                now = datetime.now()
                if now >= next_1min_run:
                    self.request_minute_chart(ticker, start_time_str, min_type=1)
                    next_1min_run = self.get_next_1min_run_time()
                if now >= next_5min_run:
                    self.request_minute_chart(ticker, start_time_str, min_type=5)
                    next_5min_run = self.get_next_5min_run_time()
                time.sleep(1)
        except Exception as e:
            self.logger.error(f'Error processing minute charts for {ticker}: {e}')

    def get_todays_tickers(self):
        conn = self.connect_to_db()
        cursor = conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        query = "SELECT TICKER, RSI_1MIN, RSI_5MIN FROM TradeParameters WHERE Date = ?"
        self.logger.debug(f"Executing query to get today's tickers: {query} with Date = {today}")
        cursor.execute(query, (today,))
        tickers = [(row[0], row[1], row[2]) for row in cursor.fetchall()]
        self.logger.debug(f"Retrieved tickers for today: {tickers}")
        conn.close()
        return tickers

    def ticker_exists_in_db(self, ticker):
        with self.db_lock:
            conn = self.connect_to_db()
            cursor = conn.cursor()
            today = datetime.now().strftime('%Y-%m-%d')
            cursor.execute("SELECT 1 FROM TradeParameters WHERE Date=? AND TICKER=?", (today, ticker))
            result = cursor.fetchone()
            conn.close()
            return result is not None

    def subscribe_to_level1(self, ticker):
        if ticker not in self.subscribed_tickers:
            command = f"SB {ticker} Lv1"
            self.send_command(command)
            self.logger.debug(f"Subscribed to level 1 data for {ticker}")
            self.subscribed_tickers.add(ticker)

    def get_ldlu_prices(self, ticker):
        try:
            command = f"GET LDLU {ticker}"
            self.logger.debug(f"Sending command to DAS to get LDLU prices for ticker {ticker}: {command}")
            self.send_command(command)
        except Exception as e:
            self.logger.error(f"Error fetching LDLU prices for {ticker}: {e}")

    def monitor_tickers(self):
        start_time_str = datetime.now().replace(hour=4, minute=0, second=0, microsecond=0).strftime('%Y/%m/%d-%H:%M')
        while True:
            ticker_data = self.get_todays_tickers()
            tickers = [ticker for ticker, rsi_1m, rsi_5m in ticker_data]
            if not tickers:
                self.logger.info("No tickers found in the database. Waiting for new entries...")
            for ticker in tickers:
                if ticker not in self.processed_tickers:
                    self.logger.info(f"New ticker found: {ticker}. Starting data collection.")
                    self.processed_tickers.add(ticker)
                    self.subscribe_to_level1(ticker)
                    self.subscribe_to_minute_chart(ticker)
                    threading.Thread(target=self.process_ticker_minute_charts, args=(ticker, start_time_str), daemon=True).start()
                    time.sleep(1)
                    self.get_ldlu_prices(ticker)
            for ticker in list(self.processed_tickers):
                if ticker not in tickers:
                    self.logger.info(f'{ticker} has been removed from the database. Stopping data collection.')
                    self.processed_tickers.discard(ticker)
                    self.subscribed_tickers.discard(ticker)
                    self.send_command(f'UNSB {ticker} Lv1')
                    self.send_command(f'UNSB {ticker} MINCHART 1')
                    self.send_command(f'UNSB {ticker} MINCHART 5')
            time.sleep(60)

    def handle_response(self):
        while True:
            if self.response_buffer:
                response = self.response_buffer.pop(0)
                self.logger.debug(f"Handling DAS response: {response}")
                if response.startswith("$LDLU"):
                    self.parse_ldlu_response(response)
                elif response.startswith("$Quote"):
                    self.handle_quote_data(response)
                elif response.startswith("$Bar"):
                    fields = response.split()
                    if len(fields) > 1:
                        ticker = fields[1]
                        self.parse_and_store_data(response, ticker)
            else:
                time.sleep(0.1)

    def parse_ldlu_response(self, response):
        if response and "$LDLU" in response:
            fields = response.split()
            ticker = fields[1]
            limit_up = fields[3]
            self.logger.debug(f"Parsed LDLU response: ticker = {ticker}, LU = {limit_up}")
            self.update_ldlu(ticker, limit_up)
            self.notify_trade_monitor(ticker, limit_up)

    def notify_trade_monitor(self, ticker, limit_up):
        self.logger.debug(f"Sending LU price update to TradeMonitor for {ticker}: LU = {limit_up}")
        self.trade_monitor.receive_latest_lu_price(ticker, limit_up)

    def update_ldlu(self, ticker, limit_up):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.logger.debug(f"Updating LU price in the database for {ticker}: LU = {limit_up}, Time = {timestamp}")
        with self.db_lock:
            conn = self.connect_to_db()
            cursor = conn.cursor()
            query = "UPDATE TradeParameters SET LU = ?, LU_TIME = ? WHERE TICKER = ?"
            try:
                cursor.execute(query, (limit_up, timestamp, ticker))
                conn.commit()
                self.logger.debug(f"Successfully updated LU value for {ticker}: LU={limit_up}, Time={timestamp}")
            except Exception as e:
                self.logger.error(f"Error updating LU value for {ticker}: {e}")
            finally:
                cursor.close()
                conn.close()

    def update_quote_data(self, ticker, last_price=None, high_price=None, quote_time=None, current_date=None):
        with self.db_lock:
            conn = self.connect_to_db()
            cursor = conn.cursor()
            query = """
                UPDATE TradeParameters 
                SET LAST = COALESCE(?, LAST), 
                    HIGH = COALESCE(?, HIGH), 
                    HI_TIME = COALESCE(?, HI_TIME)
                WHERE TICKER = ? AND Date = ?
            """
            try:
                cursor.execute(query, (last_price, high_price, quote_time, ticker, current_date))
                conn.commit()
                self.logger.debug(f"Updated TradeParameters for {ticker} on {current_date}: "
                                 f"LAST={last_price}, HIGH={high_price}, HI_TIME={quote_time}")
            except Exception as e:
                self.logger.error(f"Error updating TradeParameters for {ticker} on {current_date}: {e}")
            finally:
                conn.close()

    def handle_quote_data(self, quote_data):
        current_date = datetime.now().strftime('%Y-%m-%d')
        self.logger.debug(f"Handling quote data: {quote_data}")
        fields = quote_data.split()
        if len(fields) < 2 or fields[0] != '$Quote':
            self.logger.warning(f"Invalid quote data format: {quote_data}")
            return
        ticker = fields[1]
        last_price = None
        high_price = None
        quote_time = None
        for field in fields[2:]:
            if field.startswith('L:'):
                try:
                    last_price = float(field[2:])
                    last_price = round(last_price, 2)
                except ValueError:
                    self.logger.warning(f"Invalid last price format in quote: {field}")
            elif field.startswith('Hi:'):
                try:
                    high_price = float(field[3:])
                    # Not rounding high_price to match original VwapFetch
                except ValueError:
                    self.logger.warning(f"Invalid high price format in quote: {field}")
            elif field.startswith('T:'):
                quote_time = field[2:] if field[2:] else None
        if quote_time is None:
            self.logger.warning(f"No valid quote time in quote data for {ticker}: {quote_data}")
            quote_time = datetime.now().strftime('%H:%M:%S')  # Fallback
        if ticker not in self.initial_quote_data:
            self.initial_quote_data[ticker] = {
                'high_price': high_price if high_price is not None and high_price != 0 else None,  # Zero check; remove if valid
                'quote_time': quote_time
            }
            self.logger.debug(f"Stored initial quote data for {ticker}: {self.initial_quote_data[ticker]}")
        else:
            if high_price is not None and high_price != 0:  # Zero check; remove if valid
                current_high = self.initial_quote_data[ticker].get('high_price', 0)
                if current_high is None or high_price > current_high:
                    self.initial_quote_data[ticker]['high_price'] = high_price
                    self.initial_quote_data[ticker]['quote_time'] = quote_time
                    self.logger.debug(f"Updated high_price for {ticker} to {high_price} at {quote_time}")
        stored_data = self.initial_quote_data.get(ticker, {})
        high_price = stored_data.get('high_price')
        quote_time = stored_data.get('quote_time')
        self.update_quote_data(ticker, last_price, high_price, quote_time, current_date)
        if last_price is not None:
            self.logger.debug(f"Notify TradeMonitor of last price for {ticker}: {last_price}")
            self.trade_monitor.receive_latest_price(ticker, last_price)

    def update_account_equity(self):
        conn = self.connect_to_db()
        cursor = conn.cursor()
        try:
            command = "GET AccountInfo"
            self.logger.debug(f"Sending command to fetch account info: {command}")
            self.send_command(command)
            timeout = 10
            start_time = time.time()
            account_info_response = None
            while time.time() - start_time < timeout:
                for response in self.response_buffer:
                    if response.startswith("$AccountInfo"):
                        account_info_response = response
                        self.response_buffer.remove(response)
                        break
                if account_info_response:
                    break
                time.sleep(0.1)
            if not account_info_response:
                self.logger.warning("No $AccountInfo response received within timeout.")
                return
            current_date = datetime.now().strftime('%Y-%m-%d')
            query = "SELECT TICKER FROM TradeParameters WHERE Date = ?"
            cursor.execute(query, (current_date,))
            tickers = [row[0] for row in cursor.fetchall()]
            if not tickers:
                self.logger.warning(f"No tickers found for today ({current_date}) in TradeParameters.")
                return
            self.logger.debug(f"AccountInfo response: {account_info_response}")
            fields = account_info_response.split()
            account_equity = float(fields[2])
            self.logger.debug(f"Account equity to update: {account_equity}")
            for ticker in tickers:
                update_query = "UPDATE TradeParameters SET ACCOUNT_EQUITY = ? WHERE TICKER = ? AND Date = ?"
                cursor.execute(update_query, (account_equity, ticker, current_date))
                self.logger.debug(f"Updated account equity for ticker {ticker} to {account_equity}")
            conn.commit()
        except Exception as e:
            self.logger.error(f"Error fetching or updating account equity: {e}")
        finally:
            conn.close()

    def run(self):
        self.ensure_database_schema()
        threading.Thread(target=self.monitor_tickers, daemon=True).start()
        threading.Thread(target=self.handle_response, daemon=True).start()

if __name__ == "__main__":
    trade_monitor = TradeMonitor(db_path='EOD_data.db')
    vwap_fetch = VwapFetch(db_path='EOD_data.db', trade_monitor=trade_monitor)
    try:
        vwap_fetch.login()
        if vwap_fetch.connected:
            threading.Thread(target=vwap_fetch.keep_alive, daemon=True).start()
            vwap_fetch.run()
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        vwap_fetch.sock.close()