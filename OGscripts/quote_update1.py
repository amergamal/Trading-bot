import sqlite3
import datetime
import logging
import threading
import time
import socket
import os
import selectors
from trade_monitor import TradeMonitor

# Configuration
DAS_API_BASE_URL = '127.0.0.1'
DAS_API_PORT = 9800
DAS_API_USERNAME = 'CD4832'
DAS_API_PASSWORD = 'Gamala123'
DAS_API_ACCOUNT = '104832'
KEEP_ALIVE_INTERVAL = 30
RECONNECT_TIMEOUT = 5
MAX_RECONNECT_DELAY = 60
SUBSCRIPTION_INTERVAL = 10
ACCOUNT_EQUITY_INTERVAL = 300  # 5 minutes

# Shared database lock (compatible with app.py, APIConnection, EndOfDay, TradeMonitor, SLMonitor)
db_lock = threading.Lock()

logging.basicConfig(level=logging.DEBUG)

class VwapFetch:
    def __init__(self, db_path, trade_monitor):
        self.sock = None
        self.connected = False
        self.selector = selectors.DefaultSelector()
        self.response_buffer = []
        self.reconnecting = False
        self.trade_monitor = trade_monitor
        self.db_path = db_path
        self.db_conn = None  # Initialize in continuously_receive
        self.subscribed_tickers = set()
        self.initial_quote_data = {}  # {ticker: {'open_price': float, 'high_price': float, 'quote_time': str, 'previous_close': float}}
        self.last_echo_time = time.time()
        self.last_subscription_time = time.time()
        self.last_equity_time = time.time()
        
        self.logger = logging.getLogger('VwapFetch')
        self.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def create_socket(self):
        self.logger.debug(f'Creating socket for {DAS_API_BASE_URL}:{DAS_API_PORT}...')
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((DAS_API_BASE_URL, DAS_API_PORT))
        self.logger.debug('Connection established')
        return s

    def send_command(self, command):
        if self.sock:
            try:
                full_command = f'{command}\r\n'
                self.logger.debug(f'Sending command to DAS: {full_command}')
                self.sock.sendall(full_command.encode())
                if command == 'ECHO':
                    self.last_echo_time = time.time()
            except (OSError, BrokenPipeError) as e:
                self.logger.error(f'Error sending command: {e}')
                self.handle_disconnection()

    def receive_response(self):
        try:
            response = b""
            events = self.selector.select(timeout=1)
            for key, _ in events:
                part = key.fileobj.recv(4096)
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
            self.handle_disconnection()
            return None

    def handle_disconnection(self):
        if self.sock:
            self.selector.unregister(self.sock)
            self.sock.close()
        self.sock = None
        if self.connected:
            self.connected = False
        if self.db_conn:
            self.db_conn.close()
            self.db_conn = None
        if not self.reconnecting:
            self.reconnect()

    def continuously_receive(self):
        # Initialize database connection in this thread
        if self.db_conn is None:
            self.db_conn = sqlite3.connect(self.db_path, timeout=10)
            self.db_conn.row_factory = sqlite3.Row
            self.logger.debug(f"Initialized db_conn in continuously_receive thread: {threading.get_ident()}")
            # Share db_conn with TradeMonitor
            self.trade_monitor.set_db_conn(self.db_conn)

        while self.connected:
            # Send ECHO if 30 seconds have passed
            if time.time() - self.last_echo_time > KEEP_ALIVE_INTERVAL:
                self.send_command('ECHO')
            
            # Handle subscriptions and LDLU prices every 10 seconds
            if time.time() - self.last_subscription_time > SUBSCRIPTION_INTERVAL:
                self.ensure_subscriptions_and_fetch_ldlu_prices()
                self.last_subscription_time = time.time()
            
            # Update account equity every 5 minutes
            if time.time() - self.last_equity_time > ACCOUNT_EQUITY_INTERVAL:
                self.update_account_equity()
                self.last_equity_time = time.time()
            
            response = self.receive_response()
            if response:
                for line in response.splitlines():
                    if line.strip():
                        self.response_buffer.append(line)
                        if len(self.response_buffer) > 1000:
                            self.response_buffer = self.response_buffer[-500:]
                        self.handle_response(line)

    def login(self):
        self.sock = self.create_socket()
        self.sock.setblocking(False)
        self.selector.register(self.sock, selectors.EVENT_READ)
        login_command = f'LOGIN {DAS_API_USERNAME} {DAS_API_PASSWORD} {DAS_API_ACCOUNT} 0'
        self.send_command(login_command)
        start_time = time.time()
        while time.time() - start_time < 30:
            response = self.receive_response()
            if response:
                if 'LOGIN SUCCESSED' in response:
                    self.logger.info('Login successful')
                    self.connected = True
                    self.last_echo_time = time.time()
                    threading.Thread(target=self.continuously_receive, daemon=True).start()
                    return
                elif '#Welcome to DAS Command API' in response:
                    self.logger.info('Welcome message received...')
                elif '#Please login to continue.' in response:
                    self.logger.warning('Retrying login...')
                    self.send_command(login_command)
                else:
                    self.logger.error(f'Unexpected login response: {response}')
            time.sleep(0.1)
        self.logger.error('Login timeout')
        self.handle_disconnection()

    def reconnect(self):
        self.logger.info('Attempting to reconnect...')
        self.reconnecting = True
        retry_delay = RECONNECT_TIMEOUT
        while not self.connected and retry_delay <= MAX_RECONNECT_DELAY:
            try:
                self.sock = self.create_socket()
                self.login()
                break
            except Exception as e:
                self.logger.error(f'Reconnection failed: {e}')
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, MAX_RECONNECT_DELAY)
        self.reconnecting = False

    def get_todays_tickers(self):
        with db_lock:
            # Create a temporary connection for main thread
            conn = sqlite3.connect(self.db_path, timeout=10)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            today = datetime.datetime.now().strftime('%Y-%m-%d')
            cursor.execute("SELECT TICKER FROM TradeParameters WHERE Date = ?", (today,))
            tickers = [row[0] for row in cursor.fetchall()]
            self.logger.debug(f"Retrieved tickers for today: {tickers}")
            conn.close()
            return tickers

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

    def ensure_subscriptions_and_fetch_ldlu_prices(self):
        tickers = self.get_todays_tickers()
        for ticker in tickers:
            self.subscribe_to_level1(ticker)
            time.sleep(1)  # Avoid overwhelming DAS API
            self.get_ldlu_prices(ticker)

    def handle_response(self, response):
        self.logger.debug(f"Handling DAS response: {response}")
        if response.startswith("$LDLU"):
            self.parse_ldlu_response(response)
        elif response.startswith("$Quote"):
            self.handle_quote_data(response)
        elif response.startswith("$AccountInfo"):
            self.parse_account_info_response(response)

    def parse_ldlu_response(self, response):
        fields = response.split()
        if len(fields) < 4:
            self.logger.warning(f"Invalid $LDLU response: {response}")
            return
        ticker = fields[1]
        limit_up = fields[3]
        self.logger.debug(f"Parsed LDLU response: ticker = {ticker}, LU = {limit_up}")
        self.update_ldlu(ticker, limit_up)
        self.notify_trade_monitor(ticker, limit_up)

    def parse_account_info_response(self, response):
        fields = response.split()
        if len(fields) < 3:
            self.logger.warning(f"Invalid $AccountInfo response: {response}")
            return
        account_equity = float(fields[2])
        self.logger.debug(f"Parsed AccountInfo response: account_equity = {account_equity}")
        self.update_account_equity_value(account_equity)

    def notify_trade_monitor(self, ticker, limit_up):
        self.logger.debug(f"Sending LU price update to TradeMonitor for {ticker}: LU = {limit_up}")
        self.trade_monitor.receive_latest_lu_price(ticker, limit_up)

    def update_ldlu(self, ticker, limit_up):
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with db_lock:
            cursor = self.db_conn.cursor()
            query = "UPDATE TradeParameters SET LU = ?, LU_TIME = ? WHERE TICKER = ?"
            try:
                cursor.execute(query, (limit_up, timestamp, ticker))
                self.db_conn.commit()
                self.logger.debug(f"Updated LU value for {ticker}: LU={limit_up}, Time={timestamp}")
            except sqlite3.Error as e:
                self.logger.error(f"Error updating LU value for {ticker}: {e}")

    def update_quote_data(self, ticker, last_price=None, vwap=None, rvol=None, high_price=None, quote_time=None, previous_close=None, current_date=None):
        with db_lock:
            cursor = self.db_conn.cursor()
            query = """
                UPDATE TradeParameters 
                SET LAST = COALESCE(?, LAST), 
                    VWAP = COALESCE(?, VWAP), 
                    RVOL = COALESCE(?, RVOL), 
                    HIGH = COALESCE(?, HIGH), 
                    HI_TIME = COALESCE(?, HI_TIME), 
                    PCL = COALESCE(?, PCL)
                WHERE TICKER = ? AND Date = ?
            """
            try:
                cursor.execute(query, (last_price, vwap, rvol, high_price, quote_time, previous_close, ticker, current_date))
                self.db_conn.commit()
                self.logger.debug(f"Updated TradeParameters for {ticker} on {current_date}: "
                                 f"LAST={last_price}, VWAP={vwap}, RVOL={rvol}, "
                                 f"HIGH={high_price}, HI_TIME={quote_time}, PCL={previous_close}")
            except sqlite3.Error as e:
                self.logger.error(f"Error updating TradeParameters for {ticker} on {current_date}: {e}")

    def update_gap(self, ticker, open_price, previous_close, current_date):
        if open_price is not None and previous_close is not None and previous_close != 0:
            try:
                gap = round(((open_price - previous_close) / previous_close) * 100)
                with db_lock:
                    cursor = self.db_conn.cursor()
                    query = "UPDATE TradeParameters SET GAP = ? WHERE TICKER = ? AND Date = ?"
                    cursor.execute(query, (gap, ticker, current_date))
                    self.db_conn.commit()
                    self.logger.debug(f"Updated GAP for {ticker} on {current_date} to {gap}%")
            except sqlite3.Error as e:
                self.logger.error(f"Error updating GAP for {ticker} on {current_date}: {e}")

    def update_account_equity_value(self, account_equity):
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
        with db_lock:
            cursor = self.db_conn.cursor()
            query = "SELECT TICKER FROM TradeParameters WHERE Date = ?"
            cursor.execute(query, (current_date,))
            tickers = [row[0] for row in cursor.fetchall()]
            if not tickers:
                self.logger.warning(f"No tickers found for today ({current_date}) in TradeParameters.")
                return
            update_query = "UPDATE TradeParameters SET ACCOUNT_EQUITY = ? WHERE TICKER = ? AND Date = ?"
            for ticker in tickers:
                cursor.execute(update_query, (account_equity, ticker, current_date))
                self.logger.debug(f"Updated account equity for ticker {ticker} to {account_equity}")
            self.db_conn.commit()

    def update_account_equity(self):
        command = "GET AccountInfo"
        self.logger.debug(f"Sending command to fetch account info: {command}")
        self.send_command(command)

    def handle_quote_data(self, quote_data):
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
        fields = quote_data.split()
        if fields[0] != '$Quote':
            self.logger.warning(f"Invalid quote data format: {quote_data}")
            return
        ticker = fields[1]
        last_price = vwap = rvol = high_price = quote_time = previous_close = open_price = None
        for field in fields[2:]:
            if field.startswith('L:'):
                last_price = float(field[2:])
                last_price = round(last_price, 2)
            elif field.startswith('VWAP:'):
                vwap = float(field[5:])
            elif field.startswith('RVOL:'):
                rvol = float(field[5:])
            elif field.startswith('Hi:'):
                high_price = float(field[3:])
            elif field.startswith('T:'):
                quote_time = field[2:]
            elif field.startswith('ycl:'):
                previous_close = float(field[4:])
            elif field.startswith('op:'):
                open_price = float(field[3:])

        if ticker not in self.initial_quote_data:
            self.initial_quote_data[ticker] = {
                'open_price': open_price if open_price is not None and open_price != 0 else None,
                'high_price': high_price if high_price is not None and high_price != 0 else None,
                'quote_time': quote_time,
                'previous_close': previous_close if previous_close is not None and previous_close != 0 else None
            }
            self.logger.debug(f"Stored initial quote data for {ticker}: {self.initial_quote_data[ticker]}")
        else:
            if high_price is not None and high_price != 0:
                current_high = self.initial_quote_data[ticker].get('high_price', 0)
                if current_high is None or high_price > current_high:
                    self.initial_quote_data[ticker]['high_price'] = high_price
                    self.initial_quote_data[ticker]['quote_time'] = quote_time
                    self.logger.debug(f"Updated high_price for {ticker} to {high_price} at {quote_time}")
            if open_price is not None and open_price != 0 and self.initial_quote_data[ticker]['open_price'] is None:
                self.initial_quote_data[ticker]['open_price'] = open_price
                self.logger.debug(f"Set open_price for {ticker} to {open_price}")
            if previous_close is not None and previous_close != 0 and self.initial_quote_data[ticker]['previous_close'] is None:
                self.initial_quote_data[ticker]['previous_close'] = previous_close
                self.logger.debug(f"Set previous_close for {ticker} to {previous_close}")

        stored_data = self.initial_quote_data.get(ticker, {})
        high_price = stored_data.get('high_price')
        quote_time = stored_data.get('quote_time')
        previous_close = stored_data.get('previous_close')
        open_price = stored_data.get('open_price')

        self.update_quote_data(ticker, last_price, vwap, rvol, high_price, quote_time, previous_close, current_date)
        self.update_gap(ticker, open_price, previous_close, current_date)

    def shutdown(self):
        if self.sock:
            self.selector.unregister(self.sock)
            self.sock.close()
        if self.db_conn:
            self.db_conn.close()
        self.logger.info("VwapFetch shutdown complete")

    def run(self):
        self.login()
        if self.connected:
            self.ensure_subscriptions_and_fetch_ldlu_prices()

if __name__ == "__main__":
    trade_monitor = TradeMonitor(db_path='EOD_data.db')
    vwap_fetch = VwapFetch(db_path='EOD_data.db', trade_monitor=trade_monitor)
    try:
        vwap_fetch.run()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        vwap_fetch.shutdown()