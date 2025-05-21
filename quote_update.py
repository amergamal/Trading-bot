import sqlite3
import datetime
import logging
import threading
import time
import socket
import os
from trade_monitor import TradeMonitor

DAS_API_BASE_URL = '127.0.0.1'
DAS_API_PORT = 9800
DAS_API_USERNAME = 'CD4832'
DAS_API_PASSWORD = 'Gamala123'
DAS_API_ACCOUNT = '104832'

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
        self.logger = logging.getLogger('VwapFetch')
        logging.basicConfig(level=logging.DEBUG)

    def create_socket(self):
        logging.debug('Creating socket...')
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((DAS_API_BASE_URL, DAS_API_PORT))
        logging.debug('Socket connected to DAS API.')
        return s

    def send_command(self, command):
        if self.sock:
            try:
                full_command = f'{command}\r\n'
                logging.debug(f'Sending command to DAS: {full_command}')
                self.sock.sendall(full_command.encode())
            except (OSError, BrokenPipeError) as e:
                logging.error(f'Error sending command: {e}')
                self.connected = False
                self.reconnect()
        else:
            logging.warning('Socket is None, cannot send command')

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
                logging.debug(f'Received response: {response}')
                return response
            except (OSError, BrokenPipeError) as e:
                logging.error(f'Error receiving response: {e}')
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
                    logging.info('Login successful')
                    self.connected = True
                    logging.info('API connection is now ready')
                    threading.Thread(target=self.continuously_receive, daemon=True).start()
                    self.update_account_equity()
                    break
                elif '#Welcome to DAS Command API' in login_response:
                    logging.info('Welcome message received, waiting for login success...')
                elif '#Please login to continue.' in login_response:
                    logging.warning('Received prompt to login again, retrying...')
                    self.send_command(login_command)
                else:
                    logging.error(f'Unexpected login response: {login_response}')
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
                    logging.warning('Socket is None, skipping keep alive')
            else:
                logging.warning('Socket is None, skipping keep alive')

    def reconnect(self):
        logging.warning('Reconnecting...')
        self.sock = self.create_socket()
        self.login()

    def connect_to_db(self):
        abs_db_path = os.path.abspath(self.db_path)
        self.logger.debug(f"Connecting to database at: {abs_db_path}")
        return sqlite3.connect(abs_db_path)

    def get_todays_tickers(self):
        conn = self.connect_to_db()
        cursor = conn.cursor()
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        query = "SELECT TICKER FROM TradeParameters WHERE Date = ?"
        self.logger.debug(f"Executing query to get today's tickers: {query} with Date = {today}")
        cursor.execute(query, (today,))
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
        while True:
            tickers = self.get_todays_tickers()
            for ticker in tickers:
                self.subscribe_to_level1(ticker)
                time.sleep(1)
                self.get_ldlu_prices(ticker)
            time.sleep(10)

    def handle_response(self):
        while True:
            if self.response_buffer:
                response = self.response_buffer.pop(0)
                self.logger.debug(f"Handling DAS response: {response}")
                if response.startswith("$LDLU"):
                    self.parse_ldlu_response(response)
                elif response.startswith("$Quote"):
                    self.handle_quote_data(response)
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
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
        self.logger.debug(f"Handling quote data: {quote_data}")
        fields = quote_data.split()
        if fields[0] != '$Quote':
            self.logger.warning(f"Invalid quote data format: {quote_data}")
            return
        ticker = fields[1]
        last_price = None
        high_price = None
        quote_time = None
        for field in fields[2:]:
            if field.startswith('L:'):
                last_price = float(field[2:])
                last_price = round(last_price, 2)
            elif field.startswith('Hi:'):
                high_price = float(field[3:])
            elif field.startswith('T:'):
                quote_time = field[2:]
        if ticker not in self.initial_quote_data:
            self.initial_quote_data[ticker] = {
                'high_price': high_price if high_price is not None and high_price != 0 else None,
                'quote_time': quote_time
            }
            self.logger.debug(f"Stored initial quote data for {ticker}: {self.initial_quote_data[ticker]}")
        else:
            if high_price is not None and high_price != 0:
                current_high = self.initial_quote_data[ticker].get('high_price', 0)
                if current_high is None or high_price > current_high:
                    self.initial_quote_data[ticker]['high_price'] = high_price
                    self.initial_quote_data[ticker]['quote_time'] = quote_time
                    self.logger.debug(f"Updated high_price for {ticker} to {high_price} at {quote_time}")
        stored_data = self.initial_quote_data.get(ticker, {})
        high_price = stored_data.get('high_price')
        quote_time = stored_data.get('quote_time')
        self.update_quote_data(ticker, last_price, high_price, quote_time, current_date)
        # Notify TradeMonitor of last_price update
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
            current_date = datetime.datetime.now().strftime('%Y-%m-%d')
            query = "SELECT TICKER FROM TradeParameters WHERE Date = ?"
            cursor.execute(query, (current_date,))
            tickers = cursor.fetchall()
            if not tickers:
                self.logger.warning(f"No tickers found for today ({current_date}) in TradeParameters.")
                return
            self.logger.debug(f"AccountInfo response: {account_info_response}")
            fields = account_info_response.split()
            account_equity = float(fields[2])
            self.logger.debug(f"Account equity to update: {account_equity}")
            for ticker in tickers:
                ticker = ticker[0]
                update_query = "UPDATE TradeParameters SET ACCOUNT_EQUITY = ? WHERE TICKER = ? AND Date = ?"
                cursor.execute(update_query, (account_equity, ticker, current_date))
                self.logger.debug(f"Updated account equity for ticker {ticker} to {account_equity}")
            conn.commit()
        except Exception as e:
            self.logger.error(f"Error fetching or updating account equity: {e}")
        finally:
            conn.close()

    def run(self):
        threading.Thread(target=self.ensure_subscriptions_and_fetch_ldlu_prices, daemon=True).start()
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