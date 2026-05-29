import psycopg2
from psycopg2 import pool
import config  # Import config.py for DB_CONFIG
import datetime
import logging
import threading
import time
import socket
import os
import queue

DAS_API_BASE_URL = config.DAS_CONFIG['host']
DAS_API_PORT = config.DAS_CONFIG['port']
DAS_API_USERNAME = config.DAS_CONFIG['username']
DAS_API_PASSWORD = config.DAS_CONFIG['password']

class VwapFetch:
    def __init__(self, db_pool, trade_monitor=None, socketio=None):
        self.logger = logging.getLogger('VwapFetch')
        self.logger.debug("Initializing VwapFetch")
        self.logger.debug(f"VwapFetch logger handlers: {self.logger.handlers}")
        self.sock = None
        self.connected = False
        self.response_buffer = queue.Queue()
        self.reconnecting = False
        self._account_info_event = threading.Event()
        self._account_info_response = None
        self._db_write_ts = {}      # ticker → last DB write timestamp
        self._db_write_interval = 0.25  # seconds; throttle DB writes per ticker
        self.trade_monitor = trade_monitor
        self.db_pool = db_pool
        self.db_lock = threading.Lock()
        self.subscribed_tickers = set()
        self.initial_quote_data = {}
        self.account_equity_dict = {}
        self.logger.debug("VwapFetch initialized")
        self.last_quote_ts = {}  # ticker → time.time()
        self.quote_timeout = 45  # sec
        self.stale_threshold = 60  # sec; increased for high-volume like HKD
        self._last_price = {}    # ticker → float
        self._last_log_ts = {}   # ticker → float
        self._last_stale_time = {}  # ticker → quote_time_str
        self.socketio = socketio
        self._equity_warned_date = None  # suppress repeated "no equity" warnings

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
                return response
            except (OSError, BrokenPipeError) as e:
                self.logger.error(f'Error receiving response: {e}')
                self.connected = False
                self.reconnect()
                return None

    def continuously_receive(self):
        buffer = ""
        while self.connected:
            try:
                chunk = self.sock.recv(4096)
                if not chunk:
                    raise ConnectionError("Socket closed")
                buffer += chunk.decode('utf-8', errors='ignore')

                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    line = line.rstrip('\r')
                    if line:
                        self.response_buffer.put(line)
            except (OSError, ConnectionError) as e:
                self.logger.error(f"Receive error: {e}")
                self.connected = False
                self.reconnect()
                return
            except Exception as e:
                self.logger.error(f"Unexpected receive error: {e}")
                time.sleep(1)

    def login(self):
        self.sock = self.create_socket()
        login_command = f'LOGIN {DAS_API_USERNAME} {DAS_API_PASSWORD} {config.get_active_account()} 0'
        self.send_command(login_command)
        while True:
            login_response = self.receive_response()
            if login_response:
                if 'LOGIN SUCCESSED' in login_response:
                    self.logger.info('Login successful')
                    self.connected = True
                    self.logger.info('API connection is now ready')
                    threading.Thread(target=self.continuously_receive, daemon=True).start()
                    # Re-subscribe and fetch LDLU
                    tickers = self.get_todays_tickers()
                    for ticker in tickers:
                        self.subscribe_to_level1(ticker)
                        time.sleep(0.1)
                        self.get_ldlu_prices(ticker)
                    self.logger.info(f"Re-subscribed and fetched LDLU for {len(tickers)} tickers")
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
        if self.reconnecting:
            return
        self.reconnecting = True
        self.logger.warning('Full reconnect starting...')

        if self.sock:
            try:
                self.sock.close()
            except:
                pass
            self.sock = None

        # Drain the queue
        while not self.response_buffer.empty():
            try:
                self.response_buffer.get_nowait()
            except queue.Empty:
                break
        self.subscribed_tickers.clear()
        self.initial_quote_data.clear()
        self.last_quote_ts.clear()
        self._last_stale_time.clear()

        time.sleep(2)

        try:
            self.sock = self.create_socket()
            self.login()
        except Exception as e:
            self.logger.error(f"Reconnect failed: {e}")
            time.sleep(5)
            self.reconnecting = False
            self.reconnect()
        else:
            self.reconnecting = False

    def get_todays_tickers(self):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            today = datetime.datetime.now().strftime('%Y-%m-%d')
            query = "SELECT ticker FROM tradeparameters WHERE date = %s"
            cursor.execute(query, (today,))
            tickers = [row[0] for row in cursor.fetchall()]

            # Update account_equity for retrieved tickers using stored equity
            if tickers and today in self.account_equity_dict:
                account_equity = self.account_equity_dict[today]
                update_query = "UPDATE tradeparameters SET account_equity = %s WHERE ticker = %s AND date = %s"
                try:
                    for ticker in tickers:
                        cursor.execute(update_query, (account_equity, ticker, today))
                    conn.commit()
                except psycopg2.Error as e:
                    self.logger.error(f"Error updating account_equity for tickers: {e}")
            else:
                if today not in self.account_equity_dict and self._equity_warned_date != today:
                    self._equity_warned_date = today
                    self.logger.warning(f"No account equity stored for {today}.")

            return tickers
        except psycopg2.Error as e:
            self.logger.error(f"Error querying tradeparameters: {e}")
            return []
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def subscribe_to_level1(self, ticker):
        if ticker not in self.subscribed_tickers:
            command = f"SB {ticker} Lv1"
            self.send_command(command)
            self.logger.debug(f"Subscribed to level 1 data for {ticker}")
            self.subscribed_tickers.add(ticker)

    def unsubscribe_to_level1(self, ticker):
        if ticker in self.subscribed_tickers:
            command = f"UNSB {ticker} Lv1"
            self.send_command(command)
            self.logger.debug(f"Unsubscribed from level 1 data for {ticker}")
            self.subscribed_tickers.remove(ticker)

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
            current_tickers_set = set(tickers)
            
            # Unsubscribe from tickers no longer in the DB
            to_unsubscribe = self.subscribed_tickers - current_tickers_set
            for ticker in to_unsubscribe:
                self.unsubscribe_to_level1(ticker)
                # Clean up internal state
                self.initial_quote_data.pop(ticker, None)
                self.last_quote_ts.pop(ticker, None)
                self._last_price.pop(ticker, None)
                self._last_log_ts.pop(ticker, None)
                self._last_stale_time.pop(ticker, None)
                self.logger.info(f"Stopped processing {ticker} as it no longer exists in the DB")
            
            # Subscribe and fetch for current tickers
            for ticker in tickers:
                if ticker not in self.subscribed_tickers:
                    self.subscribe_to_level1(ticker)
                    time.sleep(0.1)
                self.get_ldlu_prices(ticker)
                time.sleep(0.1)  # Small delay to avoid overwhelming the API
            
            time.sleep(10)

    # Periodic resubscribe (reduced to 2 min for stall mitigation)
    def _periodic_resubscribe(self):
        while True:
            time.sleep(120)  # 2 minutes
            if self.connected:
                tickers = self.get_todays_tickers()
                current_tickers_set = set(tickers)
                
                # Unsubscribe from tickers no longer in the DB (though ensure_subscriptions should handle most)
                to_unsubscribe = self.subscribed_tickers - current_tickers_set
                for ticker in to_unsubscribe:
                    self.unsubscribe_to_level1(ticker)
                    # Clean up internal state
                    self.initial_quote_data.pop(ticker, None)
                    self.last_quote_ts.pop(ticker, None)
                    self._last_price.pop(ticker, None)
                    self._last_log_ts.pop(ticker, None)
                    self._last_stale_time.pop(ticker, None)
                    self.logger.info(f"Stopped processing {ticker} as it no longer exists in the DB (periodic check)")
                
                for ticker in tickers:
                    self.unsubscribe_to_level1(ticker)
                    time.sleep(0.5)
                    self.subscribe_to_level1(ticker)
                    self.get_ldlu_prices(ticker)
                self.logger.info(f"Periodic resubscribe completed for {len(tickers)} tickers to refresh quotes")

    def handle_response(self):
        while True:
            try:
                response = self.response_buffer.get(timeout=1)
            except queue.Empty:
                continue
            if response.startswith("$Quote"):
                self.handle_quote_data(response)
            elif response.startswith("$LDLU"):
                self.parse_ldlu_response(response)
            elif response.startswith("$AccountInfo"):
                self._account_info_response = response
                self._account_info_event.set()
            elif response.startswith("#QuoteServer:"):
                self.handle_quote_server_status(response)
            else:
                self.logger.debug(f"Unhandled response: {response}")

    def handle_quote_server_status(self, response):
        if "Missing heartbeat" in response or "Lost Connection" in response:
            self.logger.warning(f"Quote server issue detected: {response}. Forcing reconnect...")
            self.connected = False
            self.reconnect()

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
        if self.trade_monitor:
            self.trade_monitor.receive_latest_lu_price(ticker, limit_up)

    def update_ldlu(self, ticker, limit_up):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            limit_up_float = float(limit_up) if limit_up.strip() else None
            query = "UPDATE tradeparameters SET lu = %s, lu_time = %s WHERE ticker = %s"
            cursor.execute(query, (limit_up_float, timestamp, ticker))
            conn.commit()
            self.logger.debug(f"Successfully updated LU value for {ticker}: LU={limit_up_float}, Time={timestamp}")
        except psycopg2.Error as e:
            self.logger.error(f"Error updating LU value for {ticker}: {e}")
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def update_quote_data(self, ticker, last_price=None, high_price=None, open_price=None, quote_time=None, current_date=None, vwap=None):
        with self.db_lock:
            try:
                conn = self.db_pool.getconn()
                cursor = conn.cursor()
                last_price_float = float(last_price) if last_price is not None else None
                high_price_float = float(high_price) if high_price is not None else None
                open_price_float = float(open_price) if open_price is not None else None
                vwap = float(vwap) if vwap is not None else None
                query = """
                    UPDATE tradeparameters 
                    SET last = COALESCE(%s, last), 
                        high = COALESCE(%s, high), 
                        open = COALESCE(%s, open),
                        hi_time = COALESCE(%s, hi_time),
                        vwap = COALESCE(%s, vwap)
                    WHERE ticker = %s AND date = %s
                """
                cursor.execute(query, (last_price_float, high_price_float, open_price_float, quote_time, vwap, ticker, current_date))
                conn.commit()
            except psycopg2.Error as e:
                self.logger.error(f"Error updating tradeparameters for {ticker} on {current_date}: {e}")
            finally:
                if conn:
                    self.db_pool.putconn(conn)

    def handle_quote_data(self, quote_data):
        now = datetime.datetime.now()
        current_date = now.strftime('%Y-%m-%d')
        fields = quote_data.split()
        if fields[0] != '$Quote':
            self.logger.warning(f"Invalid quote data format: {quote_data}")
            return
        ticker = fields[1]
        last_price = high_price = open_price = quote_time_str = vwap = None
        for field in fields[2:]:
            if field.startswith('L:'):
                last_price = round(float(field[2:]), 2)
            elif field.startswith('Hi:'):
                high_price = float(field[3:])
            elif field.startswith('op:'):
                open_price = round(float(field[3:]), 2) if field[3:] else None
            elif field.startswith('T:'):
                quote_time_str = field[2:]
            elif field.startswith('VWAP:'):
                vwap = round(float(field[5:]), 2) if field[5:] else None

        if quote_time_str:
            try:
                quote_time = datetime.datetime.strptime(f"{current_date} {quote_time_str}", '%Y-%m-%d %H:%M:%S')
                time_diff = (now - quote_time).total_seconds()
                if time_diff > self.stale_threshold:
                    # Log once per unique stale timestamp to avoid flooding
                    if ticker not in self._last_stale_time or self._last_stale_time[ticker] != quote_time_str:
                        self.logger.warning(f"Stale $Quote for {ticker}: DAS time {quote_time_str} is {time_diff:.0f}s old — accepting anyway to keep frontend live.")
                        self._last_stale_time[ticker] = quote_time_str
                    # Update last_quote_ts so watchdog knows quotes are still flowing
                    self.last_quote_ts[ticker] = time.time()
                    # Do NOT return — pass the price through so the frontend doesn't freeze
            except ValueError:
                self.logger.error(f"Invalid T: format for {ticker}: {quote_time_str}")
                return

        if ticker not in self.initial_quote_data:
            self.initial_quote_data[ticker] = {
                'high_price': high_price if high_price and high_price != 0 else None,
                'open_price': open_price if open_price and open_price != 0 else None,
                'quote_time': quote_time_str,
                'vwap': vwap if vwap and vwap != 0 else None
            }
        else:
            stored_data = self.initial_quote_data[ticker]
            if high_price and high_price != 0 and (stored_data['high_price'] is None or high_price > stored_data['high_price']):
                stored_data['high_price'] = high_price
                stored_data['quote_time'] = quote_time_str
            if open_price and open_price != 0:
                stored_data['open_price'] = open_price
            if vwap and vwap != 0:
                stored_data['vwap'] = vwap

        stored_data = self.initial_quote_data[ticker]

        # Always notify trade_monitor immediately with the freshest price
        if last_price is not None:
            if self.trade_monitor:
                self.trade_monitor.receive_latest_price(ticker, last_price)
            self.last_quote_ts[ticker] = time.time()
            
                        # Send last price to AlertManager
            if hasattr(self, 'alert_manager') and self.alert_manager:
                try:
                    self.alert_manager.receive_latest_price(ticker, last_price)
                except Exception as e:
                    self.logger.warning(f"Failed to forward price to AlertManager: {e}")

            if hasattr(self, 'stopsell_monitor') and self.stopsell_monitor:
                try:
                    self.stopsell_monitor.receive_latest_price(ticker, last_price)
                except Exception as e:
                    self.logger.warning(f"Failed to forward price to SSMonitor: {e}")

            now_ts = time.time()
            changed = ticker not in self._last_price or abs(self._last_price[ticker] - last_price) > 1e-6
            if changed or (ticker not in self._last_log_ts or now_ts - self._last_log_ts[ticker] > 30):
                self.logger.info(
                    f"{'NEW' if changed else 'STILL'} LAST PRICE | Ticker: {ticker} | Last Price: {last_price:.2f} | Updated At: {now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}"
                )
                self._last_log_ts[ticker] = now_ts
                
                if changed and self.socketio:
                    try:
                        self.socketio.emit('update_last_price', {'ticker': ticker, 'last_price': last_price})
                    except Exception as e:
                        self.logger.error(f"Error emitting update_last_price for {ticker}: {e}")


            self._last_price[ticker] = last_price

        # Throttle DB writes: max once per _db_write_interval per ticker
        now_ts = time.time()
        if now_ts - self._db_write_ts.get(ticker, 0) >= self._db_write_interval:
            self._db_write_ts[ticker] = now_ts
            self.update_quote_data(
                ticker, last_price,
                stored_data['high_price'], stored_data['open_price'],
                stored_data['quote_time'], current_date, stored_data['vwap']
            )

    def update_account_equity(self, max_attempts=5, retry_delay=5):
        # Retry loop — DAS sometimes needs a moment after login before it responds
        account_info_response = None
        for attempt in range(1, max_attempts + 1):
            if attempt > 1:
                time.sleep(retry_delay)
            self._account_info_event.clear()
            self._account_info_response = None
            self.send_command("GET AccountInfo")
            if self._account_info_event.wait(timeout=10):
                account_info_response = self._account_info_response
                break
            self.logger.warning(f"No $AccountInfo response (attempt {attempt}/{max_attempts})")

        if not account_info_response:
            self.logger.error("Failed to fetch account equity after all retries")
            return

        conn = None
        try:
            current_date = datetime.datetime.now().strftime('%Y-%m-%d')
            self.logger.debug(f"AccountInfo response: {account_info_response}")
            fields = account_info_response.split()
            account_equity = float(fields[2]) if fields[2].replace('.', '', 1).isdigit() else None
            if account_equity is None:
                self.logger.warning("Invalid account equity value received")
                return
            self.account_equity_dict[current_date] = account_equity
            self.logger.info(f"Account equity fetched: ${account_equity:,.2f}")

            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("SELECT ticker FROM tradeparameters WHERE date = %s", (current_date,))
            tickers = [row[0] for row in cursor.fetchall()]
            if not tickers:
                self.logger.warning(f"No tickers found for today ({current_date}) in tradeparameters")
                return

            for ticker in tickers:
                cursor.execute(
                    "UPDATE tradeparameters SET account_equity = %s WHERE ticker = %s AND date = %s",
                    (account_equity, ticker, current_date)
                )
            conn.commit()
        except psycopg2.Error as e:
            self.logger.error(f"Error updating account equity in DB: {e}")
        finally:
            if conn:
                self.db_pool.putconn(conn)

    def _quote_watchdog(self):
        while True:
            time.sleep(15)
            now = time.time()
            for ticker in list(self.subscribed_tickers):
                if self.connected and ticker in self.last_quote_ts and (now - self.last_quote_ts[ticker] > self.quote_timeout):
                    self.logger.warning(f"No fresh $Quote for {ticker} in {self.quote_timeout}s (last at {time.strftime('%H:%M:%S', time.localtime(self.last_quote_ts[ticker]))}). Resubscribing...")
                    self.unsubscribe_to_level1(ticker)
                    time.sleep(0.5)
                    self.subscribe_to_level1(ticker)
                    self.get_ldlu_prices(ticker)
                    self.logger.info(f"Resubscribed stale ticker {ticker} to refresh quotes")

    def run(self):
        threading.Thread(target=self.ensure_subscriptions_and_fetch_ldlu_prices, daemon=True).start()
        threading.Thread(target=self.handle_response, daemon=True).start()
        threading.Thread(target=self._quote_watchdog, daemon=True).start()
        threading.Thread(target=self._periodic_resubscribe, daemon=True).start()

if __name__ == "__main__":
    # Setup logging
    logger = logging.getLogger('VwapFetch')
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler('quoteupdate.log')
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(file_format)
    logger.addHandler(console_handler)

    # Setup database pool
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=20,
        **config.DB_CONFIG
    )
    logger.info("PostgreSQL connection pool initialized")

    # Instantiate and run VwapFetch
    vwap_fetch = VwapFetch(db_pool, None)
    vwap_fetch.login()
    if vwap_fetch.connected:
        threading.Thread(target=vwap_fetch.keep_alive, daemon=True).start()
        vwap_fetch.run()

    # Keep main thread alive
    while True:
        time.sleep(1)