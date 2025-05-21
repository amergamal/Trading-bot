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
        self.trade_monitor = trade_monitor  # Pass the TradeMonitor instance
        self.db_path = db_path
        self.db_lock = threading.Lock()  # Ensure DB updates are thread-safe
        self.subscribed_tickers = set()  # Track tickers that are already subscribed

        self.logger = logging.getLogger('VwapFetch')
        logging.basicConfig(level=logging.DEBUG)  # Set to DEBUG level for detailed logs

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
                self.connected = False  # Mark as disconnected
                self.reconnect()  # Try to reconnect
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
                self.connected = False  # Mark as disconnected
                self.reconnect()  # Try to reconnect
                return None

    def continuously_receive(self):
        while self.connected:
            response = self.receive_response()
            if response:
                self.response_buffer.append(response)

    def login(self):
        self.sock = self.create_socket()
        login_command = f'LOGIN {DAS_API_USERNAME} {DAS_API_PASSWORD} {DAS_API_ACCOUNT} 0'
        self.send_command(login_command)
        while True:
            login_response = self.receive_response()
            if login_response:
                if 'LOGIN SUCCESSED' in login_response:
                    logging.info('Login successful')
                    if not self.connected:
                        self.connected = True

                    logging.info('API connection is now ready')

                    threading.Thread(target=self.continuously_receive, daemon=True).start()
                    # Now that login is complete, we can call update_account_equity
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
        """Try to reconnect if the connection is lost."""
        logging.warning('Reconnecting...')
        self.sock = self.create_socket()
        self.login()

    def connect_to_db(self):
        """Ensure each thread opens its own database connection."""
        abs_db_path = os.path.abspath(self.db_path)
        self.logger.debug(f"Connecting to database at: {abs_db_path}")
        return sqlite3.connect(abs_db_path)

    def get_todays_tickers(self):
        """Retrieve tickers from the database for today."""
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
        """Send a subscription command for Level 1 data."""
        if ticker not in self.subscribed_tickers:
            command = f"SB {ticker} Lv1"
            self.send_command(command)
            self.logger.debug(f"Subscribed to level 1 data for {ticker}")
            self.subscribed_tickers.add(ticker)  # Mark this ticker as subscribed

    def get_ldlu_prices(self, ticker):
        """Send a command to get LDLU prices for a given ticker."""
        try:
            command = f"GET LDLU {ticker}"
            self.logger.debug(f"Sending command to DAS to get LDLU prices for ticker {ticker}: {command}")
            self.send_command(command)
        except Exception as e:
            self.logger.error(f"Error fetching LDLU prices for {ticker}: {e}")
            
    def ensure_subscriptions_and_fetch_ldlu_prices(self):
        """Ensure continuous Level 1 subscriptions and fetch LDLU prices for all today's tickers."""
        while True:
            tickers = self.get_todays_tickers()
            for ticker in tickers:
                self.subscribe_to_level1(ticker)
                time.sleep(1)
                self.get_ldlu_prices(ticker)
            time.sleep(10)        

    def handle_response(self):
        """Continuously handle responses and parse LDLU prices."""
        while True:
            response = self.receive_response()
            if response:
                self.logger.debug(f"Handling DAS response: {response}")
                if response.startswith("$LDLU"):
                    self.parse_ldlu_response(response)
                elif response.startswith("$Quote"):
                    self.handle_quote_data(response)

    def parse_ldlu_response(self, response):
        """Parse the LDLU response and update the LU price in the database."""
        if response and "$LDLU" in response:
            fields = response.split()
            ticker = fields[1]
            limit_up = fields[3]
            self.logger.debug(f"Parsed LDLU response: ticker = {ticker}, LU = {limit_up}")
            
            
            
            self.update_ldlu(ticker, limit_up)
            
            # Also send the new LU price to TradeMonitor
            self.notify_trade_monitor(ticker, limit_up)
            
            
    def notify_trade_monitor(self, ticker, limit_up):
        """Send the updated LU price to the TradeMonitor module."""
        self.logger.debug(f"Sending LU price update to TradeMonitor for {ticker}: LU = {limit_up}")
        
        # Notify the TradeMonitor with the new LU price
        self.trade_monitor.receive_latest_lu_price(ticker, limit_up)        

    def update_ldlu(self, ticker, limit_up):
        """Update LU values in the database for the given ticker."""
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
                
    def update_last_price(self, ticker, last_price):
        last_price = round(last_price, 2)
        conn = self.connect_to_db()
        cursor = conn.cursor()
        query = "UPDATE TradeParameters SET LAST = ? WHERE TICKER = ?"
        try:
            cursor.execute(query, (last_price, ticker))
            conn.commit()
            self.logger.debug(f"Updated LAST price for {ticker} to {last_price}")
        except Exception as e:
            self.logger.warning(f"can't update LAST price for {ticker} to {last_price}: {e}")            
        finally:
            conn.close()    

    def handle_quote_data(self, quote_data):
        """Handle and update VWAP data based on the quote data."""
        self.logger.debug(f"Handling quote data: {quote_data}")
        fields = quote_data.split()
        ticker = fields[1] if fields[0] == '$Quote' else fields[0]
        vwap = None
        last_price = None
        
        for field in fields[1:]:
            if field.startswith('L:'):
                last_price = float(field[2:])
                last_price = round(last_price, 2)  # Round last price to 2 decimal places
            elif field.startswith('VWAP:'):  # Assuming VWAP is provided with this prefix
                vwap = float(field[5:])    
        
        if last_price is not None:
            self.update_last_price(ticker, last_price)
        else:
            self.logger.warning(f"Failed to update LAST price for {ticker} from quote data: {quote_data}")
            
        if vwap is not None:
            self.update_vwap(ticker, vwap)  # Call a new method to update VWAP
        else:
            self.logger.warning(f"VWAP data missing for {ticker} from quote data: {quote_data}")

    def update_vwap(self, ticker, vwap):
        """Update VWAP value for a given ticker."""
        conn = self.connect_to_db()
        cursor = conn.cursor()
        query = "UPDATE TradeParameters SET VWAP = ? WHERE TICKER = ?"
        self.logger.debug(f"Updating VWAP for ticker {ticker} to {vwap}")
        try:
            cursor.execute(query, (vwap, ticker))
            conn.commit()
            self.logger.debug(f"Updated VWAP for {ticker} to {vwap}")
        except Exception as e:
            self.logger.error(f"Error updating VWAP for {ticker}: {e}")
        finally:
            conn.close()

    def update_account_equity(self):
        """Fetch the account equity and update the TradeParameters table."""
        conn = self.connect_to_db()
        cursor = conn.cursor()

        try:
            command = "GET AccountInfo"
            self.logger.debug(f"Sending command to fetch account info: {command}")
            self.send_command(command)
            response = self.receive_response()
            responses = response.splitlines()
            
            # Get the current date in 'yyyy-mm-dd' format
            current_date = datetime.datetime.now().strftime('%Y-%m-%d')
            
            # Fetch all tickers, including newly added ones
            query = "SELECT TICKER FROM TradeParameters WHERE Date = ?"
            cursor.execute(query, (current_date,))
            tickers = cursor.fetchall()
            
            if not tickers:
                self.logger.warning(f"No tickers found for today ({current_date}) in TradeParameters.")
                return

            for line in responses:
                self.logger.debug(f"AccountInfo line response: {line}")
                if line.startswith("#"):
                    self.logger.debug("Skipping header line")
                    continue
                if line.startswith("$AccountInfo"):
                    fields = line.split()
                    account_equity = float(fields[2])
                    self.logger.debug(f"Account equity to update: {account_equity}")
                    
                    

                    for ticker in tickers:
                        ticker = ticker[0]  # Get the ticker symbol
                        update_query = "UPDATE TradeParameters SET ACCOUNT_EQUITY = ? WHERE TICKER = ? AND Date = ?"
                        cursor.execute(update_query, (account_equity, ticker, current_date))
                        self.logger.debug(f"Updated account equity for ticker {ticker} to {account_equity}")
                
                    conn.commit()  # Commit after updating all tickers for today
        except Exception as e:
            self.logger.error(f"Error fetching or updating account equity: {e}")
        finally:
            conn.close()
            
    def update_account_equity_periodically(self):
        """Periodically fetch the account equity and update the TradeParameters table."""
        while True:
             self.update_account_equity()
             time.sleep(30)  # Update account equity every 30 seconds
        

    def run(self):
        """Main method to start TickerMonitor processes."""
        # Start threads for subscriptions and handling responses
        threading.Thread(target=self.ensure_subscriptions_and_fetch_ldlu_prices, daemon=True).start()
        threading.Thread(target=self.handle_response, daemon=True).start()
        # Start the periodic account equity update
        threading.Thread(target=self.update_account_equity_periodically, daemon=True).start()

        # The script will keep running because the threads run in a loop


if __name__ == "__main__":
    
    # Create the TradeMonitor instance
    trade_monitor = TradeMonitor(db_path='EOD_data.db')  # You can pass any required arguments
    # Pass the trade_monitor instance to VwapFetch
    vwap_fetch = VwapFetch(db_path='EOD_data.db', trade_monitor=trade_monitor)
    
    try:
        vwap_fetch.login()
        if vwap_fetch.connected:
            threading.Thread(target=vwap_fetch.keep_alive, daemon=True).start()
            
            
            
            vwap_fetch.run()
            # Keep the main thread running
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        vwap_fetch.sock.close()
