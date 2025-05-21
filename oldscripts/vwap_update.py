import sqlite3
import datetime
import logging
import threading
import time
import socket
import os

DAS_API_BASE_URL = '127.0.0.1'
DAS_API_PORT = 9800
DAS_API_USERNAME = 'CD4832'
DAS_API_PASSWORD = 'Gamala123'
DAS_API_ACCOUNT = '104832'

class VwapFetch:
    def __init__(self, db_path):
        self.sock = None
        self.connected = False
        self.response_buffer = []
        self.reconnecting = False

        self.db_path = db_path

        self.logger = logging.getLogger('VwapFetch')
        logging.basicConfig(level=logging.DEBUG)  # Set to DEBUG level for detailed logs

        self.connect_to_db()

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
            if self.connected:
                try:
                    self.send_command('ECHO')
                except AttributeError:
                    logging.warning('Socket is None, skipping keep alive')
            else:
                logging.warning('Not connected, cannot send keep-alive')
            time.sleep(30)

    def reconnect(self):
        """Try to reconnect if the connection is lost."""
        logging.warning('Reconnecting...')
        self.sock = self.create_socket()
        self.login()

    def connect_to_db(self):
        abs_db_path = os.path.abspath(self.db_path)
        self.logger.debug(f"Connecting to database at: {abs_db_path}")
        self.conn = sqlite3.connect(abs_db_path)
        self.cursor = self.conn.cursor()
        self.logger.debug("Connected to database.")

    def get_todays_tickers(self):
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        query = "SELECT TICKER FROM TradeParameters WHERE Date = ?"
        self.cursor.execute(query, (today,))
        tickers = [row[0] for row in self.cursor.fetchall()]
        self.logger.debug(f"Retrieved tickers for today: {tickers}")
        return tickers

    def subscribe_to_level1(self, ticker):
        command = f"SB {ticker} Lv1"
        self.send_command(command)
        self.logger.debug(f"Subscribed to level 1 data for {ticker}")

    def handle_quote_data(self, quote_data):
        self.logger.debug(f"Handling quote data: {quote_data}")
        fields = quote_data.split()
        ticker = fields[1] if fields[0] == '$Quote' else fields[0]
        vwap = None
        for field in fields[1:]:
            if field.startswith('VWAP:'):
                vwap = float(field[5:])

        if vwap is not None:
            self.update_vwap(ticker, vwap)
        else:
            self.logger.warning(f"VWAP data missing for {ticker} from quote data: {quote_data}")

    def update_vwap(self, ticker, vwap):
        query = "UPDATE TradeParameters SET VWAP = ? WHERE TICKER = ?"
        try:
            self.cursor.execute(query, (vwap, ticker))
            self.conn.commit()
            self.logger.debug(f"Updated VWAP for {ticker} to {vwap}")
        except Exception as e:
            self.logger.warning(f"Can't update VWAP for {ticker} to {vwap}: {e}")

    def run(self):
        tickers = self.get_todays_tickers()
        for ticker in tickers:
            self.subscribe_to_level1(ticker)

        while True:
            if self.response_buffer:
                response = self.response_buffer.pop(0)
                if response.startswith("$Quote"):
                    self.handle_quote_data(response)
            time.sleep(0.1)


if __name__ == "__main__":
    vwap_fetch = VwapFetch(db_path='EOD_data.db')
    try:
        vwap_fetch.login()
        if vwap_fetch.connected:
            threading.Thread(target=vwap_fetch.keep_alive, daemon=True).start()
            vwap_fetch.run()
    except KeyboardInterrupt:
        vwap_fetch.sock.close()
