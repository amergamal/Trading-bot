import socket
import threading
import time
import logging
from collections import defaultdict, deque
import pyttsx3  # Text-to-Speech library
import sqlite3
import datetime



# DAS API credentials
DAS_API_BASE_URL = '127.0.0.1'
DAS_API_PORT = 9800
DAS_API_USERNAME = 'CD4832'
DAS_API_PASSWORD = 'Gamala123'
DAS_API_ACCOUNT = '104832'

class DASApiClient:
    def __init__(self, db_path="EOD_data.db"):
        self.sock = None
        self.connected = False
        self.logger = logging.getLogger("DASApiClient")
        logging.basicConfig(level=logging.INFO)
        self.cumulative_volume = defaultdict(int)  # Track cumulative volume for each ticker
        self.trade_flow = defaultdict(lambda: {"buy": 0, "sell": 0})  # Track buy/sell trades
        self.price_history = defaultdict(deque)  # Track short-term price changes
        self.db_path = db_path  # Path to the database

        # Initialize TTS engine
        self.tts_engine = pyttsx3.init()
        self.tts_engine.setProperty("rate", 150)  # Set speech rate
        self.tts_engine.setProperty("volume", 1.0)  # Set volume (0.0 to 1.0)

    def speak(self, message):
        """Speak a message using TTS."""
        self.tts_engine.say(message)
        self.tts_engine.runAndWait()

    def create_socket(self):
        """Create and connect the socket to DAS API."""
        self.logger.debug("Creating socket...")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((DAS_API_BASE_URL, DAS_API_PORT))
        self.logger.debug("Socket connected to DAS API.")
        return s

    def send_command(self, command):
        """Send a command to DAS API."""
        try:
            full_command = f"{command}\r\n"
            self.logger.debug(f"Sending command: {full_command.strip()}")
            self.sock.sendall(full_command.encode())
        except Exception as e:
            self.logger.error(f"Error sending command: {e}")
            self.connected = False
            self.reconnect()

    def receive_response(self):
        """Receive response from DAS API."""
        try:
            response = b""
            while True:
                part = self.sock.recv(4096)
                if not part:
                    raise Exception("Disconnected from server")
                response += part
                if len(part) < 4096:
                    break
            response = response.decode()
            return response
        except Exception as e:
            self.logger.error(f"Error receiving response: {e}")
            self.connected = False
            self.reconnect()

    def login(self):
        """Log in to DAS API."""
        self.sock = self.create_socket()
        login_command = f"LOGIN {DAS_API_USERNAME} {DAS_API_PASSWORD} {DAS_API_ACCOUNT} 0"
        self.send_command(login_command)
        while True:
            response = self.receive_response()
            if "LOGIN SUCCESSED" in response:
                self.logger.info("Login successful.")
                self.connected = True
                threading.Thread(target=self.keep_alive, daemon=True).start()
                break
            elif "LOGIN FAILED" in response:
                self.logger.error("Login failed.")
                self.sock.close()
                break

    def keep_alive(self):
        """Send keep-alive commands to DAS API."""
        while self.connected:
            self.send_command("ECHO")
            time.sleep(30)

    def subscribe_to_tms_and_level2(self, ticker):
        """Subscribe to TMS and Level 2 data for a ticker."""
        self.send_command(f"SB {ticker} tms")
        self.send_command(f"SB {ticker} Lv2")
        self.logger.info(f"Subscribed to TMS and Level 2 for {ticker}.")
        self.cumulative_volume[ticker] = 0  # Initialize cumulative volume
        self.price_history[ticker] = deque(maxlen=10)  # Track last 10 prices
        
    def get_todays_tickers(self):
        """Retrieve tickers from the database for today."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            today = datetime.datetime.now().strftime('%Y-%m-%d')
            query = "SELECT TICKER FROM TradeParameters WHERE Date = ?"
            self.logger.debug(f"Executing query to get today's tickers: {query} with Date = {today}")
            cursor.execute(query, (today,))
            tickers = [row[0] for row in cursor.fetchall()]
            self.logger.info(f"Retrieved tickers for today: {tickers}")
            conn.close()
            return tickers
        except Exception as e:
            self.logger.error(f"Error fetching today's tickers: {e}")
            return []    

    def detect_selling_pressure(self, response):
        """
        Detect signs of selling pressure based on Level 2 and TMS data.
        """
        lines = response.splitlines()
        for line in lines:
            try:
                if line.startswith("$Lv2"):
                    # Parse Level 2 data
                    parts = line.split()
                    ticker = parts[1]
                    condition = parts[2]
                    price = float(parts[4])
                    size = int(parts[5])
                    
                    # Update order book
                    if condition == "Ask":
                        self.order_book[ticker]["ask"][price] += size
                    elif condition == "Bid":
                        self.order_book[ticker]["bid"][price] += size

                    # Detect large ask walls
                    if condition == "Ask" and size > 1000:
                        message = f"Selling pressure: Large ask wall detected for {ticker} at {price}, size {size}."
                        self.logger.warning(message)
                        self.speak(message)
                        
                    # Detect absorption in bids
                    if condition == "Bid" and size > 2000:
                        bid_total = sum(self.order_book[ticker]["bid"].values())
                        ask_total = sum(self.order_book[ticker]["ask"].values())
                        if bid_total > ask_total * 1.5:  # Absorption threshold
                            message = f"Absorption: Strong bids absorbing selling for {ticker} at {price}, bid total {bid_total}."
                            self.logger.warning(message)
                            self.speak(message)    
                        
                    # Detect bid stack reinforcement (short trap clue)
                    if condition == "Bid" and size > 1000:
                        message = f"Short trap: Large bid stack detected for {ticker} at {price}, size {size}."
                        self.logger.warning(message)
                        self.speak(message)
                            
                elif line.startswith("$T&S"):
                    # Parse TMS data
                    parts = line.split()
                    ticker = parts[1]
                    price = float(parts[2])
                    size = int(parts[3])
                    flag = parts[7]
                    condition = int(parts[8], 16)  # Convert condition to integer

                    # Update cumulative volume
                    self.cumulative_volume[ticker] += size

                    # Update trade flow
                    if flag == "B":  # Buy trade
                        self.trade_flow[ticker]["buy"] += size
                    elif flag == "S":  # Sell trade
                        self.trade_flow[ticker]["sell"] += size
                        
                        
                        

                    # Track price movement
                    self.price_history[ticker].append(price)

                    # Detect large sell trades at or below bid price
                    if flag == "S" and (condition & 0x04 or condition & 0x08) and size > 1000:  # At or beyond bid
                        message = f"Selling pressure: Large sell trade detected for {ticker}, {size} shares at {price}."
                        self.logger.warning(message)
                        self.speak(message)

                    # Detect trade imbalance
                    sell_flow = self.trade_flow[ticker]["sell"]
                    buy_flow = self.trade_flow[ticker]["buy"]
                    if sell_flow > buy_flow * 1.5:  # Imbalance threshold
                        message = f"Selling pressure: Trade imbalance detected for {ticker} - Sell: {sell_flow}, Buy: {buy_flow}."
                        self.logger.warning(message)
                        self.speak(message)

                    # Detect short-term momentum
                    if len(self.price_history[ticker]) == 10:  # Check last 10 prices
                        min_price = min(self.price_history[ticker])
                        max_price = max(self.price_history[ticker])
                        if price < min_price and self.cumulative_volume[ticker] > 50_000:
                            message = f"Selling pressure: Short-term momentum shift for {ticker}, price dropped from {max_price} to {price}."
                            self.logger.warning(message)
                            self.speak(message)
                            
                    # Detect absorption of selling pressure (short trap clue)
                    if flag == "S" and size > 1000 and self.cumulative_volume[ticker] > 50_000:
                        message = f"Short trap: Absorption detected for {ticker}, price holding at {price} despite heavy selling."
                        self.logger.warning(message)
                        self.speak(message)

                    # Detect shift in trade imbalance (short trap clue)
                    if buy_flow > sell_flow * 1.5:  # Surge in aggressive buys
                        message = f"Short trap: Trade imbalance shift for {ticker} - Buy: {buy_flow}, Sell: {sell_flow}."
                        self.logger.warning(message)
                        self.speak(message)   
                        
                    # Detect price momentum
                    self.price_history[ticker].append(price)
                    if len(self.price_history[ticker]) == 10:  # Check last 10 prices
                        min_price = min(self.price_history[ticker])
                        max_price = max(self.price_history[ticker])
                        if price > max_price:
                            message = f"Short trap: Momentum shift for {ticker}, price breaking upwards from {min_price} to {price}."
                            self.logger.warning(message)
                            self.speak(message)         
                            
            except Exception as e:
                self.logger.error(f"Error parsing line: {line}. Error: {e}")

    def continuously_receive(self):
        """Continuously receive and process responses."""
        while self.connected:
            response = self.receive_response()
            if response:
                self.detect_selling_pressure(response)

    def reconnect(self):
        """Reconnect to DAS API in case of disconnection."""
        self.logger.warning("Attempting to reconnect...")
        time.sleep(5)
        self.login()

    def run(self):
        """Run the DAS API client with dynamic ticker recheck."""
        self.login()
        if self.connected:
            while True:
                tickers = self.get_todays_tickers()
                if not tickers:
                    self.logger.info("No tickers found in the database. Waiting for 10 seconds...")
                    time.sleep(10)
                    continue

                for ticker in tickers:
                    self.subscribe_to_tms_and_level2(ticker)

                self.logger.info(f"Monitoring tickers: {tickers}")
                threading.Thread(target=self.continuously_receive, daemon=True).start()
                break


if __name__ == "__main__":
    

    das_client = DASApiClient()
    try:
        das_client.run()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        if das_client.sock:
            das_client.sock.close()
        logging.info("Exiting DAS API client.")
