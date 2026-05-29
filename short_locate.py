import socket
import psycopg2
from psycopg2 import pool
import config
import logging
import threading
import time
from datetime import datetime
from collections import defaultdict

db_path = 'EOD_data.db'
LOCAL_SERVER_HOST = 'localhost'
LOCAL_SERVER_PORT = 5015

class Slocate:
    def __init__(self, socketio=None):
        self.logger = logging.getLogger('SLocate')
        self.logger.setLevel(logging.DEBUG)
        
        # Console handler (existing)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File handler (new)
        file_handler = logging.FileHandler('slocate.log')
        file_handler.setLevel(logging.DEBUG)  # Capture all logs (DEBUG and above)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        self.client_socket = None
        self.lock = threading.Lock()
        self.keep_listening = True
        self.offers = {}
        self.accepted_orders = {}
        self.located_offers = {}  
        self.pending_offers = {} 
        
        self.current_ticker = None
        self.socketio = socketio
        self.logger.debug(f"Received socketio instance: {socketio}, type: {type(socketio)}")
        if socketio is None:
            self.logger.warning("No SocketIO instance provided; real-time updates will be disabled.")
        else:
            self.logger.info("SocketIO instance provided; real-time updates enabled.")
        try:
            self.db_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=20,
                **config.DB_CONFIG
            )
            self.logger.info("PostgreSQL connection pool initialized")
        except psycopg2.OperationalError as e:
            self.logger.error(f"Failed to initialize PostgreSQL connection pool: {e}")
            raise
        self.ticker_locks = defaultdict(threading.Lock)
        self.connect_to_server()
        listener_thread = threading.Thread(target=self.listen_to_server, daemon=True)
        listener_thread.start()
        self.logger.info("SLocate instance created and initialized.")

    def connect_to_server(self, max_attempts=12, delay=5):
        self.logger.debug("Starting connection attempt to the server.")
        attempts = 0
        while attempts < max_attempts:
            try:
                self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.logger.info("Attempting to connect to server...")
                self.client_socket.connect((LOCAL_SERVER_HOST, LOCAL_SERVER_PORT))
                self.logger.info("Connected to server.")
                return
            except socket.error as e:
                self.logger.error(f"Socket error while connecting: {e}")
                self.client_socket = None
                attempts += 1
                self.logger.info(f"Retrying connection in 5 seconds... (Attempt {attempts})")
                time.sleep(delay)
        self.logger.error("Failed to connect to server after multiple attempts.")

    def listen_to_server(self):
        self.logger.debug("Starting to listen to the server.")
        while self.keep_listening:
            if self.client_socket:
                try:
                    response = self.client_socket.recv(4096).decode()
                    if response:
                        #self.logger.debug(f"Received response: {response}")
                        self.handle_response(response)
                    else:
                        self.logger.debug("No response received, waiting...")
                        time.sleep(0.5)
                except socket.error as e:
                    self.logger.error(f"Socket error while listening: {e}")
                    self.keep_listening = False
                except Exception as e:
                    self.logger.error(f"Error listening to server: {e}")
                    self.keep_listening = False
            else:
                self.logger.debug("Client socket is not connected.")
                time.sleep(1)

    def handle_response(self, response):
        lines = response.strip().split('\n')
        for line in lines:
            self.logger.debug(f"Processing line: {line}")
            parts = line.split()
            if len(parts) < 2:
                continue
            if parts[0].upper() == "%SLORDER":
                status = parts[7]
                order_id = parts[1]
                ticker = parts[2]
                shares = int(parts[3])
                exeshares = int(parts[5]) if len(parts) > 5 else 0
                exeprice = float(parts[6]) if len(parts) > 6 else 0.0
                route = parts[8] if len(parts) > 8 else ""
                notes = ' '.join(parts[10:]) if len(parts) > 10 else ""
                self.logger.info(f"SLORDER response - Order ID: {order_id}, Ticker: {ticker}, Status: {status}, Exeshares: {exeshares}")

                if status == "Offered":
                    total_cost = exeprice * shares
                    offer = {
                        'ticker': ticker,
                        'shares': shares,
                        'price': exeprice,
                        'route': route,
                        'order_id': order_id,
                        'total_cost': total_cost
                    }
                    if ticker not in self.offers:
                        self.offers[ticker] = []
                    self.offers[ticker].append(offer)
                    self.logger.debug(f"Offer added for ticker {ticker}: {offer}")

                elif status == "Located":
                    self.located_offers[order_id] = {
                        'ticker': ticker,
                        'exeshares': exeshares,
                        'exeprice': exeprice,
                        'route': route
                    }
                    self.logger.info(f"Located shares: {exeshares} for {ticker} at ${exeprice}")
                    if order_id in self.pending_offers:
                        self.update_borrowed_shares(ticker, exeshares, exeprice)
                        del self.pending_offers[order_id]

                elif status in ["Rejected", "Canceled", "Closed", "Declined"]:
                    self.logger.error(f"Borrow failed for {ticker}: Status={status}, Notes={notes}")
                    if order_id in self.pending_offers:
                        if order_id in self.located_offers:  # Check if key exists
                            del self.located_offers[order_id]  # Remove the entry
                            
            elif parts[0].upper() == "%SLRET":
                ticker = parts[2]
                notes = ' '.join(parts[6:]) if len(parts) > 5 else ""
                if notes in ["AlreadyShortable", "Symbolisalreadyshortable!"]:
                    self.logger.info(f"Ticker {ticker} is already shortable: {notes}")
                    with self.lock:  # Use lock to ensure thread safety
                        self.located_offers['ALREADY_SHORTABLE_' + ticker] = {
                            'ticker': ticker,
                            'exeshares': 0,  # No shares borrowed since already shortable
                            'exeprice': 0.0,
                            'route': notes
                        }

    def select_best_offer(self):
        if self.current_ticker not in self.offers or not self.offers[self.current_ticker]:
            return []
        valid_offers = [offer for offer in self.offers[self.current_ticker] if offer['price'] <= 0.5 and offer['total_cost'] <= 500]
        if not valid_offers:
            self.logger.error(f"No valid offers for {self.current_ticker} within thresholds (max $0.5/share, $500 total)")
            return []
        best_offer = min(valid_offers, key=lambda x: x['total_cost'])
        self.logger.info(f"Selected best offer: Ticker={best_offer['ticker']}, Price={best_offer['price']}, Shares={best_offer['shares']}, Route={best_offer['route']}, Total Cost=${best_offer['total_cost']:.2f}")
        return [best_offer]

    def accept_offer(self, offer, trade_id):
        self.logger.debug(f"Attempting to accept offer: {offer} for trade_id: {trade_id}")
        if trade_id not in self.accepted_orders:
            self.accepted_orders[trade_id] = set()
        if offer['order_id'] not in self.accepted_orders[trade_id]:
            command = f"SLOFFEROPERATION {offer['order_id']} Accept"
            self.send_command(command)
            self.accepted_orders[trade_id].add(offer['order_id'])
            self.pending_offers[offer['order_id']] = offer
            self.logger.info(f"Sent accept command for Order ID={offer['order_id']}, waiting for Located status")
            return True
        self.logger.error(f"Offer with Order ID {offer['order_id']} for trade_id {trade_id} was not accepted as it was already processed.")
        return False

    def send_command(self, command):
        self.logger.debug(f"Sending command to the server: {command}")
        with self.lock:
            if self.client_socket:
                try:
                    self.client_socket.sendall(command.encode())
                    self.logger.debug(f"Command sent: {command}")
                except socket.error as e:
                    self.logger.error(f"Socket error while sending command: {e}")
            else:
                self.logger.error("Client socket is not connected, cannot send command.")

    def send_locate_order_commands(self, symbol, shares):
        self.logger.debug(f"Sending locate order command for symbol={symbol}, shares={shares}")
        inquire_command = f'SLPRICEINQUIRE {symbol} {shares} ALLROUTE'
        self.send_command(inquire_command)

    def update_borrowed_shares(self, ticker, shares, price):
        self.logger.debug(f"Updating borrowedshares table for ticker={ticker}, shares={shares}, price={price}")
        try:
            conn = self.db_pool.getconn()
            conn.autocommit = False
            cursor = conn.cursor()
            today_date = datetime.now().strftime('%Y-%m-%d')
            cursor.execute("""
                SELECT borrowed_shares, available_shares, total_cost FROM borrowedshares 
                WHERE ticker = %s AND DATE(last_updated) = %s FOR UPDATE
            """, (ticker, today_date))
            record = cursor.fetchone()
            if record:
                borrowed_shares, available_shares, total_cost = record
                new_borrowed_shares = borrowed_shares + shares
                new_available_shares = available_shares + shares
                new_total_cost = float(total_cost) + (float(price) * int(shares))
                cursor.execute("""
                    UPDATE borrowedshares 
                    SET borrowed_shares = %s, available_shares = %s, cost_per_share = %s, 
                        total_cost = %s, last_updated = CURRENT_TIMESTAMP
                    WHERE ticker = %s AND DATE(last_updated) = %s
                """, (new_borrowed_shares, new_available_shares, price, new_total_cost, ticker, today_date))
            else:
                new_total_cost = float(price) * int(shares)
                cursor.execute("""
                    INSERT INTO borrowedshares 
                    (ticker, borrowed_shares, available_shares, cost_per_share, total_cost, last_updated)
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """, (ticker, shares, shares, price, new_total_cost))
            conn.commit()
            self.logger.info(f"Updated borrowedshares table for ticker {ticker} with {shares} shares at ${price} per share")
            return True
        except psycopg2.Error as e:
            self.logger.error(f"Error updating borrowed shares: {e}")
            conn.rollback()
            return False
        finally:
            if conn:
                cursor.close()
                self.db_pool.putconn(conn)

    def reset_offers(self, ticker):
        self.logger.debug(f"Resetting offers for ticker: {ticker}")
        if ticker in self.offers:
            del self.offers[ticker]
        self.located_offers = {k: v for k, v in self.located_offers.items() if v['ticker'] != ticker}
        self.pending_offers = {k: v for k, v in self.pending_offers.items() if v['ticker'] != ticker}
        self.logger.info(f"Cleared offers for ticker: {ticker}")

    def clear_accepted_orders(self, trade_id):
        self.logger.debug(f"Clearing accepted orders for trade_id: {trade_id}")
        if trade_id in self.accepted_orders:
            del self.accepted_orders[trade_id]
            self.logger.info(f"Cleared accepted orders for trade_id: {trade_id}")

    def receive_order_details(self, ticker, shares, trade_id):
        self.logger.debug(f"Received order details: ticker={ticker}, shares={shares}, trade_id={trade_id}")
        with self.ticker_locks[ticker]:
            self.current_ticker = ticker
            self.reset_offers(ticker)
            
            if not self.client_socket:
                self.logger.error(f"Cannot borrow shares for {ticker}: Client socket is not connected.")
                return {"status": "error", "message": "Client socket is not connected"}

            self.send_locate_order_commands(ticker, shares)
            self.logger.debug("Waiting for responses to accumulate...")
            
            start_time = time.time()
            collection_time = 5  # Time to collect offers
            while time.time() - start_time < collection_time:
                with self.lock:
                    if 'ALREADY_SHORTABLE_' + ticker in self.located_offers:
                        self.logger.info(f"Ticker {ticker} is already shortable, no need to borrow")
                        return {"status": "success", "message": f"Ticker {ticker} is already shortable", "already_shortable": True}
                time.sleep(0.5)
            
            best_offers = self.select_best_offer()
            if best_offers:
                offer = best_offers[0]
                if self.accept_offer(offer, trade_id):
                    # Wait for Located status
                    accept_start_time = time.time()
                    while time.time() - accept_start_time < 10:
                        if any(o['ticker'] == ticker for o in self.located_offers.values()):
                            located_offer = next(o for o in self.located_offers.values() if o['ticker'] == ticker)
                            self.logger.info(f"Borrow success: {located_offer['exeshares']} shares for {ticker}")
                            return {"status": "success", "message": f"Borrowed {located_offer['exeshares']} shares for {ticker}"}
                        time.sleep(0.5)
                    failure_message = next((o.get('failure_message', "No Located status received") for o in self.pending_offers.values() if o['ticker'] == ticker), "No Located status received")
                    self.logger.error(f"Borrow failed for {ticker}: {failure_message}")
                    return {"status": "error", "message": failure_message}
            else:
                self.logger.error(f"Failed to borrow shares for {ticker}: No valid offers received within thresholds")
                return {"status": "error", "message": "No valid offers received"}