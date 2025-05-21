import socket
import sqlite3
import logging
import threading
import time
from datetime import datetime

db_path = 'EOD_data.db'
LOCAL_SERVER_HOST = 'localhost'
LOCAL_SERVER_PORT = 5012

class Slocate:
    def __init__(self):
        self.logger = logging.getLogger('SLocate')
        self.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        self.db_path = db_path
        self.client_socket = None
        self.lock = threading.Lock()
        self.keep_listening = True
        self.offers = []  # Store offers persistently across multiple responses
        self.accepted_orders = set()  # Track accepted orders to prevent duplicates
        self.logger.info("SLocate instance created and initialized.")

    def get_db_connection(self):
        self.logger.debug("Attempting to get a database connection.")
        try:
            conn = sqlite3.connect(self.db_path)
            self.logger.debug("Database connection established.")
            return conn
        except sqlite3.Error as e:
            self.logger.error(f"Error connecting to database: {e}")
            return None               
        
    def connect_to_server(self):
        """Establish a connection to the server."""
        self.logger.debug("Starting connection attempt to the server.")
        attempts = 0
        while attempts < 5:  # Try connecting up to 5 times
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
                time.sleep(5)
        self.logger.error("Failed to connect to server after multiple attempts.")
    
    def listen_to_server(self):
        """Continuously listen for server responses."""
        self.logger.debug("Starting to listen to the server.")
        while self.keep_listening:
            if self.client_socket:
                try:
                    response = self.client_socket.recv(4096).decode()
                    if response:
                        self.logger.debug(f"Received response: {response}")
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
                time.sleep(1)  # Wait before retrying
                
    def handle_response(self, response):
        """Process responses from the server and collect all offers with status 'Offered'."""
        self.logger.debug("Starting to handle server response.")
        lines = response.strip().split('\n')

        for line in lines:
            self.logger.debug(f"Processing line: {line}")
            parts = line.split()
            self.logger.debug(f"Parts: {parts}")
            
            if len(parts) > 1 and parts[0].upper() == "%SLORDER":
                status = parts[7]
                self.logger.debug(f"Status: {status}")
                
                if status == "Offered":
                    self.logger.debug("Offer status detected. Extracting details.")
                    ticker = parts[2]
                    shares = int(parts[3])
                    price = float(parts[6])
                    route = parts[8]
                    order_id = parts[1]  # Extract the order ID
                    total_cost = price * shares

                    self.logger.debug(f"Offer found: Ticker={ticker}, Price={price}, Shares={shares}, Route={route}, Order ID={order_id}, Total Cost={total_cost}")

                    offer = {
                        'ticker': ticker,
                        'shares': shares,
                        'price': price,
                        'route': route,
                        'order_id': order_id,
                        'total_cost': total_cost
                    }
                    self.offers.append(offer)  # Store the offer
                    self.logger.debug(f"Offer added to the list: {offer}")
                else:
                    self.logger.debug(f"No 'Offered' status found in line: {line}")

    def select_best_offer(self):
        """Compare all offers and select the lowest one."""
        self.logger.debug("Selecting the best offer from available offers.")
        if not self.offers:
            self.logger.warning("No offers found to select from.")
            return False
        
        best_offer = min(self.offers, key=lambda x: x['total_cost'])
        
        self.logger.info(f"Selected best offer: Ticker={best_offer['ticker']}, Price={best_offer['price']}, Shares={best_offer['shares']}, Route={best_offer['route']}, Total Cost=${best_offer['total_cost']:.2f}")
        
        # Automatically accept the best offer if total cost is less than $15
        return self.accept_offer(best_offer)

    def accept_offer(self, offer):
        """Send a command to accept the best offer."""
        self.logger.debug(f"Attempting to accept offer: {offer}")
        if offer['order_id'] not in self.accepted_orders:
            command = f"SLOFFEROPERATION {offer['order_id']} Accept"
            self.send_command(command)
            self.accepted_orders.add(offer['order_id'])
            self.logger.info(f"Auto-accepted offer: Order ID={offer['order_id']}, Total Cost=${offer['total_cost']:.2f}")
            
            # Insert the accepted offer into the BorrowedShares table and return the success status
            if self.update_borrowed_shares(offer['ticker'], offer['shares'], offer['price']):
                self.logger.debug("BorrowedShares table updated successfully.")
                self.offers.clear()  # Clear the offers list to stop further processing
                self.keep_listening = False  # Stop the listener thread
                return True  # Indicate success
                
            else:
                self.logger.error("Failed to update BorrowedShares table.")
                return False  # Indicate failure

        self.logger.error("Offer was not accepted as it was already processed.")
        return False  # Indicate failure

    def send_command(self, command):
        """Send a command to the server."""
        self.logger.debug(f"Sending command to the server: {command}")
        with self.lock:  # Ensure thread safety
            if self.client_socket:
                try:
                    self.client_socket.sendall(command.encode())
                    self.logger.debug(f"Command sent: {command}")
                except socket.error as e:
                    self.logger.error(f"Socket error while sending command: {e}")
            else:
                self.logger.error("Client socket is not connected, cannot send command.")

    def send_locate_order_commands(self, symbol, shares):
        # Send command to inquire about the locate price
        self.logger.debug(f"Sending locate order command for symbol={symbol}, shares={shares}")
        inquire_command = f'SLPRICEINQUIRE {symbol} {shares} ALLROUTE'
        self.send_command(inquire_command)

    def update_borrowed_shares(self, ticker, shares, price):
        """Update the BorrowedShares table after accepting an offer."""
        self.logger.debug(f"Updating BorrowedShares table for ticker={ticker}, shares={shares}, price={price}")
        conn = self.get_db_connection()
        if not conn:
            self.logger.error("Failed to update BorrowedShares due to no database connection.")
            return False
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO BorrowedShares (ticker, borrowed_shares, available_shares, cost_per_share, total_cost) 
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(ticker) DO UPDATE SET 
                borrowed_shares = borrowed_shares + excluded.borrowed_shares,
                available_shares = available_shares + excluded.available_shares,
                cost_per_share = excluded.cost_per_share,
                total_cost = total_cost + excluded.total_cost,
                last_updated = CURRENT_TIMESTAMP;
            """, (ticker, shares, shares, price, price * shares))
            conn.commit()
            self.logger.info(f"Updated BorrowedShares table for ticker {ticker} with {shares} shares at ${price} per share.")
            return True  # Indicate success
        except sqlite3.Error as e:
            self.logger.error(f"Error updating borrowed shares: {e}")
            return False  # Indicate failure
        finally:
            conn.close()

    def receive_order_details(self, ticker, shares):
        """Receive order details from the Order Execution module and process borrowing."""
        self.logger.debug(f"Received order details: ticker={ticker}, shares={shares}")
        attempts = 0
        while not self.client_socket and attempts < 5:
            self.logger.warning("Client socket is not connected. Attempting to reconnect...")
            if self.connect_to_server():
                self.logger.info("Reconnected successfully.")
                break
            attempts += 1
            time.sleep(2)  # Wait before retrying

        if not self.client_socket:
            self.logger.error(f"Cannot borrow shares for {ticker}: Client socket is not connected after multiple attempts.")
            return {"status": "error", "message": "Client socket is not connected"}
        
        # Start the listener thread to listen for server responses
        listener_thread = threading.Thread(target=self.listen_to_server, daemon=True)
        listener_thread.start()

        self.send_locate_order_commands(ticker, shares)
        self.logger.debug("Waiting for responses to accumulate...")
        time.sleep(5)  # Wait for responses to accumulate

        # Use the result from select_best_offer to determine success or failure
        if self.select_best_offer():
            self.logger.debug(f"Borrow success: {shares} shares for {ticker}")
            return {"status": "success", "message": f"Borrowed {shares} shares for {ticker}."}
        else:
            self.logger.error(f"Failed to borrow shares for {ticker}.")
            return {"status": "error", "message": f"Failed to borrow shares for {ticker}."}


def main():
    pass

if __name__ == "__main__":
    main()
