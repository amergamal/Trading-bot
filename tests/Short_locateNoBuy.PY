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
        self.logger.info("SLocate instance created and initialized.")

    def get_db_connection(self):
        try:
            conn = sqlite3.connect(self.db_path)
            return conn
        except sqlite3.Error as e:
            self.logger.error(f"Error connecting to database: {e}")
            return None               
        
    def connect_to_server(self):
        """Establish a connection to the server."""
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
        lines = response.strip().split('\n')

        for line in lines:
            self.logger.debug(f"Processing line: {line}")
            parts = line.split()
            self.logger.debug(f"Parts: {parts}")
            
            if len(parts) > 1 and parts[0].upper() == "%SLORDER":
                status = parts[7]
                self.logger.debug(f"Status: {status}")
                
                if status == "Offered":
                    ticker = parts[2]
                    shares = int(parts[3])
                    price = float(parts[6])
                    order_id = parts[1]  # Extract the order ID
                    route = parts[8]
                    total_cost = price * shares

                    self.logger.debug(f"Offer found: Ticker={ticker}, Price={price}, Shares={shares}, Route={route}, Order ID={parts[1]}, Total Cost={total_cost}")

                    offer = {
                        'ticker': ticker,
                        'shares': shares,
                        'price': price,
                        'route': route,
                        'order_id': order_id,
                        'total_cost': total_cost
                    }
                    self.offers.append(offer)  # Store the offer

    def select_best_offer(self):
        """Compare all offers and select the lowest one."""
        if not self.offers:
            return
        
        best_offer = min(self.offers, key=lambda x: x['total_cost'])
        
        self.logger.info(f"Selected best offer: Ticker={best_offer['ticker']}, Price={best_offer['price']}, Shares={best_offer['shares']}, Route={best_offer['route']}, Total Cost=${best_offer['total_cost']:.2f}")
        
        # Print statement indicating the offer that would be accepted
        print(f"Accepting offer: Ticker={best_offer['ticker']}, Price={best_offer['price']}, Shares={best_offer['shares']}, Route={best_offer['route']}, Total Cost=${best_offer['total_cost']:.2f}")
        
        # Optional: Accept the offer
        # self.accept_offer(best_offer)

        # Clear the offers list after selecting the best one
        self.offers = []

    def send_command(self, command):
        """Send a command to the server."""
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
        inquire_command = f'SLPRICEINQUIRE {symbol} {shares} ALLROUTE'
        self.send_command(inquire_command)
        
    def test_dgly_order(self):
        # Check availability for DGLY
        print("Checking availability for DGLY")
        self.send_locate_order_commands('GOVX', 500)

def main():
    slocate = Slocate()
    slocate.connect_to_server()
    
    if slocate.client_socket:
        listener_thread = threading.Thread(target=slocate.listen_to_server, daemon=True)
        listener_thread.start()
        slocate.test_dgly_order()

        # Wait for responses to accumulate and select the best offer
        time.sleep(5)  # Adjust this based on how long you expect responses to take
        slocate.select_best_offer()

if __name__ == "__main__":
    main()
