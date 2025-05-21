import socket
import sqlite3
import logging
import threading
import time

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
        self.offers = {}  # Store offers per ticker
        self.accepted_orders = set()  # Track accepted orders to prevent duplicates
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
                return True
            except socket.error as e:
                self.logger.error(f"Socket error while connecting: {e}")
                self.client_socket = None
                attempts += 1
                self.logger.info(f"Retrying connection in 5 seconds... (Attempt {attempts})")
                time.sleep(5)
        self.logger.error("Failed to connect to server after multiple attempts.")
        return False
    
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
                    
                    if ticker not in self.offers:
                        self.offers[ticker] = []
                    self.offers[ticker].append(offer)  # Store the offer by ticker

    def select_best_offer(self):
        """Compare all offers and select the lowest one for each ticker."""
        best_offers = []
        for ticker, offers in self.offers.items():
            if offers:
                best_offer = min(offers, key=lambda x: x['total_cost'])
                self.logger.info(f"Selected best offer: Ticker={best_offer['ticker']}, Price={best_offer['price']}, Shares={best_offer['shares']}, Route={best_offer['route']}, Total Cost=${best_offer['total_cost']:.2f}")
                
                if best_offer['total_cost'] < 15:
                    if self.accept_offer(best_offer):
                        self.offers[ticker] = []  # Clear the offers list for this ticker
                        best_offers.append(best_offer)
                        self.logger.info("Shares fully accepted.")
                    else:
                        self.logger.error(f"Failed to accept the best offer for {best_offer['ticker']}.")
                else:
                    self.logger.warning(f"Best offer for {ticker} exceeds the acceptable total cost.")
        
        return best_offers

    def accept_offer(self, offer):
        """Send a command to accept the best offer and wait for a specified time to ensure processing."""
        if not self.client_socket:
            self.logger.error("Client socket is not connected, cannot send command.")
            return False

        if offer['order_id'] not in self.accepted_orders:
            command = f"SLOFFEROPERATION {offer['order_id']} Accept"
            if self.send_command(command):
                self.accepted_orders.add(offer['order_id'])
                self.logger.info(f"Accepted offer: Order ID={offer['order_id']}, Total Cost=${offer['total_cost']:.2f}")
                
                
                
                
                # Insert the accepted offer into the BorrowedShares table
                self.update_borrowed_shares(offer['ticker'], offer['shares'], offer['price'])
                return True
        return False

    def send_command(self, command):
        """Send a command to the server."""
        with self.lock:  # Ensure thread safety
            if self.client_socket:
                try:
                    self.client_socket.sendall(command.encode())
                    self.logger.debug(f"Command sent: {command}")
                    return True  # Indicate that the command was sent successfully
                except socket.error as e:
                    self.logger.error(f"Socket error while sending command: {e}")
                    return False  # Indicate that the command was not sent successfully
            else:
                self.logger.error("Client socket is not connected, cannot send command.")
                return False

    def send_locate_order_commands(self, symbol, shares):
        """Send a command to locate shares."""
        inquire_command = f'SLPRICEINQUIRE {symbol} {shares} ALLROUTE'
        return self.send_command(inquire_command)

    def handle_borrow_request(self, ticker, shares_requested):
        """Handle borrowing shares from the Short Locate module."""
        if not self.send_locate_order_commands(ticker, shares_requested):
            return {"status": "error", "message": f"Failed to send locate command for {ticker}."}
        
        time.sleep(1)  # Wait for responses to accumulate
        best_offer = self.select_best_offer()
        
        if best_offer:
            return {"status": "success", "message": f"Borrowed {best_offer[0]['shares']} shares for {ticker} at ${best_offer[0]['price']} per share."}
        else:
            return {"status": "error", "message": f"Failed to borrow shares for {ticker}."}

    def update_borrowed_shares(self, ticker, shares, price):
        """Update the BorrowedShares table after accepting an offer."""
        conn = self.get_db_connection()
        if not conn:
            return
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO BorrowedShares (ticker, borrowed_shares, available_shares, cost_per_share, total_cost) 
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(ticker) DO UPDATE SET 
                borrowed_shares = borrowed_shares + excluded.borrowed_shares,
                available_shares = available_shares + excluded.available_shares,  -- Update available shares
                cost_per_share = excluded.cost_per_share,
                total_cost = total_cost + excluded.total_cost,
                last_updated = CURRENT_TIMESTAMP;
            """, (ticker, shares, shares, price, price * shares))
            conn.commit()
            self.logger.info(f"Updated BorrowedShares table for ticker {ticker} with {shares} shares at ${price} per share.")
        except sqlite3.Error as e:
            self.logger.error(f"Error updating borrowed shares: {e}")
        finally:
            conn.close()

    def receive_order_details(self, ticker, shares):
        """Receive order details from the Order Execution module and process borrowing."""
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
    
        borrow_result = self.handle_borrow_request(ticker, shares)
        if borrow_result['status'] == 'error':
            self.logger.warning(f"Borrow request failed: {borrow_result['message']}")
            return borrow_result
    
        self.logger.info(f"Successfully borrowed {shares} shares for {ticker}.")
        return {"status": "success", "message": f"Borrowed {shares} shares for {ticker}."}


def main():
    slocate = Slocate()
    slocate.connect_to_server()
    
    if slocate.client_socket:
        listener_thread = threading.Thread(target=slocate.listen_to_server, daemon=True)
        listener_thread.start()

    # Keep the script running indefinitely
    try:
        while True:
            time.sleep(1)  # Sleep for a short period to keep the script alive
    except KeyboardInterrupt:
        print("Shutting down...")    

if __name__ == "__main__":
    main()
