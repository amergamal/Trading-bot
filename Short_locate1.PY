import socket
import sqlite3
import logging
import threading
import time
from flask import Flask, render_template, request, jsonify

app = Flask(__name__)
db_path = 'EOD_data.db'
LOCAL_SERVER_HOST = 'localhost'
LOCAL_SERVER_PORT = 5015

# Global variable to store offers
global_offers = []

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
                    route = parts[8]
                    order_id = parts[1]  # Extract the order ID
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
                best_offers.append(best_offer)
        
        return best_offers

    def accept_offer(self, offer):
        """Send a command to accept the best offer and insert it into the BorrowedShares table."""
        if not self.client_socket:
            self.logger.error("Client socket is not connected, cannot send command.")
            return

        if offer['order_id'] not in self.accepted_orders:
            command = f"SLOFFEROPERATION {offer['order_id']} Accept"
            self.send_command(command)
            self.accepted_orders.add(offer['order_id'])
            self.logger.info(f"Accepted offer: Order ID={offer['order_id']}, Total Cost=${offer['total_cost']:.2f}")
            
            # Insert the accepted offer into the BorrowedShares table
            conn = self.get_db_connection()
            if conn:
                try:
                    with conn:
                        conn.execute("""
                            CREATE TABLE IF NOT EXISTS BorrowedShares (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                ticker TEXT NOT NULL,
                                borrowed_shares INTEGER NOT NULL,
                                cost_per_share REAL NOT NULL,
                                total_cost REAL NOT NULL,
                                available_shares INTEGER NOT NULL,
                                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                            );
                        """)
                        conn.execute("""
                            INSERT INTO BorrowedShares (ticker, borrowed_shares, cost_per_share, total_cost, available_shares) 
                            VALUES (?, ?, ?, ?, ?)
                        """, (offer['ticker'], offer['shares'], offer['price'], offer['total_cost'], offer['shares']))
                        self.logger.info(f"Inserted {offer['shares']} shares of {offer['ticker']} into BorrowedShares table.")
                except Exception as e:
                    self.logger.error(f"Error inserting borrowed shares into database: {e}")
                finally:
                    conn.close()

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

@app.route('/')
def short_locate():
    return render_template('short_locate.html')

@app.route('/locate_shares', methods=['POST'])
def locate_shares():
    global global_offers
    global_offers = []  # Reset global offers each time a new locate request is made
    
    tickers = request.form.getlist('tickers[]')
    shares_list = request.form.getlist('shares[]')
    
    slocate = Slocate()
    slocate.connect_to_server()
    
    if slocate.client_socket:
        listener_thread = threading.Thread(target=slocate.listen_to_server, daemon=True)
        listener_thread.start()
        
        for ticker, shares in zip(tickers, shares_list):
            slocate.send_locate_order_commands(ticker, shares)
        
        time.sleep(5)  # Wait for responses to accumulate
        
        # Select and return the best offer for each ticker
        best_offers = slocate.select_best_offer()
        global_offers.extend(best_offers)  # Store in global variable
    
    # Return the offers in JSON format for the HTML to display
    return jsonify({'located_shares': best_offers})

@app.route('/accept_offer', methods=['POST'])
def accept_offer():
    ticker = request.form.get('ticker')
    
    # Find the offer associated with this ticker in the global offers
    offer = next((offer for offer in global_offers if offer['ticker'] == ticker), None)
    
    if offer:
        slocate = Slocate()
        slocate.connect_to_server()  # Ensure the connection is established
        
        if slocate.client_socket:
            slocate.accept_offer(offer)
            return jsonify({'status': 'success', 'accepted_share': offer})
        else:
            return jsonify({'status': 'error', 'message': 'Failed to connect to server'})
    else:
        return jsonify({'status': 'error', 'message': 'Offer not found'})
    
    
from datetime import datetime

@app.route('/get_borrowed_shares', methods=['GET'])
def get_borrowed_shares():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get today's date in YYYY-MM-DD format
    today_date = datetime.today().strftime('%Y-%m-%d')
    
    # Query to select records for today's date only
    cursor.execute("""
        SELECT ticker, borrowed_shares, available_shares, cost_per_share, total_cost 
        FROM BorrowedShares
        WHERE DATE(last_updated) = ?
    """, (today_date,))
    
    borrowed_shares = cursor.fetchall()
    conn.close()
    
    borrowed_shares_list = [
        {
            'ticker': row[0],
            'borrowed_shares': row[1],
            'available_shares': row[2],
            'cost_per_share': row[3],
            'total_cost': row[4]
        }
        for row in borrowed_shares
    ]
    
    return jsonify({'borrowed_shares': borrowed_shares_list})

    


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5010)
