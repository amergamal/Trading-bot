import sqlite3
import threading
import logging
import socket
import random
from datetime import datetime
import time

db_path = 'EOD_data.db'
LOCAL_SERVER_HOST = 'localhost'
LOCAL_SERVER_PORT = 5012

def generate_token():
    return str(random.randint(100000, 999999))

class EndOfDay:
    def __init__(self):
        self.logger = logging.getLogger('EndOfDay')
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        self.db_path = db_path
        self.trade_details = {}  # Dictionary to store trade details by ticker
        self.lock = threading.Lock()  # Lock for thread safety
        self.logger.info("EOD instance created and initialized.")

        # Start listening to the server for updates
        self.listen_to_server()

    def listen_to_server(self):
        """Start a thread to listen for server responses."""
        threading.Thread(target=self._listen_to_server, daemon=True).start()

    def _listen_to_server(self):
        """Continuously listen for server responses."""
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.logger.info("Attempting to connect to server...")
            self.client_socket.connect((LOCAL_SERVER_HOST, LOCAL_SERVER_PORT))
            self.logger.info(f"Connected to server on port {LOCAL_SERVER_PORT}")

            while True:
                response = self.client_socket.recv(4096).decode()
                if response:
                    self.logger.debug(f"Received response: {response}")
                    self.process_response(response)
                else:
                    self.logger.debug("No response received, waiting...")
                    time.sleep(0.5)
        except socket.error as e:
            self.logger.error(f"Socket error: {e}")
        except Exception as e:
            self.logger.error(f"Error listening to server: {e}")
        finally:
            self.client_socket.close()
            self.logger.info("Socket connection closed.")

    def receive_order_details(self, trade_details):
        """Receive trade details and store them in the trade_details dictionary."""
        trade_id = trade_details['tradeID']
        with self.lock:
            self.trade_details[trade_id] = trade_details
            self.logger.info(f"Received trade details for {trade_id}: {trade_details}")

    def send_command(self, command):
        """Send commands to the server."""
        with self.lock:  # Ensure thread safety when sending commands
            self.client_socket.sendall(command.encode())
            self.logger.debug(f'Sent command: {command}')

    def end_of_day_tasks(self):
        """Schedule to perform end of day tasks at 3:55 PM EST."""
        while True:
            current_time = datetime.now().time()
            if current_time.hour == 11 and current_time.minute == 30:
                self.close_positions()
                break  # Exit after tasks are completed
            time.sleep(60)  # Sleep for 60 seconds before checking the time again

    def close_positions(self):
        """Close all open short positions at the end of the day."""
        self.send_command('GET POSITIONS')

    def process_response(self, response):
        """Process server response lines."""
        lines = response.strip().split('\n')
        positions = []

        for line in lines:
            if line.startswith('%POS'):
                self.logger.debug(f"Processing response line: {line}")
                parts = line.split()
                try:
                    ticker = parts[1]
                    position_type = parts[2]
                    shares = int(parts[3])
                    # Add position details to the list
                    positions.append({
                        'ticker': ticker,
                        'type': position_type,
                        'shares': shares
                    })
                except (IndexError, ValueError) as e:
                    self.logger.error(f'Error parsing %POS line: {e}')
        
        # If we have positions, process them
        if positions:
            self.logger.debug(f"Processed positions: {positions}")
            self.handle_positions(positions)

    def handle_positions(self, positions):
        """Handle the closing of positions based on the processed response."""
        threads = []
        for position in positions:
            ticker = position['ticker']
            shares_pos = position['shares']
            position_type = position['type']
            
            if shares_pos == 0:
                self.logger.info(f"No action needed: {ticker} position type {position_type} has zero shares.")


            # Check if position is short (position_type == '3') and if shares_pos is greater than 0
            if position_type == '3' and shares_pos > 0:
                # Find the trade details for this ticker and open position
                for trade_id, details in self.trade_details.items():
                    if details['ticker'] == ticker:
                        # Use threads to handle multiple buy market orders
                        thread = threading.Thread(target=self.send_buy_market_order, args=(trade_id, ticker, shares_pos))
                        threads.append(thread)
                        thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

    def send_buy_market_order(self, trade_id, ticker, shares_pos):
        """Send a command to buy back the shares."""
        token = generate_token()
        buy_command = f'NEWORDER {token} B {ticker} SMAT {shares_pos} MKT TIF=DAY'
        self.send_command(buy_command)
    
    def process_buy_response(self, response, trade_id, ticker, shares_pos, token):
        """Process the response from the buy market order command."""
        act_status = ""
        notes = ""
        for line in response.split('\n'):
            if line.startswith('%ORDER'):
                parts = line.split()
                order_id = parts[1]
                status = parts[11]
                time_executed = parts[12]
                price = float(parts[9])
                action = parts[4]

                self.insert_buy_market(trade_id, time_executed, ticker, shares_pos, price, token, order_id, action, status, act_status, notes)

                if status == 'Executed':
                    self.get_orders_and_cancel_stop(trade_id, ticker, price, time_executed)

            elif line.startswith('%ORDERACT'):
                parts = line.split()
                act_status = parts[2]
                notes = ' '.join(parts[9:-1])

    def insert_buy_market(self, trade_id, time, ticker, shares, price, token, order_id, action, status, act_status, notes):
        """Insert the buy market order details into the BuyMarket table."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO BuyMarket (tradeID, time, ticker, shares, price, token, orderID, action, status, act_status, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (trade_id, time, ticker, shares, price, token, order_id, action, status, act_status, notes))
            conn.commit()

    def get_orders_and_cancel_stop(self, trade_id, ticker, exit_price, exit_time):
        """Get orders and cancel stop orders if applicable."""
        orders_response = self.send_command('GET ORDERS')
        stop_order_id = self.trade_details[ticker]['stopOrderID']

        for line in orders_response.split('\n'):
            if line.startswith('%ORDER'):
                parts = line.split()
                order_id = parts[1]
                status = parts[11]

                if order_id == stop_order_id and status == 'Accepted':
                    cancel_command = f'CANCEL {stop_order_id}'
                    cancel_response = self.send_command(cancel_command)
                    self.process_cancel_response(cancel_response, trade_id, ticker, exit_price, exit_time)

    def process_cancel_response(self, response, trade_id, ticker, exit_price, exit_time):
        """Process the cancel order response."""
        for line in response.split('\n'):
            if line.startswith('%ORDER'):
                parts = line.split()
                status = parts[11]

                if status == 'Canceled':
                    self.move_trade_to_closed(trade_id, ticker, exit_price, exit_time, 'TakeProfit')

    def move_trade_to_closed(self, trade_id, ticker, exit_price, exit_time, reason):
        """Move the trade to the ClosedTrades table."""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT tradeID, strategy, ticker, shares, entry_price, stop_loss, time 
                FROM ActiveTrades 
                WHERE tradeID = ?""", 
                (trade_id,))
            trade = cursor.fetchone()

            if trade:
                tradeID, strategy, ticker, shares, entry_price, stop_loss, entry_time = trade
                realized = (entry_price - exit_price) * shares

                cursor.execute("""
                    INSERT INTO ClosedTrades (tradeID, strategy, ticker, shares, entry_price, entry_time, exit_price, exit_time, reason, date, realized) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (trade_id, strategy, ticker, shares, entry_price, entry_time, exit_price, exit_time, reason, datetime.now().strftime('%Y-%m-%d'), realized))

                cursor.execute("DELETE FROM ActiveTrades WHERE tradeID = ?", (trade_id,))
                
                cursor.execute("""
                    UPDATE TradeStatus 
                    SET active_trade = 'closed' 
                    WHERE ticker = ? AND strategy = ? AND date = ?""",
                    (ticker, strategy, datetime.now().strftime('%Y-%m-%d')))

                conn.commit()
                self.logger.info(f"Moved trade {trade_id} to ClosedTrades and updated TradeStatus")
        except sqlite3.Error as e:
            self.logger.error(f"Error moving trade to closed: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    eod = EndOfDay()
    threading.Thread(target=eod.end_of_day_tasks, daemon=True).start()
    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
