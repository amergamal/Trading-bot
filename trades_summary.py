import socket
import sqlite3
import logging
import time
import threading
from collections import defaultdict
from datetime import datetime

# Configuration variables
LOCAL_SERVER_HOST = 'localhost'
LOCAL_SERVER_PORT = 5015

class TradesSummary:
    def __init__(self):
        self.logger = logging.getLogger('TradesSummary')
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        self.client_socket = None
        self.lock = threading.Lock()
        self.keep_listening = True  # Initialize keep_listening to True
        self.ticker_data = defaultdict(lambda: {
            "buy_shares": 0,
            "sell_shares": 0,
            "buy_total_price": 0,
            "sell_total_price": 0,
            "fees": 0,
            "pnl": 0,
            "buy_count": 0,
            "sell_count": 0
        })  # Store aggregated data per ticker
        
        

    def connect_to_server(self):
        """Establish a connection to the server."""
        while True:
            try:
                self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.logger.info("Attempting to connect to server...")
                self.client_socket.connect((LOCAL_SERVER_HOST, LOCAL_SERVER_PORT))
                self.logger.info("Connected to server on port 5015.")
                time.sleep(1)  # Add a delay to ensure connection before sending commands
                return
            except socket.error as e:
                self.logger.error(f"Socket error while connecting: {e}")
                self.client_socket = None
                self.logger.info(f"Retrying connection in 5 seconds...")
                time.sleep(5)

    def get_todays_trades(self):
        """Request today's trades from the server."""
        command = "GET TRADES"
        if not self.client_socket:
            self.connect_to_server()

        if self.client_socket:
            try:
                self.logger.debug(f"Sending command to get trades: {command}")
                self.send_command(command)

            except socket.error as e:
                self.logger.error(f"Socket error: {e}")
                self.disconnect_from_server()

    def listen_to_server(self):
        """Continuously listen for server responses."""
        while self.keep_listening:
            if self.client_socket:
                try:
                    response = self.client_socket.recv(4096).decode()
                    if response:
                        self.logger.debug(f"Received response: {response}")
                        self.process_response(response)
                    else:
                        self.logger.debug("No response received, waiting...")
                        time.sleep(0.5)
                except socket.error as e:
                    self.logger.error(f"Socket error while listening: {e}")
                    self.client_socket = None  # Mark the socket as disconnected
                    self.logger.info("Attempting to reconnect...")
                    self.connect_to_server()  # Reconnect to the server
                except Exception as e:
                    self.logger.error(f"Error listening to server: {e}")
                    self.keep_listening = False
            else:
                self.logger.debug("Client socket is not connected. Reconnecting...")
                self.connect_to_server()  # Reconnect if socket is disconnected
                time.sleep(1)  # Wait before retrying

    def process_response(self, response):
        """Process the server response, handling multiline responses."""
        self.logger.debug(f"Processing server response: {response}")

        # Split the response by newlines to process each line separately
        lines = response.splitlines()

        # Iterate through each line
        for line in lines:
            self.logger.debug(f"Received line: {line}")
        
            if line.startswith("%TRADE"):
                parts = line.split()
                
                
                if len(parts) >= 12:  # Check if there are enough elements
                    try:
                        ticker = parts[2]
                        b_s = parts[3]  # Buy/Sell indicator
                        quantity = float(parts[4])
                        price = float(parts[5])
                        fees = float(parts[10])
                        pnl = float(parts[11])

                        # Aggregate the data
                        if b_s == 'B':  # Buy side
                            self.ticker_data[ticker]["buy_shares"] += quantity
                            self.ticker_data[ticker]["buy_total_price"] += price * quantity
                            self.ticker_data[ticker]["buy_count"] += 1
                        elif b_s == 'SS':  # Sell side (short sell)
                            self.ticker_data[ticker]["sell_shares"] += quantity
                            self.ticker_data[ticker]["sell_total_price"] += price * quantity
                            self.ticker_data[ticker]["sell_count"] += 1

                        self.ticker_data[ticker]["fees"] += fees
                        self.ticker_data[ticker]["pnl"] += pnl
                    except ValueError as e:
                        self.logger.error(f"Error parsing trade data: {e}")    
                else:
                    self.logger.warning(f"Incomplete line received for trade data: {line}")
                    
            elif line.startswith("#TradeEnd"):
                # Trigger the summary print after trades are done
                self.logger.debug("Detected #TradeEnd. Printing summary...")
                self.print_summary()

            elif "ECHO OFF" in line:
                self.logger.debug("Ignoring ECHO OFF message.")
            else:
                self.logger.debug(f"Ignored unexpected line: {line}")
                
    def get_borrowed_cost(self, ticker, date):
        """Fetch the total borrowed cost for the ticker from the BorrowedShares table for the given date."""
        total_borrowed_fees = 0
        try:
            # Open a new connection to SQLite
            conn = sqlite3.connect('EOD_data.db')
            cursor = conn.cursor()

            # Fetch all records for the ticker for today's date
            cursor.execute("""
                SELECT SUM(total_cost) FROM BorrowedShares
                WHERE ticker = ? AND date(last_updated) = ?
            """, (ticker, date))

            result = cursor.fetchone()

            if result and result[0]:
                total_borrowed_fees = result[0]

            self.logger.debug(f"Fetched borrowed fees for {ticker} on {date}: {total_borrowed_fees}")
        except Exception as e:
            self.logger.error(f"Error fetching borrowed fees from BorrowedShares table: {e}")
        finally:
            conn.close()  # Ensure the connection is closed

        return total_borrowed_fees            
                
    def insert_trade_summary(self, ticker, date, total_buy_shares, total_sell_shares, avg_buy_price, avg_sell_price, total_fees, total_pnl, borrowed_fees, net_pnl):
        """Insert the trade summary into the database."""
        try:
            # Open a new connection to SQLite within this thread
            conn = sqlite3.connect('EOD_data.db')
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO TradeSummary (ticker, date, total_buy_shares, total_sell_shares, avg_buy_price, avg_sell_price, total_fees, total_pnl, borrowed_fees, net_pnl)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (ticker, date, total_buy_shares, total_sell_shares, avg_buy_price, avg_sell_price, total_fees, total_pnl, borrowed_fees, net_pnl))


            conn.commit()
            self.logger.debug(f"Inserted summary for {ticker} on {date} into the database.")
        except Exception as e:
            self.logger.error(f"Error inserting data into the database: {e}")
        finally:
            conn.close()  # Ensure the connection is closed


    def print_summary(self):
        """Print the aggregated summary of trades for each ticker."""
        today = datetime.now().strftime("%Y-%m-%d")
        for ticker, data in self.ticker_data.items():
            total_buy_shares = data["buy_shares"]
            total_sell_shares = data["sell_shares"]

            # Calculate average prices if trades exist
            avg_buy_price = round(data["buy_total_price"] / total_buy_shares, 2) if total_buy_shares > 0 else 0
            avg_sell_price = round(data["sell_total_price"] / total_sell_shares, 2) if total_sell_shares > 0 else 0
            
            total_fees = data["fees"]
            total_pnl = data["pnl"]
            
            # Fetch borrowed fees from BorrowedShares table
            borrowed_fees = self.get_borrowed_cost(ticker, today)

            # Calculate net P&L
            net_pnl = total_pnl - total_fees - borrowed_fees

            # Print summary for each ticker
            print(f"Ticker: {ticker}")
            print(f"Date: {today}")
            print(f"Total Buy Shares: {total_buy_shares}")
            print(f"Total Sell Shares: {total_sell_shares}")
            print(f"Average Buy Price: {avg_buy_price:.2f}")
            print(f"Average Sell Price: {avg_sell_price:.2f}")
            print(f"Total Fees: {total_fees:.2f}")
            print(f"Borrowed Fees: {borrowed_fees:.2f}")
            print(f"Total P&L: {total_pnl:.2f}")
            print(f"Net P&L: {net_pnl:.2f}")
            print("=" * 40)  # Separator for readability
            
            # Insert the summary into the database
            self.insert_trade_summary(
                ticker=ticker, date=today, 
                total_buy_shares=total_buy_shares, 
                total_sell_shares=total_sell_shares, 
                avg_buy_price=avg_buy_price, 
                avg_sell_price=avg_sell_price, 
                total_fees=total_fees, 
                total_pnl=total_pnl,
                borrowed_fees=borrowed_fees,
                net_pnl=net_pnl
            )

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


# Example usage
if __name__ == "__main__":
    trades_summary = TradesSummary()

    # Start listening to server responses in a separate thread
    listener_thread = threading.Thread(target=trades_summary.listen_to_server)
    listener_thread.daemon = True
    listener_thread.start()

    # Request today's trades
    trades_summary.get_todays_trades()

    # Keep the main thread running
    while True:
        time.sleep(1)
