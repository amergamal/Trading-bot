import sqlite3
import socket
import random
import logging
import time
import threading
from datetime import datetime
from trade_monitor import TradeMonitor  # Import the TradeMonitor class
from sl_monitor import SLMonitor  # Import the SLMonitor class
from end_of_day import EndOfDay
from short_locate import Slocate  # Import the Slocate class

logging.basicConfig(level=logging.DEBUG)

# Initialize a global lock for managing access to the token_map
token_map_lock = threading.Lock()

LOCAL_SERVER_PORT = 5012

def generate_token():
    return str(random.randint(100000, 999999))

def send_command(command):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', LOCAL_SERVER_PORT))
    client_socket.sendall(command.encode())
    logging.debug(f'Sent command: {command}')
    return client_socket

def process_response(response, token_map):
    lines = response.strip().split('\n')
    order_received = False
    for line in lines:
        logging.debug(f'Processing response line: {line}')
        if line.startswith('%ORDER'):
            order_received = True
            parts = line.split()
            try:
                response_token = parts[2]
                order_id = parts[1]
                time_str = parts[12]
                ticker = parts[3]
                shares = parts[6]
                price = parts[9]
                action = parts[4]
                status = parts[11]
                with token_map_lock:  # Ensure thread-safe access
                    if response_token in token_map:
                        token_map[response_token].update({
                            'order_status': status,
                            'order_time': time_str,
                            'ticker': ticker,
                            'shares': shares,
                            'price': price,
                            'action': action,
                            'order_id': order_id
                        })
                        logging.debug(f"Updated token_map for token {response_token}: {token_map[response_token]}")
                logging.info(f'Order - Order ID: {order_id}, Token: {response_token}, Status: {status}, Time: {time_str}, Ticker: {ticker}, Shares: {shares}, Price: {price}, Action: {action}')
            except IndexError as e:
                logging.error(f'Error parsing %ORDER response: {e}')
        elif line.startswith('%OrderAct'):
            parts = line.split()
            try:
                order_id = parts[1]
                action_type = parts[2]
                b_s = parts[3]
                ticker = parts[4]
                shares = parts[5]
                price = parts[6]
                route = parts[7]
                time_str = parts[8]
                note = ' '.join(parts[9:-1])  # Join everything between time and token as the note
                token = parts[-1]  # The token is the last part
                with token_map_lock:  # Ensure thread-safe access
                    if token in token_map:
                        token_map[token].update({
                            'order_act_action': action_type,
                            'note': note,
                            'order_time': time_str,
                            'ticker': ticker,
                            'shares': shares,
                            'price': price,
                            'action': b_s,
                            'order_id': order_id
                        })
                        logging.debug(f"Updated token_map for token {token}: {token_map[token]}")
                logging.info(f'Order Act - Order ID: {order_id}, Token: {token}, Action: {action_type}, Note: {note}')
            except IndexError as e:
                logging.error(f'Error parsing %OrderAct response: {e}')
    return order_received

def listen_for_responses(client_socket, token_map):
    buffer_size = 4096
    while True:
        response = client_socket.recv(buffer_size).decode()
        if response:
            logging.debug(f'Received response: {response}')
            process_response(response, token_map)
        else:
            logging.debug('No response received, waiting...')
            time.sleep(0.5)  # Wait before checking again

def send_sell_market_order(ticker, shares, trade_id, token_map):
    token = generate_token()
    token_map[token] = {'order_status': None, 'order_act_action': None, 'trade_id': trade_id}
    command_sell = f'NEWORDER {token} S {ticker} SMAT {shares} MKT TIF=DAY {trade_id}'
    client_socket = send_command(command_sell)
    response_thread = threading.Thread(target=listen_for_responses, args=(client_socket, token_map))
    response_thread.start()
    return token

def send_stop_market_order(ticker, shares, stop_price, trade_id, token_map):
    token = generate_token()
    token_map[token] = {'order_status': None, 'order_act_action': None, 'trade_id': trade_id}
    command_stop_market = f'NEWORDER {token} B {ticker} SMAT {shares} STOPMKT {stop_price} {trade_id}'
    client_socket = send_command(command_stop_market)
    response_thread = threading.Thread(target=listen_for_responses, args=(client_socket, token_map))
    response_thread.start()
    return token

def insert_into_database(table, details):
    conn = sqlite3.connect('EOD_data.db')
    cursor = conn.cursor()
    columns = ', '.join(details.keys())
    placeholders = ', '.join(['?'] * len(details))
    sql = f'INSERT INTO {table} ({columns}) VALUES ({placeholders})'
    try:
        cursor.execute(sql, list(details.values()))
        conn.commit()
        logging.info(f'Successfully inserted into {table}: {details}')
    except sqlite3.IntegrityError as e:
        logging.error(f'Error inserting into {table}: {e}')
    conn.close()

def store_order_details(token_map, token, table):
    details = token_map.get(token)
    if details:
        data = {
            'TradeID': details['trade_id'],
            'time': details.get('order_time', 'N/A'),
            'ticker': details.get('ticker', 'N/A'),
            'shares': details.get('shares', 'N/A'),
            'price': details.get('price', 'N/A'),
            'token': token,
            'orderID': details.get('order_id', 'N/A'),
            'action': details.get('action', 'N/A'),
            'status': details.get('order_status', 'N/A'),
            'act_status': details.get('order_act_action', 'N/A'),
            'notes': details.get('note', 'N/A'),
            'date': details.get('date', datetime.now().strftime('%Y-%m-%d'))  # Use date from signal or current date
        }
        logging.debug(f'Inserting into {table}: {data}')
        insert_into_database(table, data)

class OrderExecution:
    def __init__(self, trade_monitor=None, risk_management=None):
        self.trade_monitor = trade_monitor 
        self.risk_management = risk_management
        self.sock = None
        self.connected = False
        self.response_buffer = []
        self.token_map = {}
        
        self.sl_monitor = SLMonitor()  # Create an instance of SLMonitor
        self.end_of_day = EndOfDay()
        self.slocate = Slocate()  # Create an instance of Slocate
        

    def execute_command(self, command):
        order_type = command['order_type']
        ticker = command['ticker']
        shares = command['shares']
        trade_id = command['trade_id']
        stop_price = command.get('stop_price')
        strategy = command.get('strategy')  # Get the strategy from the command
        logging.info(f"Processing order: {order_type} for {ticker} with {shares} shares")

        # Check BorrowedShares table for available shares
        available_shares = self.get_available_shares(ticker)
        if available_shares < shares:
            # Calculate how many shares are needed
            needed_shares = shares - available_shares
            logging.info(f"Requesting to borrow {needed_shares} additional shares for {ticker}.")
            
            # Attempt to borrow additional shares
            borrow_result = self.slocate.receive_order_details(ticker, needed_shares)
            
            # Wait 10 seconds to allow Slocate to process the request
            logging.info("Waiting 10 seconds for Slocate to process the borrow request.")
            time.sleep(5)  # Wait for 10 seconds before proceeding

            if borrow_result['status'] == 'error':
                logging.warning(f"Borrow failed for {ticker}: {borrow_result['message']}")
                # Proceed with the trade execution even if borrowing fails
               

        # Proceed with the order execution if sufficient shares are available
        if order_type == 'market':
            sell_token = send_sell_market_order(ticker, shares, trade_id, self.token_map)
            logging.info(f"Sent sell market order for {ticker} with token {sell_token}")

            order_confirmed = self.wait_for_order_confirmation(sell_token)

            # Store the order details regardless of confirmation status
            with token_map_lock:  # Ensure thread-safe access
                store_order_details(self.token_map, sell_token, 'SellMarket')

            if order_confirmed:
                logging.info(f'Sell market order executed for {ticker}')
                if stop_price:
                    stop_token = send_stop_market_order(ticker, shares, stop_price, trade_id, self.token_map)
                    logging.info(f'Sent stop market order for {ticker} at stop price {stop_price}')
                    stop_order_confirmed = self.wait_for_stop_order_confirmation(stop_token)
                    with token_map_lock:  # Ensure thread-safe access
                        store_order_details(self.token_map, stop_token, 'StopMarket')
                    if stop_order_confirmed:
                        trade_details = {
                            'tradeID': trade_id,
                            'time': datetime.now().strftime('%H:%M:%S'),
                            'strategy': strategy,
                            'ticker': ticker,
                            'shares': shares,
                            'entry_price': self.token_map[sell_token]['price'],
                            'stop_loss': stop_price,
                            'sellOrderID': self.token_map[sell_token]['order_id'],
                            'stopOrderID': self.token_map[stop_token]['order_id']
                        }
                        logging.info(f"Sending trade details to TradeMonitor: {trade_details}")
                        self.trade_monitor.receive_order_details(trade_details)
                        logging.info("Trade details sent to TradeMonitor successfully")

                        # Send trade details to SLMonitor
                        logging.info(f"Sending trade details to SLMonitor: {trade_details}")
                        self.sl_monitor.receive_order_details(trade_details)
                        logging.info("Trade details sent to SLMonitor successfully")
                        
                        # Send trade details to EndOfDay
                        logging.info(f"Sending trade details to EndOfDay: {trade_details}")
                        self.end_of_day.receive_order_details(trade_details)
                        logging.info("Trade details sent to EndOfDay successfully")
                       
                    else:
                        logging.error(f"Stop market order for {ticker} failed.")
            else:
                logging.error(f"Sell market order for {ticker} failed.")

    def get_available_shares(self, ticker):
        """Check the BorrowedShares table to see how many shares are available."""
        conn = sqlite3.connect('EOD_data.db')
        try:
            cursor = conn.cursor()
            
            # Get today's date in the format YYYY-MM-DD
            today_date = datetime.now().strftime('%Y-%m-%d')
        
            # Query to select available shares for today's date
            cursor.execute("""
                SELECT available_shares 
                FROM BorrowedShares 
                WHERE ticker = ? 
                AND DATE(last_updated) = ?
            """, (ticker, today_date))
            
            row = cursor.fetchone()
            if row:
                available_shares = row[0]
                logging.info(f"{available_shares} shares available for {ticker}.")
                return available_shares
            else:
                logging.warning(f"No shares found for {ticker} in BorrowedShares table.")
                return 0
        except sqlite3.Error as e:
            logging.error(f"Database error while checking available shares: {e}")
            return 0
        finally:
            conn.close()

    def wait_for_order_confirmation(self, token, timeout=30):
        start_time = time.time()
        while True:
            time.sleep(1)
            with token_map_lock:
                order_status = self.token_map.get(token, {}).get('order_status')
                order_act_status = self.token_map.get(token, {}).get('order_act_status')
                if order_status == 'Executed':
                    logging.info(f"Order {token} status: Executed")
                    return True
                if order_act_status == 'Send_Rej':
                    logging.info(f"Order {token} status: Send_Rej")
                    return False
            
            if time.time() - start_time > timeout:
                logging.warning(f"Order {token} confirmation timed out.")
                return False

    def wait_for_stop_order_confirmation(self, token, timeout=30):
        start_time = time.time()
        while True:
            time.sleep(1)
            with token_map_lock:
                order_status = self.token_map.get(token, {}).get('order_status')
                order_act_status = self.token_map.get(token, {}).get('order_act_status')
                order_id = self.token_map.get(token, {}).get('order_id')
                if order_status == 'Accepted':
                    logging.info(f"Stop order {token} status: Accepted")
                    return True
                if order_act_status == 'Send_Rej':
                    logging.info(f"Stop order {token} status: Send_Rej")
                    return False
            
            if time.time() - start_time > timeout:
                logging.warning(f"Stop order {token} confirmation timed out.")
                return False

if __name__ == "__main__":
    order_execution = OrderExecution(None)
    # Initialize the EndOfDay instance
    end_of_day_instance = EndOfDay()
    
    order_execution.end_of_day = end_of_day_instance  # Pass the EndOfDay instance
    
    
    # Keep the script running indefinitely
    while True:
        time.sleep(1)
