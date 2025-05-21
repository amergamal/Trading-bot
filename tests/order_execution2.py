import sqlite3
import socket
import random
import logging
import time
import threading
from datetime import datetime

logging.basicConfig(level=logging.DEBUG)

# Initialize a global lock for managing access to the token_map
token_map_lock = threading.Lock()

LOCAL_SERVER_PORT = 5001

def generate_token():
    return str(random.randint(100000, 999999))

def process_response(response, token_map):
    lines = response.strip().split('\n')
    order_confirmed = None
    for line in lines:
        if line.startswith('%ORDER'):
            try:
                parts = line.split()
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
                logging.info(f'Order - Order ID: {order_id}, Token: {response_token}, Status: {status}, Time: {time_str}, Ticker: {ticker}, Shares: {shares}, Price: {price}, Action: {action}')
                if status == 'Executed':
                    order_confirmed = True
            except IndexError as e:
                logging.error(f'Error parsing %ORDER response: {e}')
        elif line.startswith('%OrderAct'):
            try:
                parts = line.split()
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
                logging.info(f'Order Act - Order ID: {order_id}, Token: {token}, Action: {action_type}, Note: {note}')
                if action_type == 'Send_Rej':
                    order_confirmed = False  # Set flag to false for rejection
            except IndexError as e:
                logging.error(f'Error parsing %OrderAct response: {e}')
    return order_confirmed

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

def insert_into_database(table, details):
    conn = sqlite3.connect('tms_data.db')
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
    def __init__(self, risk_management=None):
        self.risk_management = risk_management
        self.sock = None
        self.connected = False
        self.response_buffer = []
        self.token_map = {}
        
        self.connect_to_server()
        
    def connect_to_server(self):
        if not self.connected:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect(('localhost', LOCAL_SERVER_PORT))
            self.connected = True
            logging.info("Connected to server")

    def send_command(self, command):
        """Sends a command through the established socket connection.

        Args:
            command: The command string to be sent.

        Returns:
            The socket object used for sending the command.
        """
        if self.sock is None:
            self.connect_to_server()
        self.sock.sendall(command.encode())
        return self.sock

    def send_sell_market_order(self, ticker, shares, trade_id, token_map):
        token = generate_token()
        token_map[token] = {'order_status': None, 'order_act_action': None, 'trade_id': trade_id}
        command_sell = f'NEWORDER {token} S {ticker} SMAT {shares} MKT TIF=DAY {trade_id}'
        client_socket = self.send_command(command_sell)
        response_thread = threading.Thread(target=listen_for_responses, args=(client_socket, token_map))
        response_thread.start()
        return token

    def send_stop_market_order(self, ticker, shares, stop_price, trade_id, token_map):
        token = generate_token()
        token_map[token] = {'order_status': None, 'order_act_action': None, 'trade_id': trade_id}
        command_stop_market = f'NEWORDER {token} B {ticker} SMAT {shares} STOPMKT {stop_price} {trade_id}'
        client_socket = self.send_command(command_stop_market)
        response_thread = threading.Thread(target=listen_for_responses, args=(client_socket, token_map))
        response_thread.start()
        return token

    def execute_command(self, command):
        order_type = command['order_type']
        ticker = command['ticker']
        shares = command['shares']
        trade_id = command['trade_id']
        stop_price = command.get('stop_price')
        logging.info(f"Processing order: {order_type} for {ticker} with {shares} shares")

        if order_type == 'market':
            
            sell_token = self.send_sell_market_order(ticker, shares, trade_id, self.token_map)
            logging.info(f"Sent sell market order for {ticker} with token {sell_token}")

            order_confirmed = process_response(self.sock.recv(1024).decode(), self.token_map)

            if order_confirmed:
                logging.info(f'Sell market order executed for {ticker}, storing details')
                with token_map_lock:
                    store_order_details(self.token_map, sell_token, 'SellMarket')

                if stop_price:
                    stop_token = self.send_stop_market_order(ticker, shares, stop_price, trade_id, self.token_map)
                    logging.info(f'Sent stop market order for {ticker} at stop price {stop_price}')
                    with token_map_lock:  # Ensure thread-safe access
                        store_order_details(self.token_map, stop_token, 'StopMarket')
            else:
                logging.error(f"Sell market order for {ticker} failed. Stop market not sent.")

    def wait_for_order_confirmation(self, token):
        while True:
            time.sleep(1)
            response = self.sock.recv(1024).decode()
            order_confirmed = process_response(response, self.token_map)
        
            if order_confirmed is not None:
                return order_confirmed

if __name__ == "__main__":
    order_execution = OrderExecution()

    # Keep the script running indefinitely
    while True:
        time.sleep(1)
