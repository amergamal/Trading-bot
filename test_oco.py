import socket
import logging
import uuid
import time
import threading

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# DAS API Credentials
DAS_API_BASE_URL = '127.0.0.1'
DAS_API_PORT = 9800
DAS_API_USERNAME = 'CD4832'
DAS_API_PASSWORD = 'Gamala123'
DAS_API_ACCOUNT = 'TRCD4832'

class APIConnection:
    def __init__(self):
        self.logger = logging.getLogger('Server')
        self.sock = None
        self.connected = False
        self.response_buffer = []

    def create_socket(self):
        self.logger.debug('Creating socket...')
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.logger.debug(f'Socket created. Attempting to connect to {DAS_API_BASE_URL}:{DAS_API_PORT}...')
        s.connect((DAS_API_BASE_URL, DAS_API_PORT))
        self.logger.debug('Connection established')
        return s

    def send_command(self, command):
        if self.sock:
            try:
                full_command = f'{command}\r\n'
                self.logger.debug(f'Sending command to DAS: {full_command}')
                self.sock.sendall(full_command.encode())
            except (OSError, BrokenPipeError) as e:
                logging.error(f'Error sending command: {e}')
                self.sock = None
                self.connected = False
        else:
            logging.warning('Socket is None, cannot send command')

    def receive_response(self):
        if self.sock:
            try:
                response = b""
                while True:
                    part = self.sock.recv(4096)
                    if not part:
                        raise OSError("Disconnected")
                    response += part
                    if len(part) < 4096:
                        break
                response = response.decode()
                self.logger.debug(f'Received response: {response}')
                return response
            except (OSError, BrokenPipeError) as e:
                logging.error(f'Error receiving response: {e}')
                self.sock = None
                self.connected = False
                return None
        return None

    def login(self):
        self.sock = self.create_socket()
        login_command = f'LOGIN {DAS_API_USERNAME} {DAS_API_PASSWORD} {DAS_API_ACCOUNT} 0'
        self.send_command(login_command)
        while True:
            login_response = self.receive_response()
            if login_response:
                if 'LOGIN SUCCESSED' in login_response:
                    self.logger.info('Login successful')
                    self.connected = True
                    break
                elif '#Welcome to DAS Command API' in login_response:
                    self.logger.info('Welcome message received, waiting for login success...')
                elif '#Please login to continue.' in login_response:
                    logging.warning('Received prompt to login again, retrying...')
                    self.send_command(login_command)
                else:
                    logging.error(f'Unexpected login response: {login_response}')
                    self.sock.close()
                    self.connected = False
                    break

    def is_connected(self):
        return self.connected

# Generate a unique token (not used, but kept as per original script)
def generate_token():
    return str(uuid.uuid4().int)[:7]  # Generate a 7-digit unique token

# Send an OCO order for TSLA
def send_oco_order(api_connection):
    # Input parameters for the test
    ticker = "TSLA"
    shares = 1
    entry_price = 279.5  # Short entry price (limit price)
    stop_price = 282.90  # Stop loss price (higher, to limit losses if price rises)
    target_price = 275.0 # Buy target price (lower, to take profit if price falls)
    route = "SMAT"       # Route for short sell
    tif = "DAY+"         # Time in force

    # Step 1: Place the primary sell short order using NEWORDER
    primary_command = (
        f"NEWORDER 1 SS {ticker} {route} {shares} MKT {entry_price} TIF={tif}"
    )
    api_connection.send_command(primary_command)
    time.sleep(1)  # Give the API time to respond
    primary_response = api_connection.receive_response()

    # Check if primary order was placed successfully
    if primary_response is None:
        logging.error("No response received for primary order")
        return None
    if "Accepted" not in primary_response and "Executed" not in primary_response:
        logging.error(f"Primary sell short order failed: {primary_response}")
        return None

    # Step 2: Place the OCO orders using SCRIPT and send them immediately
    oco_command = (
        f"SCRIPT test1 Symbol={ticker};"
        f"ROUTE={route};Share={shares};TIF={tif};"
        f"OCO=RT:STOP STOPTYPE:MARKET StopPrice:{stop_price} ACT:BUY QTY:{shares} TIF:{tif};"
        f"OCO=RT:LIMIT PX:{target_price} ACT:BUY QTY:{shares} TIF:{tif};"
        f"BUY=Send"
    )
    api_connection.send_command(oco_command)
    time.sleep(1)  # Give the API time to respond
    oco_response = api_connection.receive_response()

    # Handle response
    if oco_response is None:
        logging.error("No response received for OCO orders")
        return None
    elif "ScriptError" in oco_response:
        logging.error(f"Failed to send OCO orders: {oco_response}")
        return None
    elif "OCO=RT:STOP" in oco_response and "OCO=RT:LIMIT" in oco_response and "Accepted" in oco_response:
        logging.info("OCO orders sent successfully")
        return True
    else:
        logging.error(f"OCO orders not confirmed in response: {oco_response}")
        return None

# Main function to run the test
def main():
    try:
        # Step 1: Initialize API connection and login
        api_connection = APIConnection()
        api_connection.login()
        if not api_connection.is_connected():
            print("Failed to login to DAS. Exiting.")
            return

        # Step 2: Send the OCO order
        result = send_oco_order(api_connection)
        if result:
            print("OCO order sent successfully.")
        else:
            print("Failed to send OCO order. Check logs for details.")

    except Exception as e:
        print(f"Error during execution: {e}")
        logging.error(f"Error during execution: {e}")
    finally:
        if api_connection.sock:
            api_connection.sock.close()

# Run the script
if __name__ == "__main__":
    main()