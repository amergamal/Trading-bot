import socket
import logging

# Local server connection details
LOCAL_SERVER_HOST = 'localhost'
LOCAL_SERVER_PORT = 5012

def send_market_order(ticker='TSLA', shares=10, stop_price=203):
    try:
        # Create a socket connection to the local server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.connect((LOCAL_SERVER_HOST, LOCAL_SERVER_PORT))
            logging.info(f"Connected to server at {LOCAL_SERVER_HOST}:{LOCAL_SERVER_PORT}")

            # Construct the market order command
            token = 99999  # Use a random token number
            
            command = f'NEWORDER {token} B {ticker} SMAT {shares} STOPMKT {stop_price}'

            # Send the command to the server
            logging.info(f"Sending market order command: {command.strip()}")
            client_socket.sendall(command.encode())

            # Close the connection after sending
            logging.info("Market order command sent, closing connection.")

    except socket.error as e:
        logging.error(f"Socket error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    # Send a market order for TSLA
    send_market_order()
