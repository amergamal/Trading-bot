import socket
import logging

# Local server connection details
LOCAL_SERVER_HOST = 'localhost'
LOCAL_SERVER_PORT = 5012

def send_get_trades():
    try:
        # Create a socket connection to the local server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.connect((LOCAL_SERVER_HOST, LOCAL_SERVER_PORT))
            logging.info(f"Connected to server at {LOCAL_SERVER_HOST}:{LOCAL_SERVER_PORT}")

            # Construct the market order command
            
            command = f'GET TRADES'
            

            # Send the command to the server
            logging.info(f"Sending get trades command: {command.strip()}")
            client_socket.sendall(command.encode())

            # Close the connection after sending
            logging.info("Get trades command sent, closing connection.")

    except socket.error as e:
        logging.error(f"Socket error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    # Send a market order for TSLA
    send_get_trades()
