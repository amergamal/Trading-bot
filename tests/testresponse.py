import socket
import logging
import time
import threading

LOCAL_SERVER_HOST = 'localhost'
LOCAL_SERVER_PORT = 5002

class TestMonitor:
    def __init__(self):
        self.logger = logging.getLogger('TestMonitor')
        self.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        self.logger.info("TestMonitor instance created and initialized.")

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

    def process_response(self, response):
        """Process server response lines."""
        lines = response.strip().split('\n')
        for line in lines:
            if line.startswith('%ORDER'):
                self.logger.debug(f"Processing response line: {line}")
                parts = line.split()
                try:
                    order_id = parts[1]  # Order ID is the second element
                    ticker = parts[3]    # Ticker is the fourth element
                    price = parts[9]     # Price is the tenth element
                    status = parts[11]   # Status is the twelfth element
                    self.logger.info(f"Ticker: {ticker}, Order ID: {order_id}, Price: {price}, Status: {status}")
                except IndexError as e:
                    self.logger.error(f'Error parsing %ORDER response: {e}')

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.DEBUG)

    # Create a TestMonitor instance
    test_monitor = TestMonitor()

    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
