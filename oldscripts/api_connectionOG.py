import sqlite3
import socket
import logging
import time
import threading
from datetime import datetime

logging.basicConfig(level=logging.DEBUG)

DAS_API_BASE_URL = '127.0.0.1'
DAS_API_PORT = 9800
DAS_API_USERNAME = 'CD4832'
DAS_API_PASSWORD = 'Gamala123'
DAS_API_ACCOUNT = '104832'
LOCAL_SERVER_PORT = 5012

class APIConnection:
    def __init__(self):
        self.sock = None
        self.connected = False
        self.server_socket = None
        self.response_buffer = []
        self.reconnecting = False
        self.connection_ready_event = threading.Event()  # Event to signal readiness
        self.clients = []  # List to keep track of connected clients
        self.clients_lock = threading.Lock()  # Lock for thread-safe client management

    def create_socket(self):
        logging.debug('Creating socket...')
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        logging.debug(f'Socket created. Attempting to connect to {DAS_API_BASE_URL}:{DAS_API_PORT}...')
        s.connect((DAS_API_BASE_URL, DAS_API_PORT))
        logging.debug('Connection established')
        return s

    def send_command(self, command):
        if self.sock:
            try:
                full_command = f'{command}\r\n'
                logging.debug(f'Sending command to DAS: {full_command}')
                self.sock.sendall(full_command.encode())
            except (OSError, BrokenPipeError) as e:
                logging.error(f'Error sending command: {e}')
                self.handle_disconnection()
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
                logging.debug(f'Received response: {response}')
                return response
            except (OSError, BrokenPipeError) as e:
                logging.error(f'Error receiving response: {e}')
                self.handle_disconnection()
                return None

    def handle_disconnection(self):
        if self.sock:
            self.sock.close()
        self.sock = None
        if self.connected:
            self.connected = False
            self.update_connection_status(False)
        if not self.reconnecting:
            self.reconnect()

    def continuously_receive(self):
        while self.connected:
            response = self.receive_response()
            if response:
                logging.info(f'Received response from DAS API: {response}')
                self.response_buffer.append(response)
                self.broadcast_response(response)  # Broadcast to all clients

    def login(self):
        self.sock = self.create_socket()
        login_command = f'LOGIN {DAS_API_USERNAME} {DAS_API_PASSWORD} {DAS_API_ACCOUNT} 0'
        self.send_command(login_command)
        while True:
            login_response = self.receive_response()
            if login_response:
                if 'LOGIN SUCCESSED' in login_response:
                    logging.info('Login successful')
                    if not self.connected:
                        self.connected = True
                        self.update_connection_status(True)
                    self.connection_ready_event.set()  # Signal that connection is ready
                    logging.info('API connection is now ready')
                    threading.Thread(target=self.continuously_receive, daemon=True).start()
                    break
                elif '#Welcome to DAS Command API' in login_response:
                    logging.info('Welcome message received, waiting for login success...')
                elif '#Please login to continue.' in login_response:
                    logging.warning('Received prompt to login again, retrying...')
                    self.send_command(login_command)
                else:
                    logging.error(f'Unexpected login response: {login_response}')
                    self.sock.close()
                    self.connected = False
                    self.update_connection_status(False)
                    break

    def keep_alive(self):
        while True:
            time.sleep(30)
            if self.sock:
                try:
                    self.send_command('ECHO')
                except AttributeError:
                    logging.warning('Socket is None, skipping keep alive')
            else:
                logging.warning('Socket is None, skipping keep alive')

    def start_local_server(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('localhost', LOCAL_SERVER_PORT))
        self.server_socket.listen(5)
        logging.info(f'Local server started on port {LOCAL_SERVER_PORT}')
        self.connection_ready_event.set()
        
        while True:
            client_socket, addr = self.server_socket.accept()
            logging.info(f'Accepted connection from {addr}')
            with self.clients_lock:
                self.clients.append(client_socket)
            threading.Thread(target=self.handle_client, args=(client_socket, addr)).start()

    def handle_client(self, client_socket, addr):
        try:
            while True:
                command = client_socket.recv(1024).decode().strip()
                if command:
                    logging.info(f'Received command from client: {command}')
                    self.send_command(command)
        except (OSError, ConnectionResetError):
            pass
        finally:
            with self.clients_lock:
                if client_socket in self.clients:
                    self.clients.remove(client_socket)
            client_socket.close()
            logging.info(f'Connection with {addr} closed')

    def broadcast_response(self, response):
        """Broadcast response to all connected clients."""
        with self.clients_lock:
            for client in self.clients:
                try:
                    client.sendall(response.encode())
                except (OSError, ConnectionResetError):
                    self.clients.remove(client)

    def is_connected(self):
        return self.connected

    def update_connection_status(self, status):
        conn = sqlite3.connect('EOD_data.db')
        c = conn.cursor()
        c.execute('INSERT INTO connection_status (timestamp, status) VALUES (?, ?)', (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 1 if status else 0))
        conn.commit()
        conn.close()
        logging.info(f'Updated connection status to {"True" if status else "False"} in database')

    def reconnect(self):
        logging.info('Attempting to reconnect...')
        self.reconnecting = True
        while not self.connected:
            try:
                self.sock = self.create_socket()
                self.login()
            except Exception as e:
                logging.error(f'Reconnection failed: {e}')
                time.sleep(5)  # Wait before retrying
        self.reconnecting = False

if __name__ == "__main__":
    api_connection = APIConnection()
    try:
        api_connection.login()
        if api_connection.connected:
            threading.Thread(target=api_connection.keep_alive, daemon=True).start()
            threading.Thread(target=api_connection.start_local_server, daemon=True).start()
            # Keep the main thread running
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
