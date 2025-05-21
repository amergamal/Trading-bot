import socket
import threading
import time

LOCAL_SERVER_HOST = 'localhost'
LOCAL_SERVER_PORT = 5002

def handle_client(client_socket):
    try:
        while True:
            message = "%ORDER 12345 67890 TICKER B 100 0 100 10.5 SMAT Executed 12:34:56 0 TRCD1234 CD1234\n"
            client_socket.sendall(message.encode())
            time.sleep(5)  # Send a message every 5 seconds
    except Exception as e:
        print(f"Error handling client: {e}")
    finally:
        client_socket.close()

def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((LOCAL_SERVER_HOST, LOCAL_SERVER_PORT))
    server.listen(5)
    print(f"Server listening on {LOCAL_SERVER_PORT}")

    try:
        while True:
            client_socket, addr = server.accept()
            print(f"Accepted connection from {addr}")
            client_thread = threading.Thread(target=handle_client, args=(client_socket,))
            client_thread.start()
    except KeyboardInterrupt:
        print("Server shutting down...")
    finally:
        server.close()

if __name__ == "__main__":
    start_server()
