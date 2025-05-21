import socket
import logging

logging.basicConfig(level=logging.DEBUG)

LOCAL_SERVER_PORT = 5001

def send_trade_details(trade_details):
    command = f"TRADE {trade_details['tradeID']} {trade_details['time']} {trade_details['strategy']} {trade_details['ticker']} {trade_details['shares']} {trade_details['entry_price']} {trade_details['stop_loss']} {trade_details['sellOrderID']} {trade_details['stopOrderID']}"
    send_command(command)

def send_command(command):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', LOCAL_SERVER_PORT))
    client_socket.sendall(command.encode())
    logging.debug(f'Sent command: {command}')
    client_socket.close()

if __name__ == "__main__":
    # Simulated trade details
    trade_details = {
        'tradeID': '12345678',
        'time': '14:32:10',
        'strategy': '1min',
        'ticker': 'AAPL',
        'shares': 100,
        'entry_price': 200.00,
        'stop_loss': 230.00,
        'sellOrderID': '42562',
        'stopOrderID': '42563'
    }

    send_trade_details(trade_details)
