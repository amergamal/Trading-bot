import logging
from datetime import datetime
from order_execution import OrderExecution
import threading

# Initialize OrderExecution instance
order_execution = OrderExecution()

# Define the sell market order commands for different tickers
orders = [
    {
        'order_type': 'market',
        'ticker': 'TSLA',
        'shares': 100,
        'trade_id': 'T1234',
        'stop_price': 257.60,
        'date': datetime.now().strftime('%Y-%m-%d')
    },
    {
        'order_type': 'market',
        'ticker': 'NAAS',
        'shares': 505,
        'trade_id': 'T1236',
        'stop_price': 2.00,
        'date': datetime.now().strftime('%Y-%m-%d')
    },
    {
            'order_type': 'market',
        'ticker': 'AAPL',
        'shares': 100,
        'trade_id': 'T1234',
        'stop_price': 236,
        'date': datetime.now().strftime('%Y-%m-%d')
    }
]

# Function to execute an order command
def execute_order(order):
    order_execution.execute_command(order)

# Create and start a thread for each order
threads = []
for order in orders:
    thread = threading.Thread(target=execute_order, args=(order,))
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()
