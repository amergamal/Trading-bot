import logging
from datetime import datetime
from order_execution import OrderExecution
import threading

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

logging.debug('Initializing OrderExecution instance')
# Initialize OrderExecution instance
order_execution = OrderExecution()

# Define the sell market order commands for different tickers
orders = [
    {
        'order_type': 'market',
        'strategy': '1Min',
        'ticker': 'TSLA',
        'shares': 100,
        'trade_id': 'T1235',
        'stop_price': 258.00,
        'date': datetime.now().strftime('%Y-%m-%d')
    }
]

logging.debug('Order definitions complete')

# Function to execute an order command
def execute_order(order):
    logging.debug(f'Starting execution of order: {order}')
    order_execution.execute_command(order)
    logging.debug(f'Finished execution of order: {order}')

logging.debug('Creating and starting threads for each order')
# Create and start a thread for each order
threads = []
for order in orders:
    logging.debug(f'Creating thread for order: {order}')
    thread = threading.Thread(target=execute_order, args=(order,))
    threads.append(thread)
    thread.start()
    logging.debug(f'Started thread for order: {order}')

logging.debug('Waiting for all threads to complete')
# Wait for all threads to complete
for thread in threads:
    thread.join()
    logging.debug('Thread has completed')

logging.debug('All orders have been processed.')
