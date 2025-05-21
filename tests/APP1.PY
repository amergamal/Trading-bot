import logging
from flask import Flask, jsonify
from flask_socketio import SocketIO
import threading
import os
from order_execution import OrderExecution
from trade_monitor import TradeMonitor

app = Flask(__name__)
socketio = SocketIO(app)

logging.basicConfig(level=logging.DEBUG)

# Initialize the TradeMonitor instance
logging.info("Initializing TradeMonitor in app.py")
trade_monitor = TradeMonitor()

# Initialize the OrderExecution instance with TradeMonitor
logging.info("Initializing OrderExecution with TradeMonitor in app.py")
order_execution = OrderExecution(trade_monitor)

# Root route
@app.route('/')
def index():
    return "Order Execution and Trade Monitor Test App"

# Route to trigger a test order
@app.route('/test_order', methods=['GET'])
def test_order():
    # Example of sending a test order
    result = order_execution.send_order('test_ticker', 100, 'buy', 'market')
    return jsonify({'result': result})

# Function to start TradeMonitor monitoring
def start_trade_monitor():
    logging.info("Starting TradeMonitor monitoring")
    threading.Thread(target=trade_monitor.monitor_lu_price, daemon=True).start()
    threading.Thread(target=trade_monitor.listen_to_server, daemon=True).start()
    threading.Thread(target=trade_monitor.schedule_end_of_day_tasks, daemon=True).start()
    logging.info("TradeMonitor threads started")

# Function to start all modules
def start_modules():
    if not hasattr(start_modules, 'initialized'):
        start_modules.initialized = False

    if start_modules.initialized:
        logging.info("Modules already started.")
        return

    start_trade_monitor()
    logging.info("Started all modules")

    start_modules.initialized = True

# Main entry point
if __name__ == "__main__":
    logging.debug("Starting the application.")
    port = int(os.environ.get('PORT', 5000))
    start_modules()
    socketio.run(app, host='0.0.0.0', port=port, debug=True)
