import socket
import psycopg2
from psycopg2 import pool
import config
import logging
import threading
import time
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from datetime import datetime

app = Flask(__name__)
db_path = 'EOD_data.db'
LOCAL_SERVER_HOST = 'localhost'
LOCAL_SERVER_PORT = 5015
ACCOUNT = '104832'
CORS(app)

# Global variable to store offers
global_offers = []

# Initialize PostgreSQL connection pool
try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=20,
        **config.DB_CONFIG
    )
    logging.info("PostgreSQL connection pool initialized")
except psycopg2.OperationalError as e:
    logging.error(f"Failed to initialize PostgreSQL connection pool: {e}")
    raise

class Slocate:
    def __init__(self):
        self.logger = logging.getLogger('SLocate')
        self.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        self.db_path = db_path
        self.client_socket = None
        self.lock = threading.Lock()
        self.keep_listening = True
        self.offers = {}
        self.accepted_orders = {}  # Track accepted orders per trade_id
        self.requested_shares = {}
        self.located_offers = {}  # Store confirmed Located offers
        self.pending_offers = {}  # Store offers awaiting Located status
        self.available_shares_map = {}  # Store SLAvailQuery results
        self.current_located = {}
        self.collecting_locates = False
        self.current_short = {}
        self.collecting_positions = False
        self.logger.info("SLocate instance created and initialized.")

    def connect_to_server(self):
        attempts = 0
        while attempts < 5:
            try:
                self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.logger.info("Attempting to connect to server...")
                self.client_socket.connect((LOCAL_SERVER_HOST, LOCAL_SERVER_PORT))
                self.logger.info("Connected to server.")
                return True
            except socket.error as e:
                self.logger.error(f"Socket error while connecting: {e}")
                self.client_socket = None
                attempts += 1
                self.logger.info(f"Retrying connection in 5 seconds... (Attempt {attempts})")
                time.sleep(5)
        self.logger.error("Failed to connect to server after multiple attempts.")
        return False

    def listen_to_server(self):
        while self.keep_listening:
            if self.client_socket:
                try:
                    response = self.client_socket.recv(4096).decode()
                    if response:
                        self.logger.debug(f"Received response: {response}")
                        self.handle_response(response)
                    else:
                        self.logger.debug("No response received, waiting...")
                        time.sleep(0.5)
                except socket.error as e:
                    self.logger.error(f"Socket error while listening: {e}")
                    self.keep_listening = False
                except Exception as e:
                    self.logger.error(f"Error listening to server: {e}")
                    self.keep_listening = False
            else:
                self.logger.debug("Client socket is not connected.")
                time.sleep(1)

    def handle_response(self, response):
        lines = response.strip().split('\n')
        for line in lines:
            self.logger.debug(f"Processing line: {line}")
            parts = line.split()
            if len(parts) < 2:
                continue

            try:
                upper_first = parts[0].upper()

                # Handle %SLRET responses
                if upper_first == "%SLRET":
                    ticker = parts[2]
                    route = parts[5] if len(parts) > 5 else ""
                    note = parts[6] if len(parts) > 6 else ""
                    order_id = parts[1]
                    shares = self.requested_shares.get(ticker, 0)

                    if note in ["AlreadyShortable", "Symbolisalreadyshortable!"]:
                        offer = {
                            'ticker': ticker,
                            'shares': shares,
                            'price': 0.0,
                            'route': route,
                            'order_id': order_id,
                            'total_cost': 0.0,
                            'note': 'already shortable'
                        }
                        self.logger.debug(f"Already shortable detected: Ticker={ticker}, Route={route}, Shares={shares}")
                    else:
                        price = float(parts[3])
                        shares = int(parts[4])
                        offer = {
                            'ticker': ticker,
                            'shares': shares,
                            'price': price,
                            'route': route,
                            'order_id': order_id,
                            'total_cost': price * shares,
                            'note': note
                        }
                        self.logger.debug(f"Valid offer: Ticker={ticker}, Price={price}, Shares={shares}, Route={route}, Total Cost={offer['total_cost']}")

                    if ticker not in self.offers:
                        self.offers[ticker] = []
                    self.offers[ticker].append(offer)

                # Handle %SLOrder responses
                elif upper_first == "%SLORDER":
                    if len(parts) < 8:
                        self.logger.warning(f"Incomplete %SLORDER response: {line}. Skipping.")
                        continue
                    status = parts[7]
                    order_id = parts[1]
                    ticker = parts[2]
                    shares = int(parts[3])
                    exeshares = int(parts[5])
                    exeprice = float(parts[6])
                    route = parts[8] if len(parts) > 8 else ""
                    notes = ' '.join(parts[10:]) if len(parts) > 10 else ""
                    self.logger.info(f"SLORDER response - Order ID: {order_id}, Ticker: {ticker}, Status: {status}, Exeshares: {exeshares}")

                    if status == "Offered":
                        total_cost = exeprice * shares
                        offer = {
                            'ticker': ticker,
                            'shares': shares,
                            'price': exeprice,
                            'route': route,
                            'order_id': order_id,
                            'total_cost': total_cost,
                            'note': ''
                        }
                        if ticker not in self.offers:
                            self.offers[ticker] = []
                        self.offers[ticker].append(offer)
                        self.logger.debug(f"Offer added: Ticker={ticker}, Price={exeprice}, Shares={shares}, Route={route}")

                    elif status == "Located":
                        self.located_offers[order_id] = {
                            'ticker': ticker,
                            'exeshares': exeshares,
                            'exeprice': exeprice,
                            'route': route
                        }
                        self.logger.info(f"Located shares: {exeshares} for {ticker} at ${exeprice}")
                        if order_id in self.pending_offers:
                            self.check_available_shares(ticker, self.pending_offers[order_id].get('trade_id', 'N/A'), self.located_offers[order_id])
                            del self.pending_offers[order_id]

                        if self.collecting_locates:
                            d = self.current_located.setdefault(ticker, {'shares': 0, 'cost': 0.0})
                            d['shares'] += exeshares
                            d['cost'] += exeprice * exeshares

                    elif status in ["Rejected", "Canceled", "Closed", "Declined"]:
                        if order_id in self.pending_offers:
                            self.logger.error(f"Borrow failed for {ticker}: Status={status}, Notes={notes}")
                            self.pending_offers[order_id]['failure_message'] = f"Status={status}, Notes={notes}"

                # Handle $SLAvailQueryRet responses
                elif upper_first == "$SLAVAILQUERYRET":
                    account, ticker, available_shares = parts[1], parts[2], int(parts[3])
                    self.available_shares_map[ticker] = available_shares
                    self.logger.info(f"Available shares for {ticker} from SLAvailQuery: {available_shares}")

                # Handle locate end markers
                elif upper_first in ["#SLORDEREND", "#LORDEREND"]:
                    self.collecting_locates = False

                # Handle position responses
                elif upper_first == "#POS":
                    self.collecting_positions = True
                    self.current_short = {}

                elif upper_first == "%POS":
                    if self.collecting_positions:
                        if len(parts) < 4:
                            continue
                        ticker = parts[1]
                        ptype = parts[2]
                        qty = int(parts[3])
                        if ptype == "3":
                            self.current_short[ticker] = qty

                elif upper_first == "#POSEND":
                    self.collecting_positions = False

            except (IndexError, ValueError) as e:
                self.logger.error(f"Error processing line '{line}': {e}. Skipping.")
            except Exception as e:
                self.logger.error(f"Unexpected error processing line '{line}': {e}")

    def check_available_shares(self, ticker, trade_id='N/A', located_offer=None):
        self.logger.info(f"Querying available shares for {ticker}...")
        command = f"SLAvailQuery {ACCOUNT} {ticker}"
        self.send_command(command)
        self.logger.info(f"Sending command: {command}")

        start_time = time.time()
        while ticker not in self.available_shares_map and time.time() - start_time < 10:
            time.sleep(0.5)

        available_shares = self.available_shares_map.get(ticker, 0)
        if available_shares == 0:
            self.logger.warning(f"No response from SLAvailQuery for {ticker}, assuming 0 available shares")

        # Query positions to get short qty (used shares)
        self.collecting_positions = True
        self.current_short = {}
        command = "GET POSITIONS"
        self.send_command(command)
        start_time = time.time()
        while self.collecting_positions and time.time() - start_time < 10:
            time.sleep(0.5)

        short_qty = self.current_short.get(ticker, 0)

        # Query locates for total borrowed and cost
        self.current_located = {}
        self.collecting_locates = True
        command = "GET LOCATES"
        self.send_command(command)
        start_time = time.time()
        while self.collecting_locates and time.time() - start_time < 10:
            time.sleep(0.5)

        total_borrowed = None
        total_cost = None
        avg_price = None
        if ticker in self.current_located:
            shares_dict = self.current_located[ticker]
            total_borrowed = shares_dict['shares']
            total_cost = shares_dict['cost']
            avg_price = total_cost / total_borrowed if total_borrowed > 0 else 0.0

        # If locates not fetched or zero, fallback to available + short_qty
        if total_borrowed is None or total_borrowed == 0:
            total_borrowed = available_shares + short_qty
            total_cost = None  # Keep existing
            avg_price = None  # Keep existing

        self.update_borrowed_shares(
            ticker,
            available_shares=available_shares,
            total_borrowed=total_borrowed,
            total_cost=total_cost,
            avg_price=avg_price
        )

    def update_borrowed_shares(self, ticker, available_shares=None, total_borrowed=None, total_cost=None, avg_price=None):
        self.logger.debug(f"Updating borrowedshares table for ticker={ticker}, available_shares={available_shares}, total_borrowed={total_borrowed}, total_cost={total_cost}, avg_price={avg_price}")
        try:
            conn = db_pool.getconn()
            conn.autocommit = False
            cursor = conn.cursor()
            today_date = datetime.now().strftime('%Y-%m-%d')
            cursor.execute("""
                SELECT borrowed_shares, available_shares, total_cost, cost_per_share 
                FROM borrowedshares 
                WHERE ticker = %s AND DATE(last_updated) = %s
            """, (ticker, today_date))
            record = cursor.fetchone()

            new_available_shares = available_shares if available_shares is not None else (record[1] if record else 0)
            new_borrowed_shares = total_borrowed if total_borrowed is not None else (record[0] if record else 0)
            new_total_cost = total_cost if total_cost is not None else (record[2] if record else 0.0)
            new_cost_per_share = avg_price if avg_price is not None else (record[3] if record else 0.0)

            if record:
                cursor.execute("""
                    UPDATE borrowedshares 
                    SET borrowed_shares = %s, available_shares = %s, cost_per_share = %s, 
                        total_cost = %s, last_updated = CURRENT_TIMESTAMP
                    WHERE ticker = %s AND DATE(last_updated) = %s
                """, (new_borrowed_shares, new_available_shares, new_cost_per_share, 
                      new_total_cost, ticker, today_date))
                self.logger.info(f"Updated borrowedshares for {ticker}: "
                                 f"borrowed_shares={new_borrowed_shares}, available_shares={new_available_shares}, "
                                 f"cost_per_share={new_cost_per_share}, total_cost={new_total_cost}")
            else:
                cursor.execute("""
                    INSERT INTO borrowedshares 
                    (ticker, borrowed_shares, available_shares, cost_per_share, total_cost, last_updated)
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """, (ticker, new_borrowed_shares, new_available_shares, new_cost_per_share, new_total_cost))
                self.logger.info(f"Inserted new record for {ticker}: "
                                 f"borrowed_shares={new_borrowed_shares}, available_shares={new_available_shares}, "
                                 f"cost_per_share={new_cost_per_share}, total_cost={new_total_cost}")
            conn.commit()
            return True
        except psycopg2.Error as e:
            self.logger.error(f"Error updating borrowed shares: {e}")
            conn.rollback()
            return False
        finally:
            if conn:
                cursor.close()
                db_pool.putconn(conn)

    def select_best_offer(self):
        best_offers = []
        for ticker, offers in self.offers.items():
            if offers:
                requested_shares = self.requested_shares.get(ticker, 0)
                shortable_offers = [offer for offer in offers if offer['note'] == 'already shortable']
                cost_offers = [
                    offer for offer in offers 
                    if offer['note'] == '' and offer['total_cost'] > 0 and offer['shares'] >= requested_shares
                ]
                error_offers = [offer for offer in offers if offer['note'] != '' and offer['note'] != 'already shortable']

                if shortable_offers:
                    best_offer = shortable_offers[0]
                elif cost_offers:
                    best_offer = min(cost_offers, key=lambda x: x['total_cost'])
                elif error_offers:
                    best_offer = error_offers[0]
                else:
                    continue
                self.logger.info(f"Selected best offer: Ticker={best_offer['ticker']}, Price={best_offer['price']}, Shares={best_offer['shares']}, Route={best_offer['route']}, Total Cost=${best_offer['total_cost']:.2f}, Note={best_offer['note']}")
                best_offers.append(best_offer)
        return best_offers

    def accept_offer(self, offer, trade_id):
        if not self.client_socket:
            self.logger.error("Client socket is not connected, cannot send command.")
            return False
        if trade_id not in self.accepted_orders:
            self.accepted_orders[trade_id] = set()
        if offer['order_id'] not in self.accepted_orders[trade_id]:
            command = f"SLOFFEROPERATION {offer['order_id']} Accept"
            self.send_command(command)
            self.accepted_orders[trade_id].add(offer['order_id'])
            self.pending_offers[offer['order_id']] = {'ticker': offer['ticker'], 'trade_id': trade_id}
            self.logger.info(f"Sent accept command for Order ID={offer['order_id']}, waiting for Located status")
            return True
        self.logger.error(f"Offer with Order ID {offer['order_id']} for trade_id {trade_id} was not accepted as it was already processed.")
        return False

    def send_command(self, command):
        with self.lock:
            if self.client_socket:
                try:
                    self.client_socket.sendall(command.encode())
                    self.logger.debug(f"Command sent: {command}")
                except socket.error as e:
                    self.logger.error(f"Socket error while sending command: {e}")
            else:
                self.logger.error("Client socket is not connected, cannot send command.")

    def send_locate_order_commands(self, ticker, shares, trade_id):
        self.requested_shares[ticker] = int(shares)
        self.check_available_shares(ticker, trade_id)
        start_time = time.time()
        while ticker not in self.available_shares_map and time.time() - start_time < 10:
            time.sleep(0.5)
        self.logger.info(f"Requesting to borrow {shares} shares for {ticker}")
        inquire_command = f'SLPRICEINQUIRE {ticker} {shares} ALLROUTE'
        self.send_command(inquire_command)
        return None

    def reset_offers(self, ticker):
        self.logger.debug(f"Resetting offers for ticker: {ticker}")
        if ticker in self.offers:
            del self.offers[ticker]
        self.located_offers = {k: v for k, v in self.located_offers.items() if v['ticker'] != ticker}
        self.pending_offers = {k: v for k, v in self.pending_offers.items() if v['ticker'] != ticker}
        self.available_shares_map.pop(ticker, None)
        self.requested_shares.pop(ticker, None)

@app.route('/')
def short_locate():
    return render_template('short_locate.html')

@app.route('/locate_shares', methods=['POST'])
def locate_shares():
    global global_offers
    global_offers = []
    
    tickers = request.form.getlist('tickers[]')
    shares_list = request.form.getlist('shares[]')
    trade_id = request.form.get('trade_id', 'N/A')
    
    slocate = Slocate()
    if not slocate.connect_to_server():
        return jsonify({'status': 'error', 'message': 'Failed to connect to server'})
    
    listener_thread = threading.Thread(target=slocate.listen_to_server, daemon=True)
    listener_thread.start()
    
    results = []
    for ticker, shares in zip(tickers, shares_list):
        slocate.reset_offers(ticker)
        slocate.send_locate_order_commands(ticker, shares, trade_id)
        start_time = time.time()
        while time.time() - start_time < 5:
            if any(o['ticker'] == ticker and 'failure_message' in o for o in slocate.pending_offers.values()):
                failure = next(o for o in slocate.pending_offers.values() if o['ticker'] == ticker)
                results.append({"status": "error", "message": f"Borrow failed for {ticker}: {failure['failure_message']}"})
                break
            time.sleep(0.5)
        else:
            best_offers = slocate.select_best_offer()
            if best_offers:
                global_offers.extend(best_offers)
                results.append({"status": "pending", "message": f"Offers requested for {ticker}"})
            else:
                results.append({"status": "error", "message": f"No valid offers received for {ticker}"})
    
    return jsonify({'status': 'success', 'located_shares': global_offers, 'results': results})

@app.route('/accept_offer', methods=['POST'])
def accept_offer():
    ticker = request.form.get('ticker')
    trade_id = request.form.get('trade_id', 'N/A')
    
    slocate = Slocate()
    if not slocate.connect_to_server():
        return jsonify({'status': 'error', 'message': 'Failed to connect to server'})
    
    listener_thread = threading.Thread(target=slocate.listen_to_server, daemon=True)
    listener_thread.start()
    
    offer = next((offer for offer in global_offers if offer['ticker'] == ticker), None)
    if offer:
        if offer['note'] == 'already shortable':
            slocate.check_available_shares(ticker, trade_id)
            return jsonify({'status': 'success', 'accepted_share': offer})
        elif slocate.accept_offer(offer, trade_id):
            start_time = time.time()
            while time.time() - start_time < 10:
                if any(o['ticker'] == ticker for o in slocate.located_offers.values()):
                    located_offer = next(o for o in slocate.located_offers.values() if o['ticker'] == ticker)
                    return jsonify({'status': 'success', 'accepted_share': located_offer})
                if any(o['ticker'] == ticker and 'failure_message' in o for o in slocate.pending_offers.values()):
                    failure = next(o for o in slocate.pending_offers.values() if o['ticker'] == ticker)
                    return jsonify({'status': 'error', 'message': failure['failure_message']})
                time.sleep(0.5)
            return jsonify({'status': 'error', 'message': 'No Located status received'})
        else:
            return jsonify({'status': 'error', 'message': 'Offer already accepted'})
    else:
        return jsonify({'status': 'error', 'message': 'Offer not found'})

@app.route('/get_borrowed_shares', methods=['GET'])
def get_borrowed_shares():
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        today_date = datetime.now().strftime('%Y-%m-%d')
        cursor.execute("""
            SELECT ticker, borrowed_shares, available_shares, cost_per_share, total_cost 
            FROM borrowedshares
            WHERE DATE(last_updated) = %s
        """, (today_date,))
        borrowed_shares = cursor.fetchall()
        borrowed_shares_list = [
            {
                'ticker': row[0],
                'borrowed_shares': int(row[1]),
                'available_shares': int(row[2]),
                'cost_per_share': float(row[3]),
                'total_cost': float(row[4])
            }
            for row in borrowed_shares
        ]
        return jsonify({'borrowed_shares': borrowed_shares_list})
    except psycopg2.Error as e:
        logging.error(f"Error querying borrowedshares: {e}")
        return jsonify({'borrowed_shares': []})
    finally:
        if conn:
            cursor.close()
            db_pool.putconn(conn)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5010)