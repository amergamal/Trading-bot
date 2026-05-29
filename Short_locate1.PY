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
CORS(app)

LOCAL_SERVER_HOST = 'localhost'
LOCAL_SERVER_PORT = 5015
ACCOUNT = '104832'

# ====================== PERSISTENT SLOCATE INSTANCE ======================
class Slocate:
    def __init__(self):
        self.logger = logging.getLogger('SLocate')
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        self.client_socket = None
        self.lock = threading.Lock()
        self.keep_listening = True

        # State
        self.offers = {}
        self.accepted_orders = {}      # trade_id -> set of order_ids
        self.requested_shares = {}
        self.located_offers = {}
        self.pending_offers = {}
        self.available_shares_map = {}
        self.current_located = {}
        self.collecting_locates = False
        self.current_short = {}
        self.collecting_positions = False

        self.logger.info("SLocate instance created.")

    def connect_to_server(self):
        attempts = 0
        while attempts < 5:
            try:
                self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.logger.info("Attempting to connect to DAS local server...")
                self.client_socket.connect((LOCAL_SERVER_HOST, LOCAL_SERVER_PORT))
                self.logger.info("✅ Connected to DAS local server.")
                return True
            except Exception as e:
                self.logger.error(f"Connection failed: {e}")
                attempts += 1
                time.sleep(5)
        self.logger.error("Failed to connect after 5 attempts.")
        return False

    def listen_to_server(self):
        while self.keep_listening:
            if self.client_socket:
                try:
                    response = self.client_socket.recv(4096).decode('utf-8')
                    if response:
                        self.handle_response(response)
                    else:
                        time.sleep(0.5)
                except socket.error as e:
                    self.logger.error(f"Socket error: {e}")
                    break
                except Exception as e:
                    self.logger.error(f"Listener error: {e}")
            else:
                time.sleep(1)

    def handle_response(self, response):
        lines = response.strip().split('\n')
        for line in lines:
            if not line.strip():
                continue
            parts = line.split()
            if len(parts) < 2:
                continue

            upper_first = parts[0].upper()

            try:
                # %SLRET - Locate offers
                if upper_first == "%SLRET":
                    ticker = parts[2]
                    route = parts[5] if len(parts) > 5 else ""
                    note = parts[6] if len(parts) > 6 else ""
                    order_id = parts[1]

                    self.logger.debug(f"%SLRET | ticker={ticker} route={route} note={note} raw={line}")

                    if route.upper() == "TESTSL":
                        self.logger.debug(f"Skipping TESTSL offer for {ticker}")
                        continue

                    if note in ["AlreadyShortable", "Symbolisalreadyshortable!"]:
                        offer = {
                            'ticker': ticker,
                            'shares': self.requested_shares.get(ticker, 0),
                            'price': 0.0,
                            'route': route,
                            'order_id': order_id,
                            'total_cost': 0.0,
                            'note': 'already shortable'
                        }
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

                    self.offers.setdefault(ticker, []).append(offer)

                # %SLORDER
                elif upper_first == "%SLORDER":
                    if len(parts) < 8:
                        continue
                    status = parts[7]
                    order_id = parts[1]
                    ticker = parts[2]
                    shares = int(parts[3])
                    exeshares = int(parts[5])
                    exeprice = float(parts[6])
                    route = parts[8] if len(parts) > 8 else ""
                    notes = ' '.join(parts[10:]) if len(parts) > 10 else ""

                    if status == "Offered":
                        offer = {
                            'ticker': ticker,
                            'shares': shares,
                            'price': exeprice,
                            'route': route,
                            'order_id': order_id,
                            'total_cost': exeprice * shares,
                            'note': ''
                        }
                        self.offers.setdefault(ticker, []).append(offer)

                    elif status == "Located":
                        self.located_offers[order_id] = {
                            'ticker': ticker,
                            'exeshares': exeshares,
                            'exeprice': exeprice,
                            'route': route
                        }
                        if order_id in self.pending_offers:
                            self.check_available_shares(ticker, self.pending_offers[order_id].get('trade_id'))
                            del self.pending_offers[order_id]

                        if self.collecting_locates:
                            d = self.current_located.setdefault(ticker, {'shares': 0, 'cost': 0.0})
                            d['shares'] += exeshares
                            d['cost'] += exeprice * exeshares

                    elif status in ["Rejected", "Canceled", "Closed", "Declined"]:
                        if order_id in self.pending_offers:
                            self.pending_offers[order_id]['failure_message'] = f"{status} - {notes}"

                # $SLAvailQueryRet
                elif upper_first == "$SLAVAILQUERYRET":
                    ticker = parts[2]
                    available = int(parts[3])
                    self.available_shares_map[ticker] = available

                # End markers
                elif upper_first in ["#SLORDEREND", "#LORDEREND"]:
                    self.collecting_locates = False
                elif upper_first == "#POS":
                    self.collecting_positions = True
                    self.current_short = {}
                elif upper_first == "%POS":
                    if self.collecting_positions and len(parts) >= 4 and parts[2] == "3":
                        self.current_short[parts[1]] = int(parts[3])
                elif upper_first == "#POSEND":
                    self.collecting_positions = False

            except Exception as e:
                self.logger.error(f"Error parsing line '{line}': {e}")

    def send_command(self, command):
        with self.lock:
            if self.client_socket:
                try:
                    self.client_socket.sendall((command + '\n').encode('utf-8'))
                    self.logger.debug(f"Sent: {command}")
                except Exception as e:
                    self.logger.error(f"Send error: {e}")

    # ====================== CORE METHODS ======================
    def reset_offers(self, ticker):
        self.offers.pop(ticker, None)
        self.located_offers = {k: v for k, v in self.located_offers.items() if v['ticker'] != ticker}
        self.pending_offers = {k: v for k, v in self.pending_offers.items() if v.get('ticker') != ticker}
        self.available_shares_map.pop(ticker, None)
        self.requested_shares.pop(ticker, None)

    def send_locate_order_commands(self, ticker, shares, trade_id):
        self.requested_shares[ticker] = int(shares)
        self.check_available_shares(ticker, trade_id)
        time.sleep(0.5)
        self.send_command(f'SLPRICEINQUIRE {ticker} {shares} ALLROUTE')

    def accept_offer(self, offer, trade_id):
        if trade_id not in self.accepted_orders:
            self.accepted_orders[trade_id] = set()

        if offer['order_id'] in self.accepted_orders[trade_id]:
            return False

        self.send_command(f"SLOFFEROPERATION {offer['order_id']} Accept")
        self.accepted_orders[trade_id].add(offer['order_id'])
        self.pending_offers[offer['order_id']] = {'ticker': offer['ticker'], 'trade_id': trade_id}
        return True

    def check_available_shares(self, ticker, trade_id='N/A'):
        self.send_command(f"SLAvailQuery {ACCOUNT} {ticker}")
        time.sleep(1)

        # Get positions
        self.collecting_positions = True
        self.current_short = {}
        self.send_command("GET POSITIONS")
        time.sleep(1.5)

        short_qty = self.current_short.get(ticker, 0)

        # Get locates
        self.collecting_locates = True
        self.current_located = {}
        self.send_command("GET LOCATES")
        time.sleep(2)

        total_borrowed = None
        total_cost = None
        avg_price = None

        if ticker in self.current_located:
            d = self.current_located[ticker]
            total_borrowed = d['shares']
            total_cost = d['cost']
            avg_price = total_cost / total_borrowed if total_borrowed > 0 else 0

        if not total_borrowed:
            total_borrowed = self.available_shares_map.get(ticker, 0) + short_qty

        self.update_borrowed_shares(ticker, self.available_shares_map.get(ticker, 0),
                                    total_borrowed, total_cost, avg_price)

    def update_borrowed_shares(self, ticker, available_shares=None, total_borrowed=None,
                               total_cost=None, avg_price=None):
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT borrowed_shares, available_shares, total_cost, cost_per_share 
                FROM borrowedshares 
                WHERE ticker = %s AND last_updated::date = CURRENT_DATE
            """, (ticker,))
            record = cursor.fetchone()

            new_available = available_shares if available_shares is not None else (record[1] if record else 0)
            new_borrowed = total_borrowed if total_borrowed is not None else (record[0] if record else 0)
            new_total_cost = total_cost if total_cost is not None else (record[2] if record else 0.0)
            new_avg = avg_price if avg_price is not None else (record[3] if record else 0.0)

            if record:
                cursor.execute("""
                    UPDATE borrowedshares 
                    SET borrowed_shares = %s, available_shares = %s, cost_per_share = %s,
                        total_cost = %s, last_updated = CURRENT_TIMESTAMP
                    WHERE ticker = %s AND last_updated::date = CURRENT_DATE
                """, (new_borrowed, new_available, new_avg, new_total_cost, ticker))
            else:
                cursor.execute("""
                    INSERT INTO borrowedshares 
                    (ticker, borrowed_shares, available_shares, cost_per_share, total_cost, last_updated)
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """, (ticker, new_borrowed, new_available, new_avg, new_total_cost))

            conn.commit()
            self.logger.info(f"✅ Borrowed shares updated for {ticker} | Borrowed: {new_borrowed} | Available: {new_available}")
        except Exception as e:
            self.logger.error(f"DB Error updating borrowed shares: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                cursor.close()
                db_pool.putconn(conn)


# =========================== INITIALIZE ===========================
try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1, maxconn=20, **config.DB_CONFIG
    )
    logging.info("PostgreSQL connection pool initialized")
except Exception as e:
    logging.error(f"DB Pool failed: {e}")
    raise

# Global persistent instance
slocate = Slocate()
if slocate.connect_to_server():
    listener_thread = threading.Thread(target=slocate.listen_to_server, daemon=True)
    listener_thread.start()
    logging.info("✅ Persistent listener thread started")
else:
    logging.error("❌ Failed to start persistent connection to DAS server")

global_offers = []

# =========================== ROUTES ===========================
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

    results = []
    for ticker, shares in zip(tickers, shares_list):
        slocate.reset_offers(ticker)
        slocate.send_locate_order_commands(ticker, shares, trade_id)

        start = time.time()
        while time.time() - start < 8:
            if any(o.get('ticker') == ticker and 'failure_message' in o for o in slocate.pending_offers.values()):
                failure = next((o for o in slocate.pending_offers.values() if o.get('ticker') == ticker), {})
                results.append({"status": "error", "message": f"Borrow failed: {failure.get('failure_message')}"})
                break
            time.sleep(0.3)
        else:
            best_offers = slocate.select_best_offer() if hasattr(slocate, 'select_best_offer') else []
            if best_offers:
                global_offers.extend(best_offers)
                results.append({"status": "pending", "message": f"Offers received for {ticker}"})
            else:
                results.append({"status": "error", "message": f"No offers for {ticker}"})

    return jsonify({'status': 'success', 'located_shares': global_offers, 'results': results})


@app.route('/accept_offer', methods=['POST'])
def accept_offer():
    global global_offers
    ticker = request.form.get('ticker')
    trade_id = request.form.get('trade_id', 'N/A')

    offer = next((o for o in global_offers if o['ticker'] == ticker), None)
    if not offer:
        return jsonify({'status': 'error', 'message': 'Offer not found'})

    if offer['note'] == 'already shortable':
        slocate.check_available_shares(ticker, trade_id)
        return jsonify({'status': 'success', 'accepted_share': offer})

    if slocate.accept_offer(offer, trade_id):
        start = time.time()
        while time.time() - start < 12:
            located = [o for o in slocate.located_offers.values() if o['ticker'] == ticker]
            if located:
                return jsonify({'status': 'success', 'accepted_share': located[0]})

            if any(o.get('ticker') == ticker and 'failure_message' in o for o in slocate.pending_offers.values()):
                failure = next((o for o in slocate.pending_offers.values() if o.get('ticker') == ticker), {})
                return jsonify({'status': 'error', 'message': failure.get('failure_message')})

            time.sleep(0.3)

    return jsonify({'status': 'error', 'message': 'Timeout or already accepted'})


@app.route('/get_borrowed_shares', methods=['GET'])
def get_borrowed_shares():
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ticker, borrowed_shares, available_shares, cost_per_share, total_cost 
            FROM borrowedshares 
            WHERE last_updated::date = CURRENT_DATE
            ORDER BY ticker
        """)
        rows = cursor.fetchall()
        data = [
            {
                'ticker': r[0],
                'borrowed_shares': int(r[1]),
                'available_shares': int(r[2]),
                'cost_per_share': float(r[3]),
                'total_cost': float(r[4])
            }
            for r in rows
        ]
        return jsonify({'borrowed_shares': data})
    except Exception as e:
        logging.error(f"Error fetching borrowed shares: {e}")
        return jsonify({'borrowed_shares': []})
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            db_pool.putconn(conn)


# Add this method back (was missing in my earlier suggestion)
def select_best_offer(self):
    best = []
    for ticker, offers in self.offers.items():
        if not offers:
            continue
        shortable = [o for o in offers if o['note'] == 'already shortable']
        valid = [o for o in offers if o['note'] == '' and o['shares'] > 0]
        if shortable:
            best.append(shortable[0])
        elif valid:
            best.append(min(valid, key=lambda x: x['total_cost']))
    return best

# Attach method to class
Slocate.select_best_offer = select_best_offer


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5010)