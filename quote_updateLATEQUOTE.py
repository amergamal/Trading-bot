# quote_update.py — FINAL, NO-MORE-ERRORS EDITION (Dec 2025)
import psycopg2
from psycopg2 import pool
import config
import datetime
import logging
import threading
import time
import socket
from queue import Queue
from collections import defaultdict

DAS_API_BASE_URL = '127.0.0.1'
DAS_API_PORT = 9800
DAS_API_USERNAME = 'CD4832'
DAS_API_PASSWORD = 'Gamal_123'
DAS_API_ACCOUNT = '104832'


# Helper — always returns a string in YYYY-MM-DD format (works with TEXT or DATE columns)
def today_str():
    return datetime.date.today().isoformat()   # → '2025-11-25'


class VwapFetch:
    def __init__(self, db_pool, trade_monitor=None, socketio=None):
        self.logger = logging.getLogger('VwapFetch')
        self.sock = None
        self.connected = False
        self.response_buffer = []
        self.reconnecting = False
        self.trade_monitor = trade_monitor
        self.db_pool = db_pool
        self.subscribed_tickers = set()
        self.initial_quote_data = {}
        self.account_equity_dict = {}
        self.last_quote_ts = {}
        self._last_price = {}
        self._last_log_ts = {}
        self.socketio = socketio

        self.quote_queue = Queue(maxsize=100000)
        self.pending_updates = defaultdict(dict)
        self.pending_lock = threading.Lock()
        threading.Thread(target=self._quote_worker, daemon=True).start()
        threading.Thread(target=self._db_batch_flusher, daemon=True).start()

    def create_socket(self):
        self.logger.debug('Creating socket...')
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((DAS_API_BASE_URL, DAS_API_PORT))
        self.logger.debug('Socket connected to DAS API.')
        return s

    def send_command(self, command):
        if self.sock:
            try:
                self.sock.sendall(f'{command}\r\n'.encode())
            except Exception as e:
                self.logger.error(f'Send error: {e}')
                self.connected = False
                self.reconnect()
        else:
            self.logger.warning('Socket is None')

    def receive_response(self):
        if not self.sock:
            return None
        try:
            data = b""
            while True:
                part = self.sock.recv(4096)
                if not part:
                    raise OSError("Disconnected")
                data += part
                if len(part) < 4096:
                    break
            return data.decode('utf-8', errors='ignore')
        except Exception as e:
            self.logger.error(f'Receive error: {e}')
            self.connected = False
            self.reconnect()
            return None

    def login(self):
        self.sock = self.create_socket()
        cmd = f'LOGIN {DAS_API_USERNAME} {DAS_API_PASSWORD} {DAS_API_ACCOUNT} 0'
        self.send_command(cmd)

        while True:
            resp = self.receive_response()
            if not resp:
                time.sleep(0.1)
                continue

            if 'LOGIN SUCCESSED' in resp:
                self.logger.info('Login successful')
                self.connected = True
                threading.Thread(target=self.continuously_receive, daemon=True).start()
                self.update_account_equity()
                tickers = self.get_todays_tickers()
                for t in tickers:
                    self.subscribe_to_level1(t)
                    time.sleep(0.08)
                    self.get_ldlu_prices(t)
                self.logger.info(f'Subscribed & LDLU fetched for {len(tickers)} tickers')
                break
            elif '#Welcome to DAS Command API' in resp:
                self.logger.info('Welcome message received')
            elif '#Please login to continue.' in resp:
                self.logger.warning('Login prompt – resending')
                self.send_command(cmd)
            else:
                self.logger.error(f'Unexpected login response: {resp}')
                time.sleep(2)
                self.reconnect()
                break

    def continuously_receive(self):
        buffer = ""
        while self.connected:
            try:
                chunk = self.sock.recv(65536)
                if not chunk:
                    raise ConnectionError("Socket closed")
                buffer += chunk.decode('utf-8', errors='ignore')
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    line = line.rstrip('\r')
                    if line:
                        self.response_buffer.append(line)
            except Exception as e:
                self.logger.error(f"Receive loop died: {e}")
                self.connected = False
                self.reconnect()
                return

    def keep_alive(self):
        while True:
            time.sleep(30)
            if self.connected and self.sock:
                try:
                    self.send_command('ECHO')
                except:
                    pass

    def reconnect(self):
        if self.reconnecting:
            return
        self.reconnecting = True
        self.logger.warning('Reconnecting to DAS...')
        if self.sock:
            try:
                self.sock.close()
            except:
                pass
            self.sock = None

        self.response_buffer.clear()
        self.subscribed_tickers.clear()
        self.initial_quote_data.clear()
        self.last_quote_ts.clear()
        self._last_price.clear()
        self._last_log_ts.clear()

        time.sleep(3)
        self.login()
        self.reconnecting = False

    # FIXED: Works with TEXT or DATE column
    def get_todays_tickers(self):
        try:
            conn = self.db_pool.getconn()
            cur = conn.cursor()
            today = today_str()                                 # ← string '2025-11-25'
            cur.execute("SELECT ticker FROM tradeparameters WHERE date = %s", (today,))
            tickers = [row[0] for row in cur.fetchall()]
            self.db_pool.putconn(conn)
            return tickers
        except Exception as e:
            self.logger.error(f"DB error in get_todays_tickers: {e}")
            return []

    def subscribe_to_level1(self, ticker):
        if ticker not in self.subscribed_tickers:
            self.send_command(f"SB {ticker} Lv1")
            self.subscribed_tickers.add(ticker)

    def get_ldlu_prices(self, ticker):
        self.send_command(f"GET LDLU {ticker}")

    def ensure_subscriptions_and_fetch_ldlu_prices(self):
        while True:
            current = set(self.get_todays_tickers())
            to_remove = self.subscribed_tickers - current
            for t in to_remove:
                self.send_command(f"UNSB {t} Lv1")
                self.subscribed_tickers.discard(t)
                self.initial_quote_data.pop(t, None)
            for t in current:
                if t not in self.subscribed_tickers:
                    self.subscribe_to_level1(t)
                    time.sleep(0.05)
                self.get_ldlu_prices(t)
            time.sleep(10)

    def _periodic_resubscribe(self):
        while True:
            time.sleep(180)
            if self.connected:
                for t in self.get_todays_tickers():
                    self.send_command(f"UNSB {t} Lv1")
                    time.sleep(0.2)
                    self.send_command(f"SB {t} Lv1")
                    self.get_ldlu_prices(t)

    def handle_response(self):
        while True:
            if self.response_buffer:
                line = self.response_buffer.pop(0)
                if line.startswith("$LDLU"):
                    self.parse_ldlu_response(line)
                elif line.startswith("$Quote"):
                    self.handle_quote_data(line)
                elif "#QuoteServer:" in line and ("Missing heartbeat" in line or "Lost Connection" in line):
                    self.logger.warning("Quote server heartbeat lost → reconnecting")
                    self.connected = False
                    self.reconnect()
            else:
                time.sleep(0.001)

    def parse_ldlu_response(self, response):
        parts = response.split()
        if len(parts) >= 4:
            ticker, lu_str = parts[1], parts[3]
            try:
                lu = float(lu_str)
                conn = self.db_pool.getconn()
                cur = conn.cursor()
                cur.execute(
                    "UPDATE tradeparameters SET lu = %s, lu_time = NOW() WHERE ticker = %s AND date = %s",
                    (lu, ticker, today_str())
                )
                conn.commit()
                self.db_pool.putconn(conn)
                if self.trade_monitor:
                    self.trade_monitor.receive_latest_lu_price(ticker, lu)
            except Exception as e:
                self.logger.error(f"LDLU DB error: {e}")

    def handle_quote_data(self, quote_data):
        fields = quote_data.split()
        if not fields or fields[0] != '$Quote':
            return
        ticker = fields[1]
        last = high = opn = t_str = vwap = None
        for f in fields[2:]:
            if f.startswith('L:'):
                try: last = round(float(f[2:]), 2)
                except: pass
            elif f.startswith('Hi:'):
                try: high = float(f[3:])
                except: pass
            elif f.startswith('op:'):
                try: opn = round(float(f[3:]), 2) if f[3:] else None
                except: pass
            elif f.startswith('T:'):
                t_str = f[2:]
            elif f.startswith('VWAP:'):
                try: vwap = round(float(f[5:]), 2) if f[5:] else None
                except: pass

        if last is not None and self.trade_monitor:
            self.trade_monitor.receive_latest_price(ticker, last)

        self.quote_queue.put({
            'ticker': ticker, 'last': last, 'high': high,
            'open': opn, 'time_str': t_str, 'vwap': vwap
        })

        if last is not None:
            now = time.time()
            changed = ticker not in self._last_price or abs(self._last_price[ticker] - last) > 1e-6
            if changed or (ticker not in self._last_log_ts or now - self._last_log_ts[ticker] > 30):
                self.logger.info(f"{'NEW' if changed else 'SAME'} | {ticker} → {last:.2f}")
                self._last_log_ts[ticker] = now
            self._last_price[ticker] = last
            self.last_quote_ts[ticker] = now

    def _quote_worker(self):
        while True:
            try:
                item = self.quote_queue.get(timeout=1)
                t = item['ticker']
                with self.pending_lock:
                    u = self.pending_updates[t]
                    if item['last'] is not None: u['last'] = item['last']
                    if item['high'] and (t not in self.initial_quote_data or item['high'] > self.initial_quote_data[t].get('high_price', 0)):
                        u['high'] = item['high']
                        u['hi_time'] = item['time_str']
                    if 'open' not in u and item['open'] is not None: u['open'] = item['open']
                    if item['vwap']: u['vwap'] = item['vwap']
                self.quote_queue.task_done()
            except:
                continue

    def _db_batch_flusher(self):
        while True:
            time.sleep(2)
            with self.pending_lock:
                if not self.pending_updates: continue
                batch = dict(self.pending_updates)
                self.pending_updates.clear()

            try:
                conn = self.db_pool.getconn()
                cur = conn.cursor()
                today = today_str()
                for ticker, data in batch.items():
                    if not data: continue
                    cur.execute("""
                        UPDATE tradeparameters SET
                            last = COALESCE(%s, last),
                            high = COALESCE(%s, high),
                            open = COALESCE(%s, open),
                            hi_time = COALESCE(%s, hi_time),
                            vwap = COALESCE(%s, vwap)
                        WHERE ticker = %s AND date = %s
                    """, (
                        data.get('last'), data.get('high'), data.get('open'),
                        data.get('hi_time'), data.get('vwap'), ticker, today
                    ))
                    if ticker not in self.initial_quote_data:
                        self.initial_quote_data[ticker] = {}
                    init = self.initial_quote_data[ticker]
                    if data.get('high'): init['high_price'] = data['high']
                    if 'open' in data: init['open_price'] = data.get('open')
                    if 'vwap' in data: init['vwap'] = data.get('vwap')
                conn.commit()
                self.db_pool.putconn(conn)
            except Exception as e:
                self.logger.error(f"Batch flush failed: {e}")

    def update_account_equity(self):
        try:
            self.send_command("GET AccountInfo")
            time.sleep(1.5)
            for _ in range(30):
                for resp in list(self.response_buffer):
                    if resp.startswith("$AccountInfo"):
                        self.response_buffer.remove(resp)
                        parts = resp.split()
                        if len(parts) > 2 and parts[2].replace('.', '', 1).isdigit():
                            equity = float(parts[2])
                            conn = self.db_pool.getconn()
                            cur = conn.cursor()
                            cur.execute(
                                "UPDATE tradeparameters SET account_equity = %s WHERE date = %s",
                                (equity, today_str())          # ← string, works with TEXT column
                            )
                            conn.commit()
                            self.db_pool.putconn(conn)
                            self.logger.info(f"Account equity updated → ${equity:,.2f}")
                            return
                time.sleep(0.2)
        except Exception as e:
            self.logger.error(f"Equity update failed: {e}")

    def run(self):
        threading.Thread(target=self.ensure_subscriptions_and_fetch_ldlu_prices, daemon=True).start()
        threading.Thread(target=self.handle_response, daemon=True).start()
        threading.Thread(target=self._periodic_resubscribe, daemon=True).start()


if __name__ == "__main__":
    logger = logging.getLogger('VwapFetch')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('quoteupdate.log')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)
    logger.addHandler(logging.StreamHandler())

    db_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, **config.DB_CONFIG)
    logger.info("Starting VwapFetch — TEXT/DATE COMPATIBLE FINAL VERSION")

    vwap = VwapFetch(db_pool)
    vwap.login()
    if vwap.connected:
        threading.Thread(target=vwap.keep_alive, daemon=True).start()
        vwap.run()

    while True:
        time.sleep(60)