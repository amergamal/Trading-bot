#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Minichart collector – robust version that never freezes the DAS API.

Run it standalone:
    python3 minichart.py

All activity is written to STDOUT (DEBUG level) and to a rotating log file
``minichart.log`` (INFO level) so you can watch it live or inspect later.
"""

import socket
import logging
import threading
import time
import psycopg2
from psycopg2 import pool
from datetime import datetime, timedelta
from threading import Lock, Thread
import config                     # <-- your DB_CONFIG
import traceback
from logging.handlers import RotatingFileHandler

# --------------------------------------------------------------------------- #
# Logging configuration – console + rotating file
# --------------------------------------------------------------------------- #
logger = logging.getLogger('Minichart')
logger.setLevel(logging.DEBUG)

# console (DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter('%(asctime)s | %(threadName)s | %(levelname)s | %(message)s'))
logger.addHandler(ch)

# file (INFO, 5 MB, 5 backups)
fh = RotatingFileHandler('minichart.log', maxBytes=5_000_000, backupCount=5)
fh.setLevel(logging.INFO)
fh.setFormatter(logging.Formatter('%(asctime)s | %(threadName)s | %(levelname)s | %(message)s'))
logger.addHandler(fh)

# --------------------------------------------------------------------------- #
# DAS API constants
# --------------------------------------------------------------------------- #
DAS_API_BASE_URL = config.DAS_CONFIG['host']
DAS_API_PORT = config.DAS_CONFIG['port']
DAS_API_USERNAME = config.DAS_CONFIG['username']
DAS_API_PASSWORD = config.DAS_CONFIG['password']
DAS_API_ACCOUNT = config.DAS_CONFIG['account_paper']


# --------------------------------------------------------------------------- #
class Minichart:
    def __init__(self, db_pool):
        self.logger = logger
        try:
            self.logger.debug("Initializing Minichart")
            self.sock = None
            self.db_pool = db_pool
            self.db_lock = Lock()
            self.processed_tickers = {'1min': set(), '5min': set()}
            self.logger.debug("Minichart initialized")
        except Exception as e:
            self.logger.error(f"Exception in __init__: {e}\n{traceback.format_exc()}")

    # --------------------------------------------------------------------- #
    # 1. Socket creation
    # --------------------------------------------------------------------- #
    def create_socket(self):
        try:
            self.logger.debug("Creating socket...")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(15)                     # prevent indefinite hang
            s.connect((DAS_API_BASE_URL, DAS_API_PORT))
            self.logger.debug("Socket connected to DAS API.")
            return s
        except Exception as e:
            self.logger.error(f"create_socket failed: {e}\n{traceback.format_exc()}")
            return None

    # --------------------------------------------------------------------- #
    # 2. Send command (always ends with \r\n)
    # --------------------------------------------------------------------- #
    def send_command(self, sock, command):
        try:
            full = f'{command}\r\n'
            sock.sendall(full.encode('utf-8'))
            
        except Exception as e:
            self.logger.error(f"send_command error: {e}\n{traceback.format_exc()}")
            try:
                sock.close()
            except Exception:
                pass

    # --------------------------------------------------------------------- #
    # 3. **FULL** response reader – loops until a blank line (DAS protocol)
    # --------------------------------------------------------------------- #
    def receive_full_response(self, sock, timeout=15):
        """
        DAS ends a multi-line answer with a blank line (\\r\\n\\r\\n).
        We keep reading until we see it or the socket times-out / closes.
        """
        sock.settimeout(timeout)
        buffer = b""
        start = time.time()
        try:
            while True:
                try:
                    chunk = sock.recv(16384)
                    if not chunk:                                 # remote closed
                        self.logger.warning("receive_full_response: remote closed connection")
                        break
                    buffer += chunk
                    # DAS terminator
                    # If we've received a lot and no newline recently → it's a huge dump
                    if len(buffer) > 200_000 and b'\n' not in buffer[-32768:]:
                        sock.settimeout(2)  # switch to short timeout
                except socket.timeout:
                    # This is EXPECTED on large bar dumps
                    if len(buffer) > 1000:
                        self.logger.debug(f"Large $Bar payload complete ({len(buffer)} bytes) – normal timeout")
                    else:
                        self.logger.warning("receive_full_response: small payload + timeout – possible issue")
                    break
        except Exception as e:
            self.logger.error(f"receive_full_response error: {e}\n{traceback.format_exc()}")
        finally:
            sock.settimeout(None)          # restore blocking mode
        response = buffer.decode('utf-8', errors='replace')
        self.logger.debug(f"<<< ({len(response)} chars)\n{response.strip()[:500]}{'...' if len(response)>500 else ''}")
        return response

    # --------------------------------------------------------------------- #
    # 4. Login
    # --------------------------------------------------------------------- #
    def login(self, sock):
        try:
            cmd = f'LOGIN {DAS_API_USERNAME} {DAS_API_PASSWORD} {DAS_API_ACCOUNT} 0'
            self.send_command(sock, cmd)
            resp = self.receive_full_response(sock)
            if 'LOGIN SUCCESSED' in resp:
                self.logger.info("Login successful")
                return True
            if '#Please login to continue.' in resp:
                self.logger.warning("Login prompt – retrying once")
                self.send_command(sock, cmd)
                resp = self.receive_full_response(sock)
                if 'LOGIN SUCCESSED' in resp:
                    self.logger.info("Retry-login successful")
                    return True
            self.logger.error(f"Login failed – response: {resp[:200]}")
            return False
        except Exception as e:
            self.logger.error(f"login exception: {e}\n{traceback.format_exc()}")
            return False

    # --------------------------------------------------------------------- #
    # 5. Request minute chart
    # --------------------------------------------------------------------- #
    def request_minute_chart(self, sock, symbol, start_time, end_time, min_type):
        try:
            cmd = f'SB {symbol} MINCHART {start_time} {end_time} {min_type}'
            self.send_command(sock, cmd)
        except Exception as e:
            self.logger.error(f"request_minute_chart error: {e}\n{traceback.format_exc()}")

    # --------------------------------------------------------------------- #
    # 6. DB insert
    # --------------------------------------------------------------------- #
    def insert_into_db(self, table, data):
        conn = None
        try:
            with self.db_lock:
                conn = self.db_pool.getconn()
                cur = conn.cursor()
                cur.executemany(f"""
                    INSERT INTO {table} (ticker, open, high, low, close, volume, timestamp)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """, data)
                conn.commit()
                self.logger.info(f"Inserted {len(data)} rows into {table}")
        except psycopg2.Error as e:
            self.logger.error(f"DB error ({table}): {e}\n{traceback.format_exc()}")
            if conn:
                conn.rollback()
        except Exception as e:
            self.logger.error(f"Unexpected DB error: {e}\n{traceback.format_exc()}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                try:
                    self.db_pool.putconn(conn)
                except Exception as e:
                    self.logger.error(f"putconn error: {e}")

    # --------------------------------------------------------------------- #
    # 7. Parse & store
    # --------------------------------------------------------------------- #
    def parse_and_store_data(self, raw, symbol, min_type):
        try:
            lines = [l.strip() for l in raw.splitlines() if l.strip()]
            bars = []
            for line in lines:
                if not line.startswith('$Bar'):
                    continue
                parts = line.split()
                if len(parts) < 8:
                    continue
                try:
                    ts = parts[2]
                    o = float(parts[5])
                    h = float(parts[3])
                    l = float(parts[4])
                    c = float(parts[6])
                    v = int(parts[7]) if len(parts) > 7 else 0
                    bars.append((symbol, o, h, l, c, v, ts))
                except Exception as e:
                    self.logger.error(f"Parse error on line '{line[:80]}': {e}")
            if bars:
                tbl = 'ohlc_1min' if min_type == 1 else 'ohlc_5min'
                self.insert_into_db(tbl, bars)
                self.logger.info(f"Parsed {len(bars)} bars for {symbol} ({min_type}min)")
        except Exception as e:
            self.logger.error(f"parse_and_store_data exception: {e}\n{traceback.format_exc()}")

    # --------------------------------------------------------------------- #
    # 8. Worker for ONE ticker
    # --------------------------------------------------------------------- #
    def process_ticker(self, symbol, start_time_str, run_duration, min_type):
        sock = None
        try:
            sock = self.create_socket()
            if not sock or not self.login(sock):
                self.logger.error(f"{symbol} ({min_type}min) – login failed")
                return

            end_time = datetime.now() + timedelta(seconds=run_duration)
            self.logger.info(f"Starting collection for {symbol} ({min_type}min) – will run {run_duration}s")

            while datetime.now() < end_time:
                now_str = datetime.now().strftime('%Y/%m/%d-%H:%M')
                self.request_minute_chart(sock, symbol, start_time_str, now_str, min_type)

                resp = self.receive_full_response(sock)
                if resp.strip():
                    self.parse_and_store_data(resp, symbol, min_type)

                # *** CRITICAL *** 5-minute bars need breathing room
                delay = 1.0 if min_type == 5 else 0.2
                time.sleep(delay)

            self.logger.info(f"Finished {symbol} ({min_type}min) – duration reached")
        except Exception as e:
            self.logger.error(f"process_ticker crash {symbol} ({min_type}min): {e}\n{traceback.format_exc()}")
        finally:
            if sock:
                try:
                    sock.close()
                except Exception:
                    pass

    # --------------------------------------------------------------------- #
    # 9. Get tickers from DB
    # --------------------------------------------------------------------- #
    def get_tickers_from_db(self):
        conn = None
        try:
            conn = self.db_pool.getconn()
            cur = conn.cursor()
            today = datetime.now().strftime('%Y-%m-%d')
            cur.execute("SELECT DISTINCT ticker FROM tradeparameters WHERE date = %s", (today,))
            tickers = [r[0] for r in cur.fetchall()]
            
            return tickers
        except Exception as e:
            self.logger.error(f"get_tickers_from_db error: {e}\n{traceback.format_exc()}")
            return []
        finally:
            if conn:
                try:
                    self.db_pool.putconn(conn)
                except Exception:
                    pass

    # --------------------------------------------------------------------- #
    # 10. Main monitor loop
    # --------------------------------------------------------------------- #
    def monitor_new_tickers(self):
        try:
            while True:
                try:
                    tickers = self.get_tickers_from_db()
                    for sym in tickers:
                        for mt, proc_set in [(1, self.processed_tickers['1min']),
                                             (5, self.processed_tickers['5min'])]:
                            if sym not in proc_set:
                                self.logger.info(f"Launching thread for {sym} ({mt}min)")
                                proc_set.add(sym)
                                start = datetime.now().replace(hour=4, minute=0, second=0, microsecond=0)
                                t = Thread(target=self.process_ticker,
                                           args=(sym, start.strftime('%Y/%m/%d-%H:%M'), 5, mt),
                                           name=f"T-{sym}-{mt}min")
                                t.daemon = True
                                t.start()
                    time.sleep(5)
                except Exception as e:
                    self.logger.error(f"monitor loop iteration error: {e}\n{traceback.format_exc()}")
                    time.sleep(5)
        except Exception as e:
            self.logger.critical(f"monitor_new_tickers fatal: {e}\n{traceback.format_exc()}")


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def main():
    try:
        # Define min and max connections
        MIN_CONN = 1
        MAX_CONN = 5

        # Create pool with explicit min/max + DB config
        db_pool = psycopg2.pool.SimpleConnectionPool(
            MIN_CONN,
            MAX_CONN,
            **config.DB_CONFIG
        )
        mc = Minichart(db_pool)
        mc.monitor_new_tickers()
    except Exception as e:
        logger.critical(f"main() fatal: {e}\n{traceback.format_exc()}")


if __name__ == "__main__":
    main()