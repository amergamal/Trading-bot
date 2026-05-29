import logging
import psycopg2
from datetime import datetime, time, timedelta
import pytz
import time as tm
import threading

# Logger setup (unchanged)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False
if not logger.handlers:
    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.INFO)
    c_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)
    logger.addHandler(c_handler)
    
    pm_handler = logging.FileHandler('pm_volume.log')
    pm_handler.setLevel(logging.DEBUG)
    pm_handler.setFormatter(c_format)
    logger.addHandler(pm_handler)
    logger.debug(f"PM_volume logger handlers: {logger.handlers}")

class PMVolume:
    """Class to handle pre-market volume updates and range calculations for tradeparameters table."""
    
    def __init__(self, db_pool):
        """Initialize PMVolume with database pool and lock."""
        self.db_pool = db_pool
        self.db_lock = threading.Lock()
        self.eastern = pytz.timezone('America/New_York')
        logger.info("PMVolume initialized")

    def update_pre_volume(self):
        """Fetch total volume from ohlc_5min from earliest timestamp to 9:25 AM and update pre_vol."""
        today = datetime.now(self.eastern).strftime('%Y/%m/%d')
        end_timestamp = f"{today}-09:25"  # End at 9:25:00 AM Eastern Time
        logger.info(f"Running update_pre_volume for date: {today}, up to timestamp: {end_timestamp}")
        
        try:
            with self.db_lock:
                conn = self.db_pool.getconn()
                cursor = conn.cursor()
                
                cursor.execute('SELECT DISTINCT ticker FROM tradeparameters WHERE date = %s', (today.replace('/', '-'),))
                tickers = [row[0] for row in cursor.fetchall()]
                logger.debug(f"Found {len(tickers)} tickers in tradeparameters for {today}: {tickers}")
                
                if not tickers:
                    logger.warning(f"No tickers found in tradeparameters for {today}")
                    return False
                
                data_found = False
                for ticker in tickers:
                    cursor.execute("""
                        SELECT SUM(volume)
                        FROM ohlc_5min
                        WHERE ticker = %s
                        AND timestamp >= %s
                        AND timestamp <= %s
                    """, (ticker, f"{today}-00:00", end_timestamp))
                    
                    total_volume = cursor.fetchone()[0]
                    if total_volume is not None:
                        data_found = True
                        total_volume = int(total_volume)
                    else:
                        total_volume = 0
                    logger.debug(f"Total volume for {ticker} from start of {today} to {end_timestamp}: {total_volume}")
                    
                    cursor.execute("""
                        UPDATE tradeparameters
                        SET pre_vol = %s
                        WHERE ticker = %s AND date = %s
                    """, (str(total_volume), ticker, today.replace('/', '-')))
                    
                    logger.info(f"Updated pre_vol for {ticker} to {total_volume}")
                
                conn.commit()
                if not data_found:
                    logger.warning(f"No volume data found in ohlc_5min for {today} up to {end_timestamp}")
                    return False
                logger.info(f"Successfully updated pre_vol for {len(tickers)} tickers")
                return True
                
        except psycopg2.Error as e:
            logger.error(f"Database error in update_pre_volume: {e}")
            if conn:
                conn.rollback()
            return False
        except Exception as e:
            logger.error(f"Unexpected error in update_pre_volume: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)

    def update_ranges(self):
        """Update hi_range and avg_range in tradeparameters based on ohlc_5min data."""
        today = datetime.now(self.eastern).strftime('%Y/%m/%d')
        start_timestamp = f"{today}-09:30"  # Start from 9:30 AM for avg_range
        logger.info(f"Running update_ranges for date: {today}")
        
        try:
            with self.db_lock:
                conn = self.db_pool.getconn()
                cursor = conn.cursor()
                
                # Fetch all tickers from tradeparameters for today
                cursor.execute('SELECT DISTINCT ticker FROM tradeparameters WHERE date = %s', (today.replace('/', '-'),))
                tickers = [row[0] for row in cursor.fetchall()]
                logger.debug(f"Found {len(tickers)} tickers in tradeparameters for {today}: {tickers}")
                
                if not tickers:
                    logger.warning(f"No tickers found in tradeparameters for {today}")
                    return False
                
                data_found = False
                for ticker in tickers:
                    # Highest candle range (high - low) from all data
                    cursor.execute("""
                        SELECT MAX(high - low)
                        FROM ohlc_5min
                        WHERE ticker = %s
                        AND timestamp LIKE %s
                    """, (ticker, f"{today}%"))
                    max_range = cursor.fetchone()[0]
                    hi_range = str(round(float(max_range), 2)) if max_range is not None else '0.00'
                    logger.debug(f"Highest range for {ticker} on {today}: {hi_range}")
                    
                    # Average 5min range from 9:30 AM onward
                    cursor.execute("""
                        SELECT AVG(high - low)
                        FROM ohlc_5min
                        WHERE ticker = %s
                        AND timestamp >= %s
                        AND timestamp LIKE %s
                    """, (ticker, start_timestamp, f"{today}%"))
                    avg_range_result = cursor.fetchone()[0]
                    avg_range = str(round(float(avg_range_result), 2)) if avg_range_result is not None else '0.00'
                    logger.debug(f"Average 5min range for {ticker} from {start_timestamp}: {avg_range}")
                    
                    # Update tradeparameters
                    cursor.execute("""
                        UPDATE tradeparameters
                        SET hi_range = %s, avg_range = %s
                        WHERE ticker = %s AND date = %s
                    """, (hi_range, avg_range, ticker, today.replace('/', '-')))
                    
                    logger.info(f"Updated hi_range to {hi_range} and avg_range to {avg_range} for {ticker}")
                    data_found = True if max_range is not None or avg_range_result is not None else data_found
                
                conn.commit()
                if not data_found:
                    logger.warning(f"No range data found in ohlc_5min for {today}")
                    return False
                logger.info(f"Successfully updated ranges for {len(tickers)} tickers")
                return True
                
        except psycopg2.Error as e:
            logger.error(f"Database error in update_ranges: {e}")
            if conn:
                conn.rollback()
            return False
        except Exception as e:
            logger.error(f"Unexpected error in update_ranges: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db_pool.putconn(conn)

    def schedule_volume_update(self):
        """Schedule volume update to run every 5 minutes and 4 seconds until 9:31 AM."""
        def run_schedule():
            now = datetime.now(self.eastern)
            today = now.strftime('%Y/%m/%d')
            cutoff_time = datetime.combine(now.date(), time(9, 31, 0), tzinfo=self.eastern)
            
            if now > cutoff_time:
                logger.info(f"App started after 9:31:00 AM ({now}), running update_pre_volume once")
                self.update_pre_volume()
                logger.info("PM_volume completed single run, exiting")
                return
            
            logger.info(f"App started at {now}, running initial update_pre_volume")
            data_found = self.update_pre_volume()
            
            while datetime.now(self.eastern) < cutoff_time:
                now = datetime.now(self.eastern)
                minutes_to_next = (5 - (now.minute % 5)) % 5
                seconds_to_next = (4 - now.second) % 60 if minutes_to_next == 0 else 4 + (60 - now.second)
                if minutes_to_next == 0 and seconds_to_next <= 4:
                    minutes_to_next = 5
                sleep_seconds = (minutes_to_next * 60 + seconds_to_next) % (5 * 60)
                next_run_time = now.replace(microsecond=0) + timedelta(seconds=sleep_seconds)
                
                logger.info(f"Scheduling next update_pre_volume in {sleep_seconds:.2f} seconds at {next_run_time}")
                tm.sleep(sleep_seconds)
                
                if datetime.now(self.eastern) >= cutoff_time:
                    logger.info(f"Reached 9:31:00 AM cutoff, stopping PM_volume updates")
                    break
                
                data_found = self.update_pre_volume()
                if data_found:
                    logger.info("Data found, continuing to check every 5 minutes and 4 seconds")
                else:
                    logger.warning("No data found, will retry in 5 minutes and 4 seconds")
            
            logger.info("PM_volume reached 9:31:00 AM, exiting loop")
        
        logger.info("Starting PM_volume scheduling thread")
        threading.Thread(target=run_schedule, daemon=True).start()
        logger.info("PM_volume scheduling thread started")

    def schedule_range_update(self):
        """Schedule range updates to run every 5 minutes and 4 seconds all day."""
        def run_schedule():
            while True:
                now = datetime.now(self.eastern)
                today = now.strftime('%Y/%m/%d')
                
                # Run update immediately
                logger.info(f"Running range update at {now}")
                data_found = self.update_ranges()
                
                # Calculate next run time (aligned to 5 minutes + 4 seconds)
                minutes_to_next = (5 - (now.minute % 5)) % 5
                seconds_to_next = (4 - now.second) % 60 if minutes_to_next == 0 else 4 + (60 - now.second)
                if minutes_to_next == 0 and seconds_to_next <= 4:
                    minutes_to_next = 5
                sleep_seconds = (minutes_to_next * 60 + seconds_to_next) % (5 * 60)
                next_run_time = now.replace(microsecond=0) + timedelta(seconds=sleep_seconds)
                
                logger.info(f"Scheduling next range update in {sleep_seconds:.2f} seconds at {next_run_time}")
                tm.sleep(sleep_seconds)
                
                # Check if the date has changed
                if datetime.now(self.eastern).strftime('%Y/%m/%d') != today:
                    logger.info("New trading day detected, resetting range update")
                    today = datetime.now(self.eastern).strftime('%Y/%m/%d')
                
                if data_found:
                    logger.info("Data found, continuing range updates")
                else:
                    logger.warning("No data found, will retry in 5 minutes and 4 seconds")
        
        logger.info("Starting range update scheduling thread")
        threading.Thread(target=run_schedule, daemon=True).start()
        logger.info("Range update scheduling thread started")

def start_pm_volume(db_pool):
    """Start the PMVolume module."""
    try:
        pm_volume = PMVolume(db_pool)
        pm_volume.schedule_volume_update()
        pm_volume.schedule_range_update()  # Start the new range update scheduler
        logger.info("PMVolume started successfully")
        return pm_volume
    except Exception as e:
        logger.error(f"Error starting PMVolume: {e}")
        raise