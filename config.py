
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'port': os.getenv('DB_PORT', '5432'),
    'dbname': os.getenv('DB_NAME', 'eod_data'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD'),
}

DAS_CONFIG = {
    'host': '127.0.0.1',
    'port': 9800,
    'username': os.getenv('DAS_USERNAME'),
    'password': os.getenv('DAS_PASSWORD'),
    'account_live': os.getenv('DAS_ACCOUNT_LIVE', '104832'),
    'account_paper': os.getenv('DAS_ACCOUNT_PAPER', 'TRCD4832'),
}

_account_mode = os.getenv('DAS_ACCOUNT_MODE', 'live')

def get_account_mode():
    return _account_mode

def get_active_account():
    return DAS_CONFIG['account_paper'] if _account_mode == 'paper' else DAS_CONFIG['account_live']

def set_account_mode(mode):
    global _account_mode
    if mode in ('live', 'paper'):
        _account_mode = mode
