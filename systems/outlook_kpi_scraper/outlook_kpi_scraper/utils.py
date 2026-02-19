import os
import logging
from datetime import datetime
from dotenv import load_dotenv

def setup_logging():
    log_dir = os.path.join(os.path.dirname(__file__), '../logs')
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f'run_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ]
    )
    return log_path

def load_env():
    env_path = os.path.join(os.path.dirname(__file__), '../.env')
    load_dotenv(env_path)
    return dict(os.environ)
