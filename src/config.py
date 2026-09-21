from pathlib import Path
import os
import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / '.env')

with (PROJECT_ROOT / 'config' / 'settings.yml').open(encoding='utf-8') as f:
    SETTINGS = yaml.safe_load(f)

DB = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'dbname': os.getenv('POSTGRES_DB', 'dss150p'),
    'user': os.getenv('POSTGRES_USER', 'dss150p'),
    'password': os.getenv('POSTGRES_PASSWORD', 'change_me'),
}

def path_for(key: str) -> Path:
    return PROJECT_ROOT / SETTINGS['pipeline'][key]
