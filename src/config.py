from pathlib import Path
import os

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
# override=False: real environment variables (e.g. docker-compose's POSTGRES_HOST=postgres) win over .env
load_dotenv(PROJECT_ROOT / '.env', override=False)

with (PROJECT_ROOT / 'config' / 'settings.yml').open(encoding='utf-8') as f:
    SETTINGS = yaml.safe_load(f)

# Password intentionally has NO default. a missing secret must fail loudly, not fall back silently.
DB = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'dbname': os.getenv('POSTGRES_DB', 'dss150p'),
    'user': os.getenv('POSTGRES_USER', 'dss150p'),
    'password': os.getenv('POSTGRES_PASSWORD'),
}

QUALITY = SETTINGS['quality']
BENCHMARK = SETTINGS['storage_benchmark']


def path_for(key: str) -> Path:
    return PROJECT_ROOT / SETTINGS['pipeline'][key]