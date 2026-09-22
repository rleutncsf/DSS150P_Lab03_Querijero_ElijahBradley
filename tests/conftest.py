import json
from pathlib import Path

import pandas as pd
import pytest

CUSTOMERS = """customer_id,first_name,last_name,email,city,customer_tier,created_at,updated_at
C1,Ana,Cruz,ana@example.com,  manila  ,Gold,2024-01-01T00:00:00+00:00,2025-01-01T00:00:00+00:00
C2,Ben,Lim,BEN@EXAMPLE.COM ,Pasig,Silver,2024-01-01T00:00:00+00:00,2025-01-01T00:00:00+00:00
C2,Ben,Lim,ben.new@example.com,Pasig,Silver,2024-01-01T00:00:00+00:00,2025-03-01T00:00:00+00:00
C3,Cy,Diaz,,Makati,Bronze,2024-01-01T00:00:00+00:00,2025-01-01T00:00:00+00:00
"""
PRODUCTS = [
    {"product_id": "P1", "name": "Widget", "category": {"name": "Tools", "department": "Hardware"},
     "brand": "Acme", "unit_price": 100.0, "active": True, "updated_at": "2025-01-01T00:00:00+00:00"},
    {"product_id": "P2", "name": "Bad", "category": {"name": "Tools", "department": "Hardware"},
     "brand": "Acme", "unit_price": -5.0, "active": True, "updated_at": "2025-01-01T00:00:00+00:00"},
]
ORDERS = """order_id,customer_id,product_id,order_timestamp,quantity,unit_price,discount_pct,status,updated_at
O1,C1,P1,2026-01-05T10:00:00+00:00,3,100.00,0.1,PAID,2026-01-05T10:00:00+00:00
O1,C1,P1,2026-01-05T10:00:00+00:00,3,100.00,0.1,DELIVERED,2026-01-07T10:00:00+00:00
O2,C2,P1,2026-02-10T10:00:00+00:00,0,100.00,0,PAID,2026-02-10T10:00:00+00:00
O3,C2,P1,2026-02-11T10:00:00+00:00,2,100.00,0,UNKNOWN,2026-02-11T10:00:00+00:00
O4,C99,P1,2026-02-12T10:00:00+00:00,2,100.00,0,PAID,2026-02-12T10:00:00+00:00
O5,C1,P77,2026-02-12T10:00:00+00:00,2,100.00,0,PAID,2026-02-12T10:00:00+00:00
O6,C1,P2,2026-02-12T10:00:00+00:00,2,100.00,0,PAID,2026-02-12T10:00:00+00:00
O7,C3,P1,2026-02-13T10:00:00+00:00,20,33.33,0.15,SHIPPED,2026-02-13T10:00:00+00:00
"""


@pytest.fixture
def raw_dir(tmp_path: Path) -> Path:
    d = tmp_path / 'raw'
    d.mkdir()
    (d / 'customers.csv').write_text(CUSTOMERS, encoding='utf-8')
    (d / 'products.json').write_text(json.dumps(PRODUCTS), encoding='utf-8')
    (d / 'orders.csv').write_text(ORDERS, encoding='utf-8')
    return d


@pytest.fixture
def built(raw_dir):
    from src.transform.curated import build_curated
    from src.transform.staging import build_staging
    staging, q_stg = build_staging(raw_dir, 'run_test')
    curated, q_cur = build_curated(staging, 'run_test', q_stg)
    return staging, q_stg, curated, q_cur