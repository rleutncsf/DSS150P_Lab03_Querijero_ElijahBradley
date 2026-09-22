import logging

import pandas as pd

from src.config import QUALITY

log = logging.getLogger(__name__)

REQUIRED_NOT_NULL = [
    'order_id', 'customer_id', 'product_id', 'order_timestamp', 'quantity', 'unit_price', 'discount_pct',
    'gross_amount', 'discount_amount', 'net_amount', 'status',
    'source_updated_at', 'pipeline_run_id', 'processed_at_utc', 'record_hash',
]


def validate_curated(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    if len(df) == 0:
        return ['curated dataset is empty']

    missing_cols = [c for c in REQUIRED_NOT_NULL if c not in df.columns]
    if missing_cols:
        return [f'missing required columns: {missing_cols}']

    for col in REQUIRED_NOT_NULL:
        n = int(df[col].isna().sum())
        if n:
            errors.append(f'{n} null value(s) in required column {col}')

    dupes = int(df['order_id'].duplicated().sum())
    if dupes:
        errors.append(f'{dupes} duplicate order_id value(s)')

    q = pd.to_numeric(df['quantity'], errors='coerce')
    bad_q = int(((q < QUALITY['min_quantity']) | (q > QUALITY['max_quantity'])).sum())
    if bad_q:
        errors.append(f'{bad_q} row(s) with quantity outside {QUALITY["min_quantity"]}..{QUALITY["max_quantity"]}')

    for col in ('unit_price', 'gross_amount', 'discount_amount', 'net_amount'):
        n = int((pd.to_numeric(df[col], errors='coerce') < 0).sum())
        if n:
            errors.append(f'{n} row(s) with negative {col}')

    d = pd.to_numeric(df['discount_pct'], errors='coerce')
    if int(((d < 0) | (d > 1)).sum()):
        errors.append('discount_pct outside 0..1')

    bad_status = int((~df['status'].isin(QUALITY['allowed_order_statuses'])).sum())
    if bad_status:
        errors.append(f'{bad_status} row(s) with status not in {QUALITY["allowed_order_statuses"]}')

    gross = pd.to_numeric(df['gross_amount'], errors='coerce').astype(float)
    disc = pd.to_numeric(df['discount_amount'], errors='coerce').astype(float)
    net = pd.to_numeric(df['net_amount'], errors='coerce').astype(float)
    expected_gross = (q * pd.to_numeric(df['unit_price'], errors='coerce')).astype(float)
    if int(((gross - expected_gross).abs() > 0.01).sum()):
        errors.append('gross_amount != quantity * unit_price for some rows')
    if int(((gross - disc - net).abs() > 0.005).sum()):
        errors.append('net_amount != gross_amount - discount_amount for some rows')

    bad_hash = int((df['record_hash'].astype(str).str.len() != 64).sum())
    if bad_hash:
        errors.append(f'{bad_hash} row(s) with malformed record_hash')
    return errors


def compare_key_sets(expected_ids: pd.Series, actual_ids: pd.Series, label: str) -> list[str]:
    missing = set(expected_ids) - set(actual_ids)
    return [f'{len(missing)} order_id(s) from {label} missing in PostgreSQL'] if missing else []