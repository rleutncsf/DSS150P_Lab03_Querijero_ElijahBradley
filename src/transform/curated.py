import logging

import numpy as np
import pandas as pd

from src.common.audit import record_hash
from src.transform.staging import QUARANTINE_COLUMNS, join_reasons, make_quarantine

log = logging.getLogger(__name__)

CURATED_COLUMNS = [
    'order_id', 'customer_id', 'product_id', 'order_timestamp', 'customer_city', 'customer_tier',
    'product_name', 'category', 'brand', 'quantity', 'unit_price', 'discount_pct',
    'gross_amount', 'discount_amount', 'net_amount', 'status',
    'source_updated_at', 'pipeline_run_id', 'processed_at_utc', 'record_hash',
]

# Business content of the row. Deliberately EXCLUDED: pipeline_run_id, processed_at_utc (change every run)
# and source_updated_at (source metadata: a re-stamped but otherwise identical order needs no update).
HASH_COLUMNS = [
    'order_id', 'customer_id', 'product_id', 'order_timestamp', 'customer_city', 'customer_tier',
    'product_name', 'category', 'brand', 'quantity', 'unit_price', 'discount_pct',
    'gross_amount', 'discount_amount', 'net_amount', 'status',
]


def compute_amounts(quantity: pd.Series, unit_price: pd.Series, discount_pct: pd.Series) -> pd.DataFrame:
    cents = (unit_price * 100).round().astype('int64')
    bp = (discount_pct * 10000).round().astype('int64')
    gross_c = quantity.astype('int64') * cents
    disc_c = (gross_c * bp + 5000) // 10000
    return pd.DataFrame({
        'gross_amount': gross_c / 100, 'discount_amount': disc_c / 100, 'net_amount': (gross_c - disc_c) / 100,
    }).round(2)


def _canonical(df: pd.DataFrame) -> pd.DataFrame:
    c = pd.DataFrame(index=df.index)
    for col in HASH_COLUMNS:
        s = df[col]
        if col == 'order_timestamp':
            c[col] = s.dt.tz_convert('UTC').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        elif col in ('gross_amount', 'discount_amount', 'net_amount', 'unit_price'):
            c[col] = s.map(lambda v: f'{v:.2f}')
        elif col == 'discount_pct':
            c[col] = s.map(lambda v: f'{v:.4f}')
        elif col == 'quantity':
            c[col] = s.astype('int64').astype(str)
        else:
            c[col] = s.astype(object).where(s.notna(), None)
    return c


def add_record_hash(df: pd.DataFrame) -> pd.Series:
    records = _canonical(df).to_dict('records')
    return pd.Series([record_hash(r, HASH_COLUMNS) for r in records], index=df.index)


def build_curated(staging: dict, run_id: str, upstream_quarantine: pd.DataFrame | None = None):
    now = pd.Timestamp.now(tz='UTC')
    orders, customers, products = staging['orders'], staging['customers'], staging['products']

    c = customers[['customer_id', 'city', 'customer_tier']].rename(
        columns={'city': 'customer_city'})
    p = products[['product_id', 'name', 'category_name', 'brand']].rename(
        columns={'name': 'product_name', 'category_name': 'category'})

    # validate= makes pandas raise if a dimension key were duplicated (would silently multiply rows)
    joined = (orders.merge(c, on='customer_id', how='left', validate='many_to_one', indicator='_c')
                    .merge(p, on='product_id', how='left', validate='many_to_one', indicator='_p'))

    quarantined_products, quarantined_customers = set(), set()
    if upstream_quarantine is not None and len(upstream_quarantine):
        uq = upstream_quarantine
        quarantined_products = set(uq.loc[uq.dataset == 'products', 'record_key'])
        quarantined_customers = set(uq.loc[uq.dataset == 'customers', 'record_key'])

    no_c, no_p = joined['_c'] == 'left_only', joined['_p'] == 'left_only'
    masks = {
        'orphan_customer_quarantined_upstream': no_c & joined['customer_id'].isin(quarantined_customers),
        'orphan_customer_not_found': no_c & ~joined['customer_id'].isin(quarantined_customers),
        'orphan_product_quarantined_upstream': no_p & joined['product_id'].isin(quarantined_products),
        'orphan_product_not_found': no_p & ~joined['product_id'].isin(quarantined_products),
    }
    reasons = join_reasons(masks, joined.index)

    dropped = joined.drop(columns=['_c', '_p'])
    q_raw = dropped.astype(object).where(dropped.notna(), None)
    q_raw['_source_row'] = range(len(q_raw))
    quarantine = make_quarantine('orders', 'curated', q_raw, 'order_id', reasons, run_id, now)

    ok = joined[reasons == ''].copy()
    amounts = compute_amounts(ok['quantity'], ok['unit_price'], ok['discount_pct'])
    ok = pd.concat([ok, amounts], axis=1)
    ok['source_updated_at'] = ok['updated_at']
    ok['pipeline_run_id'] = run_id
    ok['processed_at_utc'] = now
    ok['quantity'] = ok['quantity'].astype('int32')
    ok['record_hash'] = add_record_hash(ok)

    curated = ok[CURATED_COLUMNS].sort_values('order_id').reset_index(drop=True)
    log.info('curated rows=%d quarantined_at_curated=%d', len(curated), len(quarantine))
    return curated, quarantine