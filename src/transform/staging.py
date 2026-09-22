from pathlib import Path
import json
import logging

import pandas as pd

from src.config import QUALITY

log = logging.getLogger(__name__)

QUARANTINE_COLUMNS = ['dataset', 'stage', 'record_key', 'reason', 'source_record',
                      'pipeline_run_id', 'quarantined_at_utc']


# ----------------------------------------------------------------------------- helpers
def _clean_text(s: pd.Series) -> pd.Series:
    out = s.astype('string').str.strip().str.replace(r'\s+', ' ', regex=True)
    return out.mask(out == '')


def _to_utc(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, utc=True, errors='coerce', format='ISO8601')


def make_quarantine(dataset: str, stage: str, raw: pd.DataFrame, key_col: str,
                    reasons: pd.Series, run_id: str, now: pd.Timestamp) -> pd.DataFrame:
    bad = reasons[reasons != ''].index
    if len(bad) == 0:
        return pd.DataFrame(columns=QUARANTINE_COLUMNS)
    sub = raw.loc[bad]
    payload = sub.drop(columns=['_source_row'], errors='ignore').apply(
        lambda r: json.dumps({k: (None if pd.isna(v) else v) for k, v in r.items()}, default=str), axis=1)
    return pd.DataFrame({
        'dataset': dataset, 'stage': stage,
        'record_key': sub[key_col].astype('string').fillna('<missing>').values,
        'reason': reasons.loc[bad].values, 'source_record': payload.values,
        'pipeline_run_id': run_id, 'quarantined_at_utc': now,
    }, columns=QUARANTINE_COLUMNS)


def join_reasons(masks: dict[str, pd.Series], index: pd.Index) -> pd.Series:
    """Combine boolean masks into a ';'-joined reason string per row ('' means valid)."""
    out = pd.Series('', index=index, dtype='object')
    for name, mask in masks.items():
        mask = mask.reindex(index).fillna(False).astype(bool)
        out = out.where(~mask, (out + ';' + name).str.lstrip(';'))
    return out


def dedupe_latest(df: pd.DataFrame, key: str) -> tuple[pd.DataFrame, int]:
    """Keep the greatest updated_at per key; tie-break on file order so the result is deterministic."""
    ordered = df.sort_values([key, 'updated_at', '_source_row'], kind='mergesort')
    kept = ordered.drop_duplicates(subset=key, keep='last')
    return kept, len(df) - len(kept)


def _structural_split(raw: pd.DataFrame, key: str):
    """Return (frame with parsed key/updated_at, reasons for structurally invalid rows)."""
    df = raw.copy()
    df[key] = _clean_text(df[key])
    df['updated_at'] = _to_utc(df['updated_at'])
    masks = {'missing_' + key: df[key].isna(), 'invalid_updated_at': df['updated_at'].isna()}
    return df, join_reasons(masks, df.index)


def _read_raw_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[''])
    df['_source_row'] = range(len(df))
    return df


# Customers
def stage_customers(raw: pd.DataFrame, run_id: str, now: pd.Timestamp):
    df, reasons = _structural_split(raw, 'customer_id')
    quarantine = make_quarantine('customers', 'staging', raw, 'customer_id', reasons, run_id, now)
    df = df[reasons == '']
    df, dupes = dedupe_latest(df, 'customer_id')

    out = pd.DataFrame({
        'customer_id': df['customer_id'],
        'first_name': _clean_text(df['first_name']),
        'last_name': _clean_text(df['last_name']),
        'email': _clean_text(df['email']).str.lower(),
        'city': _clean_text(df['city']).str.title(),
        'customer_tier': _clean_text(df['customer_tier']),
        'created_at': _to_utc(df['created_at']),
        'updated_at': df['updated_at'],
    })
    out['email_missing'] = out['email'].isna()      # visible quality condition, record is kept
    out['pipeline_run_id'] = run_id
    out['staged_at_utc'] = now
    return out.sort_values('customer_id').reset_index(drop=True), quarantine, dupes


# Products
def stage_products(raw: pd.DataFrame, run_id: str, now: pd.Timestamp):
    df, reasons = _structural_split(raw, 'product_id')
    quarantine_parts = [make_quarantine('products', 'staging', raw, 'product_id', reasons, run_id, now)]
    df = df[reasons == '']
    df, dupes = dedupe_latest(df, 'product_id')

    price = pd.to_numeric(df['unit_price'], errors='coerce')
    bad = join_reasons({'invalid_price_not_numeric': price.isna(), 'invalid_price_negative': price < 0}, df.index)
    quarantine_parts.append(make_quarantine('products', 'staging', raw, 'product_id', bad.reindex(raw.index, fill_value=''),
                                            run_id, now))
    df, price = df[bad == ''], price[bad == '']

    out = pd.DataFrame({
        'product_id': df['product_id'],
        'name': _clean_text(df['name']),
        'brand': _clean_text(df['brand']),
        'category_name': _clean_text(df['category_name']),
        'category_department': _clean_text(df['category_department']),
        'unit_price': price.astype('float64'),
        'active': df['active'],
        'updated_at': df['updated_at'],
    })
    out['pipeline_run_id'] = run_id
    out['staged_at_utc'] = now
    quarantine = pd.concat([q for q in quarantine_parts if len(q)], ignore_index=True) \
        if any(len(q) for q in quarantine_parts) else quarantine_parts[0]
    return out.sort_values('product_id').reset_index(drop=True), quarantine, dupes


def _read_raw_products(path: Path) -> pd.DataFrame:
    records = json.loads(path.read_text(encoding='utf-8'))
    flat = pd.json_normalize(records, sep='.')
    flat = flat.rename(columns={'category.name': 'category_name', 'category.department': 'category_department'})
    for col in ('category_name', 'category_department'):
        if col not in flat:
            flat[col] = None
    flat['unit_price'] = flat['unit_price'].astype('string')   # keep raw text so invalid values are diagnosable
    flat['_source_row'] = range(len(flat))
    return flat


# ----------------------------------------------------------------------------- orders
def stage_orders(raw: pd.DataFrame, run_id: str, now: pd.Timestamp):
    df, reasons = _structural_split(raw, 'order_id')
    quarantine_parts = [make_quarantine('orders', 'staging', raw, 'order_id', reasons, run_id, now)]
    df = df[reasons == '']
    df, dupes = dedupe_latest(df, 'order_id')

    qty = pd.to_numeric(df['quantity'], errors='coerce')
    price = pd.to_numeric(df['unit_price'], errors='coerce')
    disc = pd.to_numeric(df['discount_pct'], errors='coerce')
    ts = _to_utc(df['order_timestamp'])
    status = _clean_text(df['status']).str.upper()
    cust, prod = _clean_text(df['customer_id']), _clean_text(df['product_id'])
    allowed = QUALITY['allowed_order_statuses']

    masks = {
        'missing_customer_id': cust.isna(),
        'missing_product_id': prod.isna(),
        'invalid_order_timestamp': ts.isna(),
        'invalid_quantity_not_integer': qty.isna() | (qty.notna() & (qty % 1 != 0)),
        'invalid_quantity_out_of_range': qty.notna() & ((qty < QUALITY['min_quantity']) | (qty > QUALITY['max_quantity'])),
        'invalid_unit_price': price.isna() | (price < 0),
        'invalid_discount_pct': disc.isna() | (disc < 0) | (disc > 1),
        'invalid_status': ~status.isin(allowed),
    }
    bad = join_reasons(masks, df.index)
    quarantine_parts.append(make_quarantine('orders', 'staging', raw, 'order_id', bad.reindex(raw.index, fill_value=''),
                                            run_id, now))
    ok = bad == ''

    out = pd.DataFrame({
        'order_id': df['order_id'], 'customer_id': cust, 'product_id': prod,
        'order_timestamp': ts, 'quantity': qty.where(ok).astype('Int64'), 'unit_price': price.astype('float64'),
        'discount_pct': disc.astype('float64'), 'status': status, 'updated_at': df['updated_at'],
    })[ok]
    out['pipeline_run_id'] = run_id
    out['staged_at_utc'] = now
    quarantine = pd.concat([q for q in quarantine_parts if len(q)], ignore_index=True) \
        if any(len(q) for q in quarantine_parts) else quarantine_parts[0]
    return out.sort_values('order_id').reset_index(drop=True), quarantine, dupes


# Entry point
def build_staging(raw_dir, run_id: str):
    raw_dir = Path(raw_dir)
    now = pd.Timestamp.now(tz='UTC')
    raw_c = _read_raw_csv(raw_dir / 'customers.csv')
    raw_p = _read_raw_products(raw_dir / 'products.json')
    raw_o = _read_raw_csv(raw_dir / 'orders.csv')

    customers, q_c, d_c = stage_customers(raw_c, run_id, now)
    products, q_p, d_p = stage_products(raw_p, run_id, now)
    orders, q_o, d_o = stage_orders(raw_o, run_id, now)

    parts = [q for q in (q_c, q_p, q_o) if len(q)]
    quarantine = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=QUARANTINE_COLUMNS)

    stats = {
        'raw_rows': {'customers': len(raw_c), 'products': len(raw_p), 'orders': len(raw_o)},
        'staging_rows': {'customers': len(customers), 'products': len(products), 'orders': len(orders)},
        'duplicates_superseded': {'customers': d_c, 'products': d_p, 'orders': d_o},
        'missing_email_customers': int(customers['email_missing'].sum()),
        'staged_at_utc': now.isoformat(),
    }
    log.info('staging stats: %s', stats)
    return {'customers': customers, 'products': products, 'orders': orders, 'stats': stats}, quarantine