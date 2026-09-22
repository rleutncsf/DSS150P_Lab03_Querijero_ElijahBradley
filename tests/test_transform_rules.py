import copy

import pandas as pd

from src.transform.curated import add_record_hash, build_curated, compute_amounts
from src.validate.quality import validate_curated


def test_customer_dedup_keeps_latest_and_normalizes(built):
    staging, *_ = built
    c = staging['customers'].set_index('customer_id')
    assert len(c) == 3                                       # C2 duplicate collapsed
    assert c.loc['C2', 'email'] == 'ben.new@example.com'     # latest updated_at wins
    assert c.loc['C1', 'city'] == 'Manila'                   # trimmed + title-cased
    assert c['email'].str.contains('[A-Z]', na=False).sum() == 0
    assert bool(c.loc['C3', 'email_missing']) and pd.isna(c.loc['C3', 'email'])   # visible, not dropped


def test_staging_adds_audit_columns_in_utc(built):
    staging, *_ = built
    for name in ('customers', 'products', 'orders'):
        df = staging[name]
        assert (df['pipeline_run_id'] == 'run_test').all()
        assert str(df['staged_at_utc'].dt.tz) == 'UTC'


def test_product_flattened_and_negative_price_quarantined(built):
    staging, q_stg, *_ = built
    p = staging['products'].set_index('product_id')
    assert p.loc['P1', 'category_name'] == 'Tools' and p.loc['P1', 'category_department'] == 'Hardware'
    assert 'P2' not in p.index
    row = q_stg[(q_stg.dataset == 'products') & (q_stg.record_key == 'P2')]
    assert len(row) == 1 and 'negative' in row.iloc[0]['reason']


def test_order_duplicate_resolved_to_latest_version(built):
    staging, *_ = built
    o = staging['orders'].set_index('order_id')
    assert o.loc['O1', 'status'] == 'DELIVERED'


def test_invalid_orders_are_quarantined_with_reasons(built):
    _, q_stg, *_ = built
    reasons = dict(zip(q_stg[q_stg.dataset == 'orders'].record_key, q_stg[q_stg.dataset == 'orders'].reason))
    assert 'invalid_quantity_out_of_range' in reasons['O2']
    assert 'invalid_status' in reasons['O3']


def test_orphans_quarantined_not_dropped(built):
    _, _, curated, q_cur = built
    r = dict(zip(q_cur.record_key, q_cur.reason))
    assert r['O4'] == 'orphan_customer_not_found'
    assert r['O5'] == 'orphan_product_not_found'
    assert r['O6'] == 'orphan_product_quarantined_upstream'   # product existed but was invalid
    assert set(curated.order_id) == {'O1', 'O7'}


def test_every_source_order_is_accounted_for(built):
    """valid + quarantined == unique source orders (nothing silently lost)."""
    staging, q_stg, curated, q_cur = built
    quarantined_orders = set(q_stg[q_stg.dataset == 'orders'].record_key) | set(q_cur.record_key)
    assert set(curated.order_id) | quarantined_orders == {f'O{i}' for i in range(1, 8)}
    assert not (set(curated.order_id) & quarantined_orders)


def test_amount_formulas_and_rounding():
    a = compute_amounts(pd.Series([20, 3]), pd.Series([33.33, 100.0]), pd.Series([0.15, 0.1]))
    assert a.loc[0].tolist() == [666.6, 99.99, 566.61]        # 666.60 * 0.15 = 99.99
    assert a.loc[1].tolist() == [300.0, 30.0, 270.0]
    assert ((a.gross_amount - a.discount_amount - a.net_amount).abs() < 1e-9).all()


def test_curated_audit_columns_and_validation_passes(built):
    *_, curated, _ = built
    assert curated[['source_updated_at', 'pipeline_run_id', 'processed_at_utc', 'record_hash']].notna().all().all()
    assert validate_curated(curated) == []


def test_record_hash_is_deterministic_and_ignores_run_metadata(built, raw_dir):
    staging, q_stg, curated, _ = built
    again, _ = build_curated(copy.deepcopy(staging), 'a_completely_different_run', q_stg)
    assert (curated.record_hash.values == again.record_hash.values).all()
    assert (curated.pipeline_run_id != again.pipeline_run_id).all()


def test_record_hash_changes_when_business_content_changes(built):
    *_, curated, _ = built
    changed = curated.copy()
    changed.loc[0, 'status'] = 'CANCELLED'
    assert add_record_hash(changed)[0] != curated.record_hash[0]


def test_validation_detects_duplicates_nulls_and_bad_values(built):
    *_, curated, _ = built
    bad = pd.concat([curated, curated.iloc[[0]]], ignore_index=True)
    bad.loc[0, 'status'] = 'WEIRD'
    bad.loc[1, 'net_amount'] = -1.0
    bad.loc[1, 'quantity'] = 99
    bad.loc[1, 'customer_id'] = None
    text = ' | '.join(validate_curated(bad))
    for expected in ('duplicate order_id', 'status not in', 'negative net_amount', 'quantity outside', 'null value'):
        assert expected in text