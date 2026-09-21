def build_curated(staging: dict, run_id: str):
    """Join staging orders/customers/products and create analysis-ready sales rows.

    Required columns include gross_amount, discount_amount, net_amount,
    processed_at_utc, pipeline_run_id, and record_hash.

    Orphan customer/product references must be quarantined, not silently dropped.
    """
    raise NotImplementedError('Implement Goal 2 curated transformation')
