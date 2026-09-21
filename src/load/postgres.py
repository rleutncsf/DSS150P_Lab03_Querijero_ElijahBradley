def upsert_curated(df, run_id: str) -> int:
    """Load curated.sales_order_lines using rerun-safe UPSERT semantics.

    Requirement: order_id is the conflict key. A rerun with unchanged records
    must not create duplicate business keys.
    """
    raise NotImplementedError('Implement Goal 2 PostgreSQL UPSERT')


def load_partition(df, year: int, month: int, run_id: str) -> int:
    """Load only a selected year/month partition and record audit.partition_loads."""
    raise NotImplementedError('Implement Goal 3 selected-partition load')
