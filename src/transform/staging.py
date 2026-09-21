import pandas as pd


def build_staging(raw_dir, run_id: str):
    """Create cleaned, typed staging datasets.

    Required rules:
    - Deduplicate by business key, keeping greatest updated_at.
    - Parse timestamps as UTC.
    - Normalize emails/cities and flatten product.category.
    - Validate order quantity/status and product price.
    - Add pipeline_run_id and staged_at_utc audit columns.
    - Write invalid records to data/quarantine/ with a reason.

    Return a dict of staging DataFrames and a quarantine DataFrame.
    """
    raise NotImplementedError('Implement Goal 2 staging transformations')
