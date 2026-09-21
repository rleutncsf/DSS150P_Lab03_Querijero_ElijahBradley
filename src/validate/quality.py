def validate_curated(df) -> list[str]:
    """Return a list of human-readable validation errors.

    Minimum checks: order_id uniqueness/non-null, quantity range,
    nonnegative amounts, allowed statuses, required audit fields.
    """
    raise NotImplementedError('Implement data validation')
