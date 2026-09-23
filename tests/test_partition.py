import pytest

from src.benchmark.storage import read_partition, write_partitioned_parquet
from src.common.errors import PipelineError


def test_partitioned_parquet_layout_and_single_partition_read(built, tmp_path):
    *_, curated, _ = built
    out = write_partitioned_parquet(curated, tmp_path / 'part')
    assert (out / 'order_year=2026' / 'order_month=1').is_dir()
    assert (out / 'order_year=2026' / 'order_month=2').is_dir()
    jan = read_partition(out, 2026, 1)
    assert list(jan.order_id) == ['O1']
    assert (jan.order_timestamp.dt.month == 1).all()
    with pytest.raises(PipelineError):
        read_partition(out, 2030, 5)