import hashlib

import pandas as pd
import pytest

from src.common.errors import PipelineError


def _sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()


def test_extract_copies_bytes_and_is_rerunnable(tmp_path, raw_dir, monkeypatch):
    import src.extract.files as ex
    import src.common.layout as layout
    monkeypatch.setattr(ex, 'path_for', lambda k: raw_dir)
    monkeypatch.setattr(layout, 'layer_root', lambda layer: tmp_path / 'layers' / layer)
    target = ex.extract_sources('airflow__2026-01-01T00:00:00+00:00')   # unsafe chars are sanitized for the folder
    assert target.parent == tmp_path / 'layers' / 'raw'
    for name in ex.SOURCE_FILES:
        assert _sha(target / name) == _sha(raw_dir / name)
    ex.extract_sources('airflow__2026-01-01T00:00:00+00:00')            # same run id again: no error
    assert not list((tmp_path / 'layers' / 'raw').glob('*.tmp'))


def test_extract_fails_clearly_when_source_missing(tmp_path, raw_dir, monkeypatch):
    import src.extract.files as ex
    import src.common.layout as layout
    (raw_dir / 'orders.csv').unlink()
    monkeypatch.setattr(ex, 'path_for', lambda k: raw_dir)
    monkeypatch.setattr(layout, 'layer_root', lambda layer: tmp_path / 'layers' / layer)
    with pytest.raises(PipelineError) as err:
        ex.extract_sources('r1')
    assert err.value.stage == 'extract' and 'orders.csv' in str(err.value)
    assert not (tmp_path / 'layers' / 'raw').exists()   # nothing partial left behind