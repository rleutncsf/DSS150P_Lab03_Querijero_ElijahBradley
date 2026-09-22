from pathlib import Path
import json
import re

from src.config import path_for

LAYER_DIR_KEYS = {
    'raw': 'raw_dir', 'staging': 'staging_dir', 'curated': 'curated_dir', 'quarantine': 'quarantine_dir',
}
RUN_META = '_run.json'


def safe_name(run_id: str) -> str:
    return re.sub(r'[^A-Za-z0-9_.=-]', '-', run_id)


def layer_root(layer: str) -> Path:
    return path_for(LAYER_DIR_KEYS[layer])


def run_dir(layer: str, run_id: str) -> Path:
    return layer_root(layer) / f'run_id={safe_name(run_id)}'


def write_run_meta(directory: Path, run_id: str, **extra) -> None:
    (directory / RUN_META).write_text(json.dumps({'pipeline_run_id': run_id, **extra}, indent=2), encoding='utf-8')


def latest_run_id(layer: str) -> str:
    root = layer_root(layer)
    candidates = [p for p in root.glob('run_id=*') if p.is_dir()] if root.exists() else []
    if not candidates:
        raise FileNotFoundError(f'No {layer} runs found under {root}. Run the previous stage first.')
    newest = max(candidates, key=lambda p: p.stat().st_mtime_ns)
    meta = newest / RUN_META
    if meta.exists():
        return json.loads(meta.read_text(encoding='utf-8'))['pipeline_run_id']
    return newest.name.removeprefix('run_id=')
