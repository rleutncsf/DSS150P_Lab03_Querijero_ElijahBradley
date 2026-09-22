from datetime import datetime, timezone
from pathlib import Path
import shutil

from src.common.errors import PipelineError
from src.common.io import sha256_file, write_json
from src.common.layout import run_dir, write_run_meta
from src.config import path_for

SOURCE_FILES = ('customers.csv', 'products.json', 'orders.csv')


def extract_sources(run_id: str) -> Path:
    source_dir = path_for('source_dir')
    missing = [name for name in SOURCE_FILES if not (source_dir / name).is_file()]
    if missing:
        raise PipelineError('extract', f'source file(s) not found: {", ".join(missing)}',
                            source_dir=source_dir, run_id=run_id)

    target = run_dir('raw', run_id)
    tmp = target.with_name(target.name + '.tmp')
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)

    files = {}
    for name in SOURCE_FILES:
        src, dst = source_dir / name, tmp / name
        shutil.copyfile(src, dst)
        src_hash, dst_hash = sha256_file(src), sha256_file(dst)
        if src_hash != dst_hash:
            shutil.rmtree(tmp)
            raise PipelineError('extract', f'checksum mismatch while copying {name}', run_id=run_id)
        files[name] = {'bytes': dst.stat().st_size, 'sha256': dst_hash}

    started = datetime.now(timezone.utc).isoformat()
    write_json({'pipeline_run_id': run_id, 'extracted_at_utc': started, 'files': files}, tmp / '_manifest.json')
    write_run_meta(tmp, run_id, extracted_at_utc=started)

    if target.exists():
        shutil.rmtree(target)
    tmp.rename(target)
    return target