# Pipeline exceptions and logging. Exceptions (PipelineError) are for system/pipeline failures: missing files, DB down, bad config...

from contextlib import contextmanager
import logging
import sys
import time


class PipelineError(Exception):
    """A failure in one pipeline stage, carrying enough context for diagnosis."""

    def __init__(self, stage: str, message: str, **context):
        self.stage = stage
        self.context = context
        ctx = ' '.join(f'{k}={v}' for k, v in context.items())
        super().__init__(f'[stage={stage}] {message}' + (f' | {ctx}' if ctx else ''))


def setup_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter('%(asctime)sZ %(levelname)s %(name)s: %(message)s', '%Y-%m-%dT%H:%M:%S')
    fmt.converter = time.gmtime  # log timestamps in UTC
    handler.setFormatter(fmt)
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)


@contextmanager
def stage_context(stage: str, run_id: str | None = None):
    """Log stage start/end + duration; wrap unexpected exceptions in PipelineError (cause preserved)."""
    log = logging.getLogger(f'stage.{stage}')
    start = time.perf_counter()
    log.info('START run_id=%s', run_id)
    try:
        yield log
    except PipelineError:
        log.error('FAILED run_id=%s after %.2fs', run_id, time.perf_counter() - start)
        raise
    except Exception as exc:  # deliberately broad HERE only: we re-raise with stage context
        log.error('FAILED run_id=%s after %.2fs: %s: %s', run_id, time.perf_counter() - start,
                  type(exc).__name__, exc)
        raise PipelineError(stage, f'{type(exc).__name__}: {exc}', run_id=run_id) from exc
    else:
        log.info('DONE run_id=%s in %.2fs', run_id, time.perf_counter() - start)