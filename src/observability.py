import json
import logging
import time
from contextlib import contextmanager


def configure_logger() -> logging.Logger:
    logger = logging.getLogger("qa_pipeline")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def log_event(logger: logging.Logger, event_type: str, payload: dict) -> None:
    logger.info(json.dumps({"event_type": event_type, **payload}))


@contextmanager
def timed_block(logger: logging.Logger, name: str):
    start = time.time()
    yield
    duration = round(time.time() - start, 3)
    log_event(logger, "timing", {"step": name, "duration_seconds": duration})
