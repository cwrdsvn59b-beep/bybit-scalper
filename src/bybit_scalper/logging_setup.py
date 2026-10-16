from __future__ import annotations

import logging

_SECRETS: list[str] = []


class RedactFilter(logging.Filter):
    """Вырезает значения ключей, если они случайно попали в текст лога."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not _SECRETS:
            return True
        message = record.getMessage()
        redacted = message
        for secret in _SECRETS:
            if secret and secret in redacted:
                redacted = redacted.replace(secret, "***")
        if redacted != message:
            record.msg = redacted
            record.args = ()
        return True


def register_secret(value: str | None) -> None:
    if not value or len(value) < 4:
        return
    if value not in _SECRETS:
        _SECRETS.append(value)


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("bybit_scalper")
    logger.setLevel(logging.INFO)
    if not any(isinstance(item, RedactFilter) for item in logger.filters):
        logger.addFilter(RedactFilter())
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        handler.addFilter(RedactFilter())
        logger.addHandler(handler)
    logger.propagate = False
    return logger
