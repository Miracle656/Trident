"""Retry policy for idempotent HTTP requests (GET).

Honours the ``Retry-After`` header on 429/503 responses, falling back to
exponential backoff with jitter otherwise. Pass ``False`` in place of a
:class:`RetryConfig` to disable retries for a client or a single call.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Literal, Optional, Union


@dataclass(frozen=True)
class RetryConfig:
    """Configurable retry policy.

    Attributes:
        max_attempts: Total number of attempts, including the first.
        base_delay: Base delay in seconds used for exponential backoff.
        max_delay: Upper bound for a single computed backoff delay.
        max_total_wait: Upper bound on total time spent waiting across all
            retries (including any honoured ``Retry-After``).
        jitter: Randomize each computed delay in ``[0, delay)``.
    """

    max_attempts: int = 3
    base_delay: float = 0.1
    max_delay: float = 2.0
    max_total_wait: float = 10.0
    jitter: bool = True


DEFAULT_RETRY_CONFIG = RetryConfig()

# `None` means "no override, use the base"; `False` disables retries. `True`
# is deliberately not accepted — only `False` carries meaning here, and
# admitting `bool` left a Literal[True] that resolve_retry_config could return
# in place of a RetryConfig.
RetryOverride = Optional[Union[RetryConfig, Literal[False]]]


def resolve_retry_config(
    override: RetryOverride,
    base: RetryOverride,
) -> Optional[RetryConfig]:
    """Merge a per-call override with the client-level base config."""
    cfg = override if override is not None else base
    if cfg is False:
        return None
    if cfg is None:
        return DEFAULT_RETRY_CONFIG
    return cfg


def is_retryable_status(status: int) -> bool:
    """Only 429 (rate limited) and 503 (service unavailable) are retried."""
    return status in (429, 503)


def parse_retry_after_seconds(header_value: object) -> Optional[float]:
    """Parse a ``Retry-After`` header, either seconds or an HTTP date."""
    if not isinstance(header_value, str):
        return None
    trimmed = header_value.strip()
    if not trimmed:
        return None
    try:
        return max(0.0, float(trimmed))
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(trimmed)
    except (TypeError, ValueError, IndexError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())


def compute_backoff_seconds(attempt: int, cfg: RetryConfig) -> float:
    """Exponential backoff with optional full jitter, capped at max_delay."""
    # `2 ** n` is typed Any (int ** int can yield float for a negative
    # exponent), which propagates through the arithmetic and makes the return
    # Any under --strict. Use float(...) to pin it.
    exp: float = cfg.base_delay * float(2 ** (attempt - 1))
    capped: float = min(exp, cfg.max_delay)
    return random.random() * capped if cfg.jitter else capped
