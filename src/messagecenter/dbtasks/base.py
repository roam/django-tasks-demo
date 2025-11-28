from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

MAX_LENGTH_WORKER_ID = 100


@dataclass(frozen=True, slots=True)
class QueueConfiguration:
    max_attempts: int | None
    backoff_factor: int | None

    def __post_init__(self):
        if self.max_attempts is not None and not isinstance(self.max_attempts, int):
            raise TypeError(f"max_attempts must be an int; got {self.max_attempts!r}")
        if self.backoff_factor is not None and not isinstance(self.backoff_factor, int):
            raise TypeError(
                f"backoff_factor must be an int; got {self.backoff_factor!r}"
            )


def now() -> datetime:
    return datetime.now(tz=UTC)
