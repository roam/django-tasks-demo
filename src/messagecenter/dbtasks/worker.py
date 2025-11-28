from __future__ import annotations

import logging
import socket
import warnings
from collections.abc import Iterable

from django.db import transaction
from django.tasks import (
    DEFAULT_TASK_BACKEND_ALIAS,
    task_backends,
)

from messagecenter.dbtasks.backend import QueueStore
from messagecenter.dbtasks.base import MAX_LENGTH_WORKER_ID

logger = logging.getLogger("messagecenter.dbtasks.worker")


class Worker:
    def __init__(
        self,
        id_: str | None,
        backend_name: str,
        only: set[str] | None,
        excluding: set[str] | None,
    ):
        self.backend = task_backends[backend_name]
        queue_store: QueueStore = self.backend.queue_store
        if only or excluding:
            queue_store = queue_store.subset(only=only, excluding=excluding)
        self.queue_store = queue_store
        self.id = (
            id_ if id_ else create_id(backend_name, queues=queue_store.queue_names)
        )

    def has_more(self) -> bool:
        return self.queue_store.has_more()

    def process(self) -> int:
        with transaction.atomic():
            tm = self.queue_store.claim_first_available(worker_id=self.id)
        if tm is None:
            return 0
        self.backend.process_task(tm)
        return 1


def create_id(backend: str, queues: Iterable[str] | None) -> str:
    parts = [socket.gethostname()]
    if backend != DEFAULT_TASK_BACKEND_ALIAS:
        parts.append(backend)
    if queues:
        parts.append("-".join(queues))
    worker_id = ":".join(parts)
    if len(worker_id) > MAX_LENGTH_WORKER_ID:
        warnings.warn(
            f"Generated worker id {worker_id} exceeds maximum length of "
            f"{MAX_LENGTH_WORKER_ID}."
        )
        worker_id = worker_id[:MAX_LENGTH_WORKER_ID]
    return worker_id
