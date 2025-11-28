from __future__ import annotations

import operator
from datetime import datetime
from functools import reduce

from django.db import models
from django.db.models import TextChoices
from django.tasks import TaskResultStatus

from messagecenter.dbtasks.base import MAX_LENGTH_WORKER_ID, QueueConfiguration


class TaskQuerySet(models.QuerySet):
    def expired(
        self,
        finished_before: datetime | None,
        unfinished_enqueued_before: datetime | None,
    ):
        """
        Select expired task results.

        This means they've either finished before `finished_before` or haven't
        finished yet, but they've been enqueued before `unfinished_enqueued_before`.
        """
        if finished_before is not None and unfinished_enqueued_before is not None:
            raise ValueError(
                "One of finished_before or unfinished_enqueued_before must be set"
            )
        filters = []
        if finished_before is not None:
            filters.append(
                models.Q(
                    status__in=(TaskResultStatus.FAILED, TaskResultStatus.SUCCESSFUL),
                    finished_at__lt=finished_before,
                )
            )
        if unfinished_enqueued_before is not None:
            filters.append(
                models.Q(
                    status=TaskResultStatus.READY,
                    enqueued_at__lt=unfinished_enqueued_before,
                )
            )
        return self.filter(reduce(operator.or_, filters))

    def claim(self, worker_id: str) -> int:
        """Claim the tasks for the worker."""
        return self.update(status=TaskResultStatus.RUNNING, worker_id=worker_id)

    def ready(self, timestamp: datetime, queues: dict[str, QueueConfiguration]):
        """Select tasks from the queues."""
        ret = self.filter(
            status=TaskResultStatus.READY, worker_id="", available_after__lte=timestamp
        )
        if queues:
            qs = []
            for name, cfg in queues.items():
                if cfg.max_attempts is not None:
                    q = models.Q(queue_name=name, attempt_count__lt=cfg.max_attempts)
                else:
                    q = models.Q(queue_name=name)
                qs.append(q)
            ret = ret.filter(reduce(operator.or_, qs))
        return ret

    def available_for_processing(self, pks: list[int]):
        return self.filter(worker_id="", status=TaskResultStatus.READY, pk__in=pks)

    def order_by_urgency(self):
        return self.order_by("available_after", "-priority", "attempt_count", "pk")


class Task(models.Model):
    priority = models.IntegerField(default=0)
    callable_path = models.CharField(max_length=255)
    backend = models.CharField(max_length=200)
    queue_name = models.CharField(max_length=100)
    run_after = models.DateTimeField(null=True, blank=True)
    # This field is used to keep track of when to run a task (again).
    # run_after remains unchanged after enqueueing.
    available_after = models.DateTimeField()
    # Denormalized count of attempts.
    attempt_count = models.IntegerField(default=0)
    takes_context = models.BooleanField(default=False)
    arguments = models.JSONField(null=True, blank=True)
    status = models.CharField(
        choices=TaskResultStatus.choices, max_length=10, default=TaskResultStatus.READY
    )
    enqueued_at = models.DateTimeField()
    started_at = models.DateTimeField(blank=True, null=True)
    finished_at = models.DateTimeField(blank=True, null=True)
    last_attempted_at = models.DateTimeField(blank=True, null=True)
    return_value = models.JSONField(null=True, blank=True)
    # Set when a worker starts processing this task.
    worker_id = models.CharField(max_length=MAX_LENGTH_WORKER_ID, blank=True)
    objects = TaskQuerySet.as_manager()

    @property
    def result_id(self) -> str:
        return str(self.id)

    @property
    def args(self) -> list:
        ret = self.arguments.get("a") if self.arguments.get("a") else None
        return [] if ret is None else ret

    @args.setter
    def args(self, value: list | None):
        if not self.arguments:
            self.arguments = {}
        self.arguments["a"] = value

    @property
    def kwargs(self) -> dict:
        ret = self.arguments.get("k") if self.arguments.get("k") else None
        return {} if ret is None else ret

    @kwargs.setter
    def kwargs(self, value: dict | None):
        if not self.arguments:
            self.arguments = {}
        self.arguments["k"] = value


class Error(models.Model):
    exception_class_path = models.TextField()
    traceback = models.TextField()


class AttemptResultStatus(TextChoices):
    FAILED = TaskResultStatus.FAILED
    SUCCESSFUL = TaskResultStatus.SUCCESSFUL


class Attempt(models.Model):
    task = models.ForeignKey(Task, related_name="attempts", on_delete=models.CASCADE)
    error = models.OneToOneField(
        Error, related_name="attempt", on_delete=models.CASCADE, null=True, blank=True
    )
    worker_id = models.CharField(max_length=MAX_LENGTH_WORKER_ID)
    started_at = models.DateTimeField()
    stopped_at = models.DateTimeField(blank=True, null=True)
    status = models.CharField(
        choices=AttemptResultStatus.choices, max_length=10, blank=True
    )
