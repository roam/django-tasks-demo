from __future__ import annotations

import logging
import math
import traceback
from datetime import UTC, datetime, timedelta
from typing import Any

from django.core.exceptions import ImproperlyConfigured
from django.db import transaction
from django.tasks import Task, TaskContext, TaskResult, TaskResultStatus
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.base import TaskError
from django.tasks.signals import task_enqueued, task_finished, task_started
from django.utils.dateparse import parse_duration
from django.utils.json import normalize_json
from django.utils.module_loading import import_string

from messagecenter.dbtasks import models
from messagecenter.dbtasks.base import QueueConfiguration, now
from messagecenter.dbtasks.models import AttemptResultStatus

logger = logging.getLogger("messagecenter.dbtasks")


class DatabaseBackend(BaseTaskBackend):
    supports_defer = True
    supports_async_task = False
    supports_get_result = True
    supports_priority = True

    def __init__(self, alias, params):
        super().__init__(alias, params)
        self.queue_store = new_queue_store(
            backend_name=self.alias, queue_names=self.queues, options=self.options
        )

    def enqueue(self, task: Task, args, kwargs):
        self.validate_task(task)
        model = self.queue_store.enqueue(task, args, kwargs)
        task_result = TaskResult(
            task=task,
            id=str(model.pk),
            status=TaskResultStatus(model.status),
            enqueued_at=model.enqueued_at,
            started_at=None,
            finished_at=None,
            last_attempted_at=None,
            args=model.args,
            kwargs=model.kwargs,
            backend=model.backend,
            errors=[],
            worker_ids=[],
        )
        task_enqueued.send(sender=type(self), task_result=task_result)
        return task_result

    def get_result(self, result_id):
        return self.model_to_result(self.queue_store.get(result_id))

    def get_results(self, task: Task) -> list[TaskResult]:
        return [
            self.model_to_result(model)
            for model in self.queue_store.all_results_of(task)
        ]

    def process_task(self, task: models.Task):
        task_result, attempt = self.prepare_task_execution(task)
        return_value, error = self.execute(task_result)
        stopped_at = now()
        with transaction.atomic():
            if error is None:
                self.handle_successful(
                    task_result,
                    attempt=attempt,
                    finished_at=stopped_at,
                    return_value=return_value,
                )
            else:
                self.handle_failed(
                    task_result, attempt=attempt, stopped_at=stopped_at, error=error
                )
            if task_result.is_finished:
                task_finished.send(sender=type(self), task_result=task_result)

    def execute(self, task_result: TaskResult) -> tuple[Any, TaskError | None]:
        try:
            task = task_result.task
            if task.takes_context:
                raw_return_value = task.call(
                    TaskContext(task_result=task_result),
                    *task_result.args,
                    **task_result.kwargs,
                )
            else:
                raw_return_value = task.call(*task_result.args, **task_result.kwargs)
            error = None
        except BaseException as e:
            exception_type = type(e)
            error = TaskError(
                exception_class_path=(
                    f"{exception_type.__module__}.{exception_type.__qualname__}"
                ),
                traceback="".join(traceback.format_exception(e)),
            )
            raw_return_value = None
        return raw_return_value, error

    def prepare_task_execution(
        self, task: models.Task
    ) -> tuple[TaskResult, models.Attempt]:
        started_at = now()
        task_result = self.get_result(task.result_id)
        if task_result.started_at is None:
            object.__setattr__(task_result, "started_at", started_at)
            models.Task.objects.filter(pk=task.pk).update(started_at=started_at)
            task.started_at = started_at
        logger.info("Executing task %s", task.id)
        attempt = models.Attempt.objects.create(
            task=task,
            worker_id=task.worker_id,
            started_at=started_at,
        )
        task_started.send(sender=type(self), task_result=task_result)
        return task_result, attempt

    def handle_successful(
        self,
        task_result: TaskResult,
        attempt: models.Attempt,
        finished_at: datetime,
        return_value: Any,
    ):
        models.Attempt.objects.filter(pk=attempt.pk).update(
            stopped_at=finished_at,
            status=AttemptResultStatus.SUCCESSFUL,
        )
        object.__setattr__(task_result, "last_attempted_at", attempt.started_at)
        task_result.worker_ids.append(attempt.worker_id)
        rv = normalize_json(return_value)
        new_status = TaskResultStatus.SUCCESSFUL
        models.Task.objects.filter(id=attempt.task_id).update(
            finished_at=finished_at,
            last_attempted_at=attempt.started_at,
            attempt_count=task_result.attempts,
            status=new_status,
            return_value=rv,
        )
        logger.info("Successfully finished task %s", task_result.id)
        object.__setattr__(
            task_result,
            "_return_value",
            rv,
        )
        object.__setattr__(task_result, "status", new_status)
        object.__setattr__(task_result, "finished_at", finished_at)

    def handle_failed(
        self,
        task_result: TaskResult,
        attempt: models.Attempt,
        stopped_at: datetime,
        error: TaskError,
    ):
        task_result.errors.append(error)
        models.Attempt.objects.filter(pk=attempt.pk).update(
            error=models.Error.objects.create(
                exception_class_path=error.exception_class_path,
                traceback=error.traceback,
            ),
            stopped_at=stopped_at,
            status=AttemptResultStatus.FAILED,
        )
        object.__setattr__(task_result, "last_attempted_at", attempt.started_at)
        task_result.worker_ids.append(attempt.worker_id)
        retry_after = self.queue_store.retry_after(
            task_result.task.queue_name, task_result.attempts
        )
        if retry_after is None:
            new_status = TaskResultStatus.FAILED
            logger.info("Task %s failed; no retry", task_result.id)
            additional = {
                "finished_at": task_result.finished_at,
            }
            object.__setattr__(task_result, "finished_at", stopped_at)
        else:
            available_after = stopped_at + retry_after
            new_status = TaskResultStatus.READY
            logger.info(
                "Task %s failed; retrying after %s", task_result.id, available_after
            )
            additional = {"available_after": available_after}
        models.Task.objects.filter(id=attempt.task_id).update(
            last_attempted_at=attempt.started_at,
            attempt_count=task_result.attempts,
            status=new_status,
            worker_id="",
            **additional,
        )
        object.__setattr__(task_result, "status", new_status)

    def model_to_result(self, model: models.Task) -> TaskResult:
        func = import_string(model.callable_path)
        task = Task(
            priority=model.priority,
            func=func.func,
            backend=model.backend,
            queue_name=model.queue_name,
            run_after=model.run_after,
            takes_context=model.takes_context,
        )
        result = TaskResult(
            task=task,
            id=str(model.pk),
            status=TaskResultStatus(model.status),
            enqueued_at=model.enqueued_at,
            started_at=model.started_at,
            last_attempted_at=model.last_attempted_at,
            args=model.args,
            kwargs=model.kwargs,
            backend=model.backend,
            errors=[],
            worker_ids=[],
            finished_at=model.finished_at,
        )
        if model.status == TaskResultStatus.SUCCESSFUL:
            object.__setattr__(
                result,
                "_return_value",
                model.return_value,
            )
        for attempt in model.attempts.all():
            if attempt.error_id:
                result.errors.append(
                    TaskError(
                        exception_class_path=attempt.error.exception_class_path,
                        traceback=attempt.error.traceback,
                    )
                )
            result.worker_ids.append(attempt.worker_id)
        return result

    def purge_expired(
        self,
        *,
        finished_before: timedelta | None = None,
        unfinished_enqueued_before: timedelta | None = None,
        limit: int | None = None,
    ) -> int:
        timestamp = now()
        finished = timestamp - finished_before if finished_before else None
        unfinished = (
            timestamp - unfinished_enqueued_before
            if unfinished_enqueued_before
            else None
        )
        qs = models.Task.objects.expired(
            finished_before=finished, unfinished_enqueued_before=unfinished
        )
        if limit:
            qs = qs.values_list("id", flat=True)[:limit]
            to_delete = models.Task.objects.filter(id__in=qs)
        else:
            to_delete = qs
        count, _ = to_delete.delete()
        return count

    def purge_finished_duration(self) -> timedelta:
        value = self.options.get("purge", {}).get("finished")
        value = value if value else "30"
        return parse_duration(value)

    def purge_unfinished_duration(self) -> timedelta:
        value = self.options.get("purge", {}).get("unfinished")
        value = value if value else "30"
        return parse_duration(value)


class QueueStore:
    def __init__(self, backend_name: str, queues: dict[str, QueueConfiguration]):
        self.backend_name = backend_name
        self.queues = queues

    @property
    def queue_names(self):
        return self.queues.keys()

    def get(self, result_id: str) -> models.Task:
        return models.Task.objects.prefetch_related("attempts", "attempts__error").get(
            id=int(result_id)
        )

    def all_results_of(self, task: Task):
        qs = models.Task.objects.prefetch_related("attempts", "attempts__error")
        path = f"{task.func.__module__}.{task.func.__qualname__}"
        return qs.filter(callable_path=path).order_by("-enqueued_at")

    def enqueue(self, task: Task, args, kwargs) -> models.Task:
        timestamp = now()
        status = TaskResultStatus.READY
        # noinspection PyUnresolvedReferences
        path = f"{task.func.__module__}.{task.func.__qualname__}"
        run_after = task.run_after
        if run_after:
            run_after = run_after.astimezone(tz=UTC)
            available_after = run_after
        else:
            available_after = timestamp
        return models.Task.objects.create(
            priority=task.priority,
            callable_path=path,
            backend=task.backend,
            queue_name=task.queue_name,
            run_after=run_after,
            available_after=available_after,
            takes_context=task.takes_context,
            args=args,
            kwargs=kwargs,
            status=status,
            enqueued_at=timestamp,
        )

    def subset(self, only: set[str] | None, excluding: set[str] | None) -> QueueStore:
        if only and excluding:
            raise ValueError("only and excluding cannot be used together")
        if not only and not excluding:
            return self
        if only:
            queues = {k: v for k, v in self.queues.items() if k in only}
        else:
            queues = {k: v for k, v in self.queues.items() if k not in excluding}
        return QueueStore(
            backend_name=self.backend_name,
            queues=queues,
        )

    def retry_after(self, queue_name: str, attempts: int) -> timedelta | None:
        queue = self.queues[queue_name]
        if not queue.max_attempts:
            return None
        if attempts >= queue.max_attempts:
            return None
        return timedelta(seconds=math.pow(queue.backoff_factor, attempts))

    def has_more(self) -> bool:
        return self.peek() is not None

    def peek(self):
        qs = models.Task.objects.filter(backend=self.backend_name)
        qs = qs.ready(now(), self.queues).order_by_urgency()
        return qs.values_list("id", flat=True).first()

    def claim_first_available(
        self, worker_id: str, attempts: int = 3
    ) -> models.Task | None:
        for _ in range(attempts):
            task_id = self.peek()
            if not task_id:
                return None
            count = (
                models.Task.objects.select_for_update()
                .available_for_processing([task_id])
                .claim(worker_id=worker_id)
            )
            if count:
                try:
                    ret = models.Task.objects.get(pk=task_id)
                except models.Task.DoesNotExist:
                    ret = None
                logger.debug(
                    "%s claimed task %s: %s", worker_id, ret.id, ret.callable_path
                )
                return ret
        return None


def new_queue_store(
    backend_name: str, queue_names: set[str], options: dict
) -> QueueStore:
    try:
        default = QueueConfiguration(
            max_attempts=options.get("max_attempts"),
            backoff_factor=options.get("backoff_factor"),
        )
    except TypeError as e:
        raise ImproperlyConfigured(f"Default task queue configuration: {str(e)}")
    configured = options.get("queues")
    queues = {}
    if configured:
        for name, qopts in configured.items():
            try:
                queues[name] = QueueConfiguration(
                    max_attempts=qopts.get("max_attempts", default.max_attempts),
                    backoff_factor=qopts.get("backoff_factor", default.backoff_factor),
                )
            except TypeError as e:
                raise ImproperlyConfigured(f"{name} task queue configuration: {str(e)}")
    for name in (q for q in queue_names if q not in queues):
        queues[name] = default
    return QueueStore(backend_name=backend_name, queues=queues)
