from __future__ import annotations

import logging
import time

from django.core.management import BaseCommand
from django.db import connections
from django.tasks.base import DEFAULT_TASK_BACKEND_ALIAS

from messagecenter.dbtasks import worker

logger = logging.getLogger("messagecenter.dbtasks.worker")


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("--id")
        parser.add_argument("--queue", "-q", nargs="*", type=str)
        parser.add_argument("--excluding", nargs="*", type=str)
        parser.add_argument("--backend", type=str, default=DEFAULT_TASK_BACKEND_ALIAS)
        parser.add_argument("--oneshot", "-o", action="store_true", default=False)
        parser.add_argument("--delay-min", type=int, default=250)
        parser.add_argument("--delay-max", type=int, default=5000)

    def handle(self, *args, **options):
        self.configure_logging(options["verbosity"])
        backend = options["backend"]
        queue_names = options.get("queue")
        only = set(queue_names if queue_names else [])
        if only:
            excluding = None
        else:
            excluding = options.get("excluding")
            if excluding:
                excluding = set(excluding)
        w = worker.Worker(
            id_=options.get("id"),
            backend_name=backend,
            only=only,
            excluding=excluding,
        )
        runner = self.create_runner(w, options)
        logger.info("Running %s", w.id)
        try:
            runner.run()
        except KeyboardInterrupt:
            pass

    def create_runner(self, w: worker.Worker, options):
        if options["oneshot"]:
            return OneShotRunner(w)
        delays = options["delay_min"], options["delay_max"]
        return ProcessRunner(w, delays=delays)

    def configure_logging(self, verbosity):
        level = logging.INFO
        match verbosity:
            case 0:
                level = logging.WARNING
            case 3:
                level = logging.DEBUG
            case _:
                level = logging.INFO
        logger.setLevel(level)
        if not logger.hasHandlers():
            logger.addHandler(logging.StreamHandler(self.stdout))


class OneShotRunner:
    def __init__(self, w: worker.Worker):
        self.w = w

    def run(self):
        while self.w.has_more():
            self.w.process()
        close_db_connections()


class ProcessRunner:
    def __init__(self, w: worker.Worker, delays: tuple[int, int]):
        self.w = w
        self.delays = delays

    def run(self):
        counts = []
        delay = self.delays[0]
        time.sleep(delay / 1000)
        while True:
            count = self.w.process()
            counts.append(count)
            if len(counts) > 10:
                counts.pop(0)
            if self.w.has_more():
                delay = self.delays[0]
            else:
                new_delay = delay
                for i, c in enumerate(counts):
                    if c == 0:
                        value = i * self.delays[0]
                    else:
                        value = -i * self.delays[0]
                    new_delay += value
                delay = new_delay
            close_db_connections()
            delay = min(max(self.delays[0], delay), self.delays[1])
            logger.debug("Sleeping for %sms", delay)
            time.sleep(delay / 1000)


def close_db_connections():
    for conn in connections.all(initialized_only=True):
        conn.close_if_unusable_or_obsolete()
