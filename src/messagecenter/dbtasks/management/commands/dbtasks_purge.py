from __future__ import annotations

from django.core.management import BaseCommand, CommandError
from django.tasks import DEFAULT_TASK_BACKEND_ALIAS, task_backends
from django.utils.dateparse import parse_duration


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("variant", choices=["finished", "unfinished"])
        parser.add_argument("--backend", "-b", default=DEFAULT_TASK_BACKEND_ALIAS)
        parser.add_argument("--before", "-d", type=duration)
        parser.add_argument("--limit", "-l", type=int)

    def handle(self, *args, **options):
        backend = task_backends[options["backend"]]
        variant = options["variant"]
        before = options.get("before")
        limit = options.get("limit")
        if variant == "finished":
            if before is None:
                before = backend.purge_finished_duration()
            count = backend.purge_expired(
                finished_before=before, unfinished_enqueued_before=None, limit=limit
            )
        elif variant == "unfinished":
            if before is None:
                before = backend.purge_unfinished_duration()
            count = backend.purge_expired(
                finished_before=None, unfinished_enqueued_before=before, limit=limit
            )
        else:
            raise CommandError("Invalid variant")
        self.stdout.write(self.style.SUCCESS(f"Purged {count} tasks"))


def duration(value):
    if not value:
        return None
    ret = parse_duration(value)
    if not ret:
        raise ValueError("Invalid duration value")
    return ret
