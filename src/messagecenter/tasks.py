from __future__ import annotations

import random
import time

import httpx
from django.conf import settings
from django.tasks import task


@task
def send_notification(message: str, title: str | None):
    # Introduce a bit of randomness.
    time.sleep(random.random() * 2)
    # Pass the title if specified.
    try:
        headers = {"title": title} if title else {}
        httpx.post(
            settings.NTFY_URL,
            content=message,
            headers=headers,
        )
    except Exception as e:
        print(e)
