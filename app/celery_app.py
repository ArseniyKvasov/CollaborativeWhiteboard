import os

from celery import Celery

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery_app = Celery("whiteboard", broker=REDIS_URL, backend=REDIS_URL, include=["app.tasks"])
celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,
    task_track_started=True,
    # Fair queuing under burst load (e.g. a lecture hall uploading images at
    # once): one in-flight task per worker child instead of prefetching a long
    # private backlog, and ack-after-done so a crashed worker re-queues the
    # image instead of losing it in "processing" forever.
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
)
