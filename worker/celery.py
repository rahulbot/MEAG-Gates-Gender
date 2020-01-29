from __future__ import absolute_import
from celery import Celery

from worker import BROKER_URL

app = Celery('worker', broker=BROKER_URL, include=['worker.tasks'])
