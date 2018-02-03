from celery import Celery
from datetime import timedelta
from celery.task import periodic_task

import os

app = Celery('tasks', broker='pyamqp://guest@localhost//')


@periodic_task(run_every=timedelta(days=1))
def add():
    print "Running"
    os.system('scrapy crawl spider')
    return
