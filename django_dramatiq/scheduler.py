from datetime import timedelta, datetime

import pytz
from apscheduler.schedulers.blocking import BlockingScheduler
from django import db
import logging
from django.conf import settings

logger = logging.getLogger(__name__)

scheduler = BlockingScheduler(timezone=pytz.UTC)
scheduled_task_registry = {}

JOBS_RETRY_WHEN_DB_ERROR = {}
if hasattr(settings, 'JOBS_RETRY_WHEN_DB_ERROR'):
    JOBS_RETRY_WHEN_DB_ERROR = settings.JOBS_RETRY_WHEN_DB_ERROR


def db_error_listener(event):
    if isinstance(event.exception, db.OperationalError) or isinstance(event.exception, db.InterfaceError):
        logger.info('DB connection error. Close old connections')
        db.close_old_connections()
        if event.job_id in JOBS_RETRY_WHEN_DB_ERROR.keys():
            logger.warning('Retry job: %s after %s min', event.job_id, JOBS_RETRY_WHEN_DB_ERROR[event.job_id])
            scheduler.modify_job(event.job_id, next_run_time=datetime.now(tz=pytz.UTC) + timedelta(minutes=JOBS_RETRY_WHEN_DB_ERROR[event.job_id]))


def scheduled_task(id, trigger=None, cron=None, **schedule_args):
    def decorator(func):
        if id in scheduled_task_registry:
            raise Exception(f'Scheduled task with id {id} already register')
        
        scheduled_task_registry[id] = {
            'func': func,
            'trigger': trigger,
            'cron': cron,
            'schedule_args': schedule_args
        }
        return func
    return decorator
