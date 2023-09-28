from datetime import timedelta, datetime

import pytz
from apscheduler.schedulers.blocking import BlockingScheduler
from django import db
import logging
from django.conf import settings

logger = logging.getLogger(__name__)

scheduler = BlockingScheduler(timezone=pytz.UTC)
job_registry = {}

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


def schedule_job(id, trigger=None, cron=None, **schedule_args):
    def decorator(func):
        if id in job_registry:
            raise Exception(f'Schedule job with id {id} already register')
        
        job_registry[id] = {
            'func': func,
            'trigger': trigger,
            'cron': cron,
            'schedule_args': schedule_args
        }
        return func
    return decorator
