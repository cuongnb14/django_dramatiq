from django.core.management import BaseCommand
import logging
from apscheduler.events import EVENT_JOB_ERROR
from django_dramatiq.scheduler import scheduler, job_registry, db_error_listener
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Run blocking scheduler to create periodical tasks'

    def handle(self, *args, **options):
        logger.info('Add job to scheduler...')

        for key, value in job_registry.items():
            if value['cron']:
                trigger = CronTrigger.from_crontab(value['cron'])
            else:
                trigger = value['trigger']
            scheduler.add_job(value['func'], id=key, trigger=trigger, **value['schedule_args'])
        
        scheduler.add_listener(db_error_listener, EVENT_JOB_ERROR)


        logger.info('Starting scheduler...')
        scheduler.start()
