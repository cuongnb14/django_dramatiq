import logging
import importlib

from django.core.management import BaseCommand
from django.apps import apps
from django.utils.module_loading import module_has_submodule
from apscheduler.events import EVENT_JOB_ERROR
from django_dramatiq.scheduler import scheduler, scheduled_task_registry, db_error_listener
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Run blocking scheduler to create periodical tasks'

    def handle(self, *args, **options):
        logger.info('Add tasks to scheduler...')
        self.discover_scheduled_task_modules()

        for key, value in scheduled_task_registry.items():
            if value['cron']:
                trigger = CronTrigger.from_crontab(value['cron'])
            else:
                trigger = value['trigger']
            scheduler.add_job(value['func'], id=key, trigger=trigger, **value['schedule_args'])
        
        scheduler.add_listener(db_error_listener, EVENT_JOB_ERROR)


        logger.info('Starting scheduler...')
        scheduler.start()


    def discover_scheduled_task_modules(self):
        task_module = 'scheduled_tasks'
        for app_config in apps.get_app_configs():
            if module_has_submodule(app_config.module, task_module):
                module = app_config.name + "." + task_module
                logger.info('Import module %s', module)
                importlib.import_module(module)
