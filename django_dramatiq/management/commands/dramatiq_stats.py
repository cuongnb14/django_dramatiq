from time import sleep

from django.conf import settings
from django.core.management import BaseCommand
import redis
from django.utils.timezone import now


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('-c', '--cycle', type=int, help='refresh cycle', default=3)

    def handle(self, *args, **options):
        cycle = options.get('cycle', 3)
        while True:
            try:
                self._run(cycle)
                sleep(cycle)
            except KeyboardInterrupt:
                break

    def _run(self, cycle):
        client = redis.Redis.from_url(settings.DRAMATIQ_BROKER['OPTIONS']['url'])
        keys = client.keys('dramatiq:*')

        format_row = '{:<80} {:<15}'

        queue_data = []
        processing_data = []
        for key in keys:
            key = key.decode()
            queue_type, queue_name = self._get_queue_name(key)
            if queue_name:
                if queue_type == 'XQ':
                    queue_data.append(format_row.format(f'{queue_name}', client.zcard(key)))
                elif queue_type == 'acks':
                    processing_data.append(format_row.format(f'[ACKS] {queue_name}', client.scard(key)))
                else:
                    queue_data.append(format_row.format(f'{queue_name}', client.llen(key)))
        self._print_result(cycle, format_row, queue_data, processing_data)


    def _print_result(self, cycle, format_row, queue_data, processing_data):
        queue_data.sort()
        processing_data.sort()

        self._clear_terminal()
        print(f'Time {now()}. Refresh cycle {cycle}s')
        print(format_row.format('Queue', 'Count'))
        print('-' * 100)
        for row in queue_data:
            print(row)
        for row in processing_data:
            print(row)

    def _clear_terminal(self):
        print("\033[H\033[J")

    def _get_queue_name(self, key):
        dramatiq_key = key[9:]
        if not dramatiq_key.startswith('__') and not dramatiq_key.endswith('.msgs'):
            if '.' in dramatiq_key:
                queue_name, queue_type = dramatiq_key.split('.')
                return queue_type, dramatiq_key
            return 'main', dramatiq_key
        if dramatiq_key.startswith('__acks__'):
            _, worker_id, *queue_name = dramatiq_key.split('.')
            queue_name = '.'.join(queue_name)
            return 'acks', f'{worker_id} - {queue_name}'
        return None, None
