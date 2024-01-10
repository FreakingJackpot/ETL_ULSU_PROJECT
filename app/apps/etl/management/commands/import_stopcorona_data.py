import logging

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import DatabaseError

from apps.etl.models import StopCoronaData
from apps.etl.utils.parsers.stopcorona_parser import StopCoronaParser
from apps.etl.utils.logging import get_task_logger


class Command(BaseCommand):
    help = "imports data from объясняем.рф/stopcoronavirus"

    def add_arguments(self, parser):
        parser.add_argument("all", type=int, help='0-latest information, 1-full available information', choices=(0, 1),
                            default=0, nargs='?')
        parser.add_argument("manual", nargs='?', type=str, default=False)

    def handle(self, *args, **options):
        self.all = options["all"]

        parsed_data = self.get_parsed_data()
        created_count = self.upload_to_db(parsed_data)

        if options['manual']:
            return bool(created_count)

    def get_parsed_data(self):
        parser = StopCoronaParser(all=self.all)
        return parser.get_parsed_data()

    def upload_to_db(self, data):
        logger = get_task_logger()

        uploaded = set(StopCoronaData.objects.values_list('start_date', 'end_date', 'region'))
        objects = [
            StopCoronaData(**item) for item in data if
            (item['start_date'], item['end_date'], item['region']) not in uploaded
        ]

        StopCoronaData.objects.bulk_create(objects, batch_size=500)

        for obj in objects:
            logger.log(logging.INFO,
                       'StopCoronaData created',
                       start_date=obj.start_date.strftime('%d-%m-%Y'),
                       end_date=obj.end_date.strftime('%d-%m-%Y'),
                       region=obj.region,
                       hospitalized=obj.hospitalized,
                       recovered=obj.recovered,
                       infected=obj.infected,
                       deaths=obj.deaths)

            return len(objects)
