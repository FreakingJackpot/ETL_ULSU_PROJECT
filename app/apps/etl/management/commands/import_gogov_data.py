import logging

from django.core.management.base import BaseCommand

from apps.etl.models import GogovData
from apps.etl.utils.parsers.gogov_parser import GogovParser
from apps.etl.utils.logging import get_task_logger


class Command(BaseCommand):
    help = "imports data from gogov"

    def handle(self, *args, **options):
        parsed_data = self.get_parsed_data()
        self.upload_to_db(parsed_data)

    def get_parsed_data(self):
        parser = GogovParser()
        return parser.get_data()

    def upload_to_db(self, data):
        obj, created = GogovData.objects.get_or_create(date=data.pop('date'), defaults=data)
        if created:
            logger = get_task_logger()
            date = obj.date if isinstance(obj.date, str) else obj.date.strftime('%d-%m-%Y')
            logger.log(logging.INFO, 'GogovData created', **data, date=date)
