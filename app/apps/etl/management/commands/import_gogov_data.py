from django.core.management.base import BaseCommand

from apps.etl.models import GogovGlobalData
from apps.etl.utils.parsers.gogov_parser import GogovParser


class Command(BaseCommand):
    help = "imports data from gogov"

    def handle(self, *args, **options):
        self.stdout.write("Import from gogov")
        parsed_data = self.get_parsed_data()
        self.upload_to_db(parsed_data)

    def get_parsed_data(self):
        parser = GogovParser()
        return parser.get_data()

    def upload_to_db(self, data):
        GogovGlobalData.objects.get_or_create(date=data.pop('date'), defaults=data)
