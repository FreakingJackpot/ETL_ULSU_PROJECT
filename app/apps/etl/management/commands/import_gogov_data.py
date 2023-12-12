from django.core.management.base import BaseCommand
from django.db import DatabaseError

from apps.etl.utils.parsers.gogov_parser import GogovParser
from apps.etl.models import GogovGlobalData, GogovRegionData


class Command(BaseCommand):
    help = "imports data from gogov"

    def handle(self, *args, **options):
        self.stdout.write("Import from gogov")

        parsed_data = self.get_parsed_data()
        self.stdout.write(f"Parsed {len(parsed_data['regions_data'])} elements")

        self.upload_to_db(parsed_data)

    def get_parsed_data(self):
        parser = GogovParser()
        return parser.get_data()

    def upload_to_db(self, data):
        self._upload_global_data(data['global_data'])
        self._upload_region_data(data['regions_data'])

    def _upload_region_data(self, regions_data):
        uploaded = set(GogovRegionData.objects.values_list('region', 'date'))
        objects = [
            GogovRegionData(**item) for item in regions_data if
            (item['region'], item['date']) not in uploaded
        ]
        try:
            GogovRegionData.objects.bulk_create(objects, batch_size=500)
        except DatabaseError as e:
            self.stdout.writelines(("Exception occurred", str(e),))

    def _upload_global_data(self, global_data):
        GogovGlobalData.objects.get_or_create(date=global_data.pop('date'), defaults=global_data)
