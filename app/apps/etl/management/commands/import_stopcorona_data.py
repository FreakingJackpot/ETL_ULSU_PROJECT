from django.core.management.base import BaseCommand
from django.db import DatabaseError

from apps.etl.models import StopCoronaData
from apps.etl.utils.parsers.stopcorona_parser import StopCoronaParser


class Command(BaseCommand):
    help = "imports data from объясняем.рф/stopcoronavirus"

    def add_arguments(self, parser):
        parser.add_argument("all", type=int, help='0-latest information, 1-full available information', choices=(0, 1),
                            default=0, nargs='?')

    def handle(self, *args, **options):
        self.all = options["all"]
        self.stdout.write("Import from stopcorona")

        parsed_data = self.get_parsed_data()
        self.stdout.write(f"Parsed {len(parsed_data)} elements")

        self.upload_to_db(parsed_data)

    def get_parsed_data(self):
        parser = StopCoronaParser(all=self.all)
        return parser.get_parsed_data()

    def upload_to_db(self, data):
        uploaded = set(StopCoronaData.objects.values_list('start_date', 'end_date', 'region'))
        objects = [
            StopCoronaData(**item) for item in data if
            (item['start_date'], item['end_date'], item['region']) not in uploaded
        ]

        try:
            StopCoronaData.objects.bulk_create(objects, batch_size=500)
        except DatabaseError as e:
            self.stdout.writelines(("Exception occurred", str(e),))
            return

        self.stdout.write("Upload complete")
