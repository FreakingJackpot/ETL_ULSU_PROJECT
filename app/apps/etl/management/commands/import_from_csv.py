import csv
from datetime import datetime

from django.core.management.base import BaseCommand
from django.conf import settings

from apps.etl.forms import CsvDataForm


class Command(BaseCommand):
    help = ("HELP")

    _csv_date_format = '%d/%m/%Y'

    def add_arguments(self, parser):
        parser.add_argument("file_path", nargs='?', type=str, default=settings.DEFAULT_CSV_PATH)

    def handle(self, *args, **options):
        self.file_path = options["file_path"]
        self.prepare()
        self.process_csv_data_to_db_model()
        self.summary()

    def prepare(self):
        self.imported_counter = 0
        self.skipped_counter = 0

    def process_csv_data_to_db_model(self):
        self.stdout.write("Import COVID")

        with open(self.file_path, mode="r") as f:
            reader = csv.DictReader(f)
            for _, row_dict in enumerate(reader):
                data = {
                    'date': datetime.strptime(row_dict['dateRep'], self._csv_date_format),
                    'cases': row_dict['cases'],
                    'deaths': row_dict['deaths'],
                    'per_100000_cases_for_2_weeks': row_dict[
                        'Cumulative_number_for_14_days_of_COVID-19_cases_per_100000']
                }

                form = CsvDataForm(data=data)
                if form.is_valid():
                    form.save()
                    self.imported_counter += 1
                else:
                    self.stderr.write(f"Errors import COVID")
                    self.stderr.write(f"{form.errors.as_json()}\n")
                    self.skipped_counter += 1

    def summary(self):
        self.stderr.write(f"----------------\n")
        self.stderr.write(f"Covid imported: {self.imported_counter}\n")
        self.stderr.write(f"Covid skipped: {self.skipped_counter}\n")
