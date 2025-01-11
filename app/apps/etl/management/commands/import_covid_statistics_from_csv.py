import csv
import logging
from datetime import datetime

from django.conf import settings
from django.core.management.base import BaseCommand

from apps.etl.forms import CsvDataForm
from apps.etl.utils.logging import get_task_logger


class Command(BaseCommand):
    help = ("Downloads from csv file, \n args: string optional file_path")

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
        self.logger = get_task_logger()

    def process_csv_data_to_db_model(self):
        self.logger.log(logging.INFO, 'Import from csv started')

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
                    obj = form.save()
                    data['id'] = obj.id
                    data['date'] = data['date'].strftime('%d-%m-%Y')
                    self.logger.log(logging.INFO, 'Imported from csv', **data, model='CsvData')
                    self.imported_counter += 1
                else:
                    errors = form.errors.get_json_data()
                    data['date'] = data['date'].strftime('%d-%m-%Y')
                    self.logger.log(logging.WARNING, 'Errors while import', **data, errors=errors, model='CsvData')
                    self.skipped_counter += 1

    def summary(self):
        self.logger.log(logging.INFO, 'Imported from csv', count=self.imported_counter)
        self.logger.log(logging.INFO, 'Skipped while csv import', count=self.skipped_counter)
