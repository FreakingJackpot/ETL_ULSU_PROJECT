import csv
import os
from unittest import TestCase

from django.conf import settings

from apps.etl.management.commands.import_covid_statistics_from_csv import Command
from apps.etl.models import CsvData


# Create your tests here.
class ImportDataTests(TestCase):
    _test_file_path = str(settings.BASE_DIR.joinpath('apps/etl/tests_data/tests_data.csv'))

    def setUp(self):
        # 1. Создаем csv файл
        with open(self._test_file_path, 'w', newline='') as csvfile:
            fieldnames = ['id', 'dateRep', 'cases', 'deaths',
                          'Cumulative_number_for_14_days_of_COVID-19_cases_per_100000']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow(
                {'id': 0, 'dateRep': '18/11/2023', 'cases': '100', 'deaths': '488',
                 'Cumulative_number_for_14_days_of_COVID-19_cases_per_100000': '263.66356427'})
            writer.writerow({'id': 1, 'dateRep': '19/11/2023', 'cases': '150', 'deaths': '15',
                             'Cumulative_number_for_14_days_of_COVID-19_cases_per_100000': '20.2'})
            writer.writerow({'id': 2, 'dateRep': '20/11/2023', 'cases': '200', 'deaths': '20',
                             'Cumulative_number_for_14_days_of_COVID-19_cases_per_100000': '25.8'})
            # добавляем строку с ошибкой
            writer.writerow({'id': 3, 'dateRep': '21/11/2023', 'cases': '250', 'deaths': '25',
                             'Cumulative_number_for_14_days_of_COVID-19_cases_per_100000': 'abc'})

    def test_csv_loading(self):
        # 2. Запускаем команду обработки csv файла
        Command().handle(file_path=self._test_file_path)

        # 3. Загружаем данные из бд
        data_in_db = CsvData.objects.count()

        # 4. Проверяем количество фактически загруженных записей
        self.assertEqual(int(data_in_db), 3)

        # 5. Проверяем что все правильные записи загружены
        self.assertTrue(
            CsvData.objects.filter(date='2023-11-18', cases=100, deaths=488,
                                   per_100000_cases_for_2_weeks=263.66356427).exists())
        self.assertTrue(
            CsvData.objects.filter(date='2023-11-19', cases=150, deaths=15, per_100000_cases_for_2_weeks=20.2).exists())
        self.assertTrue(
            CsvData.objects.filter(date='2023-11-20', cases=200, deaths=20, per_100000_cases_for_2_weeks=25.8).exists())

    def tearDown(self):
        os.remove(self._test_file_path)
