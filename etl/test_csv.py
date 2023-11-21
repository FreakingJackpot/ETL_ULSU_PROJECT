import csv
import os
from etl.models import CsvData
from unittest import TestCase

from etl.management.commands.import_covid_from_csv import Command
# Create your tests here.
class ImportDataTests(TestCase):
    def setUp(self):
        # 1. Создаем csv файл
        with open('test_data.csv', 'w', newline='') as csvfile:
            fieldnames = ['date', 'cases', 'deaths', 'days_14_cases_per_100000']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow({'date': '2023-11-18', 'cases': '100', 'deaths': '488', 'days_14_cases_per_100000': '263.66356427'})
            writer.writerow({'date': '2023-11-18', 'cases': '150', 'deaths': '15', 'days_14_cases_per_100000': '20.2'})
            writer.writerow({'date': '2023-11-18', 'cases': '200', 'deaths': '20', 'days_14_cases_per_100000': '25.8'})
            # добавляем строку с ошибкой
            writer.writerow({'date': '2023-11-18', 'cases': '250', 'deaths': '25', 'days_14_cases_per_100000': 'abc'})

    def test_csv_loading(self):
        com = Command()
        # 2. Запускаем команду обработки csv файла
        com.handle(file_path = ['test_data.csv'])

        # 3. Загружаем данные из бд
        data_in_db = CsvData.objects.count()

        # 4. Проверяем количество фактически загруженных записей
        self.assertEqual(int(data_in_db), 3)

        # 5. Проверяем что все правильные записи загружены
        self.assertTrue(
            CsvData.objects.filter(date='2023-11-18', cases=100, deaths=488, days_14_cases_per_100000=263.66356427).exists())
        self.assertTrue(
            CsvData.objects.filter(date='2023-11-18', cases=150, deaths=15, days_14_cases_per_100000=20.2).exists())
        self.assertTrue(
            CsvData.objects.filter(date='2023-11-18', cases=200, deaths=20, days_14_cases_per_100000=25.8).exists())

    def tearDown(self):
        os.remove('test_data.csv')
