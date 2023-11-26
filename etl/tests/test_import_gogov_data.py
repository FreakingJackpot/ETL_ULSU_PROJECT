from datetime import datetime
from io import StringIO
from django.test import TestCase
from unittest.mock import patch
from etl.models import GogovRegionData, GogovGlobalData
from etl.management.commands.import_gogov_data import Command
from etl.utils.parsers.gogov_parser import GogovParser

class CommandTest(TestCase):
    def setUp(self):
        self.out = StringIO()
        self.err = StringIO()
        self.parser = GogovParser()
        self.parser = GogovParser()
        self.command = Command()

    #Проверка, если данные загружались с таким же регионом и датой ранее, и только если данных нет, создает новые записи.
    def test_upload_region_data(self):
        # Создаем объект команды
        command = Command()

        # Создаем объекты GogovRegionData, которые нужно загрузить
        regions_data = [
            {'region': 'Москва',
             'vaccinated': 6199585,
             'avg_people_per_day': 7135,
             'full_vaccinated': 5803415,
             'revaccinated': 1000000,
             'need_revaccination': 0,
             'children_vaccinated': 6200,
             'date': datetime.strptime('2022-05-13', '%Y-%m-%d').date()}
        ]

        # Загружаем данные в базу данных
        command._upload_region_data(regions_data)

        # Проверяем, что данные были успешно загружены
        loaded_data = GogovRegionData.objects.all()

        self.assertEqual(len(loaded_data), len(regions_data))

        for region_data in regions_data:
            region = region_data['region']
            date = region_data['date']

            # Проверяем, что данные присутствуют в базе данных
            self.assertTrue(loaded_data.filter(region=region, date=date).exists())

        # Повторно загружаем данные
        command._upload_region_data(regions_data)

        # Проверяем, что данные не были загружены повторно
        loaded_data = GogovRegionData.objects.all()

        self.assertEqual(len(loaded_data), len(regions_data))

    def test_handle(self):
        # Создаем тестовые данные для загрузки в базу данных
        global_data = {
            'date': '2023-11-21',
            'first_component': 500,
            'full_vaccinated': 250,
            'children_vaccinated': 100,
            'revaccinated': 150,
            'need_revaccination': 50
        }
        region_data = [
            {
                'region': 'Moscow',
                'date': '2021-11-02',
                'vaccinated': 100,
                'avg_people_per_day': 50,
                'full_vaccinated': 50,
                'revaccinated': 20,
                'need_revaccination': 10,
                'children_vaccinated': 30
            },
            {
                'region': 'St. Petersburg',
                'date': '2021-09-03',
                'vaccinated': 300,
                'avg_people_per_day': 150,
                'full_vaccinated': 150,
                'revaccinated': 50,
                'need_revaccination': 30,
                'children_vaccinated': 70
            }
        ]
        parsed_data = {
            'global_data': global_data,
            'regions_data': region_data
        }

        # Мокаем функцию get_parsed_data, чтобы возвращала наши тестовые данные
        def mocked_get_parsed_data(self):
            return parsed_data

        GogovParser.get_data = mocked_get_parsed_data

        # Запускаем команду handle
        command = Command()
        command.handle()
        # Проверяем данные, загруженные в базу данных
        global_data_obj = GogovGlobalData.objects.get()
        self.assertEqual(global_data_obj.first_component, global_data['first_component'])
        self.assertEqual(global_data_obj.full_vaccinated, global_data['full_vaccinated'])
        self.assertEqual(global_data_obj.children_vaccinated, global_data['children_vaccinated'])
        self.assertEqual(global_data_obj.revaccinated, global_data['revaccinated'])
        self.assertEqual(global_data_obj.need_revaccination, global_data['need_revaccination'])

        region_data_objs = GogovRegionData.objects.filter()
        self.assertEqual(region_data_objs.count(), len(region_data))
        for region in region_data:
            self.assertTrue(region_data_objs.filter(region=region['region'], date=region['date']).exists())

    def test_get_parsed_data(self):
        data = self.parser.get_data()
        self.assertIsInstance(data, dict)
        self.assertIn('global_data', data)
        self.assertIn('regions_data', data)

