from datetime import datetime
from django.test import TestCase
from apps.etl.models import GogovRegionData, GogovGlobalData
from apps.etl.management.commands.import_gogov_data import Command
from apps.etl.utils.parsers.gogov_parser import GogovParser


class TestImportGogovData(TestCase):
    def setUp(self):
        self.parser = GogovParser()
        self.command = Command()

    def test_upload_region_data(self):
        expected_regions_data = [
            {'region': 'Москва',
             'vaccinated': 6199987,
             'avg_people_per_day': 71387,
             'full_vaccinated': 86415,
             'revaccinated': 1000000,
             'need_revaccination': 100,
             'children_vaccinated': 6200,
             'date': datetime.strptime('2021-05-13', '%Y-%m-%d').date()}
        ]

        command = Command()

        # Загружаем данные в базу данных
        command._upload_region_data(expected_regions_data)

        regions = GogovRegionData.objects.values('region', 'vaccinated', 'avg_people_per_day', 'full_vaccinated',
                                                 'revaccinated', 'need_revaccination', 'children_vaccinated', 'date')
        self.assertCountEqual(regions, expected_regions_data)

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

        region_data_objs = GogovRegionData.objects
        self.assertEqual(region_data_objs.count(), len(region_data))
