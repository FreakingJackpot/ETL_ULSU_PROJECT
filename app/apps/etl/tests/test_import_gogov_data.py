from unittest.mock import patch

from django.test import TestCase
from apps.etl.models import GogovGlobalData
from apps.etl.management.commands.import_gogov_data import Command
from apps.etl.utils.parsers.gogov_parser import GogovParser


class TestImportGogovData(TestCase):
    def setUp(self):
        self.parser = GogovParser()
        self.command = Command()

    @patch("apps.etl.utils.parsers.gogov_parser.GogovParser.get_data")
    def test_handle(self, mock_get_data):
        # Создаем тестовые данные для загрузки в базу данных
        global_data = {
            'date': '2023-11-21',
            'first_component': 500,
            'full_vaccinated': 250,
            'children_vaccinated': 100,
            'revaccinated': 150,
            'need_revaccination': 50
        }

        mock_get_data.return_value = global_data

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
