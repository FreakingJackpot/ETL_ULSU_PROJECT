import re
from datetime import datetime
from functools import partial
from unittest.mock import patch

from bs4 import BeautifulSoup
from django.conf import settings
from django.test import TestCase

from apps.etl.utils.parsers.gogov_parser import convert_str_to_date, GogovParser


class GogovTestCase(TestCase):

    def setUp(self):
        self.parser = GogovParser()

    def test_convert_str_to_date(self):
        date_str = "01.01.22"
        date_format = "%d.%m.%y"
        expected_date = datetime.strptime(date_str, date_format).date()
        self.assertEqual(convert_str_to_date(date_str, date_format), expected_date)

        with self.assertRaises(ValueError):
            convert_str_to_date("invalid_date", date_format)

    @patch('requests.get')
    def test__get_page_html_success(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.text = '<html>Some HTML response</html>'

        result = GogovParser._get_page_html()

        self.assertEqual(result, '<html>Some HTML response</html>')

    @patch('requests.get')
    def test__get_page_html_connection_error(self, mock_get):
        mock_get.side_effect = ConnectionError('Connection error')

        result = GogovParser._get_page_html()

        self.assertIsNone(result)

    @patch('requests.get')
    def test__get_page_html_non_200_status_code(self, mock_get):
        mock_get.return_value.status_code = 404

        result = GogovParser._get_page_html()

        self.assertEqual(mock_get.call_args[0][0], GogovParser._url)
        self.assertEqual(result, None)

    def test_parse_global_data(self):
        global_data = open(settings.BASE_DIR.joinpath('apps/etl/tests_data/global_data.text'), 'r', encoding='utf-8')
        soup = BeautifulSoup(global_data.read(), 'html.parser')
        global_data = self.parser._parse_global_data(soup)

        expected_data = {
            'date': datetime.strptime('25.11.2023', '%d.%m.%Y').date(),
            'first_component': 89081596,
            'full_vaccinated': 79702396,
            'children_vaccinated': 202961,
            'need_revaccination': 79699284,
            'revaccinated': 20829310,
        }
        self.assertEqual(global_data, expected_data)

    def test_parse_page(self):
        test_data = open(settings.BASE_DIR.joinpath('apps/etl/tests_data/global_region_data.text'), 'r',
                         encoding='cp1251')
        result = self.parser._parse_page(test_data.read())

        global_data = {
            'date': datetime.strptime('25.11.2023', '%d.%m.%Y').date(),
            'first_component': 89081596,
            'full_vaccinated': 79702396,
            'children_vaccinated': 202961,
            'need_revaccination': 79699284,
            'revaccinated': 20829310,
        }

        self.assertEqual(result, global_data)
