from unittest.mock import patch
from django.test import TestCase
from etl.utils.parsers.gogov_parser import *

class GogovTestCase(TestCase):

    def setUp(self):
        self.parser = GogovParser()

    def test_convert_int_str_to_int(self):
        self.assertEqual(convert_int_str_to_int("123"), 123)
        self.assertEqual(convert_int_str_to_int("0"), 0)
        self.assertEqual(convert_int_str_to_int(""), 0)

    def test_convert_str_to_date(self):
        date_str = "01.01.22"
        date_format = "%d.%m.%y"
        expected_date = datetime.strptime(date_str, date_format).date()
        self.assertEqual(convert_str_to_date(date_str, date_format), expected_date)

        with self.assertRaises(ValueError):
            convert_str_to_date("invalid_date", date_format)

    def test_GogovParser_clean_region_fields(self):
        field = {"name": "region", "clean_method": partial(re.sub, "обл.", "область")}
        cleaned_value = field["clean_method"]("Москва обл.")
        self.assertEqual(cleaned_value, "Москва область")

    def test_GovParser_init(self):
        parser = GogovParser()
        self.assertEqual(parser._url, settings.GOGOV_URL)

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

        soup = BeautifulSoup(open('./etl/tests_data/global_data.text', 'r', encoding='utf-8').read(), 'html.parser')
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

    def test_get_regions_data(self):

        soup = BeautifulSoup(open('./etl/tests_data/regions_data.text', 'r').read(), 'html.parser')
        soup = soup.find('tbody')
        regions_data = self.parser._get_regions_data(soup)

        expected_data = [{
            'region': 'Санкт-Петербург',
            'vaccinated': 3341922,
            'avg_people_per_day': 45,
            'full_vaccinated': 3132550,
            'revaccinated': 927964,
            'need_revaccination': 3132550,
            'children_vaccinated': 7673,
            'date': datetime.strptime("29.04.2023", '%d.%m.%Y').date()
        }]
        self.assertEqual(regions_data, expected_data)

    def test_parse_page(self):

        result = self.parser._parse_page(open('./etl/tests_data/global_region_data.text', 'r').read())

        global_data = {
            'date': datetime.strptime('25.11.2023', '%d.%m.%Y').date(),
            'first_component': 89081596,
            'full_vaccinated': 79702396,
            'children_vaccinated': 202961,
            'need_revaccination': 79699284,
            'revaccinated': 20829310,
        }
        region_data = [{
            'region': 'Санкт-Петербург',
            'vaccinated': 3341922,
            'avg_people_per_day': 45,
            'full_vaccinated': 3132550,
            'revaccinated': 927964,
            'need_revaccination': 3132550,
            'children_vaccinated': 7673,
            'date': datetime.strptime("29.04.2023", '%d.%m.%Y').date()
        }]

        parsed_data = {
            'global_data': global_data,
            'regions_data': region_data
        }

        self.assertEqual(result, parsed_data)






