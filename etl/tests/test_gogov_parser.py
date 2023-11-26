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

    def test_clean_method(self):
        clean_method = partial(re.sub, " |чел.", "")
        self.assertEqual(clean_method("10 чел."), "10")
        self.assertEqual(clean_method("5 чел."), "5")
        self.assertEqual(clean_method("0 чел."), "0")

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
        html = """
        <p><span style="color:brown;">На сегодня</span> (25.11.23):<br/>
        <span class="bigint">89 081 596 чел.</span> (60.9% от <a href="/articles/population-ru">населения</a>, 76.7% взрослого) - привито хотя бы одним компонентом вакцины<br/>
        <span class="bignt">79 702 396 чел.</span> (54.5% от населения, 68.8% взрослого) - полностью привито<br/>
        <span class="bigint">202 961</span> - привито детей<br/>
        <span class="bigint">187 374 508 ш.</span> - всего прививок сделано 
        <span class="show-no-link" title="Рассчитывается как 'привито хотя бы одним компонентом вакцины' + 'полностью привито' + 'привито детей' + 'прошли ревакцинацию' - 'привито Спутником Лайт'. Данные о количестве привитых 'Спутником Лайт' есть только из 19 регионов - 2 238 794 чел.">?</span><br/>
        <b>20 829 310 чел.</b> - прошли <a href="/articles/covid-v-stats/revacc">ревакцинацию</a>, 
        <span class="bigint">79 699 284 чел.</span> - подлежит ревакцинации 
        <span class="show-no-link" title="подлежит ревакцинации - кол-во людей, которые полностью привились более 6 месяцев назад">?</span></p>
        """
        soup = BeautifulSoup(html, 'html.parser')
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
        html = """
        <tbody>
            <tr>
            <td><a href="/covid-v-stats/spb#data">Санкт-Петербург</a></td>
            <td data-text="3341922">3 341 922</td>
            <td>62.1%</td>
            <td>75.3%</td>
            <td data-text="45">45</td>
            <td>0%</td>
            <td data-text="3132550">3 132 550</td>
            <td data-text="927964">927 964</td>
            <td data-text="3132550">3 132 550</td>
            <td data-text="7673">7 673</td>
            <td data-text="2023-04-29"><a href="https://www.gov.spb.ru/covid-19/" rel="nofollow" target="_blank">29.04</a></td><td></td><td></td>
            <td data-text="645263">645 263</td>
            <td>0.00%</td>
            </tr>
        </tbody>
        """
        soup = BeautifulSoup(html, 'lxml')
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
        html = '''
                 <html>
                     <body>
                         <div id="data">
                             <p><span style="color:brown;">На сегодня</span> (25.11.23):<br/>
                                <span class="bigint">89 081 596 чел.</span> (60.9% от <a href="/articles/population-ru">населения</a>, 76.7% взрослого) - привито хотя бы одним компонентом вакцины<br/>
                                <span class="bignt">79 702 396 чел.</span> (54.5% от населения, 68.8% взрослого) - полностью привито<br/>
                                <span class="bigint">202 961</span> - привито детей<br/>
                                <span class="bigint">187 374 508 ш.</span> - всего прививок сделано 
                                <span class="show-no-link" title="Рассчитывается как 'привито хотя бы одним компонентом вакцины' + 'полностью привито' + 'привито детей' + 'прошли ревакцинацию' - 'привито Спутником Лайт'. Данные о количестве привитых 'Спутником Лайт' есть только из 19 регионов - 2 238 794 чел.">?</span><br/>
                                <b>20 829 310 чел.</b> - прошли <a href="/articles/covid-v-stats/revacc">ревакцинацию</a>, 
                                <span class="bigint">79 699 284 чел.</span> - подлежит ревакцинации 
                                <span class="show-no-link" title="подлежит ревакцинации - кол-во людей, которые полностью привились более 6 месяцев назад">?</span></p>
                             <table id="m-table">
                                 <tbody>
                                     <tr>
                                        <td><a href="/covid-v-stats/spb#data">Санкт-Петербург</a></td>
                                        <td data-text="3341922">3 341 922</td>
                                        <td>62.1%</td>
                                        <td>75.3%</td>
                                        <td data-text="45">45</td>
                                        <td>0%</td>
                                        <td data-text="3132550">3 132 550</td>
                                        <td data-text="927964">927 964</td>
                                        <td data-text="3132550">3 132 550</td>
                                        <td data-text="7673">7 673</td>
                                        <td data-text="2023-04-29"><a href="https://www.gov.spb.ru/covid-19/" rel="nofollow" target="_blank">29.04</a></td><td></td><td></td>
                                        <td data-text="645263">645 263</td>
                                        <td>0.00%</td>
                                     </tr>
                                 </tbody>
                             </table>
                         </div>
                     </body>
                 </html>
             '''

        result = self.parser._parse_page(html)

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






