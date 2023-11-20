from unittest import TestCase
from datetime import datetime
from unittest.mock import patch
from etl.utils.parsers.stopcorona_parser import StopCoronaParser
from bs4 import BeautifulSoup

class StopParserTestCase(TestCase):

    @patch("etl.utils.parsers.stopcorona_parser.requests.get")
    def test_get_url_list(self, mock_get):
        mock_response = mock_get.return_value
        mock_response.text = """
              <body>
                <div>
                  <a class="u-material-card u-material-cards__card" href="v-rossii-za-nedelyu-vyzdorovelo-article1"></a>
                </div>
                <div>
                  <a class="u-material-card u-material-cards__card" href="v-rossii-za-nedelyu-vyzdorovelo-article2"></a>
                </div>
                  <a class="u-material-card u-material-cards__card" href="/stopkoronavirus/molodym-mamam-o-vakcinacii-ot-koronavirusa/"></a>
                <div>  
                  <a class="u-material-card u-material-cards__card" href="v-rossii-za-nedelyu-vyzdorovelo-article3"></a>
                </div> 
                <div>
                  <a class="u-material-card u-material-cards__card" href="/stopkoronavirus/chto-izvestno-o-rossijskih-vakcinah/"></a>
                </div>
              </body>
          """

        expected_urls = ['v-rossii-za-nedelyu-vyzdorovelo-article1', 'v-rossii-za-nedelyu-vyzdorovelo-article2', 'v-rossii-za-nedelyu-vyzdorovelo-article3']
        StopCoronaParser._max_page = 1
        urls = StopCoronaParser._get_url_list()
        self.assertListEqual(urls, expected_urls)

    def test_parse_page(self):
        tests_data = [
            """
                <div class="article-detail__body">
                    <h3> По состоянию за 44 нед. 2023 г. (23.10 - 29.10.2023)</h3>
                        <tbody>
                            <tr>
                                <td>Наименование субъекта</td>
                                <td>hospitalized</td>
                                <td>recovered</td>
                                <td>infected</td>
                                <td>deaths</td>
                            </tr>
                            <tr>
                                <td>Region 1</td>
                                <td>10</td>
                                <td>5</td>
                                <td>20</td>
                                <td>2</td>
                            </tr>
                        </tbody>
                    </div>
                """,

            """
            <div class="article-detail__body">
                 <h3> По состоянию за 44 нед. 2023 г. (4423.10 - 29.10.2023)</h3>
                     <tbody>
                         <tr>
                            <td>Region</td>
                            <td>hospitalized</td>
                            <td>recovered</td>
                            <td>infected</td>
                            <td>deaths</td>
                        </tr>
                        <tr>
                            <td>Region 1</td>
                            <td>10</td>
                            <td>5</td>
                            <td>20</td>
                            <td>2</td>
                        </tr>
                    </tbody>
            </div>
        """]

        expected_data = [[{
            'start_date': datetime.strptime('23.10.2023', '%d.%m.%Y').date(),
            'end_date': datetime.strptime('29.10.2023', '%d.%m.%Y').date(),
            'region': 'Region 1',
            'hospitalized': 10,
            'recovered': 5,
            'infected': 20,
            'deaths': 2
        }],
            None]

        for i in range(2):
            data = StopCoronaParser._parse_page(tests_data[i])
            self.assertEqual(data, expected_data[i])

    def test_get_dates(self):
            tests_data = ["<html><body><h3> По состоянию за 44 нед. 2023 г. (23.10 - 29.10.2023)</h3></body></html>",
             "<html><body><h3> По состоянию за 44 нед. 2023 г. (23.11 - 29.10.2023)</h3></body></html>",
             "<html><body><h3> По состоянию за 44 нед. 2023 г. (4423.11 - 29.10.2023)</h3></body></html>"]



            expected_urls = [[datetime.strptime('23.10.2023', '%d.%m.%Y').date(),
                             datetime.strptime('29.10.2023', '%d.%m.%Y').date()],

                            [datetime.strptime('23.11.2022', '%d.%m.%Y').date(),
                             datetime.strptime('29.10.2023', '%d.%m.%Y').date()],

                             None]

            soup = [BeautifulSoup(tests_data[0], 'lxml'), BeautifulSoup(tests_data[1], 'lxml')]

            for i in range(2):
                urls = StopCoronaParser._get_dates(soup[i])
                self.assertEqual(urls, expected_urls[i])

    def test_clean_table_data(self):
            tests_data = "[<td>Наименование субъекта</td>,<td>hospitalized</td>,<td>recovered</td>,<td>infected</td>,<td>deaths</td>,<td>Region 1</td>, <td>10</td>, <td>5</td>, <td>20</td>, <td>2</td>]"
            soup = BeautifulSoup(tests_data, 'lxml')
            tests_data = soup.find_all("td")
            expected_urls = ['Region 1', 10, 5, 20, 2]
            urls = StopCoronaParser._clean_table_data(tests_data)
            self.assertListEqual(urls, expected_urls)

    def test_get_regions_data(self):
        tests_table_data = ['Region 1', 10, 5, 20, 2]
        tests_dates = [datetime.strptime('23.10.2023', '%d.%m.%Y').date(), datetime.strptime('29.10.2023', '%d.%m.%Y').date()]
        expected_urls = [{'start_date': datetime.strptime('23.10.2023', '%d.%m.%Y').date(), 'end_date': datetime.strptime('29.10.2023', '%d.%m.%Y').date(), 'region': 'Region 1', 'hospitalized': 10, 'recovered': 5, 'infected': 20, 'deaths': 2}]
        urls = StopCoronaParser._get_regions_data(tests_table_data, tests_dates)
        self.assertListEqual(urls, expected_urls)

    @patch("etl.utils.parsers.stopcorona_parser.requests.get")
    def test_parse_url_list(self, mock_get):
        mock_response = mock_get.return_value
        mock_response.text = """
                <div class="article-detail__body">
                    <h3> По состоянию за 44 нед. 2023 г. (23.10 - 29.10.2023)</h3>
                        <tbody>
                            <tr>
                                <td>Наименование субъекта</td>
                                <td>ospitalized</td>
                                <td>recovered</td>
                                <td>infected</td>
                                <td>deaths</td>
                            </tr>
                            <tr>
                                <td>Region 1</td>
                                <td>10</td>
                                <td>5</td>
                                <td>20</td>
                                <td>2</td>
                            </tr>
                        </tbody>
                </div>
            """

        expected_data = [
            {
                'start_date': datetime.strptime('23.10.2023', '%d.%m.%Y').date(),
                'end_date': datetime.strptime('29.10.2023', '%d.%m.%Y').date(),
                'region': 'Region 1',
                'hospitalized': 10,
                'recovered': 5,
                'infected': 20,
                'deaths': 2
            },
        ]
        urls = StopCoronaParser._parse_url_list([1])
        self.assertListEqual(urls, expected_data)