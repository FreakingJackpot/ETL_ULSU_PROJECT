import re
from datetime import datetime

from bs4 import BeautifulSoup
import requests


class Parser:
    _url_base = "https://xn--90aivcdt6dxbc.xn--p1ai/{}"
    _url_articles_list = _url_base.format('stopkoronavirus/')
    _subject_fields = ['hospitalized', 'recovered', 'cases', 'deaths']
    _date_format = '%d.%m.%Y'

    def __init__(self):
        self.url_list = self._get_url_list()

    @classmethod
    def _get_url_list(cls):
        urls = []
        req = requests.get(cls._url_articles_list)

        src = req.text
        soup = BeautifulSoup(src, 'lxml')

        media_page = soup.find("body")
        material_cards = media_page.find_all("a", class_="u-material-card u-material-cards__card")

        for link in material_cards:
            href = str(link.get('href'))
            if "v-rossii-za-nedelyu-vyzdorovelo-" in href:
                urls.append(href)

        return urls

    @classmethod
    def _parse_url_list(cls, url_list):
        parsed_data = []

        for url in url_list:
            req = requests.get(cls._url_base.format(url))

            parsed_data.append(cls._parse_page(req.text))

        return parsed_data

    @classmethod
    def _parse_page(cls, src):
        soup = BeautifulSoup(src, 'lxml')

        detail__body = soup.find("div", class_="article-detail__body")
        tbody = detail__body.find("tbody")
        table_data = tbody.find_all("td")

        dates = cls._get_dates(detail__body)

        table_data = cls._clean_table_data(table_data)
        subjects_dict = cls._get_subjects_dict(table_data)

        return *dates, subjects_dict

    @classmethod
    def _get_dates(cls, detail__body):

        data = str(detail__body.find("h3"))
        data = re.sub('<.*?>', '', data)
        matches = re.findall(r"\d+\.?\d*- \d+\.?\d*", data)
        dates = matches[0].split('-')

        current_year = datetime.now().year
        if int(dates[0].split('.')[1]) > int(dates[1].split('.')[1]):
            dates[0] = datetime.strptime(dates[0].strip() + f'.{current_year - 1}', cls._date_format)
            dates[1] = datetime.strptime(dates[1].strip() + f'.{current_year}', cls._date_format)
        else:
            dates[0] = datetime.strptime(dates[0].strip() + f'.{current_year}', cls._date_format)
            dates[1] = datetime.strptime(dates[1].strip() + f'.{current_year}', cls._date_format)

        return dates

    @staticmethod
    def _clean_table_data(table_data):
        table_data = table_data[5:]

        for i, td in enumerate(table_data):
            table_data[i] = re.sub('<.*?>|[\\n\\t\\r]', '', str(td))

            temp = table_data[i].replace(' ', '')
            if temp.isdecimal():
                table_data[i] = int(temp)

        return table_data

    @classmethod
    def _get_subjects_dict(cls, tds):
        dictionary = {}
        for i in range(0, len(tds), 5):
            dictionary[tds[i]] = dict(zip(cls._subject_fields, tds[i + 1:i + 5]))

        return dictionary

    def get_all(self):
        return self._parse_url_list(self.url_list)

    def get_latest(self):
        return self._parse_url_list(self.url_list[:1])


if __name__ == '__main__':
    a = Parser()
    print(*a.get_all(), sep="\n")
    print(*a.get_latest())

