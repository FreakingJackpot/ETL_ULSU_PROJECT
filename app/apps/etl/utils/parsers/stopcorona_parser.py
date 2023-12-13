import re
from datetime import datetime
from itertools import chain

from django.conf import settings
from bs4 import BeautifulSoup
import requests


class StopCoronaParser:
    _url_base = settings.STOPCORONA_URL_BASE
    _url_articles_page = settings.STOPCORONA_URL_ARTICLES_PAGE
    _max_page = settings.MAX_STOPCORONA_PAGE

    _region_fields = ['start_date', 'end_date', 'region', 'hospitalized', 'recovered', 'infected', 'deaths']
    _date_matching_pattern = r"\d+\.?\d*\.? - \d+\.?\d*|\d+\.?\d*- \d+\.?\d*|\d+\.?\d*\.\d{4} *– *\d+\.?\d*\.\d{4}"
    _date_format = '%d.%m.%Y'

    def __init__(self):
        self.url_list = self._get_url_list()

    @classmethod
    def _get_url_list(cls):
        urls = []
        for page in range(1, cls._max_page + 1):
            req = requests.get(cls._url_articles_page.format(page))

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

            regions_data = cls._parse_page(req.text)
            if regions_data:
                parsed_data.extend(regions_data)

        return parsed_data

    @classmethod
    def _parse_page(cls, src):
        soup = BeautifulSoup(src, 'html5lib')

        detail__body = soup.find("div", class_="article-detail__body")

        dates = cls._get_dates(detail__body)
        if not dates:
            return None

        tbody = detail__body.find("tbody")
        table_data = tbody.find_all("td")

        table_data = cls._clean_table_data(table_data)

        regions_data = cls._get_regions_data(table_data, dates)

        return regions_data

    @classmethod
    def _get_dates(cls, detail__body):
        date_text = detail__body.find("h3").text
        matches = re.findall(cls._date_matching_pattern, date_text)
        if len(matches) != 1:
            print({'error': f'Can\'t get date from text. No matching pattern.', 'date_text': date_text})
            return

        dates = re.split('-|–', matches[0])
        if len(dates) != 2:
            print({'error': f'Can\'t split dates properly. No matching pattern.', 'dates': dates})
            return

        if len(dates[0].split('.')) == 3:
            dates[0], dates[1] = (datetime.strptime(dates[0], cls._date_format),
                                  datetime.strptime(dates[1], cls._date_format))
        else:
            current_year = datetime.now().year

            try:
                if int(dates[0].split('.')[1].strip()) > int(dates[1].split('.')[1].strip()):
                    dates[0] = datetime.strptime(dates[0].strip() + f'.{current_year - 1}', cls._date_format).date()
                else:
                    dates[0] = datetime.strptime(dates[0].strip() + f'.{current_year}', cls._date_format).date()

                dates[1] = datetime.strptime(dates[1].strip() + f'.{current_year}', cls._date_format).date()
            except ValueError:
                print(f"Invalid date format: {dates}")
                return

        return dates

    @staticmethod
    def _clean_table_data(table_data):
        if 'Наименование субъекта' in table_data[0].text:
            table_data = table_data[5:]
        else:
            table_data = table_data[6:]

        for i, td in enumerate(table_data):
            table_data[i] = re.sub('\\n\\t\\r', '', td.text).strip()

            temp = table_data[i].replace(' ', '')
            if temp.isdecimal():
                table_data[i] = int(temp)

        return table_data

    @classmethod
    def _get_regions_data(cls, table_data, dates):
        regions_data = [
            dict(zip(cls._region_fields, chain(dates, table_data[i:i + 5])))
            for i in range(0, len(table_data), 5)
        ]

        return regions_data

    def get_all(self):
        return self._parse_url_list(self.url_list)

    def get_latest(self):
        return self._parse_url_list(self.url_list[:1])
