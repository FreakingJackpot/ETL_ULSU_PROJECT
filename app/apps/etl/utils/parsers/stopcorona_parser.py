import re
import logging
from datetime import datetime
from itertools import chain
from copy import deepcopy, copy

import requests
from bs4 import BeautifulSoup
from django.conf import settings

from apps.etl.utils.logging import get_task_logger


class StopCoronaParser:
    _url_base = settings.STOPCORONA_URL_BASE
    _url_articles_page = settings.STOPCORONA_URL_ARTICLES_PAGE
    _weekly_article_url_templates = ["v-rossii-za-nedelyu-vyzdorovelo-", "v-rossii-za-nedelyu-vyzdoroveli-"]

    _region_fields = ['start_date', 'end_date', 'region', 'hospitalized', 'recovered', 'infected', 'deaths']
    _date_matching_pattern = r"\d+\.\d+\.\d{4} *[-–] *\d+\.?\d+\.\d{4}|\d+\.?\d+\.? *[-–] *\d+\.?\d+\.\d{4}"
    _date_format = '%d.%m.%Y'

    _excluded_regions = ('Центральный федеральный округ', 'Южный федеральный округ', 'Уральский федеральный округ',
                         'Сибирский федеральный округ', 'Северо-Кавказский федеральный округ',
                         'Северо-Западный федеральный округ', 'Приволжский федеральный округ',
                         'Дальневосточный федеральный округ')

    def __init__(self, all=False):
        self.url_list = self._get_url_list(all)
        self.logger = get_task_logger()

    @classmethod
    def _get_url_list(cls, all):
        urls = []
        page = 1
        search_function = cls._get_all_report_urls_on_page if all else cls._get_first_report_url_on_page
        while page:
            req = requests.get(cls._url_articles_page.format(page))

            src = req.text
            soup = BeautifulSoup(src, 'lxml')

            media_page = soup.find("body")
            material_cards = media_page.find_all("a", class_="u-material-card u-material-cards__card")

            page = None if search_function(material_cards, urls) else page + 1

        return urls

    @classmethod
    def _get_all_report_urls_on_page(cls, material_cards, urls):
        repeated = False
        for card in material_cards:
            href = str(card.get('href'))
            if href in urls:
                repeated = True
                break

            if any(template in href for template in cls._weekly_article_url_templates):
                urls.append(href)

        return repeated

    @classmethod
    def _get_first_report_url_on_page(cls, material_cards, urls):
        got_url = False
        for card in material_cards:
            href = str(card.get('href'))
            if "v-rossii-za-nedelyu-vyzdorovelo-" in href:
                urls.append(href)
                got_url = True
                break

        return got_url

    def _parse_url_list(self, url_list):
        parsed_data = []

        for url in url_list:
            req = requests.get(self._url_base.format(url))

            regions_data = self._parse_page(req.text)
            if regions_data:
                parsed_data.extend(regions_data)

        self._log_parsed_data(parsed_data)
        return parsed_data

    def _parse_page(self, src):
        soup = BeautifulSoup(src, 'html5lib')

        detail__body = soup.find("div", class_="article-detail__body")

        dates = self._get_dates(detail__body)
        if not dates:
            return None

        tbody = detail__body.find("tbody")
        table_data = tbody.find_all("td")

        table_data = self._clean_table_data(table_data)

        regions_data = self._get_regions_data(table_data, dates)

        return regions_data

    def _get_dates(self, detail__body):
        date_text = detail__body.find("h3").text
        matches = re.findall(self._date_matching_pattern, date_text)
        if len(matches) != 1:
            self.logger.log(logging.ERROR, f'Can\'t get date from text. No matching pattern.', date_text=date_text)
            return

        dates = re.split('-|–', matches[0])
        if len(dates) != 2:
            self.logger.log(logging.ERROR, f'Can\'t split dates properly. No matching pattern.', dates=dates)
            return

        dates = list(map(lambda x: x.strip(), dates))
        dates_copy = copy(dates)

        if len(dates[0].split('.')) == 3 and len(dates[0]) > 6:
            dates[0], dates[1] = (datetime.strptime(dates[0], self._date_format).date(),
                                  datetime.strptime(dates[1], self._date_format).date())
        else:
            year = dates[1].split('.')[-1]
            try:
                dates[0] = dates[0][:-1] if dates[0][-1] == '.' else dates[0]
                dates[0] = datetime.strptime(dates[0] + f'.{year}', self._date_format).date()
                dates[1] = datetime.strptime(dates[1], self._date_format).date()
            except ValueError:
                self.logger.log(logging.ERROR, f"Invalid date format: {dates_copy}")
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

        regions_data = list(filter(lambda x: x['region'] not in cls._excluded_regions, regions_data))
        regions_data = list(filter(lambda x: x['region'], regions_data))

        return regions_data

    def _log_parsed_data(self, parsed_data):
        for item in parsed_data:
            start_date = item['start_date'].strftime('%d-%m-%Y')
            end_date = item['end_date'].strftime('%d-%m-%Y')

            log_data = {**item,
                        'start_date': item['start_date'].strftime('%d-%m-%Y'),
                        'end_date': item['end_date'].strftime('%d-%m-%Y')}
            self.logger.log(logging.INFO, 'Parsed from stopcorona', **log_data, )

    def get_parsed_data(self):
        return self._parse_url_list(self.url_list)
