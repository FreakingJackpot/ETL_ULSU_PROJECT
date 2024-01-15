import re
import logging
from datetime import datetime
from tempfile import TemporaryFile
from functools import partial
from io import BytesIO

import requests
import pandas as pd
from bs4 import BeautifulSoup
from django.conf import settings
from rarfile import RarFile

from apps.etl.utils.logging import get_task_logger

_excluded_regions = (
    'Центральный федеральный округ', 'Южный федеральный округ', 'Уральский федеральный округ',
    'Сибирский федеральный округ', 'Северо-Кавказский федеральный округ',
    'Северо-Западный федеральный округ', 'Приволжский федеральный округ',
    'Дальневосточный федеральный округ'
)


class RosstatParser:
    _media_url_path = '/storage/mediabank/'
    _covid_first_year = 2019
    _rar_xls_filename_template = 'Tabl-01-{}'
    _sheet_names = ('Таб_1', 'Табл. 1',)
    _new_table_style_year = 2021

    def __init__(self, all):
        self.year_url_pairs = self._get_year_url_pairs(all)

    @classmethod
    def _get_year_url_pairs(cls, all):
        urls = []

        response = requests.get(settings.ROSSTAT_URL_POPULATION_PAGE)
        soup = BeautifulSoup(response.text, 'lxml')

        a_with_refs = soup.find_all("a", href=True)
        file_urls = tuple(a['href'] for a in filter(lambda x: cls._media_url_path in x['href'], a_with_refs))

        current_year = datetime.now().year
        years = list(map(lambda x: str(x), range(cls._covid_first_year, current_year + 1)))
        years.sort(reverse=True)

        year_url_pairs = []
        for url in file_urls[:None if all else 1]:  # files descending on site, first is newest
            for year in years:
                if year in url:
                    year_url_pairs.append((int(year), url,))

        return year_url_pairs

    def _parse_url_list(self, year_url_pairs):
        parsed_data = []
        for year, url in year_url_pairs:
            response = requests.get(settings.ROSSTAT_URL_BASE.format(url))
            if 'rar' in url:
                parsed_data.extend(self._get_data_from_rar(response.content, year))
            else:
                parsed_data.extend(self._get_data_from_xlsx(response.content, year))

        self._log_parsed_data(parsed_data)

        return parsed_data

    def _get_data_from_rar(self, bytes, year):
        filename_template = self._rar_xls_filename_template.format(int(year) % 100)
        with TemporaryFile() as tmp:
            tmp.write(bytes)
            with RarFile(tmp, 'r') as rf:
                filename = list(filter(lambda x: filename_template in x, rf.namelist()))[0]
                bytes = rf.read(filename)
                return self._get_data_from_xlsx(bytes, year, True)

    def _get_data_from_xlsx(self, bytes, year, one_sheet=False):
        read_excel = partial(pd.read_excel, BytesIO(bytes), skiprows=4, usecols='A,B', )
        if one_sheet:
            df = read_excel()
        else:
            for sheet_name in self._sheet_names:
                df = read_excel(sheet_name=sheet_name)

                if 'Все население' in df:
                    break
        if year < self._new_table_style_year:
            df.rename(columns={'Таблица 1.': 'region', 'Все население': 'population'}, inplace=True)
            df = df.iloc[1:, :]
        else:
            df.rename(columns={'Unnamed: 0': 'region', 'Все население': 'population'}, inplace=True)

        df['year'] = year
        df.dropna(inplace=True)
        return df.to_dict('records')

    def _log_parsed_data(self, parsed_data):
        logger = get_task_logger()
        for item in parsed_data:
            logger.log(logging.INFO, 'Parsed from rosstat', **item, )

    def get_parsed_data(self):
        return self._parse_url_list(self.year_url_pairs)
