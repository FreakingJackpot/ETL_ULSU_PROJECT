import re
from datetime import datetime
from functools import partial

import requests
from bs4 import BeautifulSoup
from django.conf import settings
from requests.status_codes import codes as status_codes


def convert_str_to_date(date_str, date_format):
    return datetime.strptime(date_str, date_format).date()


class GogovParser:
    _url = settings.GOGOV_URL

    _global_spans = {1: "first_component", 2: "full_vaccinated", 3: "children_vaccinated", 6: "need_revaccination"}
    _global_span_clean_function = partial(re.sub, " |чел.", "")
    _global_date_format = '%d.%m.%y'

    @classmethod
    def _get_page_html(cls):
        headers = requests.utils.default_headers()
        headers.update({'User-agent': 'Mozilla/5.0'})
        try:
            response = requests.get(cls._url, headers=headers)
        except ConnectionError as e:
            print(str(e))
            return None

        if response.status_code == status_codes.ok:
            return response.text

        print(f'Gogov responded with status code {response.status_code}')

    @classmethod
    def _parse_page(cls, src):
        soup = BeautifulSoup(src, 'lxml')

        data = soup.find(id="data")
        global_section = data.p
        global_data = cls._parse_global_data(global_section)
        return global_data

    @classmethod
    def _parse_global_data(cls, global_section):
        global_data = {}

        date_str = re.search("\d{1,2}\.\d{1,2}\.\d{1,2}", global_section.text).group()
        global_data['date'] = convert_str_to_date(date_str, cls._global_date_format)

        spans = global_section.find_all("span")
        for i, field_name in cls._global_spans.items():
            global_data[field_name] = int(cls._global_span_clean_function(spans[i].text))
        global_data['revaccinated'] = int(cls._global_span_clean_function(global_section.find("b").text))
        return global_data

    @classmethod
    def get_data(cls):
        html = cls._get_page_html()
        if html:
            return cls._parse_page(html)
