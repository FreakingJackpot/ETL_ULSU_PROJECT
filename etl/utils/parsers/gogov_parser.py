import re
from datetime import datetime
from functools import partial

from django.conf import settings
from bs4 import BeautifulSoup
import requests
from requests.status_codes import codes as status_codes


def convert_int_str_to_int(int_str):
    return int(int_str) if int_str else 0


def convert_str_to_date(date_str, date_format):
    return datetime.strptime(date_str, date_format).date()


class GogovParser:
    _url = settings.GOGOV_URL

    _global_spans = {1: "first_component", 2: "full_vaccinated", 3: "children_vaccinated", 6: "need_revaccination"}
    _global_span_clean_function = partial(re.sub, " |чел.", "")
    _global_date_format = '%d.%m.%y'

    _region_date_format = '%Y-%m-%d'
    # число - номер столбца
    _region_fields = {
        0: {"name": "region", "clean_method": partial(re.sub, "обл.", "область"), },
        1: {"name": "vaccinated", "clean_method": convert_int_str_to_int},
        4: {"name": "avg_people_per_day", "clean_method": convert_int_str_to_int},
        6: {"name": "full_vaccinated", "clean_method": convert_int_str_to_int},
        7: {"name": "revaccinated", "clean_method": convert_int_str_to_int},
        8: {"name": "need_revaccination", "clean_method": convert_int_str_to_int},
        9: {"name": "children_vaccinated", "clean_method": convert_int_str_to_int},
        10: {"name": "date", "clean_method": partial(convert_str_to_date, date_format=_region_date_format)},
    }

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

        regions_tbody = data.find(id='m-table').find("tbody")
        regions_data = cls._get_regions_data(regions_tbody)

        return {"global_data": global_data, "regions_data": regions_data}

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
    def _get_regions_data(cls, regions_tbody):
        regions_data = []
        for tr in regions_tbody.find_all('tr'):
            region_info = {}

            for i, td in enumerate(tr.find_all('td')):
                field_info = cls._region_fields.get(i)
                if field_info:
                    value = td.a.text if field_info['name'] == 'region' else td.attrs.get('data-text')
                    clean_method = field_info.get('clean_method')
                    region_info[field_info['name']] = clean_method(value) if clean_method else value

            regions_data.append(region_info)

        return regions_data

    @classmethod
    def get_data(cls):
        html = cls._get_page_html()
        if html:
            return cls._parse_page(html)
