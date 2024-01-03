from datetime import datetime
from unittest.mock import patch

from bs4 import BeautifulSoup
from django.conf import settings
from django.test import TestCase

from apps.api.models import DatasetInfo


class DatasetsInfoViewTestCase(TestCase):
    def setUp(self):
        self.global_info = DatasetInfo.objects.create(
            dataset_name='global_data', description={
                'fields': [
                    {'name': 'start_date', 'type': 'datetime'},
                    {'name': 'end_date', 'type': 'datetime'},
                    {'name': 'deaths', 'type': 'int'},
                    {'name': 'recovered', 'type': 'int'},
                    {'name': 'infected', 'type': 'int'}
                ],
                'description': 'Датасет содержит информацию о всей росии по недельно'}
        )

        self.region_info = DatasetInfo.objects.create(
            dataset_name='region_data', description={
                'fields': [
                    {'name': 'start_date', 'type': 'datetime'},
                    {'name': 'end_date', 'type': 'datetime'},
                    {'name': 'deaths', 'type': 'int'},
                    {'name': 'recovered', 'type': 'int'},
                    {'name': 'infected', 'type': 'int'},
                    {'name': 'region', 'type'},
                ],
                'description': 'Датасет содержит информацию о регионах по недельно'}
        )
