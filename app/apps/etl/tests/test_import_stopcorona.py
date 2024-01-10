from datetime import datetime
from unittest.mock import patch

from django.test import TestCase

from apps.etl.management.commands.import_stopcorona_data import Command
from apps.etl.models import StopCoronaData
from apps.etl.tests.mocks import LoggerMock


class ImportStopcoronaDataTestCase(TestCase):

    @patch("apps.etl.utils.logging.Logger", LoggerMock)
    def test_upload_to_db_when_new_data(self):
        data = [{
            'start_date': datetime.strptime('23.10.2023', '%d.%m.%Y').date(),
            'end_date': datetime.strptime('29.10.2023', '%d.%m.%Y').date(),
            'region': 'Region 1',
            'hospitalized': 10,
            'recovered': 5,
            'infected': 20,
            'deaths': 2},
            {
                'start_date': datetime.strptime('10.9.2023', '%d.%m.%Y').date(),
                'end_date': datetime.strptime('15.10.2023', '%d.%m.%Y').date(),
                'region': 'Region 1',
                'hospitalized': 11,
                'recovered': 55,
                'infected': 29,
                'deaths': 255},
            {
                'start_date': datetime.strptime('5.9.2023', '%d.%m.%Y').date(),
                'end_date': datetime.strptime('14.9.2023', '%d.%m.%Y').date(),
                'region': 'Region 1',
                'hospitalized': 11,
                'recovered': 55,
                'infected': 29,
                'deaths': 255},
        ]

        count = StopCoronaData.objects.count()
        сommand = Command()
        сommand.upload_to_db(data)
        self.assertEqual(count + 3, StopCoronaData.objects.count())

        uploaded_data = StopCoronaData.objects.filter(region='Region 1').values_list('start_date', 'end_date', 'region')
        expected_data = set([(d['start_date'], d['end_date'], d['region']) for d in data])
        self.assertEqual(set(uploaded_data), expected_data)

        StopCoronaData.objects.filter(region='Region 1').delete()

    @patch("apps.etl.utils.logging.Logger", LoggerMock)
    def test_upload_to_db_no_create_when_dublicates(self):
        StopCoronaData.objects.create(
            start_date='2022-01-01',
            end_date='2022-01-09',
            region='Region 1',
            hospitalized=10,
            infected=100,
            recovered=50,
            deaths=5
        )

        data = [{
            'start_date': datetime.strptime('01.01.2022', '%d.%m.%Y').date(),
            'end_date': datetime.strptime('09.01.2022', '%d.%m.%Y').date(),
            'region': 'Region 1',
            'hospitalized': 10,
            'infected': 100,
            'recovered': 50,
            'deaths': 5}]

        count = StopCoronaData.objects.count()
        сommand = Command()
        сommand.upload_to_db(data)
        self.assertEqual(count, StopCoronaData.objects.count())

        StopCoronaData.objects.filter(region='Region 1').delete()
