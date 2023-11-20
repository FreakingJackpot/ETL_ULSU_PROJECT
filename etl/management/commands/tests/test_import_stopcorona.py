from unittest import TestCase
from datetime import datetime
from etl.models import StopCoronaData
from etl.management.commands.import_stopcorona_data import Command

class StopParserTestCase(TestCase):
    def test_upload_to_db_(self):
        StopCoronaData.objects.filter(region='Region 1').delete()
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
        self.assertEqual(count+3, StopCoronaData.objects.count())

        uploaded_data = StopCoronaData.objects.filter(region='Region 1').values_list('start_date', 'end_date', 'region')
        expected_data = set([(d['start_date'], d['end_date'], d['region']) for d in data])
        self.assertEqual(set(uploaded_data), expected_data)

        StopCoronaData.objects.filter(region='Region 1').delete()

    def test_upload_to_db_duble(self):
            StopCoronaData.objects.filter(region='Region 1').delete()
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
