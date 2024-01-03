import datetime
from copy import deepcopy

from django.test import TestCase

from apps.etl.models import GlobalTransformedData, RegionTransformedData
from apps.etl.utils.mappers.transformed_data_mappers import RegionTransformedDataMapper, GlobalTransformedDataMapper


class TransformedDataMapperBaseTestCase(TestCase):
    """Testing on GlobalTransformedDataMapper, beacause it's already specify attributes"""

    def setUp(self):
        self.data = [
            {'end_date': datetime.date(2020, 1, 6), 'weekly_infected': 0, 'weekly_deaths': 0, 'weekly_recovered': 0.0,
             'weekly_first_component': 0.0, 'weekly_vaccinations': 0.0, 'start_date': datetime.date(2019, 12, 31),
             'weekly_second_component': 0.0, 'recovered': 0.0, 'deaths': 0, 'infected': 0, 'first_component': 0.0,
             'second_component': 0.0, 'weekly_infected_per_100000': 0.0, 'weekly_deaths_per_100000': 0.0,
             'weekly_recovered_per_100000': 0.0, 'infected_per_100000': 0.0, 'deaths_per_100000': 0.0,
             'recovered_per_100000': 0.0, 'weekly_recovered_infected_ratio': None, 'weekly_deaths_infected_ratio': None,
             'vaccinations_population_ratio': 0.0, 'weekly_vaccinations_infected_ratio': None},
            {'end_date': datetime.date(2021, 9, 6), 'weekly_infected': 129303, 'weekly_deaths': 5561,
             'weekly_recovered': 122577.0, 'weekly_first_component': 0.0, 'weekly_vaccinations': 0.0,
             'start_date': datetime.date(2021, 8, 31), 'weekly_second_component': 0.0, 'recovered': 4160210.0,
             'deaths': 187540, 'infected': 7003127, 'first_component': 0.0, 'second_component': 0.0,
             'weekly_infected_per_100000': 88.293120130266, 'weekly_deaths_per_100000': 3.797267202187182,
             'weekly_recovered_per_100000': 83.70034559296857, 'infected_per_100000': 4782.007637088926,
             'deaths_per_100000': 128.05960997989285, 'recovered_per_100000': 2840.7532794841104,
             'weekly_recovered_infected_ratio': 0.9479826454142596, 'weekly_deaths_infected_ratio': 0.04300750949320588,
             'vaccinations_population_ratio': 0.0, 'weekly_vaccinations_infected_ratio': 0.0},
            {'end_date': datetime.date(2021, 9, 13), 'weekly_infected': 127793, 'weekly_deaths': 5478,
             'weekly_recovered': 117119.0, 'weekly_first_component': 0.0, 'weekly_vaccinations': 0.0,
             'start_date': datetime.date(2021, 9, 7), 'weekly_second_component': 0.0, 'recovered': 4277329.0,
             'deaths': 193018, 'infected': 7130920, 'first_component': 0.0, 'second_component': 0.0,
             'weekly_infected_per_100000': 87.26203336973684, 'weekly_deaths_per_100000': 3.7405915723037917,
             'weekly_recovered_per_100000': 79.97341079894994, 'infected_per_100000': 4869.269670458662,
             'deaths_per_100000': 131.80020155219665, 'recovered_per_100000': 2920.72669028306,
             'weekly_recovered_infected_ratio': 0.9164742982792484, 'weekly_deaths_infected_ratio': 0.0428661976790591,
             'vaccinations_population_ratio': 0.0, 'weekly_vaccinations_infected_ratio': 0.0},
            {'end_date': datetime.date(2023, 1, 16), 'weekly_infected': 16986, 'weekly_deaths': 188,
             'weekly_recovered': 18996.0, 'weekly_first_component': 0.0, 'weekly_vaccinations': 0.0,
             'start_date': datetime.date(2023, 1, 10), 'weekly_second_component': 0.0, 'recovered': 19142748.0,
             'deaths': 393859, 'infected': 21819394, 'first_component': 24564150.0, 'second_component': 31945581.0,
             'weekly_infected_per_100000': 11.598701797581636, 'weekly_deaths_per_100000': 0.1283737158804514,
             'weekly_recovered_per_100000': 12.971208015239652, 'infected_per_100000': 14899.131308721413,
             'deaths_per_100000': 268.94225193063147, 'recovered_per_100000': 13071.413260229145,
             'weekly_recovered_infected_ratio': 1.118332744613211, 'weekly_deaths_infected_ratio': 0.011067938302131167,
             'vaccinations_population_ratio': 0.21813685845372058, 'weekly_vaccinations_infected_ratio': 0.0},
        ]

    def test__get_item_key(self):
        mapper = GlobalTransformedDataMapper()

        item_key = mapper._get_item_key(self.data[0])
        self.assertSequenceEqual((self.data[0]['start_date'], self.data[0]['end_date']), item_key)

        item_key = mapper._get_item_key(self.data[1])
        self.assertSequenceEqual((self.data[1]['start_date'], self.data[1]['end_date']), item_key)

        item_key = mapper._get_item_key(self.data[2])
        self.assertSequenceEqual((self.data[2]['start_date'], self.data[2]['end_date']), item_key)

        item_key = mapper._get_item_key(self.data[3])
        self.assertSequenceEqual((self.data[3]['start_date'], self.data[3]['end_date']), item_key)

    def test__update_existing(self):
        existing = GlobalTransformedData.objects.create(**self.data[0])

        def check_fields_equality():
            for field in GlobalTransformedDataMapper._update_fields:
                self.assertEqual(getattr(existing, field), self.data[0][field])

        updated = GlobalTransformedDataMapper()._update_existing(existing, self.data[0])
        self.assertFalse(updated)
        check_fields_equality()

        self.data[0]['weekly_recovered'] = 504
        updated = GlobalTransformedDataMapper()._update_existing(existing, self.data[0])
        self.assertTrue(updated)
        check_fields_equality()

        for field in GlobalTransformedDataMapper._update_fields:
            self.data[0][field] = 1337

        updated = GlobalTransformedDataMapper()._update_existing(existing, self.data[0])
        self.assertTrue(updated)
        check_fields_equality()

    def test__split_data(self):
        get_item_key = lambda item: tuple(
            getattr(item, key) if isinstance(item, GlobalTransformedData) else item[key] for key in
            GlobalTransformedDataMapper._object_key_fields
        )

        existing = GlobalTransformedData.objects.create(**self.data[0])
        existing = GlobalTransformedData.objects.create(**self.data[1])

        self.data[1]['weekly_deaths'] = 435

        insert, update = GlobalTransformedDataMapper()._split_data(self.data)

        insert_keys = tuple(get_item_key(item) for item in insert)
        self.assertSequenceEqual((get_item_key(self.data[2]), get_item_key(self.data[3])), insert_keys)

        update_keys = tuple(get_item_key(item) for item in update)
        self.assertSequenceEqual((get_item_key(self.data[1]),), update_keys)


class GlobalTransformedDataMapperTestCase(TestCase):
    def setUp(self):
        self.data = [
            {'end_date': datetime.date(2020, 1, 6), 'weekly_infected': 0, 'weekly_deaths': 0, 'weekly_recovered': 0.0,
             'weekly_first_component': 0.0, 'weekly_vaccinations': 0.0, 'start_date': datetime.date(2019, 12, 31),
             'weekly_second_component': 0.0, 'recovered': 0.0, 'deaths': 0, 'infected': 0, 'first_component': 0.0,
             'second_component': 0.0, 'weekly_infected_per_100000': 0.0, 'weekly_deaths_per_100000': 0.0,
             'weekly_recovered_per_100000': 0.0, 'infected_per_100000': 0.0, 'deaths_per_100000': 0.0,
             'recovered_per_100000': 0.0, 'weekly_recovered_infected_ratio': None, 'weekly_deaths_infected_ratio': None,
             'vaccinations_population_ratio': 0.0, 'weekly_vaccinations_infected_ratio': None},
            {'end_date': datetime.date(2021, 9, 6), 'weekly_infected': 129303, 'weekly_deaths': 5561,
             'weekly_recovered': 122577.0, 'weekly_first_component': 0.0, 'weekly_vaccinations': 0.0,
             'start_date': datetime.date(2021, 8, 31), 'weekly_second_component': 0.0, 'recovered': 4160210.0,
             'deaths': 187540, 'infected': 7003127, 'first_component': 0.0, 'second_component': 0.0,
             'weekly_infected_per_100000': 88.293120130266, 'weekly_deaths_per_100000': 3.797267202187182,
             'weekly_recovered_per_100000': 83.70034559296857, 'infected_per_100000': 4782.007637088926,
             'deaths_per_100000': 128.05960997989285, 'recovered_per_100000': 2840.7532794841104,
             'weekly_recovered_infected_ratio': 0.9479826454142596, 'weekly_deaths_infected_ratio': 0.04300750949320588,
             'vaccinations_population_ratio': 0.0, 'weekly_vaccinations_infected_ratio': 0.0},
            {'end_date': datetime.date(2021, 9, 13), 'weekly_infected': 127793, 'weekly_deaths': 5478,
             'weekly_recovered': 117119.0, 'weekly_first_component': 0.0, 'weekly_vaccinations': 0.0,
             'start_date': datetime.date(2021, 9, 7), 'weekly_second_component': 0.0, 'recovered': 4277329.0,
             'deaths': 193018, 'infected': 7130920, 'first_component': 0.0, 'second_component': 0.0,
             'weekly_infected_per_100000': 87.26203336973684, 'weekly_deaths_per_100000': 3.7405915723037917,
             'weekly_recovered_per_100000': 79.97341079894994, 'infected_per_100000': 4869.269670458662,
             'deaths_per_100000': 131.80020155219665, 'recovered_per_100000': 2920.72669028306,
             'weekly_recovered_infected_ratio': 0.9164742982792484, 'weekly_deaths_infected_ratio': 0.0428661976790591,
             'vaccinations_population_ratio': 0.0, 'weekly_vaccinations_infected_ratio': 0.0},
            {'end_date': datetime.date(2023, 1, 16), 'weekly_infected': 16986, 'weekly_deaths': 188,
             'weekly_recovered': 18996.0, 'weekly_first_component': 0.0, 'weekly_vaccinations': 0.0,
             'start_date': datetime.date(2023, 1, 10), 'weekly_second_component': 0.0, 'recovered': 19142748.0,
             'deaths': 393859, 'infected': 21819394, 'first_component': 24564150.0, 'second_component': 31945581.0,
             'weekly_infected_per_100000': 11.598701797581636, 'weekly_deaths_per_100000': 0.1283737158804514,
             'weekly_recovered_per_100000': 12.971208015239652, 'infected_per_100000': 14899.131308721413,
             'deaths_per_100000': 268.94225193063147, 'recovered_per_100000': 13071.413260229145,
             'weekly_recovered_infected_ratio': 1.118332744613211, 'weekly_deaths_infected_ratio': 0.011067938302131167,
             'vaccinations_population_ratio': 0.21813685845372058, 'weekly_vaccinations_infected_ratio': 0.0},
        ]

    def test_map_on_empty_db(self):
        GlobalTransformedDataMapper().map(self.data)

        db_data = list(GlobalTransformedData.objects.values().order_by('id'))
        for item in db_data:
            item.pop('id')

        self.assertListEqual(self.data, db_data)

    def test_map_with_updates(self):
        GlobalTransformedData.objects.create(**self.data[0])
        GlobalTransformedData.objects.create(**self.data[1])
        GlobalTransformedData.objects.create(**self.data[2])
        GlobalTransformedData.objects.create(**self.data[3])

        for field in filter(lambda x: x not in ('start_date', 'end_date',), self.data[0].keys()):
            self.data[0][field] = 255
            self.data[1][field] = 366

        GlobalTransformedDataMapper().map(self.data)

        db_data = list(GlobalTransformedData.objects.values().order_by('id'))
        for item in db_data:
            item.pop('id')

        self.assertListEqual(self.data, db_data)


class RegionTransformedDataMapperTestCase(TestCase):
    def setUp(self):
        self.data = [
            {'region': 'Ульяновская обл.', 'end_date': datetime.date(2020, 1, 6), 'weekly_infected': 0,
             'weekly_deaths': 0, 'weekly_recovered': 0.0, 'start_date': datetime.date(2019, 12, 31),
             'recovered': 0.0, 'deaths': 0, 'infected': 0, 'weekly_infected_per_100000': 0.0,
             'weekly_deaths_per_100000': 0.0, 'weekly_recovered_per_100000': 0.0, 'infected_per_100000': 0.0,
             'deaths_per_100000': 0.0, 'recovered_per_100000': 0.0, 'weekly_recovered_infected_ratio': None,
             'weekly_deaths_infected_ratio': None,
             },
            {'region': 'Самарская обл.', 'end_date': datetime.date(2021, 9, 6), 'weekly_infected': 129303,
             'weekly_deaths': 5561, 'weekly_recovered': 122577.0, 'start_date': datetime.date(2021, 8, 31),
             'recovered': 4160210.0, 'deaths': 187540, 'infected': 7003127,
             'weekly_infected_per_100000': 88.293120130266, 'weekly_deaths_per_100000': 3.797267202187182,
             'weekly_recovered_per_100000': 83.70034559296857, 'infected_per_100000': 4782.007637088926,
             'deaths_per_100000': 128.05960997989285, 'recovered_per_100000': 2840.7532794841104,
             'weekly_recovered_infected_ratio': 0.9479826454142596, 'weekly_deaths_infected_ratio': 0.04300750949320588,
             },
            {'region': 'Ульяновская обл.', 'end_date': datetime.date(2021, 9, 13), 'weekly_infected': 127793,
             'weekly_deaths': 5478, 'weekly_recovered': 117119.0,
             'start_date': datetime.date(2021, 9, 7), 'recovered': 4277329.0,
             'deaths': 193018, 'infected': 7130920,
             'weekly_infected_per_100000': 87.26203336973684, 'weekly_deaths_per_100000': 3.7405915723037917,
             'weekly_recovered_per_100000': 79.97341079894994, 'infected_per_100000': 4869.269670458662,
             'deaths_per_100000': 131.80020155219665, 'recovered_per_100000': 2920.72669028306,
             'weekly_recovered_infected_ratio': 0.9164742982792484, 'weekly_deaths_infected_ratio': 0.0428661976790591,
             },
            {'region': 'Ульяновская обл.', 'end_date': datetime.date(2023, 1, 16), 'weekly_infected': 16986,
             'weekly_deaths': 188, 'weekly_recovered': 18996.0, 'start_date': datetime.date(2023, 1, 10),
             'recovered': 19142748.0, 'deaths': 393859, 'infected': 21819394,
             'weekly_infected_per_100000': 11.598701797581636, 'weekly_deaths_per_100000': 0.1283737158804514,
             'weekly_recovered_per_100000': 12.971208015239652, 'infected_per_100000': 14899.131308721413,
             'deaths_per_100000': 268.94225193063147, 'recovered_per_100000': 13071.413260229145,
             'weekly_recovered_infected_ratio': 1.118332744613211, 'weekly_deaths_infected_ratio': 0.011067938302131167
             },
        ]

    def test_map_on_empty_db(self):
        RegionTransformedDataMapper().map(self.data)

        db_data = list(RegionTransformedData.objects.values().order_by('id'))
        for item in db_data:
            item.pop('id')

        self.assertListEqual(self.data, db_data)

    def test_map_with_updates(self):
        RegionTransformedData.objects.create(**self.data[0])
        RegionTransformedData.objects.create(**self.data[1])
        RegionTransformedData.objects.create(**self.data[2])
        RegionTransformedData.objects.create(**self.data[3])

        RegionTransformedDataMapper().map(self.data)

        for field in filter(lambda x: x not in ('start_date', 'end_date', 'region',), self.data[0].keys()):
            self.data[0][field] = 255
            self.data[1][field] = 366

        RegionTransformedDataMapper().map(self.data)

        db_data = list(RegionTransformedData.objects.values().order_by('id'))
        for item in db_data:
            item.pop('id')

        self.assertListEqual(self.data, db_data)
