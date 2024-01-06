from calendar import month
from datetime import date
from unittest import mock

from django.test import TestCase
from pandas import DataFrame
from numpy import nan

from apps.etl.utils.data_transformers.transforming_functions import GenericTransformingFunctions
from apps.etl.utils.data_transformers.global_transformers import LegacyGlobalDataTransformer, GlobalDataTransformer
from apps.etl.utils.data_transformers.regional_transformers import LegacyRegionDataTransformer, RegionDataTransformer
from apps.etl.models import ExternalDatabaseStatistic, ExternalDatabaseVaccination, CsvData, StopCoronaData, GogovData, \
    GlobalTransformedData, RegionTransformedData
from apps.etl.tests.mocks import ExternalDatabaseStatisticMock, ExternalDatabaseVaccinationMock
from apps.etl.management.commands.transform_legacy_global_data import Command as TransformLegacyGlobalData
from apps.etl.management.commands.transform_global_data import Command as TransformGlobalData
from apps.etl.management.commands.transform_legacy_region_data import Command as TransformLegacyRegionData
from apps.etl.management.commands.transform_region_data import Command as TransformRegionData


class GenericTransformingFunctionsTestCase(TestCase):

    def setUp(self):
        self.estimated_global_data = DataFrame(
            [
                {
                    'end_date': date(2020, 12, 14),
                    'start_date': date(2020, 12, 8),
                    'weekly_infected': 138919,
                    'weekly_deaths': 2782,
                    'weekly_recovered': 0,
                    'weekly_vaccinations': 0,
                    'weekly_first_component': 0,
                    'weekly_second_component': 0,
                    'recovered': 0,
                    'deaths': 2782,
                    'infected': 138919,
                    'first_component': 0,
                    'second_component': 0,
                    'weekly_infected_per_100000': 94.85929912976823,
                    'weekly_deaths_per_100000': 1.8996578594649778,
                    'weekly_recovered_per_100000': 0,
                    'infected_per_100000': 94.85929912976823,
                    'deaths_per_100000': 1.8996578594649778,
                    'recovered_per_100000': 0,
                    'weekly_recovered_infected_ratio': 0,
                    'weekly_deaths_infected_ratio': 0.0200260583505496,
                    'vaccinations_population_ratio': 0,
                    'weekly_vaccinations_infected_ratio': 0
                },
                {
                    'end_date': date(2020, 12, 21),
                    'start_date': date(2020, 12, 15),
                    'weekly_infected': 33253,
                    'weekly_deaths': 390,
                    'weekly_recovered': 31554,
                    'weekly_vaccinations': 713489,
                    'weekly_first_component': 350683,
                    'weekly_second_component': 362806,
                    'recovered': 31554,
                    'deaths': 3172,
                    'infected': 172172,
                    'first_component': 350683,
                    'second_component': 362806,
                    'weekly_infected_per_100000': 22.70644241581197,
                    'weekly_deaths_per_100000': 0.2663071765605109,
                    'weekly_recovered_per_100000': 21.546299100488106,
                    'infected_per_100000': 117.5657415455802,
                    'deaths_per_100000': 2.1659650360254883,
                    'recovered_per_100000': 21.546299100488106,
                    'weekly_recovered_infected_ratio': 0.9489068655459658,
                    'weekly_deaths_infected_ratio': 0.011728265118936636,
                    'vaccinations_population_ratio': 0.0024773805512618647,
                    'weekly_vaccinations_infected_ratio': 21.456379875499955
                }
            ]
        )

        self.estimated_region_data = DataFrame(
            [
                {
                    "region": "Карелия",
                    "end_date": date(2020, 12, 14),
                    "start_date": date(2020, 12, 8),
                    "weekly_deaths": 3,
                    "weekly_infected": 786,
                    "weekly_recovered": 472,
                    "recovered": 472,
                    "deaths": 3,
                    "infected": 786,
                    "weekly_infected_per_100000": 0.5367113866065681,
                    "weekly_deaths_per_100000": 0.0020485167427731605,
                    "weekly_recovered_per_100000": 0.32229996752964396,
                    "infected_per_100000": 0.5367113866065681,
                    "deaths_per_100000": 0.0020485167427731605,
                    "recovered_per_100000": 0.32229996752964396,
                    "weekly_recovered_infected_ratio": 0.6005089058524173,
                    "weekly_deaths_infected_ratio": 0.003816793893129771
                },
                {
                    "region": "Карелия",
                    "start_date": date(2020, 12, 15),
                    "end_date": date(2020, 12, 21),
                    "weekly_deaths": 11,
                    "weekly_infected": 1750,
                    "weekly_recovered": 2114,
                    "recovered": 2586,
                    "deaths": 14,
                    "infected": 2536,
                    "weekly_infected_per_100000": 1.1949680999510104,
                    "weekly_deaths_per_100000": 0.007511228056834922,
                    "weekly_recovered_per_100000": 1.4435214647408205,
                    "infected_per_100000": 1.7316794865575786,
                    "deaths_per_100000": 0.009559744799608083,
                    "recovered_per_100000": 1.7658214322704646,
                    "weekly_recovered_infected_ratio": 1.208,
                    "weekly_deaths_infected_ratio": 0.006285714285714286
                },
                {
                    "region": "Москва",
                    "start_date": date(2020, 12, 8),
                    "end_date": date(2020, 12, 14),
                    "weekly_deaths": 147,
                    "weekly_infected": 12299,
                    "weekly_recovered": 9773,
                    "recovered": 9773,
                    "deaths": 147,
                    "infected": 12299,
                    "weekly_infected_per_100000": 8.3982358064557,
                    "weekly_deaths_per_100000": 0.10037732039588489,
                    "weekly_recovered_per_100000": 6.6733847090407,
                    "infected_per_100000": 8.3982358064557,
                    "deaths_per_100000": 0.10037732039588489,
                    "recovered_per_100000": 6.6733847090407,
                    "weekly_recovered_infected_ratio": 0.7946174485730547,
                    "weekly_deaths_infected_ratio": 0.01195219123505976
                },
                {
                    "region": "Москва",
                    "start_date": date(2020, 12, 15),
                    "end_date": date(2020, 12, 21),
                    "weekly_deaths": 372,
                    "weekly_infected": 30553,
                    "weekly_recovered": 28401,
                    "recovered": 38174,
                    "deaths": 519,
                    "infected": 42852,
                    "weekly_infected_per_100000": 20.862777347316126,
                    "weekly_deaths_per_100000": 0.25401607610387195,
                    "weekly_recovered_per_100000": 19.39330800383351,
                    "infected_per_100000": 29.261013153771827,
                    "deaths_per_100000": 0.3543933964997568,
                    "recovered_per_100000": 26.06669271287421,
                    "weekly_recovered_infected_ratio": 0.9295650181651556,
                    "weekly_deaths_infected_ratio": 0.01217556377442477
                },
                {
                    "region": "Томская обл.",
                    "start_date": date(2020, 12, 8),
                    "end_date": date(2020, 12, 14),
                    "weekly_deaths": 4,
                    "weekly_infected": 376,
                    "weekly_recovered": 393,
                    "recovered": 393,
                    "deaths": 4,
                    "infected": 376,
                    "weekly_infected_per_100000": 0.2567474317609028,
                    "weekly_deaths_per_100000": 0.0027313556570308806,
                    "weekly_recovered_per_100000": 0.26835569330328407,
                    "infected_per_100000": 0.2567474317609028,
                    "deaths_per_100000": 0.0027313556570308806,
                    "recovered_per_100000": 0.26835569330328407,
                    "weekly_recovered_infected_ratio": 1.0452127659574468,
                    "weekly_deaths_infected_ratio": 0.010638297872340425
                },
                {
                    "region": "Томская обл.",
                    "start_date": date(2020, 12, 15),
                    "end_date": date(2020, 12, 21),
                    "weekly_deaths": 7,
                    "weekly_infected": 950,
                    "weekly_recovered": 1039,
                    "recovered": 1432,
                    "deaths": 11,
                    "infected": 1326,
                    "weekly_infected_per_100000": 0.6486969685448342,
                    "weekly_deaths_per_100000": 0.0047798723998040415,
                    "weekly_recovered_per_100000": 0.7094696319137713,
                    "infected_per_100000": 0.905444400305737,
                    "deaths_per_100000": 0.007511228056834922,
                    "recovered_per_100000": 0.9778253252170553,
                    "weekly_recovered_infected_ratio": 1.0936842105263158,
                    "weekly_deaths_infected_ratio": 0.007368421052631579
                }
            ]
        )

    def test_add_cumulative_stats_global(self):
        df = self.estimated_global_data[
            ['weekly_recovered', 'weekly_deaths', 'weekly_infected', 'weekly_first_component',
             'weekly_second_component']]
        GenericTransformingFunctions.add_cumulative_stats(df)

        fields = ['recovered', 'deaths', 'infected', 'first_component', 'second_component', ]
        self.assertTrue(df[fields].equals(self.estimated_global_data[fields]))

    def test_add_cumulative_stats_region(self):
        df = self.estimated_region_data[
            ['region', 'weekly_recovered', 'weekly_deaths', 'weekly_infected', ]]
        GenericTransformingFunctions.add_cumulative_stats(df, True)

        fields = ['region', 'recovered', 'deaths', 'infected', ]
        self.assertTrue(df[fields].equals(self.estimated_region_data[fields]))

    def test_add_cumulative_stats_fillna(self):
        self.estimated_global_data['weekly_infected'][1] = 0
        self.estimated_global_data['weekly_deaths'][1] = 0
        self.estimated_global_data['weekly_recovered'][1] = 0
        self.estimated_global_data['weekly_first_component'][1] = 0
        self.estimated_global_data['weekly_second_component'][1] = 0

        self.estimated_global_data['recovered'][1] = self.estimated_global_data['recovered'][0]
        self.estimated_global_data['deaths'][1] = self.estimated_global_data['deaths'][0]
        self.estimated_global_data['infected'][1] = self.estimated_global_data['infected'][0]
        self.estimated_global_data['first_component'][1] = self.estimated_global_data['first_component'][0]
        self.estimated_global_data['second_component'][1] = self.estimated_global_data['second_component'][0]

        df = self.estimated_global_data[
            ['weekly_recovered', 'weekly_deaths', 'weekly_infected', 'weekly_first_component',
             'weekly_second_component']]
        GenericTransformingFunctions.add_cumulative_stats(df)

        fields = ['recovered', 'deaths', 'infected', 'first_component', 'second_component', ]
        self.assertTrue(df[fields].equals(self.estimated_global_data[fields]))

    def test_add_per_100000_stats_global(self):
        df = self.estimated_global_data[
            ['weekly_recovered', 'weekly_deaths', 'weekly_infected', 'recovered', 'deaths', 'infected', ]]
        df = GenericTransformingFunctions.add_per_100000_stats(df)

        fields = ['weekly_infected_per_100000', 'weekly_deaths_per_100000', 'weekly_recovered_per_100000',
                  'infected_per_100000', 'deaths_per_100000', 'recovered_per_100000']
        self.assertTrue(df[fields].equals(self.estimated_global_data[fields]))

    def test_add_per_100000_stats_region(self):
        df = self.estimated_region_data[
            ['region', 'weekly_recovered', 'weekly_deaths', 'weekly_infected', 'recovered', 'deaths', 'infected', ]]
        df = GenericTransformingFunctions.add_per_100000_stats(df)

        fields = ['region', 'weekly_infected_per_100000', 'weekly_deaths_per_100000', 'weekly_recovered_per_100000',
                  'infected_per_100000', 'deaths_per_100000', 'recovered_per_100000']
        self.assertTrue(df[fields].equals(self.estimated_region_data[fields]))

    def test_add_ratio_stats_global(self):
        df = self.estimated_global_data[
            ['weekly_recovered', 'weekly_deaths', 'weekly_infected', 'second_component', 'weekly_vaccinations', ]]
        df = GenericTransformingFunctions.add_ratio_stats(df)

        fields = ['weekly_recovered_infected_ratio', 'weekly_deaths_infected_ratio', 'vaccinations_population_ratio',
                  'weekly_vaccinations_infected_ratio', ]
        self.assertTrue(df[fields].equals(self.estimated_global_data[fields]))

    def test_add_ratio_stats_region(self):
        df = self.estimated_region_data[
            ['region', 'weekly_recovered', 'weekly_deaths', 'weekly_infected', ]]
        df = GenericTransformingFunctions.add_ratio_stats(df, True)

        fields = ['region', 'weekly_recovered_infected_ratio', 'weekly_deaths_infected_ratio', ]
        self.assertTrue(df[fields].equals(self.estimated_region_data[fields]))

    def test_add_ratio_stats_with_zero_weekly_infected(self):
        self.estimated_global_data['weekly_infected'][0] = 0
        self.estimated_global_data['weekly_recovered_infected_ratio'][0] = None
        self.estimated_global_data['weekly_deaths_infected_ratio'][0] = None
        self.estimated_global_data['weekly_vaccinations_infected_ratio'][0] = None

        df = self.estimated_global_data[
            ['weekly_recovered', 'weekly_deaths', 'weekly_infected', 'second_component', 'weekly_vaccinations', ]]
        df = GenericTransformingFunctions.add_ratio_stats(df)

        fields = ['weekly_recovered_infected_ratio', 'weekly_deaths_infected_ratio', 'vaccinations_population_ratio',
                  'weekly_vaccinations_infected_ratio', ]
        self.assertTrue(df[fields].equals(self.estimated_global_data[fields]))

    def test_apply_all_transforms_global(self):
        df = self.estimated_global_data[
            ['end_date', 'start_date', 'weekly_recovered', 'weekly_deaths', 'weekly_infected', 'weekly_first_component',
             'weekly_second_component', 'weekly_vaccinations', ]
        ]
        df = GenericTransformingFunctions.apply_all_transforms(df)

        df = df.to_dict('records')
        estimated = self.estimated_global_data.to_dict('records')
        self.assertListEqual(estimated, df)

    def test_apply_all_transforms_region(self):
        df = self.estimated_region_data[
            ['region', 'end_date', 'start_date', 'weekly_recovered', 'weekly_deaths', 'weekly_infected', ]
        ]
        df = GenericTransformingFunctions.apply_all_transforms(df, True)

        df = df.to_dict('records')
        estimated = self.estimated_region_data.to_dict('records')
        self.assertListEqual(estimated, df)


class LegacyGlobalDataTransformerTestCase(TestCase):
    def setUp(self):
        csv_data = [
            {
                "id": 1,
                "date": "2020-12-14",
                "cases": 28080,
                "deaths": 488,
            },
            {
                "id": 2,
                "date": "2020-12-13",
                "cases": 28137,
                "deaths": 560,
            },
            {
                "id": 3,
                "date": "2020-12-12",
                "cases": 28585,
                "deaths": 613,
            },
            {
                "id": 4,
                "date": "2020-12-11",
                "cases": 27927,
                "deaths": 562,
            },
            {
                "id": 5,
                "date": "2020-12-10",
                "cases": 26190,
                "deaths": 559,
            },
        ]
        for item in csv_data:
            CsvData.objects.create(**item)

    @mock.patch('apps.etl.models.ExternalDatabaseStatistic.get_all_transform_data',
                ExternalDatabaseStatisticMock.get_all_transform_data)
    @mock.patch('apps.etl.models.ExternalDatabaseVaccination.get_all_transform_data',
                ExternalDatabaseVaccinationMock.get_all_transform_data)
    def test__summarize_external_data_main(self):
        external_main, _, _ = LegacyGlobalDataTransformer._get_dataframes()
        external_main = LegacyGlobalDataTransformer._summarize_external_data_main(external_main)
        result = external_main.to_dict('records')

        estimated_result = [
            {
                "date": "2020-12-13",
                "death_per_day": 75,
                "infection_per_day": 7031,
                "recovery_per_day": 5348,
            },
            {
                "date": "2020-12-14",
                "death_per_day": 79,
                "infection_per_day": 6430,
                "recovery_per_day": 5290,
            },
            {
                "date": "2020-12-15",
                "death_per_day": 79,
                "infection_per_day": 5937,
                "recovery_per_day": 5954,
            },
            {
                "date": "2020-12-16",
                "death_per_day": 80,
                "infection_per_day": 5558,
                "recovery_per_day": 6204,
            },
            {
                "date": "2020-12-17",
                "death_per_day": 77,
                "infection_per_day": 7237,
                "recovery_per_day": 6408,
            },
            {
                "date": "2020-12-18",
                "death_per_day": 78,
                "infection_per_day": 7475,
                "recovery_per_day": 6455,
            },
            {
                "date": "2020-12-19",
                "death_per_day": 76,
                "infection_per_day": 7046,
                "recovery_per_day": 6533,
            }
        ]

        self.assertListEqual(estimated_result, result)

    @mock.patch('apps.etl.models.ExternalDatabaseStatistic.get_all_transform_data',
                ExternalDatabaseStatisticMock.get_all_transform_data)
    @mock.patch('apps.etl.models.ExternalDatabaseVaccination.get_all_transform_data',
                ExternalDatabaseVaccinationMock.get_all_transform_data)
    def test__merge_all_dfs(self):
        _, vaccinations_data, csv_data = LegacyGlobalDataTransformer._get_dataframes()
        external_main = [
            {
                "date": "2020-12-13",
                "death_per_day": 75,
                "infection_per_day": 7031,
                "recovery_per_day": 5348,
            },
            {
                "date": "2020-12-14",
                "death_per_day": 79,
                "infection_per_day": 6430,
                "recovery_per_day": 5290,
            },
            {
                "date": "2020-12-15",
                "death_per_day": 79,
                "infection_per_day": 5937,
                "recovery_per_day": 5954,
            },
            {
                "date": "2020-12-16",
                "death_per_day": 80,
                "infection_per_day": 5558,
                "recovery_per_day": 6204,
            },
            {
                "date": "2020-12-17",
                "death_per_day": 77,
                "infection_per_day": 7237,
                "recovery_per_day": 6408,
            },
            {
                "date": "2020-12-18",
                "death_per_day": 78,
                "infection_per_day": 7475,
                "recovery_per_day": 6455,
            },
            {
                "date": "2020-12-19",
                "death_per_day": 76,
                "infection_per_day": 7046,
                "recovery_per_day": 6533,
            }
        ]
        external_main = DataFrame(external_main)

        result = LegacyGlobalDataTransformer._merge_all_dfs(external_main, vaccinations_data, csv_data)
        estimated_result = [
            {
                "date": "2020-12-10",
                "infection_per_day": 26190,
                "death_per_day": 559,
                "recovery_per_day": None,
                "daily_vaccinations": None,
                "daily_people_vaccinated": None,
            },
            {
                "date": "2020-12-11",
                "infection_per_day": 27927,
                "death_per_day": 562,
                "recovery_per_day": None,
                "daily_vaccinations": None,
                "daily_people_vaccinated": None,
            },
            {
                "date": "2020-12-12",
                "infection_per_day": 28585,
                "death_per_day": 613,
                "recovery_per_day": None,
                "daily_vaccinations": None,
                "daily_people_vaccinated": None,
            },
            {
                "date": "2020-12-13",
                "infection_per_day": 28137,
                "death_per_day": 560,
                "recovery_per_day": None,
                "daily_vaccinations": None,
                "daily_people_vaccinated": None,
            },
            {
                "date": "2020-12-14",
                "infection_per_day": 28080,
                "death_per_day": 488,
                "recovery_per_day": None,
                "daily_vaccinations": None,
                "daily_people_vaccinated": None,
            },
            {
                "date": "2020-12-15",
                "infection_per_day": 5937,
                "death_per_day": 79,
                "recovery_per_day": 5954,
                "daily_vaccinations": None,
                "daily_people_vaccinated": None,
            },
            {
                "date": "2020-12-16",
                "infection_per_day": 5558,
                "death_per_day": 80,
                "recovery_per_day": 6204,
                "daily_vaccinations": None,
                "daily_people_vaccinated": None,
            },
            {
                "date": "2020-12-17",
                "infection_per_day": 7237,
                "death_per_day": 77,
                "recovery_per_day": 6408,
                "daily_vaccinations": 243233,
                "daily_people_vaccinated": 109873,
            },
            {
                "date": "2020-12-18",
                "infection_per_day": 7475,
                "death_per_day": 78,
                "recovery_per_day": 6455,
                "daily_vaccinations": 223501,
                "daily_people_vaccinated": 111925,
            },
            {
                "date": "2020-12-19",
                "infection_per_day": 7046,
                "death_per_day": 76,
                "recovery_per_day": 6533,
                "daily_vaccinations": 246755,
                "daily_people_vaccinated": 128885,
            }
        ]

        result.replace({nan: None}, inplace=True)
        result['date'] = result['date'].dt.strftime('%Y-%m-%d')
        result = result.to_dict('records')

        self.assertListEqual(estimated_result, result)

    def test__apply_transforms(self):
        data = [
            {
                "date": "2020-12-10",
                "infection_per_day": 26190,
                "death_per_day": 559,
                "recovery_per_day": None,
                "daily_vaccinations": None,
                "daily_people_vaccinated": None,
            },
            {
                "date": "2020-12-11",
                "infection_per_day": 27927,
                "death_per_day": 562,
                "recovery_per_day": None,
                "daily_vaccinations": None,
                "daily_people_vaccinated": None,
            },
            {
                "date": "2020-12-12",
                "infection_per_day": 28585,
                "death_per_day": 613,
                "recovery_per_day": None,
                "daily_vaccinations": None,
                "daily_people_vaccinated": None,
            },
            {
                "date": "2020-12-13",
                "infection_per_day": 28137,
                "death_per_day": 560,
                "recovery_per_day": None,
                "daily_vaccinations": None,
                "daily_people_vaccinated": None,
            },
            {
                "date": "2020-12-14",
                "infection_per_day": 28080,
                "death_per_day": 488,
                "recovery_per_day": None,
                "daily_vaccinations": None,
                "daily_people_vaccinated": None,
            },
            {
                "date": "2020-12-15",
                "infection_per_day": 5937,
                "death_per_day": 79,
                "recovery_per_day": 5954,
                "daily_vaccinations": None,
                "daily_people_vaccinated": None,
            },
            {
                "date": "2020-12-16",
                "infection_per_day": 5558,
                "death_per_day": 80,
                "recovery_per_day": 6204,
                "daily_vaccinations": None,
                "daily_people_vaccinated": None,
            },
            {

                "date": "2020-12-17",
                "infection_per_day": 7237,
                "death_per_day": 77,
                "recovery_per_day": 6408,
                "daily_vaccinations": 243233,
                "daily_people_vaccinated": 109873,
            },
            {
                "date": "2020-12-18",
                "infection_per_day": 7475,
                "death_per_day": 78,
                "recovery_per_day": 6455,
                "daily_vaccinations": 223501,
                "daily_people_vaccinated": 111925,
            },
            {
                "date": "2020-12-19",
                "infection_per_day": 7046,
                "death_per_day": 76,
                "recovery_per_day": 6533,
                "daily_vaccinations": 246755,
                "daily_people_vaccinated": 128885,
            }
        ]
        data = DataFrame(data)

        result = LegacyGlobalDataTransformer._apply_transforms(data)

        estimated_result = [
            {
                'end_date': date(2020, 12, 14),
                'start_date': date(2020, 12, 8),
                'weekly_infected': 138919,
                'weekly_deaths': 2782,
                'weekly_recovered': 0,
                'weekly_vaccinations': 0,
                'weekly_first_component': 0,
                'weekly_second_component': 0,
                'recovered': 0,
                'deaths': 2782,
                'infected': 138919,
                'first_component': 0,
                'second_component': 0,
                'weekly_infected_per_100000': 94.85929912976823,
                'weekly_deaths_per_100000': 1.8996578594649778,
                'weekly_recovered_per_100000': 0,
                'infected_per_100000': 94.85929912976823,
                'deaths_per_100000': 1.8996578594649778,
                'recovered_per_100000': 0,
                'weekly_recovered_infected_ratio': 0,
                'weekly_deaths_infected_ratio': 0.0200260583505496,
                'vaccinations_population_ratio': 0,
                'weekly_vaccinations_infected_ratio': 0
            },
            {
                'end_date': date(2020, 12, 21),
                'start_date': date(2020, 12, 15),
                'weekly_infected': 33253,
                'weekly_deaths': 390,
                'weekly_recovered': 31554,
                'weekly_vaccinations': 713489,
                'weekly_first_component': 350683,
                'weekly_second_component': 362806,
                'recovered': 31554,
                'deaths': 3172,
                'infected': 172172,
                'first_component': 350683,
                'second_component': 362806,
                'weekly_infected_per_100000': 22.70644241581197,
                'weekly_deaths_per_100000': 0.2663071765605109,
                'weekly_recovered_per_100000': 21.546299100488106,
                'infected_per_100000': 117.5657415455802,
                'deaths_per_100000': 2.1659650360254883,
                'recovered_per_100000': 21.546299100488106,
                'weekly_recovered_infected_ratio': 0.9489068655459658,
                'weekly_deaths_infected_ratio': 0.011728265118936636,
                'vaccinations_population_ratio': 0.0024773805512618647,
                'weekly_vaccinations_infected_ratio': 21.456379875499955
            }
        ]

        result.replace({nan: None}, inplace=True)
        result = result.to_dict('records')

        self.assertListEqual(estimated_result, result)

    @mock.patch('apps.etl.models.ExternalDatabaseStatistic.get_all_transform_data',
                ExternalDatabaseStatisticMock.get_all_transform_data)
    @mock.patch('apps.etl.models.ExternalDatabaseVaccination.get_all_transform_data',
                ExternalDatabaseVaccinationMock.get_all_transform_data)
    def test_run(self):
        result = LegacyGlobalDataTransformer.run()

        estimated_result = [
            {
                'end_date': date(2020, 12, 14),
                'start_date': date(2020, 12, 8),
                'weekly_infected': 138919,
                'weekly_deaths': 2782,
                'weekly_recovered': 0,
                'weekly_vaccinations': 0,
                'weekly_first_component': 0,
                'weekly_second_component': 0,
                'recovered': 0,
                'deaths': 2782,
                'infected': 138919,
                'first_component': 0,
                'second_component': 0,
                'weekly_infected_per_100000': 94.85929912976823,
                'weekly_deaths_per_100000': 1.8996578594649778,
                'weekly_recovered_per_100000': 0,
                'infected_per_100000': 94.85929912976823,
                'deaths_per_100000': 1.8996578594649778,
                'recovered_per_100000': 0,
                'weekly_recovered_infected_ratio': 0,
                'weekly_deaths_infected_ratio': 0.0200260583505496,
                'vaccinations_population_ratio': 0,
                'weekly_vaccinations_infected_ratio': 0
            },
            {
                'end_date': date(2020, 12, 21),
                'start_date': date(2020, 12, 15),
                'weekly_infected': 33253,
                'weekly_deaths': 390,
                'weekly_recovered': 31554,
                'weekly_vaccinations': 713489,
                'weekly_first_component': 350683,
                'weekly_second_component': 362806,
                'recovered': 31554,
                'deaths': 3172,
                'infected': 172172,
                'first_component': 350683,
                'second_component': 362806,
                'weekly_infected_per_100000': 22.70644241581197,
                'weekly_deaths_per_100000': 0.2663071765605109,
                'weekly_recovered_per_100000': 21.546299100488106,
                'infected_per_100000': 117.5657415455802,
                'deaths_per_100000': 2.1659650360254883,
                'recovered_per_100000': 21.546299100488106,
                'weekly_recovered_infected_ratio': 0.9489068655459658,
                'weekly_deaths_infected_ratio': 0.011728265118936636,
                'vaccinations_population_ratio': 0.0024773805512618647,
                'weekly_vaccinations_infected_ratio': 21.456379875499955
            }
        ]

        self.assertListEqual(estimated_result, result)


class GlobalDataTransformerTestCase(TestCase):
    def setUp(self):
        self.existing_global_data = [
            {
                'start_date': date(2023, 12, 8),
                'end_date': date(2023, 12, 14),
                'weekly_infected': 138919,
                'weekly_deaths': 2782,
                'weekly_recovered': 0,
                'weekly_vaccinations': 0,
                'weekly_first_component': 0,
                'weekly_second_component': 0,
                'recovered': 0,
                'deaths': 2782,
                'infected': 138919,
                'first_component': 0,
                'second_component': 0,
                'weekly_infected_per_100000': 94.85929912976823,
                'weekly_deaths_per_100000': 1.8996578594649778,
                'weekly_recovered_per_100000': 0,
                'infected_per_100000': 94.85929912976823,
                'deaths_per_100000': 1.8996578594649778,
                'recovered_per_100000': 0,
                'weekly_recovered_infected_ratio': 0,
                'weekly_deaths_infected_ratio': 0.0200260583505496,
                'vaccinations_population_ratio': 0,
                'weekly_vaccinations_infected_ratio': 0
            },
            {
                'start_date': date(2023, 12, 15),
                'end_date': date(2023, 12, 21),
                'weekly_infected': 33253,
                'weekly_deaths': 390,
                'weekly_recovered': 31554,
                'weekly_vaccinations': 713489,
                'weekly_first_component': 350683,
                'weekly_second_component': 362806,
                'recovered': 31554,
                'deaths': 3172,
                'infected': None,
                'first_component': 350683,
                'second_component': 362806,
                'weekly_infected_per_100000': 22.70644241581197,
                'weekly_deaths_per_100000': 0.2663071765605109,
                'weekly_recovered_per_100000': 21.546299100488106,
                'infected_per_100000': 117.5657415455802,
                'deaths_per_100000': 2.1659650360254883,
                'recovered_per_100000': 21.546299100488106,
                'weekly_recovered_infected_ratio': 0.9489068655459658,
                'weekly_deaths_infected_ratio': 0.011728265118936636,
                'vaccinations_population_ratio': 0.0024773805512618647,
                'weekly_vaccinations_infected_ratio': 21.456379875499955
            }
        ]
        for item in self.existing_global_data:
            GlobalTransformedData.objects.create(**item)

        StopCoronaData.objects.create(
            start_date=date(2023, 12, 22),
            end_date=date(2023, 12, 28),
            region=StopCoronaData.RUSSIAN_FEDERATION,
            hospitalized=5000,
            infected=200,
            recovered=100,
            deaths=12,
        )
        StopCoronaData.objects.create(
            start_date=date(2023, 12, 29),
            end_date=date(2024, 1, 4),
            region=StopCoronaData.RUSSIAN_FEDERATION,
            hospitalized=5000,
            infected=200,
            recovered=100,
            deaths=12,
        )

        StopCoronaData.objects.create(
            start_date=date(2023, 12, 22),
            end_date=date(2023, 12, 28),
            region='Ульяновская обл.',
            hospitalized=5001,
            infected=201,
            recovered=101,
            deaths=10,
        )
        StopCoronaData.objects.create(
            start_date=date(2023, 12, 29),
            end_date=date(2024, 1, 4),
            region='Ульяновская обл.',
            hospitalized=5001,
            infected=201,
            recovered=101,
            deaths=10,
        )

        GogovData.objects.create(
            date=date(2023, 12, 22),
            first_component=350685,
            second_component=362810,
        )

        GogovData.objects.create(
            date=date(2023, 12, 23),
            first_component=350687,
            second_component=362812,
        )

        GogovData.objects.create(
            date=date(2023, 12, 24),
            first_component=350688,
            second_component=362815,
        )

        GogovData.objects.create(
            date=date(2023, 12, 29),
            first_component=350690,
            second_component=362820,
        )

        GogovData.objects.create(
            date=date(2023, 12, 30),
            first_component=350690,
            second_component=362821,
        )

    def test__get_dataframes(self):
        stopcorona_data, gogov_data = GlobalDataTransformer()._get_dataframes()
        stopcorona_data = stopcorona_data.to_dict('records')
        gogov_data = gogov_data.to_dict('records')

        estimated_stopcorona = [
            {
                'start_date': date(2023, 12, 22),
                'end_date': date(2023, 12, 28),
                'infected': 200,
                'recovered': 100,
                'deaths': 12
            },
            {
                'start_date': date(2023, 12, 29),
                'end_date': date(2024, 1, 4),
                'infected': 200,
                'recovered': 100,
                'deaths': 12
            }
        ]
        self.assertListEqual(estimated_stopcorona, stopcorona_data)

        estimated_gogov = [
            {'date': date(2023, 12, 22), 'first_component': 350685, 'second_component': 362810},
            {'date': date(2023, 12, 23), 'first_component': 350687, 'second_component': 362812},
            {'date': date(2023, 12, 24), 'first_component': 350688, 'second_component': 362815},
            {'date': date(2023, 12, 29), 'first_component': 350690, 'second_component': 362820},
            {'date': date(2023, 12, 30), 'first_component': 350690, 'second_component': 362821}
        ]
        self.assertListEqual(estimated_gogov, gogov_data)

    def test__get_dataframes_latest(self):
        stopcorona_data, gogov_data = GlobalDataTransformer(True)._get_dataframes()
        stopcorona_data = stopcorona_data.to_dict('records')
        gogov_data = gogov_data.to_dict('records')

        estimated_stopcorona = [
            {
                'start_date': date(2023, 12, 29),
                'end_date': date(2024, 1, 4),
                'infected': 200,
                'recovered': 100,
                'deaths': 12
            }
        ]
        self.assertListEqual(estimated_stopcorona, stopcorona_data)

        estimated_gogov = [
            {'date': date(2023, 12, 29), 'first_component': 350690, 'second_component': 362820},
            {'date': date(2023, 12, 30), 'first_component': 350690, 'second_component': 362821}
        ]
        self.assertListEqual(estimated_gogov, gogov_data)

    def test__transform_gogov_data(self):
        # When using latest, initial daily values computes from latest in database, otherwise they equals to None
        stopcorona_data = [
            {
                'start_date': date(2023, 12, 22),
                'end_date': date(2023, 12, 28),
                'infected': 200,
                'recovered': 100,
                'deaths': 12
            },
            {
                'start_date': date(2023, 12, 29),
                'end_date': date(2024, 1, 4),
                'infected': 200,
                'recovered': 100,
                'deaths': 12
            }
        ]
        gogov_data = [
            {'date': date(2023, 12, 22), 'first_component': 350685, 'second_component': 362810},
            {'date': date(2023, 12, 23), 'first_component': 350687, 'second_component': 362812},
            {'date': date(2023, 12, 24), 'first_component': 350688, 'second_component': 362815},
            {'date': date(2023, 12, 29), 'first_component': 350690, 'second_component': 362820},
            {'date': date(2023, 12, 30), 'first_component': 350690, 'second_component': 362821}
        ]

        stopcorona_data = DataFrame(stopcorona_data)
        gogov_data = DataFrame(gogov_data)

        transformed_gogov_data = GlobalDataTransformer()._transform_gogov_data(gogov_data, stopcorona_data)
        transformed_gogov_data = transformed_gogov_data.to_dict('records')

        estimated_data = [
            {
                'start_date': date(2023, 12, 22),
                'end_date': date(2023, 12, 28),
                'weekly_vaccinations': 8,
                'weekly_first_component': 3,
                'weekly_second_component': 5,
                'first_component': 350688,
                'second_component': 362815,
            },
            {
                'start_date': date(2023, 12, 29),
                'end_date': date(2024, 1, 4),
                'weekly_vaccinations': 8,
                'weekly_first_component': 2,
                'weekly_second_component': 6,
                'first_component': 350690,
                'second_component': 362821,
            }
        ]
        self.assertListEqual(estimated_data, transformed_gogov_data)

    def test__transform_gogov_data_latest(self):
        stopcorona_data = [
            {
                'start_date': date(2023, 12, 29),
                'end_date': date(2024, 1, 4),
                'infected': 200,
                'recovered': 100,
                'deaths': 12
            }
        ]
        gogov_data = [
            {'date': date(2023, 12, 29), 'first_component': 350690, 'second_component': 362820},
            {'date': date(2023, 12, 30), 'first_component': 350690, 'second_component': 362821}
        ]

        stopcorona_data = DataFrame(stopcorona_data)
        gogov_data = DataFrame(gogov_data)

        transformed_gogov_data = GlobalDataTransformer(True)._transform_gogov_data(gogov_data, stopcorona_data)
        transformed_gogov_data = transformed_gogov_data.to_dict('records')

        estimated_data = [
            {
                'start_date': date(2023, 12, 29),
                'end_date': date(2024, 1, 4),
                'weekly_first_component': 7,
                'weekly_second_component': 15,
                'weekly_vaccinations': 22,
                'first_component': 350690,
                'second_component': 362821,
            },
        ]
        self.assertListEqual(estimated_data, transformed_gogov_data)

    def test__prepare_transformed_data(self):
        stopcorona_data = [
            {
                'start_date': date(2023, 12, 22),
                'end_date': date(2023, 12, 28),
                'weekly_infected': 200,
                'weekly_recovered': 100,
                'weekly_deaths': 12
            },
            {
                'start_date': date(2023, 12, 29),
                'end_date': date(2024, 1, 4),
                'weekly_infected': 200,
                'weekly_recovered': 100,
                'weekly_deaths': 12
            },
        ]
        gogov_data = [
            {
                'start_date': date(2023, 12, 22),
                'end_date': date(2023, 12, 28),
                'weekly_vaccinations': 8,
                'weekly_first_component': 3,
                'weekly_second_component': 5,
                'first_component': 350688,
                'second_component': 362815,
            },
            {
                'start_date': date(2023, 12, 29),
                'end_date': date(2024, 1, 4),
                'weekly_vaccinations': 8,
                'weekly_first_component': 2,
                'weekly_second_component': 6,
                'first_component': 350690,
                'second_component': 362821,
            },
        ]

        stopcorona_data = DataFrame(stopcorona_data)
        gogov_data = DataFrame(gogov_data)

        transformed_data = GlobalDataTransformer()._prepare_transformed_data(gogov_data, stopcorona_data)
        transformed_data = transformed_data.to_dict('records')

        estimated_data = [
            {
                'start_date': date(2023, 12, 22),
                'end_date': date(2023, 12, 28),
                'weekly_vaccinations': 8,
                'weekly_first_component': 3,
                'weekly_second_component': 5,
                'first_component': 350688,
                'second_component': 362815,
                'weekly_infected': 200,
                'weekly_recovered': 100,
                'weekly_deaths': 12,
                'infected': 139119,
                'recovered': 31654,
                'deaths': 3184,
                'weekly_infected_per_100000': 0.13656778285154406,
                'weekly_deaths_per_100000': 0.008194066971092642,
                'weekly_recovered_per_100000': 0.06828389142577203,
                'infected_per_100000': 94.99586691261977,
                'deaths_per_100000': 2.174159102996581,
                'recovered_per_100000': 21.614582991913874,
                'weekly_recovered_infected_ratio': 0.5,
                'weekly_deaths_infected_ratio': 0.06,
                'vaccinations_population_ratio': 0.0024774420067641477,
                'weekly_vaccinations_infected_ratio': 0.04
            },
            {
                'start_date': date(2023, 12, 29),
                'end_date': date(2024, 1, 4),
                'weekly_vaccinations': 8,
                'weekly_first_component': 2,
                'weekly_second_component': 6,
                'first_component': 350690,
                'second_component': 362821,
                'weekly_infected': 200,
                'weekly_recovered': 100,
                'weekly_deaths': 12,
                'infected': 139319,
                'recovered': 31754,
                'deaths': 3196,
                'weekly_infected_per_100000': 0.13656778285154406,
                'weekly_deaths_per_100000': 0.008194066971092642,
                'weekly_recovered_per_100000': 0.06828389142577203,
                'infected_per_100000': 95.13243469547133,
                'deaths_per_100000': 2.182353169967674,
                'recovered_per_100000': 21.68286688333965,
                'weekly_recovered_infected_ratio': 0.5,
                'weekly_deaths_infected_ratio': 0.06,
                'vaccinations_population_ratio': 0.0024774829770990033,
                'weekly_vaccinations_infected_ratio': 0.04
            }
        ]
        self.assertListEqual(estimated_data, transformed_data)

    def test__prepare_transformed_data_latest(self):
        stopcorona_data = [
            {
                'start_date': date(2023, 12, 29),
                'end_date': date(2024, 1, 4),
                'weekly_infected': 200,
                'weekly_recovered': 100,
                'weekly_deaths': 12
            },
        ]
        gogov_data = [
            {
                'start_date': date(2023, 12, 29),
                'end_date': date(2024, 1, 4),
                'weekly_first_component': 7,
                'weekly_second_component': 15,
                'weekly_vaccinations': 22,
                'first_component': 350690,
                'second_component': 362821,
            }
        ]

        stopcorona_data = DataFrame(stopcorona_data)
        gogov_data = DataFrame(gogov_data)

        transformed_data = GlobalDataTransformer()._prepare_transformed_data(gogov_data, stopcorona_data)
        transformed_data = transformed_data.to_dict('records')

        estimated_data = [
            {
                'start_date': date(2023, 12, 29),
                'end_date': date(2024, 1, 4),
                'weekly_first_component': 7,
                'weekly_second_component': 15,
                'weekly_vaccinations': 22,
                'first_component': 350690,
                'second_component': 362821,
                'weekly_infected': 200,
                'weekly_recovered': 100,
                'weekly_deaths': 12,
                'infected': 139119,
                'recovered': 31654,
                'deaths': 3184,
                'deaths_per_100000': 2.174159102996581,
                'infected_per_100000': 94.99586691261977,
                'recovered_per_100000': 21.614582991913874,
                'weekly_deaths_per_100000': 0.008194066971092642,
                'weekly_infected_per_100000': 0.13656778285154406,
                'weekly_recovered_per_100000': 0.06828389142577203,
                'vaccinations_population_ratio': 0.0024774829770990033,
                'weekly_deaths_infected_ratio': 0.06,
                'weekly_recovered_infected_ratio': 0.5,
                'weekly_vaccinations_infected_ratio': 0.11
            }
        ]
        self.assertListEqual(estimated_data, transformed_data)

    def test_run(self):
        data = GlobalDataTransformer().run()

        estimated_data = [
            {
                'start_date': date(2023, 12, 22),
                'end_date': date(2023, 12, 28),
                'weekly_vaccinations': 8,
                'weekly_first_component': 3,
                'weekly_second_component': 5,
                'first_component': 350688,
                'second_component': 362815,
                'weekly_infected': 200,
                'weekly_recovered': 100,
                'weekly_deaths': 12,
                'infected': 139119,
                'recovered': 31654,
                'deaths': 3184,
                'weekly_infected_per_100000': 0.13656778285154406,
                'weekly_deaths_per_100000': 0.008194066971092642,
                'weekly_recovered_per_100000': 0.06828389142577203,
                'infected_per_100000': 94.99586691261977,
                'deaths_per_100000': 2.174159102996581,
                'recovered_per_100000': 21.614582991913874,
                'weekly_recovered_infected_ratio': 0.5,
                'weekly_deaths_infected_ratio': 0.06,
                'vaccinations_population_ratio': 0.0024774420067641477,
                'weekly_vaccinations_infected_ratio': 0.04
            },
            {
                'start_date': date(2023, 12, 29),
                'end_date': date(2024, 1, 4),
                'weekly_vaccinations': 8,
                'weekly_first_component': 2,
                'weekly_second_component': 6,
                'first_component': 350690,
                'second_component': 362821,
                'weekly_infected': 200,
                'weekly_recovered': 100,
                'weekly_deaths': 12,
                'infected': 139319,
                'recovered': 31754,
                'deaths': 3196,
                'weekly_infected_per_100000': 0.13656778285154406,
                'weekly_deaths_per_100000': 0.008194066971092642,
                'weekly_recovered_per_100000': 0.06828389142577203,
                'infected_per_100000': 95.13243469547133,
                'deaths_per_100000': 2.182353169967674,
                'recovered_per_100000': 21.68286688333965,
                'weekly_recovered_infected_ratio': 0.5,
                'weekly_deaths_infected_ratio': 0.06,
                'vaccinations_population_ratio': 0.0024774829770990033,
                'weekly_vaccinations_infected_ratio': 0.04
            }
        ]
        self.assertListEqual(estimated_data, data)

    def test_run_latest(self):
        data = GlobalDataTransformer(True).run()

        estimated_data = [
            {
                'start_date': date(2023, 12, 29),
                'end_date': date(2024, 1, 4),
                'weekly_first_component': 7,
                'weekly_second_component': 15,
                'weekly_vaccinations': 22,
                'first_component': 350690,
                'second_component': 362821,
                'weekly_infected': 200,
                'weekly_recovered': 100,
                'weekly_deaths': 12,
                'infected': 139119,
                'recovered': 31654,
                'deaths': 3184,
                'deaths_per_100000': 2.174159102996581,
                'infected_per_100000': 94.99586691261977,
                'recovered_per_100000': 21.614582991913874,
                'weekly_deaths_per_100000': 0.008194066971092642,
                'weekly_infected_per_100000': 0.13656778285154406,
                'weekly_recovered_per_100000': 0.06828389142577203,
                'vaccinations_population_ratio': 0.0024774829770990033,
                'weekly_deaths_infected_ratio': 0.06,
                'weekly_recovered_infected_ratio': 0.5,
                'weekly_vaccinations_infected_ratio': 0.11
            }
        ]
        self.assertListEqual(estimated_data, data)


class LegacyRegionDataTransformerTestCase(TestCase):
    @mock.patch('apps.etl.models.ExternalDatabaseStatistic.get_all_transform_data',
                ExternalDatabaseStatisticMock.get_all_transform_data)
    def test_run(self):
        data = LegacyRegionDataTransformer._get_dataframe()
        result = LegacyRegionDataTransformer.run()
        estimated_result = [
            {
                "region": "Карелия",
                "end_date": date(2020, 12, 14),
                "start_date": date(2020, 12, 8),
                "weekly_deaths": 3,
                "weekly_infected": 786,
                "weekly_recovered": 472,
                "recovered": 472,
                "deaths": 3,
                "infected": 786,
                "weekly_infected_per_100000": 0.5367113866065681,
                "weekly_deaths_per_100000": 0.0020485167427731605,
                "weekly_recovered_per_100000": 0.32229996752964396,
                "infected_per_100000": 0.5367113866065681,
                "deaths_per_100000": 0.0020485167427731605,
                "recovered_per_100000": 0.32229996752964396,
                "weekly_recovered_infected_ratio": 0.6005089058524173,
                "weekly_deaths_infected_ratio": 0.003816793893129771
            },
            {
                "region": "Карелия",
                "start_date": date(2020, 12, 15),
                "end_date": date(2020, 12, 21),
                "weekly_deaths": 11,
                "weekly_infected": 1750,
                "weekly_recovered": 2114,
                "recovered": 2586,
                "deaths": 14,
                "infected": 2536,
                "weekly_infected_per_100000": 1.1949680999510104,
                "weekly_deaths_per_100000": 0.007511228056834922,
                "weekly_recovered_per_100000": 1.4435214647408205,
                "infected_per_100000": 1.7316794865575786,
                "deaths_per_100000": 0.009559744799608083,
                "recovered_per_100000": 1.7658214322704646,
                "weekly_recovered_infected_ratio": 1.208,
                "weekly_deaths_infected_ratio": 0.006285714285714286
            },
            {
                "region": "Москва",
                "start_date": date(2020, 12, 8),
                "end_date": date(2020, 12, 14),
                "weekly_deaths": 147,
                "weekly_infected": 12299,
                "weekly_recovered": 9773,
                "recovered": 9773,
                "deaths": 147,
                "infected": 12299,
                "weekly_infected_per_100000": 8.3982358064557,
                "weekly_deaths_per_100000": 0.10037732039588489,
                "weekly_recovered_per_100000": 6.6733847090407,
                "infected_per_100000": 8.3982358064557,
                "deaths_per_100000": 0.10037732039588489,
                "recovered_per_100000": 6.6733847090407,
                "weekly_recovered_infected_ratio": 0.7946174485730547,
                "weekly_deaths_infected_ratio": 0.01195219123505976
            },
            {
                "region": "Москва",
                "start_date": date(2020, 12, 15),
                "end_date": date(2020, 12, 21),
                "weekly_deaths": 372,
                "weekly_infected": 30553,
                "weekly_recovered": 28401,
                "recovered": 38174,
                "deaths": 519,
                "infected": 42852,
                "weekly_infected_per_100000": 20.862777347316126,
                "weekly_deaths_per_100000": 0.25401607610387195,
                "weekly_recovered_per_100000": 19.39330800383351,
                "infected_per_100000": 29.261013153771827,
                "deaths_per_100000": 0.3543933964997568,
                "recovered_per_100000": 26.06669271287421,
                "weekly_recovered_infected_ratio": 0.9295650181651556,
                "weekly_deaths_infected_ratio": 0.01217556377442477
            },
            {
                "region": "Томская обл.",
                "start_date": date(2020, 12, 8),
                "end_date": date(2020, 12, 14),
                "weekly_deaths": 4,
                "weekly_infected": 376,
                "weekly_recovered": 393,
                "recovered": 393,
                "deaths": 4,
                "infected": 376,
                "weekly_infected_per_100000": 0.2567474317609028,
                "weekly_deaths_per_100000": 0.0027313556570308806,
                "weekly_recovered_per_100000": 0.26835569330328407,
                "infected_per_100000": 0.2567474317609028,
                "deaths_per_100000": 0.0027313556570308806,
                "recovered_per_100000": 0.26835569330328407,
                "weekly_recovered_infected_ratio": 1.0452127659574468,
                "weekly_deaths_infected_ratio": 0.010638297872340425
            },
            {
                "region": "Томская обл.",
                "start_date": date(2020, 12, 15),
                "end_date": date(2020, 12, 21),
                "weekly_deaths": 7,
                "weekly_infected": 950,
                "weekly_recovered": 1039,
                "recovered": 1432,
                "deaths": 11,
                "infected": 1326,
                "weekly_infected_per_100000": 0.6486969685448342,
                "weekly_deaths_per_100000": 0.0047798723998040415,
                "weekly_recovered_per_100000": 0.7094696319137713,
                "infected_per_100000": 0.905444400305737,
                "deaths_per_100000": 0.007511228056834922,
                "recovered_per_100000": 0.9778253252170553,
                "weekly_recovered_infected_ratio": 1.0936842105263158,
                "weekly_deaths_infected_ratio": 0.007368421052631579
            },
        ]
        self.assertListEqual(estimated_result, result)


class RegionDataTransformerTestCase(TestCase):
    def setUp(self):
        self.existing_region_data = [
            {
                "region": "Карелия",
                "start_date": date(2023, 12, 8),
                "end_date": date(2023, 12, 14),
                "weekly_deaths": 3,
                "weekly_infected": 786,
                "weekly_recovered": 472,
                "recovered": 472,
                "deaths": 3,
                "infected": 786,
                "weekly_infected_per_100000": 0.5367113866065681,
                "weekly_deaths_per_100000": 0.0020485167427731605,
                "weekly_recovered_per_100000": 0.32229996752964396,
                "infected_per_100000": 0.5367113866065681,
                "deaths_per_100000": 0.0020485167427731605,
                "recovered_per_100000": 0.32229996752964396,
                "weekly_recovered_infected_ratio": 0.6005089058524173,
                "weekly_deaths_infected_ratio": 0.003816793893129771
            },
            {
                "region": "Карелия",
                "start_date": date(2023, 12, 15),
                "end_date": date(2023, 12, 21),
                "weekly_deaths": 11,
                "weekly_infected": 1750,
                "weekly_recovered": 2114,
                "recovered": 2586,
                "deaths": 14,
                "infected": 2536,
                "weekly_infected_per_100000": 1.1949680999510104,
                "weekly_deaths_per_100000": 0.007511228056834922,
                "weekly_recovered_per_100000": 1.4435214647408205,
                "infected_per_100000": 1.7316794865575786,
                "deaths_per_100000": 0.009559744799608083,
                "recovered_per_100000": 1.7658214322704646,
                "weekly_recovered_infected_ratio": 1.208,
                "weekly_deaths_infected_ratio": 0.006285714285714286
            },
            {
                "region": "Московская обл.",
                "start_date": date(2023, 12, 8),
                "end_date": date(2023, 12, 14),
                "weekly_deaths": 147,
                "weekly_infected": 12299,
                "weekly_recovered": 9773,
                "recovered": 9773,
                "deaths": 147,
                "infected": 12299,
                "weekly_infected_per_100000": 8.3982358064557,
                "weekly_deaths_per_100000": 0.10037732039588489,
                "weekly_recovered_per_100000": 6.6733847090407,
                "infected_per_100000": 8.3982358064557,
                "deaths_per_100000": 0.10037732039588489,
                "recovered_per_100000": 6.6733847090407,
                "weekly_recovered_infected_ratio": 0.7946174485730547,
                "weekly_deaths_infected_ratio": 0.01195219123505976
            },
            {
                "region": "Московская обл.",
                "start_date": date(2023, 12, 15),
                "end_date": date(2023, 12, 21),
                "weekly_deaths": 372,
                "weekly_infected": 30553,
                "weekly_recovered": 28401,
                "recovered": 38174,
                "deaths": 519,
                "infected": 42852,
                "weekly_infected_per_100000": 20.862777347316126,
                "weekly_deaths_per_100000": 0.25401607610387195,
                "weekly_recovered_per_100000": 19.39330800383351,
                "infected_per_100000": 29.261013153771827,
                "deaths_per_100000": 0.3543933964997568,
                "recovered_per_100000": 26.06669271287421,
                "weekly_recovered_infected_ratio": 0.9295650181651556,
                "weekly_deaths_infected_ratio": 0.01217556377442477
            },
            {
                "region": "Томская обл.",
                "start_date": date(2023, 12, 8),
                "end_date": date(2023, 12, 14),
                "weekly_deaths": 4,
                "weekly_infected": 376,
                "weekly_recovered": 393,
                "recovered": 393,
                "deaths": 4,
                "infected": 376,
                "weekly_infected_per_100000": 0.2567474317609028,
                "weekly_deaths_per_100000": 0.0027313556570308806,
                "weekly_recovered_per_100000": 0.26835569330328407,
                "infected_per_100000": 0.2567474317609028,
                "deaths_per_100000": 0.0027313556570308806,
                "recovered_per_100000": 0.26835569330328407,
                "weekly_recovered_infected_ratio": 1.0452127659574468,
                "weekly_deaths_infected_ratio": 0.010638297872340425
            },
            {
                "region": "Томская обл.",
                "start_date": date(2023, 12, 15),
                "end_date": date(2023, 12, 21),
                "weekly_deaths": 7,
                "weekly_infected": 950,
                "weekly_recovered": 1039,
                "recovered": 1432,
                "deaths": 11,
                "infected": 1326,
                "weekly_infected_per_100000": 0.6486969685448342,
                "weekly_deaths_per_100000": 0.0047798723998040415,
                "weekly_recovered_per_100000": 0.7094696319137713,
                "infected_per_100000": 0.905444400305737,
                "deaths_per_100000": 0.007511228056834922,
                "recovered_per_100000": 0.9778253252170553,
                "weekly_recovered_infected_ratio": 1.0936842105263158,
                "weekly_deaths_infected_ratio": 0.007368421052631579
            }
        ]
        for item in self.existing_region_data:
            RegionTransformedData.objects.create(**item)

        StopCoronaData.objects.create(
            start_date=date(2023, 12, 22),
            end_date=date(2023, 12, 28),
            region=StopCoronaData.RUSSIAN_FEDERATION,
            hospitalized=5000,
            infected=200,
            recovered=100,
            deaths=12,
        )
        StopCoronaData.objects.create(
            start_date=date(2023, 12, 29),
            end_date=date(2024, 1, 4),
            region=StopCoronaData.RUSSIAN_FEDERATION,
            hospitalized=5000,
            infected=200,
            recovered=100,
            deaths=12,
        )

        StopCoronaData.objects.create(
            start_date=date(2023, 12, 22),
            end_date=date(2023, 12, 28),
            region='Республика Карелия',
            hospitalized=1,
            infected=30,
            recovered=32,
            deaths=1,
        )
        StopCoronaData.objects.create(
            start_date=date(2023, 12, 29),
            end_date=date(2024, 1, 4),
            region='Республика Карелия',
            hospitalized=20,
            infected=21,
            recovered=31,
            deaths=0,
        )

        StopCoronaData.objects.create(
            start_date=date(2023, 12, 29),
            end_date=date(2024, 1, 4),
            region='Московская область',
            hospitalized=201,
            infected=32,
            recovered=60,
            deaths=2,
        )

        StopCoronaData.objects.create(
            start_date=date(2023, 12, 29),
            end_date=date(2024, 1, 4),
            region='Томская область',
            hospitalized=1,
            infected=1,
            recovered=30,
            deaths=0,
        )

    def test_run(self):
        data = RegionDataTransformer().run()

        estimated_data = [
            {
                "start_date": date(2023, 12, 22),
                "end_date": date(2023, 12, 28),
                "region": "Карелия",
                "weekly_infected": 30,
                "weekly_recovered": 32,
                "weekly_deaths": 1,
                "infected": 2566,
                "recovered": 2618,
                "deaths": 15,
                "weekly_infected_per_100000": 0.020485167427731606,
                "weekly_deaths_per_100000": 0.0006828389142577202,
                "weekly_recovered_per_100000": 0.021850845256247045,
                "infected_per_100000": 1.7521646539853102,
                "deaths_per_100000": 0.010242583713865803,
                "recovered_per_100000": 1.7876722775267118,
                "weekly_recovered_infected_ratio": 1.0666666666666667,
                "weekly_deaths_infected_ratio": 0.03333333333333333
            },
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": "Карелия",
                "weekly_infected": 21,
                "weekly_recovered": 31,
                "weekly_deaths": 0,
                "infected": 2587,
                "recovered": 2649,
                "deaths": 15,
                "weekly_infected_per_100000": 0.014339617199412126,
                "weekly_deaths_per_100000": 0.0,
                "weekly_recovered_per_100000": 0.02116800634198933,
                "infected_per_100000": 1.7665042711847223,
                "deaths_per_100000": 0.010242583713865803,
                "recovered_per_100000": 1.808840283868701,
                "weekly_recovered_infected_ratio": 1.4761904761904763,
                "weekly_deaths_infected_ratio": 0.0
            },
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": "Московская обл.",
                "weekly_infected": 32,
                "weekly_recovered": 60,
                "weekly_deaths": 2,
                "infected": 42884,
                "recovered": 38234,
                "deaths": 521,
                "weekly_infected_per_100000": 0.021850845256247045,
                "weekly_deaths_per_100000": 0.0013656778285154403,
                "weekly_recovered_per_100000": 0.04097033485546321,
                "infected_per_100000": 29.282863999028073,
                "deaths_per_100000": 0.35575907432827225,
                "recovered_per_100000": 26.107663047729673,
                "weekly_recovered_infected_ratio": 1.875,
                "weekly_deaths_infected_ratio": 0.0625
            },
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": "Томская обл.",
                "weekly_infected": 1,
                "weekly_recovered": 30,
                "weekly_deaths": 0,
                "infected": 1327,
                "recovered": 1462,
                "deaths": 11,
                "weekly_infected_per_100000": 0.0006828389142577202,
                "weekly_deaths_per_100000": 0.0,
                "weekly_recovered_per_100000": 0.020485167427731606,
                "infected_per_100000": 0.9061272392199947,
                "deaths_per_100000": 0.007511228056834922,
                "recovered_per_100000": 0.998310492644787,
                "weekly_recovered_infected_ratio": 30.0,
                "weekly_deaths_infected_ratio": 0.0
            }
        ]
        self.assertListEqual(estimated_data, data)

    def test_run_latest(self):
        data = RegionDataTransformer(True).run()

        estimated_data = [
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": "Московская обл.",
                "weekly_infected": 32,
                "weekly_recovered": 60,
                "weekly_deaths": 2,
                "infected": 42884,
                "recovered": 38234,
                "deaths": 521,
                "weekly_infected_per_100000": 0.021850845256247045,
                "weekly_deaths_per_100000": 0.0013656778285154403,
                "weekly_recovered_per_100000": 0.04097033485546321,
                "infected_per_100000": 29.282863999028073,
                "deaths_per_100000": 0.35575907432827225,
                "recovered_per_100000": 26.107663047729673,
                "weekly_recovered_infected_ratio": 1.875,
                "weekly_deaths_infected_ratio": 0.0625
            },
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": "Карелия",
                "weekly_infected": 21,
                "weekly_recovered": 31,
                "weekly_deaths": 0,
                "infected": 2557,
                "recovered": 2617,
                "deaths": 14,
                "weekly_infected_per_100000": 0.014339617199412126,
                "weekly_deaths_per_100000": 0.0,
                "weekly_recovered_per_100000": 0.02116800634198933,
                "infected_per_100000": 1.7460191037569908,
                "deaths_per_100000": 0.009559744799608083,
                "recovered_per_100000": 1.786989438612454,
                "weekly_recovered_infected_ratio": 1.4761904761904763,
                "weekly_deaths_infected_ratio": 0.0
            },
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": "Томская обл.",
                "weekly_infected": 1,
                "weekly_recovered": 30,
                "weekly_deaths": 0,
                "infected": 1327,
                "recovered": 1462,
                "deaths": 11,
                "weekly_infected_per_100000": 0.0006828389142577202,
                "weekly_deaths_per_100000": 0.0,
                "weekly_recovered_per_100000": 0.020485167427731606,
                "infected_per_100000": 0.9061272392199947,
                "deaths_per_100000": 0.007511228056834922,
                "recovered_per_100000": 0.998310492644787,
                "weekly_recovered_infected_ratio": 30.0,
                "weekly_deaths_infected_ratio": 0.0
            }
        ]
        self.assertListEqual(estimated_data, data)


class TransformLegacyGlobalDataCommandTestCase(TestCase):
    def setUp(self):
        csv_data = [
            {
                "id": 1,
                "date": "2020-12-14",
                "cases": 28080,
                "deaths": 488,
            },
            {
                "id": 2,
                "date": "2020-12-13",
                "cases": 28137,
                "deaths": 560,
            },
            {
                "id": 3,
                "date": "2020-12-12",
                "cases": 28585,
                "deaths": 613,
            },
            {
                "id": 4,
                "date": "2020-12-11",
                "cases": 27927,
                "deaths": 562,
            },
            {
                "id": 5,
                "date": "2020-12-10",
                "cases": 26190,
                "deaths": 559,
            },
        ]
        for item in csv_data:
            CsvData.objects.create(**item)

    @mock.patch('apps.etl.models.ExternalDatabaseStatistic.get_all_transform_data',
                ExternalDatabaseStatisticMock.get_all_transform_data)
    @mock.patch('apps.etl.models.ExternalDatabaseVaccination.get_all_transform_data',
                ExternalDatabaseVaccinationMock.get_all_transform_data)
    def test_handle_debug(self):
        TransformLegacyGlobalData().handle(debug=True)

        data = list(GlobalTransformedData.objects.values())
        for item in data:
            item.pop('id')

        estimated_data = [
            {
                'end_date': date(2020, 12, 14),
                'start_date': date(2020, 12, 8),
                'weekly_infected': 138919,
                'weekly_deaths': 2782,
                'weekly_recovered': 0,
                'weekly_vaccinations': 0,
                'weekly_first_component': 0,
                'weekly_second_component': 0,
                'recovered': 0,
                'deaths': 2782,
                'infected': 138919,
                'first_component': 0,
                'second_component': 0,
                'weekly_infected_per_100000': 94.85929912976823,
                'weekly_deaths_per_100000': 1.8996578594649778,
                'weekly_recovered_per_100000': 0,
                'infected_per_100000': 94.85929912976823,
                'deaths_per_100000': 1.8996578594649778,
                'recovered_per_100000': 0,
                'weekly_recovered_infected_ratio': 0,
                'weekly_deaths_infected_ratio': 0.0200260583505496,
                'vaccinations_population_ratio': 0,
                'weekly_vaccinations_infected_ratio': 0
            },
            {
                'end_date': date(2020, 12, 21),
                'start_date': date(2020, 12, 15),
                'weekly_infected': 33253,
                'weekly_deaths': 390,
                'weekly_recovered': 31554,
                'weekly_vaccinations': 713489,
                'weekly_first_component': 350683,
                'weekly_second_component': 362806,
                'recovered': 31554,
                'deaths': 3172,
                'infected': 172172,
                'first_component': 350683,
                'second_component': 362806,
                'weekly_infected_per_100000': 22.70644241581197,
                'weekly_deaths_per_100000': 0.2663071765605109,
                'weekly_recovered_per_100000': 21.546299100488106,
                'infected_per_100000': 117.5657415455802,
                'deaths_per_100000': 2.1659650360254883,
                'recovered_per_100000': 21.546299100488106,
                'weekly_recovered_infected_ratio': 0.9489068655459658,
                'weekly_deaths_infected_ratio': 0.011728265118936636,
                'vaccinations_population_ratio': 0.0024773805512618647,
                'weekly_vaccinations_infected_ratio': 21.456379875499955
            }
        ]

        self.assertListEqual(estimated_data, data)

    @mock.patch('apps.etl.models.ExternalDatabaseStatistic.get_all_transform_data',
                ExternalDatabaseStatisticMock.get_all_transform_data)
    @mock.patch('apps.etl.models.ExternalDatabaseVaccination.get_all_transform_data',
                ExternalDatabaseVaccinationMock.get_all_transform_data)
    def test_handle(self):
        data = TransformLegacyGlobalData().handle(debug=False)

        estimated_data = [
            {
                'end_date': date(2020, 12, 14),
                'start_date': date(2020, 12, 8),
                'weekly_infected': 138919,
                'weekly_deaths': 2782,
                'weekly_recovered': 0,
                'weekly_vaccinations': 0,
                'weekly_first_component': 0,
                'weekly_second_component': 0,
                'recovered': 0,
                'deaths': 2782,
                'infected': 138919,
                'first_component': 0,
                'second_component': 0,
                'weekly_infected_per_100000': 94.85929912976823,
                'weekly_deaths_per_100000': 1.8996578594649778,
                'weekly_recovered_per_100000': 0,
                'infected_per_100000': 94.85929912976823,
                'deaths_per_100000': 1.8996578594649778,
                'recovered_per_100000': 0,
                'weekly_recovered_infected_ratio': 0,
                'weekly_deaths_infected_ratio': 0.0200260583505496,
                'vaccinations_population_ratio': 0,
                'weekly_vaccinations_infected_ratio': 0
            },
            {
                'end_date': date(2020, 12, 21),
                'start_date': date(2020, 12, 15),
                'weekly_infected': 33253,
                'weekly_deaths': 390,
                'weekly_recovered': 31554,
                'weekly_vaccinations': 713489,
                'weekly_first_component': 350683,
                'weekly_second_component': 362806,
                'recovered': 31554,
                'deaths': 3172,
                'infected': 172172,
                'first_component': 350683,
                'second_component': 362806,
                'weekly_infected_per_100000': 22.70644241581197,
                'weekly_deaths_per_100000': 0.2663071765605109,
                'weekly_recovered_per_100000': 21.546299100488106,
                'infected_per_100000': 117.5657415455802,
                'deaths_per_100000': 2.1659650360254883,
                'recovered_per_100000': 21.546299100488106,
                'weekly_recovered_infected_ratio': 0.9489068655459658,
                'weekly_deaths_infected_ratio': 0.011728265118936636,
                'vaccinations_population_ratio': 0.0024773805512618647,
                'weekly_vaccinations_infected_ratio': 21.456379875499955
            }
        ]

        self.assertListEqual(estimated_data, data)


class TransformGlobalDataCommandTestCase(TestCase):
    def setUp(self):
        self.existing_global_data = [
            {
                'start_date': date(2023, 12, 8),
                'end_date': date(2023, 12, 14),
                'weekly_infected': 138919,
                'weekly_deaths': 2782,
                'weekly_recovered': 0,
                'weekly_vaccinations': 0,
                'weekly_first_component': 0,
                'weekly_second_component': 0,
                'recovered': 0,
                'deaths': 2782,
                'infected': 138919,
                'first_component': 0,
                'second_component': 0,
                'weekly_infected_per_100000': 94.85929912976823,
                'weekly_deaths_per_100000': 1.8996578594649778,
                'weekly_recovered_per_100000': 0,
                'infected_per_100000': 94.85929912976823,
                'deaths_per_100000': 1.8996578594649778,
                'recovered_per_100000': 0,
                'weekly_recovered_infected_ratio': 0,
                'weekly_deaths_infected_ratio': 0.0200260583505496,
                'vaccinations_population_ratio': 0,
                'weekly_vaccinations_infected_ratio': 0
            },
            {
                'start_date': date(2023, 12, 15),
                'end_date': date(2023, 12, 21),
                'weekly_infected': 33253,
                'weekly_deaths': 390,
                'weekly_recovered': 31554,
                'weekly_vaccinations': 713489,
                'weekly_first_component': 350683,
                'weekly_second_component': 362806,
                'recovered': 31554,
                'deaths': 3172,
                'infected': None,
                'first_component': 350683,
                'second_component': 362806,
                'weekly_infected_per_100000': 22.70644241581197,
                'weekly_deaths_per_100000': 0.2663071765605109,
                'weekly_recovered_per_100000': 21.546299100488106,
                'infected_per_100000': 117.5657415455802,
                'deaths_per_100000': 2.1659650360254883,
                'recovered_per_100000': 21.546299100488106,
                'weekly_recovered_infected_ratio': 0.9489068655459658,
                'weekly_deaths_infected_ratio': 0.011728265118936636,
                'vaccinations_population_ratio': 0.0024773805512618647,
                'weekly_vaccinations_infected_ratio': 21.456379875499955
            }
        ]
        for item in self.existing_global_data:
            GlobalTransformedData.objects.create(**item)

        StopCoronaData.objects.create(
            start_date=date(2023, 12, 22),
            end_date=date(2023, 12, 28),
            region=StopCoronaData.RUSSIAN_FEDERATION,
            hospitalized=5000,
            infected=200,
            recovered=100,
            deaths=12,
        )
        StopCoronaData.objects.create(
            start_date=date(2023, 12, 29),
            end_date=date(2024, 1, 4),
            region=StopCoronaData.RUSSIAN_FEDERATION,
            hospitalized=5000,
            infected=200,
            recovered=100,
            deaths=12,
        )

        StopCoronaData.objects.create(
            start_date=date(2023, 12, 22),
            end_date=date(2023, 12, 28),
            region='Ульяновская обл.',
            hospitalized=5001,
            infected=201,
            recovered=101,
            deaths=10,
        )
        StopCoronaData.objects.create(
            start_date=date(2023, 12, 29),
            end_date=date(2024, 1, 4),
            region='Ульяновская обл.',
            hospitalized=5001,
            infected=201,
            recovered=101,
            deaths=10,
        )

        GogovData.objects.create(
            date=date(2023, 12, 22),
            first_component=350685,
            second_component=362810,
        )

        GogovData.objects.create(
            date=date(2023, 12, 23),
            first_component=350687,
            second_component=362812,
        )

        GogovData.objects.create(
            date=date(2023, 12, 24),
            first_component=350688,
            second_component=362815,
        )

        GogovData.objects.create(
            date=date(2023, 12, 29),
            first_component=350690,
            second_component=362820,
        )

        GogovData.objects.create(
            date=date(2023, 12, 30),
            first_component=350690,
            second_component=362821,
        )

    def test_handle(self):
        data = TransformGlobalData().handle(debug=False, latest=False)

        estimated_data = [
            {
                'start_date': date(2023, 12, 22),
                'end_date': date(2023, 12, 28),
                'weekly_vaccinations': 8,
                'weekly_first_component': 3,
                'weekly_second_component': 5,
                'first_component': 350688,
                'second_component': 362815,
                'weekly_infected': 200,
                'weekly_recovered': 100,
                'weekly_deaths': 12,
                'infected': 139119,
                'recovered': 31654,
                'deaths': 3184,
                'weekly_infected_per_100000': 0.13656778285154406,
                'weekly_deaths_per_100000': 0.008194066971092642,
                'weekly_recovered_per_100000': 0.06828389142577203,
                'infected_per_100000': 94.99586691261977,
                'deaths_per_100000': 2.174159102996581,
                'recovered_per_100000': 21.614582991913874,
                'weekly_recovered_infected_ratio': 0.5,
                'weekly_deaths_infected_ratio': 0.06,
                'vaccinations_population_ratio': 0.0024774420067641477,
                'weekly_vaccinations_infected_ratio': 0.04
            },
            {
                'start_date': date(2023, 12, 29),
                'end_date': date(2024, 1, 4),
                'weekly_vaccinations': 8,
                'weekly_first_component': 2,
                'weekly_second_component': 6,
                'first_component': 350690,
                'second_component': 362821,
                'weekly_infected': 200,
                'weekly_recovered': 100,
                'weekly_deaths': 12,
                'infected': 139319,
                'recovered': 31754,
                'deaths': 3196,
                'weekly_infected_per_100000': 0.13656778285154406,
                'weekly_deaths_per_100000': 0.008194066971092642,
                'weekly_recovered_per_100000': 0.06828389142577203,
                'infected_per_100000': 95.13243469547133,
                'deaths_per_100000': 2.182353169967674,
                'recovered_per_100000': 21.68286688333965,
                'weekly_recovered_infected_ratio': 0.5,
                'weekly_deaths_infected_ratio': 0.06,
                'vaccinations_population_ratio': 0.0024774829770990033,
                'weekly_vaccinations_infected_ratio': 0.04
            }
        ]
        self.assertListEqual(estimated_data, data)

    def test_handle_debug(self):
        TransformGlobalData().handle(debug=True, latest=False)

        data = list(GlobalTransformedData.objects.order_by('start_date').values())
        for item in data:
            item.pop('id')

        estimated_data = [
            {
                'start_date': date(2023, 12, 22),
                'end_date': date(2023, 12, 28),
                'weekly_vaccinations': 8,
                'weekly_first_component': 3,
                'weekly_second_component': 5,
                'first_component': 350688,
                'second_component': 362815,
                'weekly_infected': 200,
                'weekly_recovered': 100,
                'weekly_deaths': 12,
                'infected': 139119,
                'recovered': 31654,
                'deaths': 3184,
                'weekly_infected_per_100000': 0.13656778285154406,
                'weekly_deaths_per_100000': 0.008194066971092642,
                'weekly_recovered_per_100000': 0.06828389142577203,
                'infected_per_100000': 94.99586691261977,
                'deaths_per_100000': 2.174159102996581,
                'recovered_per_100000': 21.614582991913874,
                'weekly_recovered_infected_ratio': 0.5,
                'weekly_deaths_infected_ratio': 0.06,
                'vaccinations_population_ratio': 0.0024774420067641477,
                'weekly_vaccinations_infected_ratio': 0.04
            },
            {
                'start_date': date(2023, 12, 29),
                'end_date': date(2024, 1, 4),
                'weekly_vaccinations': 8,
                'weekly_first_component': 2,
                'weekly_second_component': 6,
                'first_component': 350690,
                'second_component': 362821,
                'weekly_infected': 200,
                'weekly_recovered': 100,
                'weekly_deaths': 12,
                'infected': 139319,
                'recovered': 31754,
                'deaths': 3196,
                'weekly_infected_per_100000': 0.13656778285154406,
                'weekly_deaths_per_100000': 0.008194066971092642,
                'weekly_recovered_per_100000': 0.06828389142577203,
                'infected_per_100000': 95.13243469547133,
                'deaths_per_100000': 2.182353169967674,
                'recovered_per_100000': 21.68286688333965,
                'weekly_recovered_infected_ratio': 0.5,
                'weekly_deaths_infected_ratio': 0.06,
                'vaccinations_population_ratio': 0.0024774829770990033,
                'weekly_vaccinations_infected_ratio': 0.04
            },
        ]

        estimated_data = self.existing_global_data + estimated_data
        estimated_data.sort(key=lambda x: x['start_date'])
        data.sort(key=lambda x: x['start_date'])
        self.assertListEqual(estimated_data, data)

    def test_handle_latest(self):
        data = TransformGlobalData().handle(debug=False, latest=True)

        estimated_data = [
            {
                'start_date': date(2023, 12, 29),
                'end_date': date(2024, 1, 4),
                'weekly_first_component': 7,
                'weekly_second_component': 15,
                'weekly_vaccinations': 22,
                'first_component': 350690,
                'second_component': 362821,
                'weekly_infected': 200,
                'weekly_recovered': 100,
                'weekly_deaths': 12,
                'infected': 139119,
                'recovered': 31654,
                'deaths': 3184,
                'deaths_per_100000': 2.174159102996581,
                'infected_per_100000': 94.99586691261977,
                'recovered_per_100000': 21.614582991913874,
                'weekly_deaths_per_100000': 0.008194066971092642,
                'weekly_infected_per_100000': 0.13656778285154406,
                'weekly_recovered_per_100000': 0.06828389142577203,
                'vaccinations_population_ratio': 0.0024774829770990033,
                'weekly_deaths_infected_ratio': 0.06,
                'weekly_recovered_infected_ratio': 0.5,
                'weekly_vaccinations_infected_ratio': 0.11
            }
        ]
        self.assertListEqual(estimated_data, data)


class TransformLegacyRegionDataCommandTestCase(TestCase):
    @mock.patch('apps.etl.models.ExternalDatabaseStatistic.get_all_transform_data',
                ExternalDatabaseStatisticMock.get_all_transform_data)
    def test_handle(self):
        data = TransformLegacyRegionData().handle(debug=False)
        estimated_data = [
            {
                "region": "Карелия",
                "end_date": date(2020, 12, 14),
                "start_date": date(2020, 12, 8),
                "weekly_deaths": 3,
                "weekly_infected": 786,
                "weekly_recovered": 472,
                "recovered": 472,
                "deaths": 3,
                "infected": 786,
                "weekly_infected_per_100000": 0.5367113866065681,
                "weekly_deaths_per_100000": 0.0020485167427731605,
                "weekly_recovered_per_100000": 0.32229996752964396,
                "infected_per_100000": 0.5367113866065681,
                "deaths_per_100000": 0.0020485167427731605,
                "recovered_per_100000": 0.32229996752964396,
                "weekly_recovered_infected_ratio": 0.6005089058524173,
                "weekly_deaths_infected_ratio": 0.003816793893129771
            },
            {
                "region": "Карелия",
                "start_date": date(2020, 12, 15),
                "end_date": date(2020, 12, 21),
                "weekly_deaths": 11,
                "weekly_infected": 1750,
                "weekly_recovered": 2114,
                "recovered": 2586,
                "deaths": 14,
                "infected": 2536,
                "weekly_infected_per_100000": 1.1949680999510104,
                "weekly_deaths_per_100000": 0.007511228056834922,
                "weekly_recovered_per_100000": 1.4435214647408205,
                "infected_per_100000": 1.7316794865575786,
                "deaths_per_100000": 0.009559744799608083,
                "recovered_per_100000": 1.7658214322704646,
                "weekly_recovered_infected_ratio": 1.208,
                "weekly_deaths_infected_ratio": 0.006285714285714286
            },
            {
                "region": "Москва",
                "start_date": date(2020, 12, 8),
                "end_date": date(2020, 12, 14),
                "weekly_deaths": 147,
                "weekly_infected": 12299,
                "weekly_recovered": 9773,
                "recovered": 9773,
                "deaths": 147,
                "infected": 12299,
                "weekly_infected_per_100000": 8.3982358064557,
                "weekly_deaths_per_100000": 0.10037732039588489,
                "weekly_recovered_per_100000": 6.6733847090407,
                "infected_per_100000": 8.3982358064557,
                "deaths_per_100000": 0.10037732039588489,
                "recovered_per_100000": 6.6733847090407,
                "weekly_recovered_infected_ratio": 0.7946174485730547,
                "weekly_deaths_infected_ratio": 0.01195219123505976
            },
            {
                "region": "Москва",
                "start_date": date(2020, 12, 15),
                "end_date": date(2020, 12, 21),
                "weekly_deaths": 372,
                "weekly_infected": 30553,
                "weekly_recovered": 28401,
                "recovered": 38174,
                "deaths": 519,
                "infected": 42852,
                "weekly_infected_per_100000": 20.862777347316126,
                "weekly_deaths_per_100000": 0.25401607610387195,
                "weekly_recovered_per_100000": 19.39330800383351,
                "infected_per_100000": 29.261013153771827,
                "deaths_per_100000": 0.3543933964997568,
                "recovered_per_100000": 26.06669271287421,
                "weekly_recovered_infected_ratio": 0.9295650181651556,
                "weekly_deaths_infected_ratio": 0.01217556377442477
            },
            {
                "region": "Томская обл.",
                "start_date": date(2020, 12, 8),
                "end_date": date(2020, 12, 14),
                "weekly_deaths": 4,
                "weekly_infected": 376,
                "weekly_recovered": 393,
                "recovered": 393,
                "deaths": 4,
                "infected": 376,
                "weekly_infected_per_100000": 0.2567474317609028,
                "weekly_deaths_per_100000": 0.0027313556570308806,
                "weekly_recovered_per_100000": 0.26835569330328407,
                "infected_per_100000": 0.2567474317609028,
                "deaths_per_100000": 0.0027313556570308806,
                "recovered_per_100000": 0.26835569330328407,
                "weekly_recovered_infected_ratio": 1.0452127659574468,
                "weekly_deaths_infected_ratio": 0.010638297872340425
            },
            {
                "region": "Томская обл.",
                "start_date": date(2020, 12, 15),
                "end_date": date(2020, 12, 21),
                "weekly_deaths": 7,
                "weekly_infected": 950,
                "weekly_recovered": 1039,
                "recovered": 1432,
                "deaths": 11,
                "infected": 1326,
                "weekly_infected_per_100000": 0.6486969685448342,
                "weekly_deaths_per_100000": 0.0047798723998040415,
                "weekly_recovered_per_100000": 0.7094696319137713,
                "infected_per_100000": 0.905444400305737,
                "deaths_per_100000": 0.007511228056834922,
                "recovered_per_100000": 0.9778253252170553,
                "weekly_recovered_infected_ratio": 1.0936842105263158,
                "weekly_deaths_infected_ratio": 0.007368421052631579
            }
        ]
        self.assertListEqual(estimated_data, data)

    @mock.patch('apps.etl.models.ExternalDatabaseStatistic.get_all_transform_data',
                ExternalDatabaseStatisticMock.get_all_transform_data)
    def test_handle_debug(self):
        TransformLegacyRegionData().handle(debug=True)

        data = list(RegionTransformedData.objects.values())
        for item in data:
            item.pop('id')

        estimated_data = [
            {
                "region": "Карелия",
                "end_date": date(2020, 12, 14),
                "start_date": date(2020, 12, 8),
                "weekly_deaths": 3,
                "weekly_infected": 786,
                "weekly_recovered": 472,
                "recovered": 472,
                "deaths": 3,
                "infected": 786,
                "weekly_infected_per_100000": 0.5367113866065681,
                "weekly_deaths_per_100000": 0.0020485167427731605,
                "weekly_recovered_per_100000": 0.32229996752964396,
                "infected_per_100000": 0.5367113866065681,
                "deaths_per_100000": 0.0020485167427731605,
                "recovered_per_100000": 0.32229996752964396,
                "weekly_recovered_infected_ratio": 0.6005089058524173,
                "weekly_deaths_infected_ratio": 0.003816793893129771
            },
            {
                "region": "Карелия",
                "start_date": date(2020, 12, 15),
                "end_date": date(2020, 12, 21),
                "weekly_deaths": 11,
                "weekly_infected": 1750,
                "weekly_recovered": 2114,
                "recovered": 2586,
                "deaths": 14,
                "infected": 2536,
                "weekly_infected_per_100000": 1.1949680999510104,
                "weekly_deaths_per_100000": 0.007511228056834922,
                "weekly_recovered_per_100000": 1.4435214647408205,
                "infected_per_100000": 1.7316794865575786,
                "deaths_per_100000": 0.009559744799608083,
                "recovered_per_100000": 1.7658214322704646,
                "weekly_recovered_infected_ratio": 1.208,
                "weekly_deaths_infected_ratio": 0.006285714285714286
            },
            {
                "region": "Москва",
                "start_date": date(2020, 12, 8),
                "end_date": date(2020, 12, 14),
                "weekly_deaths": 147,
                "weekly_infected": 12299,
                "weekly_recovered": 9773,
                "recovered": 9773,
                "deaths": 147,
                "infected": 12299,
                "weekly_infected_per_100000": 8.3982358064557,
                "weekly_deaths_per_100000": 0.10037732039588489,
                "weekly_recovered_per_100000": 6.6733847090407,
                "infected_per_100000": 8.3982358064557,
                "deaths_per_100000": 0.10037732039588489,
                "recovered_per_100000": 6.6733847090407,
                "weekly_recovered_infected_ratio": 0.7946174485730547,
                "weekly_deaths_infected_ratio": 0.01195219123505976
            },
            {
                "region": "Москва",
                "start_date": date(2020, 12, 15),
                "end_date": date(2020, 12, 21),
                "weekly_deaths": 372,
                "weekly_infected": 30553,
                "weekly_recovered": 28401,
                "recovered": 38174,
                "deaths": 519,
                "infected": 42852,
                "weekly_infected_per_100000": 20.862777347316126,
                "weekly_deaths_per_100000": 0.25401607610387195,
                "weekly_recovered_per_100000": 19.39330800383351,
                "infected_per_100000": 29.261013153771827,
                "deaths_per_100000": 0.3543933964997568,
                "recovered_per_100000": 26.06669271287421,
                "weekly_recovered_infected_ratio": 0.9295650181651556,
                "weekly_deaths_infected_ratio": 0.01217556377442477
            },
            {
                "region": "Томская обл.",
                "start_date": date(2020, 12, 8),
                "end_date": date(2020, 12, 14),
                "weekly_deaths": 4,
                "weekly_infected": 376,
                "weekly_recovered": 393,
                "recovered": 393,
                "deaths": 4,
                "infected": 376,
                "weekly_infected_per_100000": 0.2567474317609028,
                "weekly_deaths_per_100000": 0.0027313556570308806,
                "weekly_recovered_per_100000": 0.26835569330328407,
                "infected_per_100000": 0.2567474317609028,
                "deaths_per_100000": 0.0027313556570308806,
                "recovered_per_100000": 0.26835569330328407,
                "weekly_recovered_infected_ratio": 1.0452127659574468,
                "weekly_deaths_infected_ratio": 0.010638297872340425
            },
            {
                "region": "Томская обл.",
                "start_date": date(2020, 12, 15),
                "end_date": date(2020, 12, 21),
                "weekly_deaths": 7,
                "weekly_infected": 950,
                "weekly_recovered": 1039,
                "recovered": 1432,
                "deaths": 11,
                "infected": 1326,
                "weekly_infected_per_100000": 0.6486969685448342,
                "weekly_deaths_per_100000": 0.0047798723998040415,
                "weekly_recovered_per_100000": 0.7094696319137713,
                "infected_per_100000": 0.905444400305737,
                "deaths_per_100000": 0.007511228056834922,
                "recovered_per_100000": 0.9778253252170553,
                "weekly_recovered_infected_ratio": 1.0936842105263158,
                "weekly_deaths_infected_ratio": 0.007368421052631579
            }
        ]
        self.assertListEqual(estimated_data, data)


class TransformRegionDataCommandTestCase(TestCase):
    def setUp(self):
        self.existing_region_data = [
            {
                "region": "Карелия",
                "start_date": date(2023, 12, 8),
                "end_date": date(2023, 12, 14),
                "weekly_deaths": 3,
                "weekly_infected": 786,
                "weekly_recovered": 472,
                "recovered": 472,
                "deaths": 3,
                "infected": 786,
                "weekly_infected_per_100000": 0.5367113866065681,
                "weekly_deaths_per_100000": 0.0020485167427731605,
                "weekly_recovered_per_100000": 0.32229996752964396,
                "infected_per_100000": 0.5367113866065681,
                "deaths_per_100000": 0.0020485167427731605,
                "recovered_per_100000": 0.32229996752964396,
                "weekly_recovered_infected_ratio": 0.6005089058524173,
                "weekly_deaths_infected_ratio": 0.003816793893129771
            },
            {
                "region": "Карелия",
                "start_date": date(2023, 12, 15),
                "end_date": date(2023, 12, 21),
                "weekly_deaths": 11,
                "weekly_infected": 1750,
                "weekly_recovered": 2114,
                "recovered": 2586,
                "deaths": 14,
                "infected": 2536,
                "weekly_infected_per_100000": 1.1949680999510104,
                "weekly_deaths_per_100000": 0.007511228056834922,
                "weekly_recovered_per_100000": 1.4435214647408205,
                "infected_per_100000": 1.7316794865575786,
                "deaths_per_100000": 0.009559744799608083,
                "recovered_per_100000": 1.7658214322704646,
                "weekly_recovered_infected_ratio": 1.208,
                "weekly_deaths_infected_ratio": 0.006285714285714286
            },
            {
                "region": "Московская обл.",
                "start_date": date(2023, 12, 8),
                "end_date": date(2023, 12, 14),
                "weekly_deaths": 147,
                "weekly_infected": 12299,
                "weekly_recovered": 9773,
                "recovered": 9773,
                "deaths": 147,
                "infected": 12299,
                "weekly_infected_per_100000": 8.3982358064557,
                "weekly_deaths_per_100000": 0.10037732039588489,
                "weekly_recovered_per_100000": 6.6733847090407,
                "infected_per_100000": 8.3982358064557,
                "deaths_per_100000": 0.10037732039588489,
                "recovered_per_100000": 6.6733847090407,
                "weekly_recovered_infected_ratio": 0.7946174485730547,
                "weekly_deaths_infected_ratio": 0.01195219123505976
            },
            {
                "region": "Московская обл.",
                "start_date": date(2023, 12, 15),
                "end_date": date(2023, 12, 21),
                "weekly_deaths": 372,
                "weekly_infected": 30553,
                "weekly_recovered": 28401,
                "recovered": 38174,
                "deaths": 519,
                "infected": 42852,
                "weekly_infected_per_100000": 20.862777347316126,
                "weekly_deaths_per_100000": 0.25401607610387195,
                "weekly_recovered_per_100000": 19.39330800383351,
                "infected_per_100000": 29.261013153771827,
                "deaths_per_100000": 0.3543933964997568,
                "recovered_per_100000": 26.06669271287421,
                "weekly_recovered_infected_ratio": 0.9295650181651556,
                "weekly_deaths_infected_ratio": 0.01217556377442477
            },
            {
                "region": "Томская обл.",
                "start_date": date(2023, 12, 8),
                "end_date": date(2023, 12, 14),
                "weekly_deaths": 4,
                "weekly_infected": 376,
                "weekly_recovered": 393,
                "recovered": 393,
                "deaths": 4,
                "infected": 376,
                "weekly_infected_per_100000": 0.2567474317609028,
                "weekly_deaths_per_100000": 0.0027313556570308806,
                "weekly_recovered_per_100000": 0.26835569330328407,
                "infected_per_100000": 0.2567474317609028,
                "deaths_per_100000": 0.0027313556570308806,
                "recovered_per_100000": 0.26835569330328407,
                "weekly_recovered_infected_ratio": 1.0452127659574468,
                "weekly_deaths_infected_ratio": 0.010638297872340425
            },
            {
                "region": "Томская обл.",
                "start_date": date(2023, 12, 15),
                "end_date": date(2023, 12, 21),
                "weekly_deaths": 7,
                "weekly_infected": 950,
                "weekly_recovered": 1039,
                "recovered": 1432,
                "deaths": 11,
                "infected": 1326,
                "weekly_infected_per_100000": 0.6486969685448342,
                "weekly_deaths_per_100000": 0.0047798723998040415,
                "weekly_recovered_per_100000": 0.7094696319137713,
                "infected_per_100000": 0.905444400305737,
                "deaths_per_100000": 0.007511228056834922,
                "recovered_per_100000": 0.9778253252170553,
                "weekly_recovered_infected_ratio": 1.0936842105263158,
                "weekly_deaths_infected_ratio": 0.007368421052631579
            }
        ]
        for item in self.existing_region_data:
            RegionTransformedData.objects.create(**item)

        StopCoronaData.objects.create(
            start_date=date(2023, 12, 22),
            end_date=date(2023, 12, 28),
            region=StopCoronaData.RUSSIAN_FEDERATION,
            hospitalized=5000,
            infected=200,
            recovered=100,
            deaths=12,
        )
        StopCoronaData.objects.create(
            start_date=date(2023, 12, 29),
            end_date=date(2024, 1, 4),
            region=StopCoronaData.RUSSIAN_FEDERATION,
            hospitalized=5000,
            infected=200,
            recovered=100,
            deaths=12,
        )

        StopCoronaData.objects.create(
            start_date=date(2023, 12, 22),
            end_date=date(2023, 12, 28),
            region='Республика Карелия',
            hospitalized=1,
            infected=30,
            recovered=32,
            deaths=1,
        )
        StopCoronaData.objects.create(
            start_date=date(2023, 12, 29),
            end_date=date(2024, 1, 4),
            region='Республика Карелия',
            hospitalized=20,
            infected=21,
            recovered=31,
            deaths=0,
        )

        StopCoronaData.objects.create(
            start_date=date(2023, 12, 29),
            end_date=date(2024, 1, 4),
            region='Московская область',
            hospitalized=201,
            infected=32,
            recovered=60,
            deaths=2,
        )

        StopCoronaData.objects.create(
            start_date=date(2023, 12, 29),
            end_date=date(2024, 1, 4),
            region='Томская область',
            hospitalized=1,
            infected=1,
            recovered=30,
            deaths=0,
        )

    def test_handle(self):
        data = TransformRegionData().handle(debug=False, latest=False)

        estimated_data = [
            {
                "start_date": date(2023, 12, 22),
                "end_date": date(2023, 12, 28),
                "region": "Карелия",
                "weekly_infected": 30,
                "weekly_recovered": 32,
                "weekly_deaths": 1,
                "infected": 2566,
                "recovered": 2618,
                "deaths": 15,
                "weekly_infected_per_100000": 0.020485167427731606,
                "weekly_deaths_per_100000": 0.0006828389142577202,
                "weekly_recovered_per_100000": 0.021850845256247045,
                "infected_per_100000": 1.7521646539853102,
                "deaths_per_100000": 0.010242583713865803,
                "recovered_per_100000": 1.7876722775267118,
                "weekly_recovered_infected_ratio": 1.0666666666666667,
                "weekly_deaths_infected_ratio": 0.03333333333333333
            },
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": "Карелия",
                "weekly_infected": 21,
                "weekly_recovered": 31,
                "weekly_deaths": 0,
                "infected": 2587,
                "recovered": 2649,
                "deaths": 15,
                "weekly_infected_per_100000": 0.014339617199412126,
                "weekly_deaths_per_100000": 0.0,
                "weekly_recovered_per_100000": 0.02116800634198933,
                "infected_per_100000": 1.7665042711847223,
                "deaths_per_100000": 0.010242583713865803,
                "recovered_per_100000": 1.808840283868701,
                "weekly_recovered_infected_ratio": 1.4761904761904763,
                "weekly_deaths_infected_ratio": 0.0
            },
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": "Московская обл.",
                "weekly_infected": 32,
                "weekly_recovered": 60,
                "weekly_deaths": 2,
                "infected": 42884,
                "recovered": 38234,
                "deaths": 521,
                "weekly_infected_per_100000": 0.021850845256247045,
                "weekly_deaths_per_100000": 0.0013656778285154403,
                "weekly_recovered_per_100000": 0.04097033485546321,
                "infected_per_100000": 29.282863999028073,
                "deaths_per_100000": 0.35575907432827225,
                "recovered_per_100000": 26.107663047729673,
                "weekly_recovered_infected_ratio": 1.875,
                "weekly_deaths_infected_ratio": 0.0625
            },
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": "Томская обл.",
                "weekly_infected": 1,
                "weekly_recovered": 30,
                "weekly_deaths": 0,
                "infected": 1327,
                "recovered": 1462,
                "deaths": 11,
                "weekly_infected_per_100000": 0.0006828389142577202,
                "weekly_deaths_per_100000": 0.0,
                "weekly_recovered_per_100000": 0.020485167427731606,
                "infected_per_100000": 0.9061272392199947,
                "deaths_per_100000": 0.007511228056834922,
                "recovered_per_100000": 0.998310492644787,
                "weekly_recovered_infected_ratio": 30.0,
                "weekly_deaths_infected_ratio": 0.0
            }
        ]
        self.assertListEqual(estimated_data, data)

    def test_handle_debug(self):
        TransformRegionData().handle(debug=True, latest=False)

        data = list(RegionTransformedData.objects.values())
        for item in data:
            item.pop('id')

        estimated_data = [
            {
                "start_date": date(2023, 12, 22),
                "end_date": date(2023, 12, 28),
                "region": "Карелия",
                "weekly_infected": 30,
                "weekly_recovered": 32,
                "weekly_deaths": 1,
                "infected": 2566,
                "recovered": 2618,
                "deaths": 15,
                "weekly_infected_per_100000": 0.020485167427731606,
                "weekly_deaths_per_100000": 0.0006828389142577202,
                "weekly_recovered_per_100000": 0.021850845256247045,
                "infected_per_100000": 1.7521646539853102,
                "deaths_per_100000": 0.010242583713865803,
                "recovered_per_100000": 1.7876722775267118,
                "weekly_recovered_infected_ratio": 1.0666666666666667,
                "weekly_deaths_infected_ratio": 0.03333333333333333
            },
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": "Карелия",
                "weekly_infected": 21,
                "weekly_recovered": 31,
                "weekly_deaths": 0,
                "infected": 2587,
                "recovered": 2649,
                "deaths": 15,
                "weekly_infected_per_100000": 0.014339617199412126,
                "weekly_deaths_per_100000": 0.0,
                "weekly_recovered_per_100000": 0.02116800634198933,
                "infected_per_100000": 1.7665042711847223,
                "deaths_per_100000": 0.010242583713865803,
                "recovered_per_100000": 1.808840283868701,
                "weekly_recovered_infected_ratio": 1.4761904761904763,
                "weekly_deaths_infected_ratio": 0.0
            },
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": "Московская обл.",
                "weekly_infected": 32,
                "weekly_recovered": 60,
                "weekly_deaths": 2,
                "infected": 42884,
                "recovered": 38234,
                "deaths": 521,
                "weekly_infected_per_100000": 0.021850845256247045,
                "weekly_deaths_per_100000": 0.0013656778285154403,
                "weekly_recovered_per_100000": 0.04097033485546321,
                "infected_per_100000": 29.282863999028073,
                "deaths_per_100000": 0.35575907432827225,
                "recovered_per_100000": 26.107663047729673,
                "weekly_recovered_infected_ratio": 1.875,
                "weekly_deaths_infected_ratio": 0.0625
            },
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": "Томская обл.",
                "weekly_infected": 1,
                "weekly_recovered": 30,
                "weekly_deaths": 0,
                "infected": 1327,
                "recovered": 1462,
                "deaths": 11,
                "weekly_infected_per_100000": 0.0006828389142577202,
                "weekly_deaths_per_100000": 0.0,
                "weekly_recovered_per_100000": 0.020485167427731606,
                "infected_per_100000": 0.9061272392199947,
                "deaths_per_100000": 0.007511228056834922,
                "recovered_per_100000": 0.998310492644787,
                "weekly_recovered_infected_ratio": 30.0,
                "weekly_deaths_infected_ratio": 0.0
            }
        ]

        estimated_data = self.existing_region_data + estimated_data
        estimated_data.sort(key=lambda x: (x['start_date'], x['region']))
        data.sort(key=lambda x: (x['start_date'], x['region']))
        self.assertListEqual(estimated_data, data)

    def test_handle_latest(self):
        data = TransformRegionData().handle(debug=False, latest=True)

        estimated_data = [
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": "Московская обл.",
                "weekly_infected": 32,
                "weekly_recovered": 60,
                "weekly_deaths": 2,
                "infected": 42884,
                "recovered": 38234,
                "deaths": 521,
                "weekly_infected_per_100000": 0.021850845256247045,
                "weekly_deaths_per_100000": 0.0013656778285154403,
                "weekly_recovered_per_100000": 0.04097033485546321,
                "infected_per_100000": 29.282863999028073,
                "deaths_per_100000": 0.35575907432827225,
                "recovered_per_100000": 26.107663047729673,
                "weekly_recovered_infected_ratio": 1.875,
                "weekly_deaths_infected_ratio": 0.0625
            },
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": "Карелия",
                "weekly_infected": 21,
                "weekly_recovered": 31,
                "weekly_deaths": 0,
                "infected": 2557,
                "recovered": 2617,
                "deaths": 14,
                "weekly_infected_per_100000": 0.014339617199412126,
                "weekly_deaths_per_100000": 0.0,
                "weekly_recovered_per_100000": 0.02116800634198933,
                "infected_per_100000": 1.7460191037569908,
                "deaths_per_100000": 0.009559744799608083,
                "recovered_per_100000": 1.786989438612454,
                "weekly_recovered_infected_ratio": 1.4761904761904763,
                "weekly_deaths_infected_ratio": 0.0
            },
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": "Томская обл.",
                "weekly_infected": 1,
                "weekly_recovered": 30,
                "weekly_deaths": 0,
                "infected": 1327,
                "recovered": 1462,
                "deaths": 11,
                "weekly_infected_per_100000": 0.0006828389142577202,
                "weekly_deaths_per_100000": 0.0,
                "weekly_recovered_per_100000": 0.020485167427731606,
                "infected_per_100000": 0.9061272392199947,
                "deaths_per_100000": 0.007511228056834922,
                "recovered_per_100000": 0.998310492644787,
                "weekly_recovered_infected_ratio": 30.0,
                "weekly_deaths_infected_ratio": 0.0
            }
        ]

        estimated_data.sort(key=lambda x: (x['region'], x['start_date']))
        data.sort(key=lambda x: (x['region'], x['start_date']))
        self.assertListEqual(estimated_data, data)
