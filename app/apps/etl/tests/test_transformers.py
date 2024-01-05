from calendar import month
from datetime import date
from unittest import mock

from django.test import TestCase
from pandas import DataFrame
from numpy import nan

from apps.etl.utils.data_transformers.transforming_functions import GenericTransformingFunctions
from apps.etl.utils.data_transformers.global_transformers import LegacyGlobalDataTransformer
from apps.etl.utils.data_transformers.regional_transformers import LegacyRegionDataTransformer
from apps.etl.models import ExternalDatabaseStatistic, ExternalDatabaseVaccination, CsvData
from apps.etl.tests.mocks import ExternalDatabaseStatisticMock, ExternalDatabaseVaccinationMock


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
            }
        ]
        self.assertListEqual(estimated_result, result)
