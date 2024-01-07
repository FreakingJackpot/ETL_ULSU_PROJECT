from datetime import date

from django.test import TestCase

from apps.etl.models import StopCoronaData, GogovData, GlobalTransformedData, RegionTransformedData


class StopCoronaDataTestCase(TestCase):
    def setUp(self):
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

    def test_get_global_transform_data(self):
        data = list(StopCoronaData.get_global_transform_data(False))
        estimated_data = [
            {
                "start_date": date(2023, 12, 22),
                "end_date": date(2023, 12, 28),
                "infected": 200,
                "recovered": 100,
                "deaths": 12,
            },
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "infected": 200,
                "recovered": 100,
                "deaths": 12,
            }
        ]

        self.assertListEqual(estimated_data, data)

        data = list(StopCoronaData.get_global_transform_data(True))
        estimated_data = [
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "infected": 200,
                "recovered": 100,
                "deaths": 12,
            }
        ]

        self.assertListEqual(estimated_data, data)

    def test_get_region_transform_data(self):
        data = list(StopCoronaData.get_region_transform_data(False))
        estimated_data = [
            {
                "start_date": date(2023, 12, 22),
                "end_date": date(2023, 12, 28),
                "region": 'Республика Карелия',
                "infected": 30,
                "recovered": 32,
                "deaths": 1,
            },
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": 'Республика Карелия',
                "infected": 21,
                "recovered": 31,
                "deaths": 0,
            },

            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": 'Московская область',
                "infected": 32,
                "recovered": 60,
                "deaths": 2,
            },

            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": 'Томская область',
                "infected": 1,
                "recovered": 30,
                "deaths": 0,
            }
        ]

        self.assertListEqual(estimated_data, data)

        data = list(StopCoronaData.get_region_transform_data(True))
        estimated_data = estimated_data = [
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": 'Московская область',
                "infected": 32,
                "recovered": 60,
                "deaths": 2,
            },
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": 'Республика Карелия',
                "infected": 21,
                "recovered": 31,
                "deaths": 0,
            },
            {
                "start_date": date(2023, 12, 29),
                "end_date": date(2024, 1, 4),
                "region": 'Томская область',
                "infected": 1,
                "recovered": 30,
                "deaths": 0,
            }
        ]

        self.assertListEqual(estimated_data, data)


class GlobalTransformedDataTestCase(TestCase):
    def setUp(self):
        existing_global_data = [
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
            },
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
        ]
        for item in existing_global_data:
            GlobalTransformedData.objects.create(**item)

    def test_get_highest_not_null_values(self):
        stopcorona_obj = StopCoronaData.objects.create(
            start_date=date(2023, 12, 22),
            end_date=date(2023, 12, 28),
            region=StopCoronaData.RUSSIAN_FEDERATION,
            hospitalized=5000,
            infected=200,
            recovered=100,
            deaths=12,
        )

        stopcorona_obj_2 = StopCoronaData.objects.create(
            start_date=date(2023, 12, 29),
            end_date=date(2024, 1, 4),
            region=StopCoronaData.RUSSIAN_FEDERATION,
            hospitalized=5000,
            infected=200,
            recovered=100,
            deaths=12,
        )

        data = GlobalTransformedData.get_highest_not_null_values(False)
        estimated_data = {
            'recovered': 31554,
            'deaths': 3172,
            'infected': 138919,
            'first_component': 350688,
            'second_component': 362815,
        }
        self.assertDictEqual(estimated_data, data)

        stopcorona_obj.delete()
        stopcorona_obj_2.delete()

        gogov_obj = GogovData.objects.create(
            date=date(2023, 12, 22),
            first_component=350688,
            second_component=362815,

        )
        gogov_obj_2 = GogovData.objects.create(
            date=date(2023, 12, 29),
            first_component=350688,
            second_component=362815,

        )

        data = GlobalTransformedData.get_highest_not_null_values(False)
        estimated_data = {
            'infected': 139119,
            'recovered': 31654,
            'deaths': 3184,
            'first_component': 350683,
            'second_component': 362806,
        }
        self.assertDictEqual(estimated_data, data)

        stopcorona_obj = StopCoronaData.objects.create(
            start_date=date(2023, 12, 22),
            end_date=date(2023, 12, 28),
            region=StopCoronaData.RUSSIAN_FEDERATION,
            hospitalized=5000,
            infected=200,
            recovered=100,
            deaths=12,
        )

        stopcorona_obj_2 = StopCoronaData.objects.create(
            start_date=date(2023, 12, 29),
            end_date=date(2024, 1, 4),
            region=StopCoronaData.RUSSIAN_FEDERATION,
            hospitalized=5000,
            infected=200,
            recovered=100,
            deaths=12,
        )

        data = GlobalTransformedData.get_highest_not_null_values(False)
        estimated_data = {
            'recovered': 31554,
            'deaths': 3172,
            'infected': 138919,
            'first_component': 350683,
            'second_component': 362806,
        }
        self.assertDictEqual(estimated_data, data)

        data = GlobalTransformedData.get_highest_not_null_values(True)
        estimated_data = {
            'infected': 139119,
            'recovered': 31654,
            'deaths': 3184,
            'first_component': 350688,
            'second_component': 362815,
        }
        self.assertDictEqual(estimated_data, data)


class RegionTransformedDataTestCase(TestCase):
    def setUp(self):
        existing_region_data = [
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
        ]
        for item in existing_region_data:
            RegionTransformedData.objects.create(**item)

    def test_get_highest_not_null_values(self):
        stopcorona_obj = StopCoronaData.objects.create(
            start_date=date(2023, 12, 22),
            end_date=date(2023, 12, 28),
            region=StopCoronaData.RUSSIAN_FEDERATION,
            hospitalized=5000,
            infected=200,
            recovered=100,
            deaths=12,
        )

        stopcorona_obj_2 = StopCoronaData.objects.create(
            start_date=date(2023, 12, 29),
            end_date=date(2024, 1, 4),
            region=StopCoronaData.RUSSIAN_FEDERATION,
            hospitalized=5000,
            infected=200,
            recovered=100,
            deaths=12,
        )

        data = RegionTransformedData.get_highest_not_null_values(False)
        estimated_data = {
            'Московская обл.': {'region': 'Московская обл.', 'infected': 42852, 'deaths': 519, 'recovered': 38174},
            'Карелия': {'region': 'Карелия', 'infected': 2536, 'deaths': 14, 'recovered': 2586},
            'Томская обл.': {'region': 'Томская обл.', 'infected': 1326, 'deaths': 11, 'recovered': 1432}
        }
        self.assertDictEqual(estimated_data, data)

        stopcorona_obj.delete()
        stopcorona_obj_2.delete()

        data = RegionTransformedData.get_highest_not_null_values(False)
        estimated_data = {
            'Московская обл.': {'region': 'Московская обл.', 'infected': 42852, 'deaths': 519, 'recovered': 38174},
            'Карелия': {'region': 'Карелия', "infected": 2587, "recovered": 2649, "deaths": 15, },
            'Томская обл.': {'region': 'Томская обл.', 'infected': 1326, 'deaths': 11, 'recovered': 1432}
        }
        self.assertDictEqual(estimated_data, data)

        stopcorona_obj = StopCoronaData.objects.create(
            start_date=date(2023, 12, 22),
            end_date=date(2023, 12, 28),
            region=StopCoronaData.RUSSIAN_FEDERATION,
            hospitalized=5000,
            infected=200,
            recovered=100,
            deaths=12,
        )

        stopcorona_obj_2 = StopCoronaData.objects.create(
            start_date=date(2023, 12, 29),
            end_date=date(2024, 1, 4),
            region=StopCoronaData.RUSSIAN_FEDERATION,
            hospitalized=5000,
            infected=200,
            recovered=100,
            deaths=12,
        )

        data = RegionTransformedData.get_highest_not_null_values(True)
        estimated_data = {
            'Московская обл.': {'region': 'Московская обл.', 'infected': 42852, 'deaths': 519, 'recovered': 38174},
            'Карелия': {'region': 'Карелия', "recovered": 2586, "deaths": 14, "infected": 2536, },
            'Томская обл.': {'region': 'Томская обл.', 'infected': 1326, 'deaths': 11, 'recovered': 1432}
        }
        self.assertDictEqual(estimated_data, data)
