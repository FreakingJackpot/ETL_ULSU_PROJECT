class ExternalDatabaseStatisticMock:
    @classmethod
    def get_all_transform_data(cls, with_region=False):
        mock = [
            {
                "date": "2020-12-13",
                "region": "Карелия",
                "death_per_day": 3,
                "infection_per_day": 417,
                "recovery_per_day": 302
            },
            {
                "date": "2020-12-13",
                "region": "Москва",
                "death_per_day": 72,
                "infection_per_day": 6425,
                "recovery_per_day": 4841
            },
            {
                "date": "2020-12-13",
                "region": "Томская обл.",
                "death_per_day": 0,
                "infection_per_day": 189,
                "recovery_per_day": 205
            },
            {
                "date": "2020-12-14",
                "region": "Томская обл.",
                "death_per_day": 4,
                "infection_per_day": 187,
                "recovery_per_day": 188
            },
            {
                "date": "2020-12-14",
                "region": "Москва",
                "death_per_day": 75,
                "infection_per_day": 5874,
                "recovery_per_day": 4932
            },
            {
                "date": "2020-12-14",
                "region": "Карелия",
                "death_per_day": 0,
                "infection_per_day": 369,
                "recovery_per_day": 170
            },
            {
                "date": "2020-12-15",
                "region": "Москва",
                "death_per_day": 77,
                "infection_per_day": 5418,
                "recovery_per_day": 5307
            },
            {
                "date": "2020-12-15",
                "region": "Томская обл.",
                "death_per_day": 0,
                "infection_per_day": 195,
                "recovery_per_day": 194
            },
            {
                "date": "2020-12-15",
                "region": "Карелия",
                "death_per_day": 2,
                "infection_per_day": 324,
                "recovery_per_day": 453
            },
            {
                "date": "2020-12-16",
                "region": "Москва",
                "death_per_day": 73,
                "infection_per_day": 5028,
                "recovery_per_day": 5571
            },
            {
                "date": "2020-12-16",
                "region": "Томская обл.",
                "death_per_day": 4,
                "infection_per_day": 190,
                "recovery_per_day": 215
            },
            {
                "date": "2020-12-16",
                "region": "Карелия",
                "death_per_day": 3,
                "infection_per_day": 340,
                "recovery_per_day": 418
            },
            {
                "date": "2020-12-17",
                "region": "Карелия",
                "death_per_day": 1,
                "infection_per_day": 341,
                "recovery_per_day": 407
            },
            {
                "date": "2020-12-17",
                "region": "Москва",
                "death_per_day": 76,
                "infection_per_day": 6711,
                "recovery_per_day": 5777
            },
            {
                "date": "2020-12-17",
                "region": "Томская обл.",
                "death_per_day": 0,
                "infection_per_day": 185,
                "recovery_per_day": 224
            },
            {
                "date": "2020-12-18",
                "region": "Москва",
                "death_per_day": 72,
                "infection_per_day": 6937,
                "recovery_per_day": 5821
            },
            {
                "date": "2020-12-18",
                "region": "Томская обл.",
                "death_per_day": 3,
                "infection_per_day": 187,
                "recovery_per_day": 218
            },
            {
                "date": "2020-12-18",
                "region": "Карелия",
                "death_per_day": 3,
                "infection_per_day": 351,
                "recovery_per_day": 416
            },
            {
                "date": "2020-12-19",
                "region": "Москва",
                "death_per_day": 74,
                "infection_per_day": 6459,
                "recovery_per_day": 5925
            },
            {
                "date": "2020-12-19",
                "region": "Томская обл.",
                "death_per_day": 0,
                "infection_per_day": 193,
                "recovery_per_day": 188
            },
            {
                "date": "2020-12-19",
                "region": "Карелия",
                "death_per_day": 2,
                "infection_per_day": 394,
                "recovery_per_day": 420
            }
        ]

        if not with_region:
            for item in mock:
                item.pop('region')

        return mock


class ExternalDatabaseVaccinationMock:
    @classmethod
    def get_all_transform_data(cls):
        mock = [
            {
                "date": "2020-12-17",
                "daily_vaccinations": 243233,
                "daily_people_vaccinated": 109873,
            },
            {
                "date": "2020-12-18",
                "daily_vaccinations": 223501,
                "daily_people_vaccinated": 111925,
            },
            {
                "date": "2020-12-19",
                "daily_vaccinations": 246755,
                "daily_people_vaccinated": 128885,
            },
            {
                "date": "2020-12-20",
                "daily_vaccinations": 223730,
                "daily_people_vaccinated": 110612,
            },
            {
                "date": "2020-12-21",
                "daily_vaccinations": 206825,
                "daily_people_vaccinated": 97514,
            },
            {
                "date": "2020-12-22",
                "daily_vaccinations": 214324,
                "daily_people_vaccinated": 100392,
            },
            {
                "date": "2020-12-23",
                "daily_vaccinations": 234088,
                "daily_people_vaccinated": 124994,
            },
        ]

        return mock
