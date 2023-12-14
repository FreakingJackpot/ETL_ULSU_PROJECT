from django.db import models


class ExternalDatabaseVaccination(models.Model):
    date = models.DateField()
    total_vaccinations = models.BigIntegerField()
    people_vaccinated = models.BigIntegerField()
    people_fully_vaccinated = models.BigIntegerField()
    total_boosters = models.BigIntegerField()
    daily_vaccinations_raw = models.BigIntegerField()
    daily_vaccinations = models.BigIntegerField()
    total_vaccinations_per_hundred = models.FloatField()
    people_vaccinated_per_hundred = models.FloatField()
    people_fully_vaccinated_per_hundred = models.FloatField()
    total_boosters_per_hundred = models.FloatField()
    daily_vaccinations_per_million = models.BigIntegerField()
    daily_people_vaccinated = models.BigIntegerField()
    daily_people_vaccinated_per_hundred = models.FloatField()

    class Meta:
        managed = False

    def __str__(self):
        return f"{self.date}"

    @classmethod
    def get_all_transform_data(cls):
        return cls.objects.values('date', 'daily_people_vaccinated', 'daily_vaccinations')


class ExternalDatabaseStatistic(models.Model):
    date = models.DateField()
    region = models.CharField(max_length=255)
    infection = models.BigIntegerField()
    recovery = models.BigIntegerField()
    death = models.BigIntegerField()
    death_per_day = models.IntegerField()
    infection_per_day = models.IntegerField()
    recovery_per_day = models.IntegerField()

    class Meta:
        managed = False

    def __str__(self):
        return f"{self.date} - {self.region}"

    @classmethod
    def get_all_transform_data(cls):
        return cls.objects.values('date', 'death_per_day', 'infection_per_day', 'recovery_per_day')


class CsvData(models.Model):
    date = models.DateField(auto_now=False, unique=True)
    cases = models.IntegerField(null=True)
    deaths = models.IntegerField(null=True)
    per_100000_cases_for_2_weeks = models.FloatField(null=True, blank=True)

    @classmethod
    def get_all_transform_data(cls):
        return cls.objects.values('date', 'cases', 'deaths')


class StopCoronaData(models.Model):
    start_date = models.DateField()
    end_date = models.DateField()
    region = models.CharField(max_length=255)
    hospitalized = models.IntegerField()
    infected = models.IntegerField()
    recovered = models.IntegerField()
    deaths = models.IntegerField()

    class Meta:
        unique_together = ('start_date', 'end_date', 'region')

    def __str__(self):
        return f"{self.region}: {self.start_date} - {self.end_date}"

    @classmethod
    def get_transform_global_data(cls, latest):
        query = cls.objects.filter(region='Российская Федерация')
        if latest:
            object = query.latest('end_date')
            data = [{'start-date': object.start_date,
                     'end_date': object.end_date,
                     'infected': object.infected,
                     'recovered': object.recovered,
                     'deaths': object.deaths, }]
        else:
            data = cls.objects.filter(region='Российская Федерация').values('start_date', 'end_date',
                                                                            'infected', 'recovered', 'deaths')

        return data


class GogovGlobalData(models.Model):
    date = models.DateField(unique=True)
    first_component = models.IntegerField()
    full_vaccinated = models.IntegerField()
    children_vaccinated = models.IntegerField()
    revaccinated = models.IntegerField()
    need_revaccination = models.IntegerField()

    @classmethod
    def get_transform_data(cls, start_date, end_date):
        query = cls.objects.values('date', 'first_component', 'full_vaccinated')
        if start_date and end_date:
            query = query.filter(date__gte=start_date, date__lte=end_date)
        return query


class GlobalTransformedData(models.Model):
    start_date = models.DateField(unique=True)
    end_date = models.DateField(unique=True)
    weekly_infected = models.IntegerField(null=True, blank=True)
    weekly_deaths = models.IntegerField(null=True, blank=True)
    weekly_recovered = models.IntegerField(null=True, blank=True)
    infected = models.IntegerField(null=True, blank=True)
    deaths = models.IntegerField(null=True, blank=True)
    recovered = models.IntegerField(null=True, blank=True)
    first_component = models.IntegerField(null=True, blank=True)
    second_component = models.IntegerField(null=True, blank=True)
    weekly_infected_per_100000 = models.FloatField(null=True, blank=True)
    weekly_deaths_per_100000 = models.FloatField(null=True, blank=True)
    weekly_recovered_per_100000 = models.FloatField(null=True, blank=True)
    infected_per_100000 = models.FloatField(null=True, blank=True)
    deaths_per_100000 = models.FloatField(null=True, blank=True)
    recovered_per_100000 = models.FloatField(null=True, blank=True)
    weekly_recovered_infected_ratio = models.FloatField(null=True, blank=True)
    weekly_deaths_infected_ratio = models.FloatField(null=True, blank=True)
    weekly_vaccinations_infected_ratio = models.FloatField(null=True, blank=True)
    vaccinations_population_ratio = models.FloatField(null=True, blank=True)
