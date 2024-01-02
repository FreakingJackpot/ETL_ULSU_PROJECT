from django.db import models
from django.db.models import Max


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
        db_table = 'vactination'

    def __str__(self):
        return f"{self.date}"

    @classmethod
    def get_all_transform_data(cls):
        return cls.objects.using('external_covid').values('date', 'daily_people_vaccinated', 'daily_vaccinations')


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
        db_table = 'covid'

    def __str__(self):
        return f"{self.date} - {self.region}"

    @classmethod
    def get_all_transform_data(cls, with_region=False):
        values_list = ['date', 'death_per_day', 'infection_per_day', 'recovery_per_day']
        if with_region:
            values_list.append('region')

        return cls.objects.using('external_covid').values(*values_list)


class CsvData(models.Model):
    date = models.DateField(auto_now=False, unique=True)
    cases = models.IntegerField(null=True)
    deaths = models.IntegerField(null=True)
    per_100000_cases_for_2_weeks = models.FloatField(null=True, blank=True)

    @classmethod
    def get_all_transform_data(cls):
        return cls.objects.values('date', 'cases', 'deaths')


class StopCoronaData(models.Model):
    RUSSIAN_FEDERATION = 'Российская Федерация'

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
    def get_global_transform_data(cls, latest):
        query = cls.objects.filter(region=cls.RUSSIAN_FEDERATION)
        if latest:
            object = query.latest('start_date')
            data = [{'start_date': object.start_date,
                     'end_date': object.end_date,
                     'infected': object.infected,
                     'recovered': object.recovered,
                     'deaths': object.deaths, }]
        else:
            data = query.values('start_date', 'end_date', 'infected', 'recovered', 'deaths').order_by('start_date')

        return data

    @classmethod
    def get_transform_region_data(cls, latest):
        query = cls.objects.exclude(region=cls.RUSSIAN_FEDERATION)
        if latest:
            latest_date = query.aggregate(latest_date=Max('start_date'))['latest_date']
            query = query.filter(start_date=latest_date)

        return query.order_by('start_date').values('start_date', 'end_date', 'region', 'infected', 'recovered',
                                                   'deaths')


class GogovData(models.Model):
    date = models.DateField(unique=True)
    first_component = models.IntegerField()
    second_component = models.IntegerField()

    @classmethod
    def get_transform_data(cls, start_date, end_date):
        query = cls.objects.values('date', 'first_component', 'second_component')
        if start_date and end_date:
            query = query.filter(date__gte=start_date, date__lte=end_date)
        return query.order_by('date')


class GlobalTransformedData(models.Model):
    start_date = models.DateField()
    end_date = models.DateField()
    weekly_infected = models.IntegerField(null=True, blank=True)
    weekly_deaths = models.IntegerField(null=True, blank=True)
    weekly_recovered = models.IntegerField(null=True, blank=True)
    weekly_first_component = models.IntegerField(null=True, blank=True)
    weekly_vaccinations = models.IntegerField(null=True, blank=True)
    weekly_second_component = models.IntegerField(null=True, blank=True)
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

    class Meta:
        unique_together = ['start_date', 'end_date', ]

    @classmethod
    def get_highest_not_null_values(cls, latest):
        vaccinations_queryset = cls.objects
        main_stats_queryset = cls.objects
        gogov_earliest = GogovData.objects.earliest('date')
        stopcorona_earliest = StopCoronaData.objects.earliest('start_date')

        if not latest:
            if gogov_earliest:
                vaccinations_queryset = vaccinations_queryset.filter(end_date__lt=gogov_earliest.date)
            if stopcorona_earliest:
                main_stats_queryset = main_stats_queryset.filter(end_date__lt=stopcorona_earliest.start_date)

        data = main_stats_queryset.aggregate(infected=Max('infected'), deaths=Max('deaths'), recovered=Max('recovered'))
        vaccinations_data = vaccinations_queryset.aggregate(first_component=Max('first_component'),
                                                            second_component=Max('second_component'))
        data.update(vaccinations_data)

        return data


class RegionTransformedData(models.Model):
    start_date = models.DateField()
    end_date = models.DateField()
    region = models.TextField()
    weekly_infected = models.IntegerField(null=True, blank=True)
    weekly_deaths = models.IntegerField(null=True, blank=True)
    weekly_recovered = models.IntegerField(null=True, blank=True)
    infected = models.IntegerField(null=True, blank=True)
    deaths = models.IntegerField(null=True, blank=True)
    recovered = models.IntegerField(null=True, blank=True)
    weekly_infected_per_100000 = models.FloatField(null=True, blank=True)
    weekly_deaths_per_100000 = models.FloatField(null=True, blank=True)
    weekly_recovered_per_100000 = models.FloatField(null=True, blank=True)
    infected_per_100000 = models.FloatField(null=True, blank=True)
    deaths_per_100000 = models.FloatField(null=True, blank=True)
    recovered_per_100000 = models.FloatField(null=True, blank=True)
    weekly_recovered_infected_ratio = models.FloatField(null=True, blank=True)
    weekly_deaths_infected_ratio = models.FloatField(null=True, blank=True)

    class Meta:
        unique_together = ['start_date', 'end_date', 'region']

    @classmethod
    def get_latest_data_map(cls):
        items = (cls.objects
                 .values('region')
                 .annotate(infected=Max('infected'), deaths=Max('deaths'), recovered=Max('recovered'))
                 )
        return {itm['region']: itm for itm in items}


class Region(models.Model):
    name = models.TextField()
