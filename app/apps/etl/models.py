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


class CsvData(models.Model):
    date = models.DateField(auto_now=False, unique=True)
    cases = models.IntegerField(null=True)
    deaths = models.IntegerField(null=True)
    per_100000_cases_for_2_weeks = models.FloatField(null=True, blank=True)


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


class GogovGlobalData(models.Model):
    date = models.DateField(unique=True)
    first_component = models.IntegerField()
    full_vaccinated = models.IntegerField()
    children_vaccinated = models.IntegerField()
    revaccinated = models.IntegerField()
    need_revaccination = models.IntegerField()
