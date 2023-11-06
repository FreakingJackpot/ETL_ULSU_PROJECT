from django.db import models

class Model(models.Model):
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
    region = models.CharField(max_length=255)
    infection = models.BigIntegerField()
    recovery = models.BigIntegerField()
    death = models.BigIntegerField()
    death_per_day = models.IntegerField()
    infection_per_day = models.IntegerField()
    recovery_per_day = models.IntegerField()

    def __str__(self):
        return f"{self.date} - {self.region}"
