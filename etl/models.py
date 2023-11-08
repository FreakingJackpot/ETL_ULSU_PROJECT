from django.db import models


class CsvData(models.Model):
    date = models.DateField(auto_now=False)
    cases = models.IntegerField(default=True)
    deaths = models.IntegerField(default=True)
    days_14_cases_per_100000 = models.FloatField(default=True, null=True, blank=True)


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
