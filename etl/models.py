from django.db import models
class CsvData(models.Model):
    date = models.DateField(auto_now=True)
    cases = models.IntegerField(default=True)
    deaths = models.IntegerField(default=True)
    days_14_cases_per_100000 = models.FloatField(default=True, null=True, blank=True)

    #def __str__(self):
    #    return str(self.date)



