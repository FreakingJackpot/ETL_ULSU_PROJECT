from django.db import models
class Virus(models.Model):
    dateRep = models.DateField(auto_now= False)
    cases = models.IntegerField(default=True)
    deaths = models.IntegerField(default=True)
    Cumulative_number_for_14_days_of_COVID_19_cases_per_100000 = models.FloatField(default=True, null=True, blank=True)
