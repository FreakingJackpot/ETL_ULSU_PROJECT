from django.contrib import admin
from etl.models import StopCoronaData, CsvData

# Register your models here.
admin.site.register(CsvData)
admin.site.register(StopCoronaData)
