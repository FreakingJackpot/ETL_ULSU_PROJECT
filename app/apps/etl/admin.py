from django.contrib import admin
from apps.etl.models import StopCoronaData, CsvData,GogovData

# Register your models here.
admin.site.register(CsvData)
admin.site.register(StopCoronaData)
admin.site.register(GogovData)
