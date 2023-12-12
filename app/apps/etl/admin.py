from django.contrib import admin
from apps.etl.models import StopCoronaData, CsvData,GogovRegionData,GogovGlobalData,ExternalDatabaseStatistic,ExternalDatabaseVaccination

# Register your models here.
admin.site.register(CsvData)
admin.site.register(StopCoronaData)
admin.site.register(GogovGlobalData)
admin.site.register(GogovRegionData)
