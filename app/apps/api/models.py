from django.db import models

from apps.etl.models import GlobalTransformedData, RegionTransformedData


class DatasetInfo(models.Model):
    DATASETS = {
        'global_data': GlobalTransformedData,
        'region_data': RegionTransformedData,
    }
    dataset_name = models.TextField(unique=True)
    description = models.JSONField()
