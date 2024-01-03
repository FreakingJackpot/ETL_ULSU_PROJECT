from django.db import models

from apps.etl.models import GlobalTransformedData, RegionTransformedData

INVALID = 'INVL'


class DatasetInfo(models.Model):
    MODEL_NAME_CHOICES = (
        (INVALID, 'Invalid'),
        ('GlobalTransformedData', 'GlobalTransformedData'),
        ('RegionTransformedData', 'RegionTransformedData'),
    )
    dataset_name = models.TextField(unique=True)
    description = models.JSONField()
    model_name = models.TextField(choices=MODEL_NAME_CHOICES, default='INVL')

    class Meta:
        unique_together = ['dataset_name', 'model_name', ]
