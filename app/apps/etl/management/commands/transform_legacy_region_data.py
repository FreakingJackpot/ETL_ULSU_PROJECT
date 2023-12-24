from django.core.management.base import BaseCommand

from apps.etl.utils.data_transformers.regional_transformers import LegacyRegionDataTransformer
from apps.etl.utils.mappers.transformed_data_mappers import RegionTransformedDataMapper


class Command(BaseCommand):
    help = "transforms and write legacy region data, data from external database"

    def handle(self, *args, **options):
        data = LegacyRegionDataTransformer.run()
        RegionTransformedDataMapper().map(data)
