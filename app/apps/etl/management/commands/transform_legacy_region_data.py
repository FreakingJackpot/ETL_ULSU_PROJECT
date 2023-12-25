from django.core.management.base import BaseCommand
from django.conf import settings

from apps.etl.utils.data_transformers.regional_transformers import LegacyRegionDataTransformer
from apps.etl.utils.mappers.transformed_data_mappers import RegionTransformedDataMapper


class Command(BaseCommand):
    help = "transforms and write legacy region data, data from external database"

    def add_arguments(self, parser):
        parser.add_argument("debug", nargs='?', type=str, default=settings.DEBUG)

    def handle(self, *args, **options):
        data = LegacyRegionDataTransformer.run()
        if options['debug']:
            RegionTransformedDataMapper().map(data)
        else:
            return data
