from django.core.management.base import BaseCommand
from django.conf import settings

from apps.etl.utils.data_transformers.regional_transformers import RegionDataTransformer
from apps.etl.utils.mappers.transformed_data_mappers import RegionTransformedDataMapper


class Command(BaseCommand):
    help = "transforms and writes region data, data from stopcorona"

    def add_arguments(self, parser):
        parser.add_argument("latest", nargs='?', type=str, default=0)
        parser.add_argument("debug", nargs='?', type=str, default=settings.DEBUG)

    def handle(self, *args, **options):
        data = RegionDataTransformer(options['latest']).run()
        if options['debug']:
            RegionTransformedDataMapper().map(data)
        else:
            return data
