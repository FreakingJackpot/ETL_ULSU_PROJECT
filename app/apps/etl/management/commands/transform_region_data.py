from django.core.management.base import BaseCommand

from apps.etl.utils.data_transformers.regional_transformers import RegionDataTransformer
from apps.etl.utils.mappers.transformed_data_mappers import RegionTransformedDataMapper


class Command(BaseCommand):
    help = "transforms and writes region data, data from stopcorona"

    def add_arguments(self, parser):
        parser.add_argument("latest", nargs='?', type=str, default=0)

    def handle(self, *args, **options):
        data = RegionDataTransformer(options['latest']).run()
        RegionTransformedDataMapper().map(data)
