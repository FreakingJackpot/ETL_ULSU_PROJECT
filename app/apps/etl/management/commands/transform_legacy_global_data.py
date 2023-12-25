from django.core.management.base import BaseCommand
from django.conf import settings

from apps.etl.utils.data_transformers.global_transformers import LegacyGlobalDataTransformer
from apps.etl.utils.mappers.transformed_data_mappers import GlobalTransformedDataMapper


class Command(BaseCommand):
    help = "transforms and writes legacy global data, data from external database, csv"

    def add_arguments(self, parser):
        parser.add_argument("debug", nargs='?', type=str, default=settings.DEBUG)

    def handle(self, *args, **options):
        data = LegacyGlobalDataTransformer.run()
        if options['debug']:
            GlobalTransformedDataMapper().map(data)
        else:
            return data
