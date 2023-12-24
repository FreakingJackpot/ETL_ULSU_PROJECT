from django.core.management.base import BaseCommand

from apps.etl.utils.data_transformers.global_transformers import LegacyGlobalDataTransformer
from apps.etl.utils.mappers.transformed_data_mappers import GlobalTransformedDataMapper


class Command(BaseCommand):
    help = "transforms and writes legacy global data, data from external database, csv"

    def handle(self, *args, **options):
        data = LegacyGlobalDataTransformer.run()
        GlobalTransformedDataMapper().map(data)
