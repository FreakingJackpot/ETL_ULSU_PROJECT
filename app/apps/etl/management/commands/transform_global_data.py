from django.core.management.base import BaseCommand
from django.conf import settings


from apps.etl.utils.data_transformers.global_transformers import GlobalDataTransformer
from apps.etl.utils.mappers.transformed_data_mappers import GlobalTransformedDataMapper


class Command(BaseCommand):
    help = "transforms and writes global data, data from stopcorona and gogov"

    def add_arguments(self, parser):
        parser.add_argument("latest", nargs='?', type=str, default=0)
        parser.add_argument("debug", nargs='?', type=str, default=settings.DEBUG)

    def handle(self, *args, **options):
        data = GlobalDataTransformer(options['latest']).run()
        if options['debug']:
            GlobalTransformedDataMapper().map(data)
        else:
            return data
