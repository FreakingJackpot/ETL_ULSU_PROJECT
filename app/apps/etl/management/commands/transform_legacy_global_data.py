from django.core.management.base import BaseCommand
from django.db import DatabaseError

from apps.etl.models import StopCoronaData
from apps.etl.utils.data_transformers.global_transformers import LegacyGlobalDataTransformer
from apps.etl.utils.parsers.stopcorona_parser import StopCoronaParser


class Command(BaseCommand):
    help = "imports data from объясняем.рф/stopcoronavirus"

    def handle(self, *args, **options):
        kek = LegacyGlobalDataTransformer.run()
        print(1)
