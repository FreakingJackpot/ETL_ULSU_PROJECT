import csv
import pickle
from datetime import datetime

from django.conf import settings
from django.core.management.base import BaseCommand

from apps.etl.models import Region


class Command(BaseCommand):

    def handle(self, *args, **options):
        with open(settings.REGIONS_PATH, 'rb') as f:
            regions = pickle.load(f)

        existing_regions = set(Region.objects.filter(name__in=regions).values_list('name', flat=True))
        for region in filter(lambda x: x not in existing_regions, regions):
            Region.objects.create(name=region)
