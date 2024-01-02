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

        for region in regions:
            Region.objects.create(name=region)
