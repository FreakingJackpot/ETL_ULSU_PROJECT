import csv
import pickle
import logging
from datetime import datetime

from django.conf import settings
from django.core.management.base import BaseCommand

from apps.etl.models import Region
from apps.etl.utils.logging import get_task_logger


class Command(BaseCommand):

    def handle(self, *args, **options):
        logger = get_task_logger()

        with open(settings.REGIONS_PATH, 'rb') as f:
            regions = pickle.load(f)

        existing_regions = set(Region.objects.filter(name__in=regions).values_list('name', flat=True))
        for region in filter(lambda x: x not in existing_regions, regions):
            obj = Region.objects.create(name=region)
            logger.log(logging.INFO, 'Region inserted', region_id=obj.id, name=obj.name)
