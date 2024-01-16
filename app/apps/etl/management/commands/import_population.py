from datetime import timedelta
import requests
from difflib import SequenceMatcher

from django.core.management.base import BaseCommand

from apps.etl.models import Population, Region
from apps.etl.utils.parsers.population_parser import PopulationParser
from apps.etl.utils.data_transformers.population_transformer import PopulationTransformer
from apps.etl.utils.mappers.population_mapper import PopulationMapper

import time


class Command(BaseCommand):
    help = "parse, transform, insert/update population data from rosstat"

    def add_arguments(self, parser):
        parser.add_argument("all", type=int, help='0-latest information, 1-full available information', choices=(0, 1),
                            default=0, nargs='?')

    def handle(self, *args, **options):
        data = PopulationParser(options['all']).get_parsed_data()
        data = PopulationTransformer().run(data)
        PopulationMapper().map(data)
