from apps.etl.utils.mappers.base import BaseMapper
from apps.etl.models import Population


class PopulationMapper:
    _model = Population
    _object_key_fields = ('year', 'region',)
    _batch_size = 100
    _update_fields = ('population',)
