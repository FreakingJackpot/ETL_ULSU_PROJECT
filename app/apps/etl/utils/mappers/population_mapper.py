from apps.etl.utils.mappers.base import BaseMapper
from apps.etl.models import Population


class PopulationMapper(BaseMapper):
    _model = Population
    _object_key_fields = ('year', 'region',)
    _batch_size = 100
    _update_fields = ('population',)

    def _serialize_key_fields(self, obj):
        return {'year': obj.year, 'region': obj.region, }
