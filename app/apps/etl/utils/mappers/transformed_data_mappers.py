from apps.etl.models import GlobalTransformedData, RegionTransformedData
from apps.etl.utils.mappers.base import BaseMapper


class GlobalTransformedDataMapper(BaseMapper):
    _model = GlobalTransformedData
    _object_key_fields = ('start_date', 'end_date',)
    _update_fields = (
        'weekly_infected', 'weekly_deaths', 'weekly_recovered', 'weekly_first_component', 'weekly_vaccinations',
        'weekly_second_component', 'infected', 'deaths', 'recovered', 'first_component', 'second_component',
        'weekly_infected_per_100000', 'weekly_deaths_per_100000', 'weekly_recovered_per_100000', 'infected_per_100000',
        'deaths_per_100000', 'recovered_per_100000', 'weekly_recovered_infected_ratio', 'weekly_deaths_infected_ratio',
        'weekly_vaccinations_infected_ratio', 'vaccinations_population_ratio',
    )

    def _serialize_key_fields(self, obj):
        return {'start_date': obj.start_date.strftime('%d-%m-%Y'), 'end_date': obj.end_date.strftime('%d-%m-%Y')}


class RegionTransformedDataMapper(BaseMapper):
    _model = RegionTransformedData
    _object_key_fields = ('start_date', 'end_date', 'region',)
    _update_fields = (
        'weekly_infected', 'weekly_deaths', 'weekly_recovered', 'weekly_infected_per_100000',
        'weekly_deaths_per_100000', 'weekly_recovered_per_100000', 'infected_per_100000', 'deaths_per_100000',
        'recovered_per_100000', 'weekly_recovered_infected_ratio', 'weekly_deaths_infected_ratio', 'infected', 'deaths',
        'recovered',
    )

    def _serialize_key_fields(self, obj):
        return {'region': obj.region,
                'start_date': obj.start_date.strftime('%d-%m-%Y'),
                'end_date': obj.end_date.strftime('%d-%m-%Y')}
