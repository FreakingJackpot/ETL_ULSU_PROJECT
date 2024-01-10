import logging
from datetime import datetime
from abc import ABC

from django.db.models import Model

from apps.etl.models import GlobalTransformedData, RegionTransformedData
from apps.etl.utils.logging import get_task_logger


class TransformedDataMapperBase(ABC):
    _model = Model
    _object_key_fields = ()
    _batch_size = 500
    _update_fields = ()

    def __init__(self):
        self.existing_objects = {self._get_item_key(obj): obj for obj in self._model.objects.all()}
        self.logger = get_task_logger()

    def map(self, data):
        insert, update = self._split_data(data)

        if insert:
            self._model.objects.bulk_create(insert, batch_size=self._batch_size)
            self._log_update(f'Inserted {self._model.__name__}', insert)

        if update:
            self._model.objects.bulk_update(update, fields=self._update_fields, batch_size=self._batch_size)
            self._log_update(f'Updated {self._model.__name__}', update)

    def _split_data(self, data):
        insert = []
        update = []

        for item in data:
            key = self._get_item_key(item)
            existing_obj = self.existing_objects.get(key)

            if existing_obj:
                changed = self._update_existing(existing_obj, item)
                if changed:
                    update.append(existing_obj)
            else:
                insert.append(self._model(**item))

        return insert, update

    def _get_item_key(self, item):
        return tuple(
            getattr(item, key) if isinstance(item, self._model) else item[key] for key in self._object_key_fields
        )

    def _update_existing(self, existing_obj, item):
        is_changed = False
        for key, value in item.items():
            if key not in self._object_key_fields and getattr(existing_obj, key) != value:
                setattr(existing_obj, key, value)
                is_changed = True

        return is_changed

    def _log_update(self, message, objects):
        for obj in objects:

            log_data = {}
            for field in self._update_fields:
                log_data[field] = getattr(obj, field)

            log_data.update(self._serialize_key_fields(obj))

            self.logger.log(logging.INFO, message, **log_data)

    def _serialize_key_fields(self, obj):
        raise NotImplemented


class GlobalTransformedDataMapper(TransformedDataMapperBase):
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


class RegionTransformedDataMapper(TransformedDataMapperBase):
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
