import logging
from abc import ABC

from django.db.models import Model

from apps.etl.utils.logging import get_task_logger


class BaseMapper(ABC):
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
