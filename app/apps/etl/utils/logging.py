import logging
import json

from django.conf import settings


def get_task_logger():
    return Logger(logging.getLogger('django')) if settings.DEBUG else Logger(logging.getLogger('task'))


class Logger:
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def log(self, level, msg, **extra):
        dict_message = {
            'message': msg,
            **extra,
        }
        self.logger.log(level, dict_message)
