from environs import Env

env = Env()
env.read_env()

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '[%(asctime)s] {%(module)s} [%(levelname)s] - %(message)s',
            'datefmt': '%d-%m-%Y %H:%M:%S'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
        },
        'loki': {
            'level': 'INFO',
            'class': 'logging_loki.LokiHandler',
            'url': f"http://{env.str('LOKI_HOST', 'loki')}:3100/loki/api/v1/push",
            'tags': {"app": "web", },
            'version': "1",
            'formatter': 'standard',
        },
    },
    'loggers': {
        'django': {
            'handlers': ['loki', 'console', ],
            'level': 'INFO',
            'propagate': True,
        },
    },
}
