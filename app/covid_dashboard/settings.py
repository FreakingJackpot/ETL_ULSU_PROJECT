from pathlib import Path

from environs import Env

env = Env()
env.read_env()

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = env.str("DJANGO_SECRET_KEY")

DEBUG = env.bool("DJANGO_DEBUG")

ALLOWED_HOSTS = env.list("DJANGO_ALLOWED_HOSTS")

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    'django_prometheus',
    'apps.etl',
    'apps.api',
    'drf_spectacular',
    'drf_spectacular_sidecar',
    'rest_framework_simplejwt',
    'rest_framework_simplejwt.token_blacklist',
]

MIDDLEWARE = [
    'django_prometheus.middleware.PrometheusBeforeMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'django_prometheus.middleware.PrometheusAfterMiddleware',
]

ROOT_URLCONF = 'covid_dashboard.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / 'templates', ],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'covid_dashboard.wsgi.application'

DATABASES = {
    'default': {
        'ENGINE': 'django_prometheus.db.backends.postgresql',
        "NAME": env.str("DB_NAME", "postgres"),
        "USER": env.str("DB_USER", "postgres"),
        "PASSWORD": env.str("DB_PASS", "postgres"),
        "HOST": env.str("DB_HOST", "localhost"),
        "PORT": env.str("DB_PORT", "5432"),
    },
    'external_covid': {
        'ENGINE': 'django_prometheus.db.backends.postgresql',
        "NAME": env.str("EXTERNAL_DB_NAME", "external_covid"),
        "USER": env.str("EXTERNAL_DB_USER", "postgres"),
        "PASSWORD": env.str("EXTERNAL_DB_PASS", "postgres"),
        "HOST": env.str("EXTERNAL_DB_HOST", "localhost"),
        "PORT": env.str("EXTERNAL_DB_PORT", "5433"),
    }
}

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

REST_FRAMEWORK = {
    'DEFAULT_SCHEMA_CLASS': 'drf_spectacular.openapi.AutoSchema',
    'DEFAULT_RENDERER_CLASSES': [
        'rest_framework.renderers.JSONRenderer',
        'rest_framework_xml.renderers.XMLRenderer',
    ],
}

SPECTACULAR_SETTINGS = {
    'TITLE': 'Covid dashboard API',
    'DESCRIPTION': 'API provides covid-19 data for BI systems',
    'VERSION': '1.0.0',
    'SERVE_INCLUDE_SCHEMA': False,
    'SWAGGER_UI_DIST': 'SIDECAR',  # shorthand to use the sidecar instead
    'SWAGGER_UI_FAVICON_HREF': 'SIDECAR',
    'REDOC_DIST': 'SIDECAR',
}

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_TZ = True

STATIC_URL = 'static/'
STATIC_ROOT = BASE_DIR / 'static'

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

CSRF_TRUSTED_ORIGINS = env.list("DJANGO_CSRF_TRUSTED_ORIGINS")
#
# LOGGING = {
#     'version': 1,
#     "disable_existing_loggers": False,
#     'formatters': {
#         'loki': {
#             'class': 'django_loki.LokiFormatter',
#             'format': '[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] [%(funcName)s] %(message)s',
#             'datefmt': '%d-%m-%Y %H:%M:%S',
#         },
#     },
#     'handlers': {
#         'loki': {
#             'level': 'DEBUG',
#             'class': 'django_loki.LokiHttpHandler',
#             'host': env.str("LOKI_HOST", "loki"),
#             'formatter': 'loki',
#             'src_host': 'web',
#         },
#     },
#     'loggers': {
#         'django': {
#             'handlers': ['loki', ],
#             'level': 'DEBUG',
#             'propagate': False,
#         },
#     }
# }

DEFAULT_CSV_PATH = str(BASE_DIR.joinpath('apps/etl/data/data.csv'))
STOPCORONA_URL_BASE = 'https://xn--90aivcdt6dxbc.xn--p1ai/{}'
STOPCORONA_URL_ARTICLES_PAGE = STOPCORONA_URL_BASE.format('stopkoronavirus/?isAjax=Y&action=itemsMore&PAGEN_1={}')

GOGOV_URL = 'https://gogov.ru/articles/covid-v-stats'
