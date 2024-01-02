from django.urls import path
from rest_framework_simplejwt.views import (
    TokenObtainPairView,
    TokenRefreshView,
    TokenVerifyView,
)

from .views import PermittedTokenBlacklistView, DatasetsInfo, Regions, Dataset

urlpatterns = [
    path('token/', TokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
    path('token/verify/', TokenVerifyView.as_view(), name='token_verify'),
    path('token/blacklist/', PermittedTokenBlacklistView.as_view(), name='token_blacklist'),
    path('datasets-info/', DatasetsInfo.as_view(), name='datasets-info'),
    path('regions/', Regions.as_view(), name='regions'),
    path('datasets/<str:dataset>/', Dataset.as_view(), name='dataset'),
]
