from django.urls import path

from .views import DecoratedTokenObtainPairView, DecoratedTokenRefreshView, DecoratedTokenVerifyView, \
    DecoratedTokenBlacklistView, DatasetsInfo, Dataset

urlpatterns = [
    path('token/', DecoratedTokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('token/refresh/', DecoratedTokenRefreshView.as_view(), name='token_refresh'),
    path('token/verify/', DecoratedTokenVerifyView.as_view(), name='token_verify'),
    path('token/blacklist/', DecoratedTokenBlacklistView.as_view(), name='token_blacklist'),
    path('datasets-info/', DatasetsInfo.as_view(), name='datasets-info'),
    path('datasets/<str:dataset>/', Dataset.as_view(), name='dataset'),
]
