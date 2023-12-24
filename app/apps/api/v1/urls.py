from django.urls import path
from .views import LoginAPI, DatasetsInfo, Dataset

urlpatterns = [
    path('login/', LoginAPI.as_view(), name='login'),
    path('datasets-info/', DatasetsInfo.as_view(), name='datasets-info'),
    path('datasets/<str:dataset>/', Dataset.as_view(), name='dataset'),

]
