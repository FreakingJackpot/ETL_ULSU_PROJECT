from django.urls import path
from .views import LoginAPI

urlpatterns = [
    path('api/login/', LoginAPI.as_view(), name='login'),
]
