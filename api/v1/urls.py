from django.urls import path
from .views import LoginAPI

urlpatterns = [
    path('v1/login/', LoginAPI.as_view(), name='login'),
]
