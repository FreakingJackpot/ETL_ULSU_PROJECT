from rest_framework import generics
from rest_framework.response import Response
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiExample
from drf_spectacular.types import OpenApiTypes
from rest_framework.settings import api_settings
from rest_framework_csv import renderers as r
from knox.models import AuthToken
from knox.auth import TokenAuthentication
from django.contrib.auth.signals import user_logged_in

from .serializers import UserSerializer, DatasetInfoSerializer, DatasetRequestSerializer
from apps.api.models import DatasetInfo
from apps.etl.models import RegionTransformedData


class LoginAPI(generics.GenericAPIView):
    serializer_class = UserSerializer
    authentication = [TokenAuthentication]

    def post(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.validated_data
        AuthToken.objects.filter(user=user).delete()  # удаление старых токенов
        _, token = AuthToken.objects.create(user)
        user_logged_in.send(sender=user.__class__, request=request, user=user)  # отправка о входе
        return Response({
            "user": UserSerializer(user, context=self.get_serializer_context()).data,
            "token": token
        })


class DatasetsInfo(generics.ListAPIView):
    queryset = DatasetInfo.objects.all()
    serializer_class = DatasetInfoSerializer


class Dataset(generics.ListAPIView):
    renderer_classes = (r.CSVRenderer,) + tuple(api_settings.DEFAULT_RENDERER_CLASSES)

    @extend_schema(
        responses={200: [{}, ]},
        parameters=[
            OpenApiParameter(name='fields', description='comma separated list of fields', required=False, type=str),
            OpenApiParameter(name='regions', description='comma separated list of regions', required=False, type=str),
            OpenApiParameter(name='start_date', description='start_date filter, values are greater than specified',
                             required=False, type=OpenApiTypes.DATE, default='2020-08-23'),
            OpenApiParameter(name='end_date', description='end_date filter, values are below than specified',
                             required=False, type=OpenApiTypes.DATE, default='2023-08-23')

        ]
    )
    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)

    def list(self, request, *args, **kwargs):
        data = list(self.get_queryset())
        return Response(data)

    def get_queryset(self):
        params = self.request.query_params.dict()
        params.update(self.kwargs)
        params['regions'] = params['regions'].split(',') if 'regions' in params else []
        params['fields'] = params['fields'].split(',') if 'fields' in params else []

        serializer = DatasetRequestSerializer(data=params)
        serializer.is_valid(raise_exception=True)

        model = DatasetInfo.DATASETS[serializer.validated_data['dataset']]
        queryset = model.objects

        if model is RegionTransformedData and serializer.validated_data['regions']:
            queryset = queryset.filter(region__in=serializer.validated_data['regions'])

        start_date = serializer.validated_data.get('start_date')
        if start_date:
            queryset = queryset.filter(start_date__gt=start_date)

        end_date = serializer.validated_data.get('end_date')
        if end_date:
            queryset = queryset.filter(end_date__lt=end_date)

        return queryset.values(*serializer.validated_data['fields'])
