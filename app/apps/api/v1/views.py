from rest_framework import generics, status,permissions
from rest_framework.response import Response
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiExample, OpenApiResponse, \
    PolymorphicProxySerializer
from drf_spectacular.types import OpenApiTypes
from rest_framework.settings import api_settings
from rest_framework_csv import renderers as r
from rest_framework_simplejwt.views import (
    TokenBlacklistView,
    TokenObtainPairView,
    TokenRefreshView,
    TokenVerifyView,
)
from rest_framework_simplejwt.authentication import JWTAuthentication

from .serializers import (TokenObtainPairResponseSerializer, TokenRefreshResponseSerializer,
                          Response401InvalidOrExpiredSerializer, Response401BlacklistedSerializer,
                          Response401NoAccountOrWrongCredentialsSerializer, DatasetInfoSerializer,
                          DatasetRequestSerializer)
from apps.api.models import DatasetInfo
from apps.etl.models import RegionTransformedData


class DecoratedTokenObtainPairView(TokenObtainPairView):
    @extend_schema(
        responses={
            status.HTTP_200_OK: TokenObtainPairResponseSerializer,
            status.HTTP_401_UNAUTHORIZED: Response401NoAccountOrWrongCredentialsSerializer,
        }
    )
    def post(self, request, *args, **kwargs):
        return super().post(request, *args, **kwargs)


class DecoratedTokenRefreshView(TokenRefreshView):
    @extend_schema(
        responses={
            status.HTTP_200_OK: TokenRefreshResponseSerializer,
            status.HTTP_401_UNAUTHORIZED: PolymorphicProxySerializer(
                component_name='MetaError', resource_type_field_name='error_type',
                serializers=[Response401InvalidOrExpiredSerializer, Response401BlacklistedSerializer]
            ),
        }
    )
    def post(self, request, *args, **kwargs):
        return super().post(request, *args, **kwargs)


class DecoratedTokenBlacklistView(TokenBlacklistView):
    @extend_schema(
        responses={
            status.HTTP_401_UNAUTHORIZED: PolymorphicProxySerializer(
                component_name='MetaError', resource_type_field_name='error_type',
                serializers=[Response401InvalidOrExpiredSerializer, Response401BlacklistedSerializer]
            ),
        },
    )
    def post(self, request, *args, **kwargs):
        return super().post(request, *args, **kwargs)


class DecoratedTokenVerifyView(TokenVerifyView):
    @extend_schema(
        responses={
            status.HTTP_401_UNAUTHORIZED: PolymorphicProxySerializer(
                component_name='MetaError', resource_type_field_name='error_type',
                serializers=[Response401InvalidOrExpiredSerializer, Response401BlacklistedSerializer]
            ),
        },
    )
    def post(self, request, *args, **kwargs):
        return super().post(request, *args, **kwargs)


class DatasetsInfo(generics.ListAPIView):
    permission_classes = [permissions.IsAuthenticated]
    authentication_classes = [JWTAuthentication, ]
    queryset = DatasetInfo.objects.all()
    serializer_class = DatasetInfoSerializer

    @extend_schema(
        responses={
            status.HTTP_200_OK: DatasetInfoSerializer(many=True),
            status.HTTP_401_UNAUTHORIZED: PolymorphicProxySerializer(
                component_name='MetaError', resource_type_field_name='error_type',
                serializers=[Response401InvalidOrExpiredSerializer, Response401BlacklistedSerializer]
            ),
        },
    )
    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)


class Dataset(generics.ListAPIView):
    renderer_classes = tuple(api_settings.DEFAULT_RENDERER_CLASSES) + (r.CSVRenderer,)
    permission_classes = [permissions.IsAuthenticated]
    authentication_classes = [JWTAuthentication, ]

    @extend_schema(
        responses={
            status.HTTP_200_OK: [{}, ],
            status.HTTP_401_UNAUTHORIZED: PolymorphicProxySerializer(
                component_name='MetaError', resource_type_field_name='error_type',
                serializers=[Response401InvalidOrExpiredSerializer, Response401BlacklistedSerializer]
            ),
        },
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
