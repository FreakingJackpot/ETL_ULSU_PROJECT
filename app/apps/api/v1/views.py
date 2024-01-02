from rest_framework import generics, status, permissions
from rest_framework.response import Response
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiExample
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

from .serializers import (DatasetInfoSerializer, DatasetParamsSerializer,
                          DatasetparamsValidationErrorResponseSerializer)
from apps.api.models import DatasetInfo
from apps.api.paginators import LargeResultsSetPagination
from apps.etl.models import RegionTransformedData


class PermittedTokenBlacklistView(TokenBlacklistView):
    authentication_classes = [JWTAuthentication, ]
    permission_classes = [permissions.IsAdminUser, ]

    def post(self, request, *args, **kwargs):
        return super().post(request, *args, **kwargs)


class DatasetsInfo(generics.ListAPIView):
    """Returns lists of datasets infos"""
    permission_classes = [permissions.IsAuthenticated]
    authentication_classes = [JWTAuthentication, ]
    queryset = DatasetInfo.objects.all()
    serializer_class = DatasetInfoSerializer

    @extend_schema(
        responses={
            status.HTTP_200_OK: DatasetInfoSerializer(many=True),
        },
    )
    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)


class Dataset(generics.ListAPIView):
    """Returns dataset by name."""
    renderer_classes = tuple(api_settings.DEFAULT_RENDERER_CLASSES) + (r.CSVRenderer,)
    permission_classes = [permissions.IsAuthenticated]
    authentication_classes = [JWTAuthentication, ]
    pagination_class = LargeResultsSetPagination
    serializer_class = DatasetParamsSerializer

    @extend_schema(
        responses={status.HTTP_200_OK: [{}, ],
                   status.HTTP_400_BAD_REQUEST: DatasetparamsValidationErrorResponseSerializer(),
                   },
        parameters=[
            OpenApiParameter(name='page', required=False, type=OpenApiTypes.INT, default=1),
            OpenApiParameter(name='page_size', required=False, type=OpenApiTypes.INT, default=100),
            OpenApiParameter(name='all',
                             description="return all dataset items, next, previous don't appear in response",
                             required=False, type=OpenApiTypes.BOOL, default=False, ),
            OpenApiParameter(name='fields', description='comma separated list of fields', required=False,
                             type=OpenApiTypes.STR),
            OpenApiParameter(name='regions', description='comma separated list of regions', required=False,
                             type=OpenApiTypes.STR),
            OpenApiParameter(name='start_date', description='start_date filter, values are greater than specified',
                             required=False, type=OpenApiTypes.DATE,
                             examples=[OpenApiExample(name='non legacy data example', value='2023-08-23'), ]),
            OpenApiParameter(name='end_date', description='end_date filter, values are below than specified',
                             required=False, type=OpenApiTypes.DATE,
                             examples=[OpenApiExample(name='legacy data example', value='2023-08-23'), ]),

        ]
    )
    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)

    def list(self, request, *args, **kwargs):
        self.validated_data = self.validate_query_params()

        queryset = self.filter_queryset(self.get_queryset())
        if not self.validated_data['all']:
            page = self.paginate_queryset(queryset)
            if page is not None:
                return self.get_paginated_response(page)

        data = {
            'count': queryset.count(),
            'results': list(queryset),
        }
        return Response(data)

    def validate_query_params(self):
        params = self.request.query_params.dict()
        params.update(self.kwargs)
        params['regions'] = params['regions'].split(',') if 'regions' in params else []
        params['fields'] = params['fields'].split(',') if 'fields' in params else []

        serializer = DatasetParamsSerializer(data=params)
        serializer.is_valid(raise_exception=True)

        return serializer.validated_data

    def get_queryset(self):
        model = DatasetInfo.DATASETS[self.validated_data['dataset']]
        return model.objects.values()

    def filter_queryset(self, queryset):
        if queryset.model is RegionTransformedData and self.validated_data['regions']:
            queryset = queryset.filter(region__in=self.validated_data['regions'])

        start_date = self.validated_data.get('start_date')
        if start_date:
            queryset = queryset.filter(start_date__gt=start_date)

        end_date = self.validated_data.get('end_date')
        if end_date:
            queryset = queryset.filter(end_date__lt=end_date)

        return queryset.values(*self.validated_data['fields'])
