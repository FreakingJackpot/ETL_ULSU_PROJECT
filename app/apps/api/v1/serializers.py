from django.core.exceptions import FieldDoesNotExist
from rest_framework import serializers
from django.apps import apps

from apps.api.models import DatasetInfo
from apps.etl.models import RegionTransformedData, Region


class DatasetInfoSerializer(serializers.ModelSerializer):
    class Meta:
        model = DatasetInfo
        fields = ['dataset_name', 'description', ]


class RegionsSerializer(serializers.ModelSerializer):
    class Meta:
        model = Region
        fields = '__all__'


class DatasetParamsSerializer(serializers.Serializer):
    default_error_messages = {"dataset doesn't exist": "Dataset with name {dataset} does not exist",
                              'unknown dataset fields': "Request contains unknown fields: {fields}",
                              'unknown regions': "Request contains unknown regions: {regions}",
                              }
    dataset_name = serializers.CharField(max_length=50, required=True)
    fields = serializers.ListField(help_text='comma separated list of fields', required=False)
    regions = serializers.ListField(help_text='comma separated list of regions names', required=False)
    all = serializers.BooleanField(required=False, help_text='return all dataset items', default=False)
    model = serializers.SerializerMethodField()


    def validate(self, attrs):

        dataset_name = attrs['dataset_name']
        info_object = DatasetInfo.objects.filter(dataset_name=attrs['dataset_name']).first()

        if not info_object:
            self.fail("dataset doesn't exist", dataset=dataset_name)

        model = apps.get_model(app_label='etl', model_name=info_object.model_name)
        model_fields = set(field.name for field in model._meta.get_fields())

        if 'fields' in attrs:
            fields = set(attrs['fields'])

            unknown_fields = fields - model_fields
            if unknown_fields:
                self.fail("unknown dataset fields", fields=','.join(unknown_fields))

        if self._has_field(model, 'region') and 'regions' in attrs:
            regions = set(attrs['regions'])
            awailable_regions = set(Region.objects.values_list('name', flat=True))

            unknown_regions = regions - awailable_regions
            if unknown_regions:
                self.fail("unknown regions", regions=regions)
        else:
            attrs['regions'] = []

        return attrs

    def _has_field(self, model, field):
        try:
            model._meta.get_field(field)
        except FieldDoesNotExist:
            return False

        return True

    def get_model(self, obj):
        info_object = DatasetInfo.objects.filter(dataset_name=obj['dataset_name']).first()
        return apps.get_model(app_label='etl', model_name=info_object.model_name)


class DatasetParamValidationErrorResponseSerializer(serializers.Serializer):
    attr = serializers.CharField()
    code = serializers.CharField()
    detail = serializers.CharField()


class DatasetparamsValidationErrorResponseSerializer(serializers.Serializer):
    errors = DatasetParamValidationErrorResponseSerializer(many=True)
