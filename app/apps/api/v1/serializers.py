from rest_framework import serializers
from django.contrib.auth.models import User

from apps.api.models import DatasetInfo
from apps.etl.models import RegionTransformedData


class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ['id', 'username', 'email', 'password']


class DatasetInfoSerializer(serializers.ModelSerializer):
    class Meta:
        model = DatasetInfo
        fields = ['dataset_name', 'description', ]


class DatasetRequestSerializer(serializers.Serializer):
    dataset = serializers.CharField(max_length=50, required=True)
    fields = serializers.ListField(help_text='comma separated list of fields', required=False)
    regions = serializers.ListField(help_text='comma separated list of regions names', required=False)
    start_date = serializers.DateField(required=False,
                                       help_text='start_date filter, values are greater than specified')
    end_date = serializers.DateField(required=False, help_text='end_date filter, values are below than specified')

    def validate(self, attrs):
        if attrs['dataset'] not in DatasetInfo.DATASETS:
            raise serializers.ValidationError(f"Dataset with name {attrs['dataset']} does not exist")

        model = DatasetInfo.DATASETS[attrs['dataset']]
        model_fields = set(field.name for field in model._meta.get_fields())

        if 'fields' in attrs:
            fields = set(attrs['fields'])

            unknown_fields = fields - model_fields
            if unknown_fields:
                raise serializers.ValidationError(f"Request contains unknown fields: {','.join(unknown_fields)}")

        if 'regions' in attrs:
            regions = set(attrs['regions'])
            awailable_regions = set(RegionTransformedData.objects.values_list('region', flat=True))

            unknown_regions = regions - awailable_regions
            if unknown_regions:
                raise serializers.ValidationError(f"Request contains unknown regions: {','.join(unknown_regions)}")

        return attrs
