from rest_framework import serializers

from apps.api.models import DatasetInfo
from apps.etl.models import RegionTransformedData


class TokenObtainPairResponseSerializer(serializers.Serializer):
    access = serializers.CharField()
    refresh = serializers.CharField()

    def create(self, validated_data):
        raise NotImplementedError()

    def update(self, instance, validated_data):
        raise NotImplementedError()


class TokenRefreshResponseSerializer(serializers.Serializer):
    access = serializers.CharField()

    def create(self, validated_data):
        raise NotImplementedError()

    def update(self, instance, validated_data):
        raise NotImplementedError()


class Response401InvalidOrExpiredSerializer(serializers.Serializer):
    detail = serializers.CharField(help_text='Token is invalid or expired')
    code = serializers.CharField(help_text='token_not_valid')

    def create(self, validated_data):
        raise NotImplementedError()

    def update(self, instance, validated_data):
        raise NotImplementedError()


class Response401BlacklistedSerializer(serializers.Serializer):
    detail = serializers.CharField(help_text='Token is blacklisted')
    code = serializers.CharField(help_text='token_not_valid')

    def create(self, validated_data):
        raise NotImplementedError()

    def update(self, instance, validated_data):
        raise NotImplementedError()


class Response401NoAccountOrWrongCredentialsSerializer(serializers.Serializer):
    detail = serializers.CharField(help_text='No active account found with the given credentials')

    def create(self, validated_data):
        raise NotImplementedError()

    def update(self, instance, validated_data):
        raise NotImplementedError()


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
