from rest_framework import serializers

from apps.api.models import DatasetInfo
from apps.etl.models import RegionTransformedData


class DatasetInfoSerializer(serializers.ModelSerializer):
    class Meta:
        model = DatasetInfo
        fields = ['dataset_name', 'description', ]


class DatasetParamsSerializer(serializers.Serializer):
    default_error_messages = {"dataset doesn't exist": "Dataset with name {dataset} does not exist",
                              'unknown dataset fields': "Request contains unknown fields: {fields}",
                              'unknown regions': "Request contains unknown regions: {regions}",
                              }
    dataset = serializers.CharField(max_length=50, required=True)
    fields = serializers.ListField(help_text='comma separated list of fields', required=False)
    regions = serializers.ListField(help_text='comma separated list of regions names', required=False)
    start_date = serializers.DateField(required=False,
                                       help_text='start_date filter, values are greater than specified')
    end_date = serializers.DateField(required=False, help_text='end_date filter, values are below than specified')
    all = serializers.BooleanField(required=False, help_text='return all dataset items', default=False)

    def validate(self, attrs):

        dataset = attrs['dataset']
        if attrs['dataset'] not in DatasetInfo.DATASETS:
            self.fail("dataset doesn't exist", dataset=dataset)

        model = DatasetInfo.DATASETS[dataset]
        model_fields = set(field.name for field in model._meta.get_fields())

        if 'fields' in attrs:
            fields = set(attrs['fields'])

            unknown_fields = fields - model_fields
            if unknown_fields:
                self.fail("unknown dataset fields", fields=','.join(unknown_fields))

        if 'regions' in attrs:
            regions = set(attrs['regions'])
            awailable_regions = set(RegionTransformedData.objects.values_list('region', flat=True))

            unknown_regions = regions - awailable_regions
            if unknown_regions:
                self.fail("unknown regions", regions=regions)

        return attrs


class DatasetParamValidationErrorResponseSerializer(serializers.Serializer):
    attr = serializers.CharField()
    code = serializers.CharField()
    detail = serializers.CharField()


class DatasetparamsValidationErrorResponseSerializer(serializers.Serializer):
    errors = DatasetParamValidationErrorResponseSerializer(many=True)
