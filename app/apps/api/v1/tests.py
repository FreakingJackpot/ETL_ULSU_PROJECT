from collections import OrderedDict
from datetime import date

from django.urls import reverse
from django.contrib.auth import get_user_model
from rest_framework import status
from rest_framework.test import APITestCase

from apps.api.models import DatasetInfo
from apps.api.v1.views import DatasetsInfo, Regions, Dataset
from apps.etl.models import RegionTransformedData, Region, GlobalTransformedData


class DatasetsInfoViewTestCase(APITestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_user(username='admin', password='admin', email='admin@admin.ru')

        self.global_info = DatasetInfo.objects.create(
            dataset_name='global_data',
            description={
                'fields': [
                    {'name': 'start_date', 'type': 'datetime', },
                    {'name': 'end_date', 'type': 'datetime', },
                    {'name': 'deaths', 'type': 'int', },
                    {'name': 'recovered', 'type': 'int', },
                    {'name': 'infected', 'type': 'int', }
                ],
                'description': 'Датасет содержит информацию о всей росии по недельно',
            },
            model_name='GlobalTransformedData',
        )

        self.region_info = DatasetInfo.objects.create(
            dataset_name='region_data',
            description={
                'fields': [
                    {'name': 'start_date', 'type': 'datetime', },
                    {'name': 'end_date', 'type': 'datetime', },
                    {'name': 'deaths', 'type': 'int', },
                    {'name': 'recovered', 'type': 'int', },
                    {'name': 'infected', 'type': 'int', },
                    {'name': 'region', 'type': 'str', },
                ],
                'description': 'Датасет содержит информацию о регионах по недельно'
            },
            model_name='RegionTransformedData',
        )

    def test_get_request(self):
        self.client.force_authenticate(user=self.user)
        response = self.client.get('/api/v1/datasets-info/', format='json')

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        estimated_result = [OrderedDict({'dataset_name': obj.dataset_name, 'description': obj.description, }) for obj in
                            (self.global_info, self.region_info)]
        self.assertListEqual(estimated_result, response.data)


class RegionsViewTestCase(APITestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_user(username='admin', password='admin', email='admin@admin.ru')

        regions_names = ['Ульяновская обл.', 'Москва', 'Московская обл.', ]
        self.estimated_result = []
        for name in regions_names:
            region = Region.objects.create(name=name)
            self.estimated_result.append(OrderedDict({'id': region.id, 'name': region.name, }))

    def test_get_request(self):
        self.client.force_authenticate(user=self.user)
        response = self.client.get('/api/v1/regions/', format='json')

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertListEqual(self.estimated_result, response.data)


class DatasetViewTestCase(APITestCase):
    request_template = '/api/v1/datasets/{}/'

    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_user(username='admin', password='admin', email='admin@admin.ru')

        self.global_info = DatasetInfo.objects.create(
            dataset_name='global_data',
            description={
                'fields': [
                    {'name': 'start_date', 'type': 'datetime', },
                    {'name': 'end_date', 'type': 'datetime', },
                    {'name': 'deaths', 'type': 'int', },
                    {'name': 'recovered', 'type': 'int', },
                    {'name': 'infected', 'type': 'int', }
                ],
                'description': 'Датасет содержит информацию о всей росии по недельно',
            },
            model_name='GlobalTransformedData',
        )

        self.region_info = DatasetInfo.objects.create(
            dataset_name='region_data',
            description={
                'fields': [
                    {'name': 'start_date', 'type': 'datetime', },
                    {'name': 'end_date', 'type': 'datetime', },
                    {'name': 'deaths', 'type': 'int', },
                    {'name': 'recovered', 'type': 'int', },
                    {'name': 'infected', 'type': 'int', },
                    {'name': 'region', 'type': 'str', },
                ],
                'description': 'Датасет содержит информацию о регионах по недельно'
            },
            model_name='RegionTransformedData',
        )

        self.regions_names = ['Ульяновская обл.', 'Москва', 'Московская обл.', ]
        for name in self.regions_names:
            Region.objects.create(name=name)

        self.global_data_1 = GlobalTransformedData.objects.create(
            start_date=date(day=1, month=1, year=2020),
            end_date=date(day=7, month=1, year=2020),
            weekly_infected=1,
            weekly_deaths=1,
            weekly_recovered=1,
        )

        self.global_data_2 = GlobalTransformedData.objects.create(
            start_date=date(day=8, month=1, year=2020),
            end_date=date(day=15, month=1, year=2020),
            weekly_infected=3,
            weekly_deaths=2,
            weekly_recovered=5,
        )

        self.global_data_3 = GlobalTransformedData.objects.create(
            start_date=date(day=16, month=1, year=2020),
            end_date=date(day=23, month=1, year=2020),
            weekly_infected=1,
            weekly_deaths=1,
            weekly_recovered=3,
        )

        self.region_data_1 = RegionTransformedData.objects.create(
            start_date=date(day=1, month=1, year=2020),
            end_date=date(day=7, month=1, year=2020),
            region='Ульяновская обл.',
            weekly_infected=0,
            weekly_deaths=0,
            weekly_recovered=None,
        )

        self.region_data_2 = RegionTransformedData.objects.create(
            start_date=date(day=1, month=1, year=2020),
            end_date=date(day=7, month=1, year=2020),
            region='Москва',
            weekly_infected=1,
            weekly_deaths=1,
            weekly_recovered=1,
        )

        self.region_data_3 = RegionTransformedData.objects.create(
            start_date=date(day=8, month=1, year=2020),
            end_date=date(day=15, month=1, year=2020),
            region='Ульяновская обл.',
            weekly_infected=3,
            weekly_deaths=2,
            weekly_recovered=5,
        )

        self.region_data_4 = RegionTransformedData.objects.create(
            start_date=date(day=16, month=1, year=2020),
            end_date=date(day=23, month=1, year=2020),
            region='Москва',
            weekly_infected=1,
            weekly_deaths=1,
            weekly_recovered=3,
        )

    def test_global_dataset_request_without_filters(self):
        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.request_template.format(self.global_info.dataset_name), format='json')

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        estimated_result = OrderedDict({
            'count': GlobalTransformedData.objects.count(),
            'results': list(GlobalTransformedData.objects.values()),
        })
        response.data.pop('next')
        response.data.pop('previous')
        self.assertDictEqual(estimated_result, response.data)

    def test_global_dataset_request_with_regions_filter(self):
        """Filter mustn't work"""
        self.client.force_authenticate(user=self.user)
        url = self.request_template.format(self.global_info.dataset_name) + '?regions=Москва'
        response = self.client.get(url, format='json')

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        estimated_result = OrderedDict({
            'count': GlobalTransformedData.objects.count(),
            'results': list(GlobalTransformedData.objects.values()),
        })
        response.data.pop('next')
        response.data.pop('previous')
        self.assertDictEqual(estimated_result, response.data)

    def test_region_dataset_request_without_filters(self):
        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.request_template.format(self.region_info.dataset_name), format='json')

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        estimated_result = OrderedDict({
            'count': RegionTransformedData.objects.count(),
            'results': list(RegionTransformedData.objects.values()),
        })
        response.data.pop('next')
        response.data.pop('previous')
        self.assertDictEqual(estimated_result, response.data)

    def test_region_dataset_request_with_regions_filter(self):
        self.client.force_authenticate(user=self.user)
        url = self.request_template.format(self.region_info.dataset_name) + '?regions=Москва'
        response = self.client.get(url, format='json')

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        estimated_result = OrderedDict({
            'count': RegionTransformedData.objects.filter(region='Москва').count(),
            'results': list(RegionTransformedData.objects.filter(region='Москва').values()),
        })
        response.data.pop('next')
        response.data.pop('previous')
        self.assertDictEqual(estimated_result, response.data)

        self.client.force_authenticate(user=self.user)
        url = self.request_template.format(self.region_info.dataset_name) + '?regions=Московская обл.'
        response = self.client.get(url, format='json')

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        estimated_result = OrderedDict({
            'count': RegionTransformedData.objects.filter(region='Московская обл.').count(),
            'results': list(RegionTransformedData.objects.filter(region='Московская обл.').values()),
        })
        response.data.pop('next')
        response.data.pop('previous')
        self.assertDictEqual(estimated_result, response.data)

    def test_requests_with_all_param(self):
        self.client.force_authenticate(user=self.user)
        url = self.request_template.format(self.region_info.dataset_name) + '?all=true'
        response = self.client.get(url, format='json')

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        estimated_result = OrderedDict({
            'count': RegionTransformedData.objects.count(),
            'results': list(RegionTransformedData.objects.values()),
        })

        self.assertDictEqual(estimated_result, response.data)

        self.client.force_authenticate(user=self.user)
        url = self.request_template.format(self.global_info.dataset_name) + '?all=true'
        response = self.client.get(url, format='json')

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        estimated_result = OrderedDict({
            'count': GlobalTransformedData.objects.count(),
            'results': list(GlobalTransformedData.objects.values()),
        })

        self.assertDictEqual(estimated_result, response.data)

    def test_requests_with_fields_filter(self):
        fields = ['weekly_infected', 'weekly_deaths', 'weekly_recovered', ]
        self.client.force_authenticate(user=self.user)
        url = self.request_template.format(self.global_info.dataset_name) + f'?fields={",".join(fields)}&all=true'
        response = self.client.get(url, format='json')

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        estimated_result = OrderedDict({
            'count': GlobalTransformedData.objects.count(),
            'results': list(GlobalTransformedData.objects.values(*fields)),
        })

        self.assertDictEqual(estimated_result, response.data)

        fields.append('region')
        self.client.force_authenticate(user=self.user)
        url = self.request_template.format(self.region_info.dataset_name) + f'?fields={",".join(fields)}&all=true'
        response = self.client.get(url, format='json')

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        estimated_result = OrderedDict({
            'count': RegionTransformedData.objects.count(),
            'results': list(RegionTransformedData.objects.values(*fields)),
        })

        self.assertDictEqual(estimated_result, response.data)

