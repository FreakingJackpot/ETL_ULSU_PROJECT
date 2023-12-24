# -*- coding: utf-8 -*-
from datetime import timedelta

import pandas as pd
from numpy import nan

from apps.etl.models import ExternalDatabaseStatistic, StopCoronaData, RegionTransformedData
from .transforming_functions import TransformingFunctions

pd.options.mode.chained_assignment = None


class LegacyRegionDataTransformer:
    @classmethod
    def run(cls):
        external_data = cls._get_dataframes()
        transformed_external_data = cls._transform_external_data(external_data)
        return transformed_external_data.to_dict('records')

    @classmethod
    def _get_dataframes(cls):
        external_data = ExternalDatabaseStatistic.get_all_transform_data(with_region=True)

        external_data = pd.DataFrame(data=external_data)

        return external_data

    @classmethod
    def _transform_external_data(cls, external_data):
        external_data['date'] = pd.to_datetime(external_data['date'])
        external_data = external_data.groupby('region').resample('W-MON', on='date').sum(numeric_only=True)
        external_data.reset_index(['date', 'region'], inplace=True)
        external_data['date'] = pd.to_datetime(external_data['date'])

        external_data.rename(
            columns={'death_per_day': 'weekly_deaths',
                     'infection_per_day': 'weekly_infected',
                     'recovery_per_day': 'weekly_recovered'}, inplace=True)

        external_data['start_date'] = external_data.apply(lambda row: (row.date - timedelta(days=6)).date(), axis=1)
        external_data.rename(columns={'date': 'end_date'}, inplace=True)
        external_data['end_date'] = external_data.apply(lambda row: row.end_date.date(), axis=1)

        external_data = TransformingFunctions.apply_all_transforms(external_data, True)

        external_data.replace({nan: None}, inplace=True)

        return external_data


class RegionDataTransformer:
    _regions_map = (
        ('Республика Карелия', 'Карелия'), ('Еврейская автономная область', 'Еврейская АО'),
        ('Республика Коми', 'Коми'), ('Республика Адыгея', 'Адыгея'),
        ('Кабардино-Балкарская Республика', 'Кабардино-Балкария'), ('Республика Хакасия', 'Хакасия'),
        ('Ямало-Ненецкий автономный округ', 'Ямало-Ненецкий АО'), ('Республика Крым', 'Крым'),
        ('Чувашская Республика', 'Чувашия'), ('Ханты-Мансийский АО', 'ХМАО – Югра'),
        ('Ханты-Мансийский автономный округ', 'ХМАО – Югра'), ('Ненецкий автономный округ', 'Ненецкий АО'),
        ('Чеченская Республика', 'Чечня'), ('Республика Тыва', 'Тыва'),
        ('Республика Калмыкия', 'Калмыкия'), ('Республика Саха (Якутия)', 'Саха (Якутия)'),
        ('Республика Мордовия', 'Мордовия'), ('Удмуртская Республика', 'Удмуртия'),
        ('Республика Башкортостан', 'Башкортостан'), ('Республика Татарстан', 'Татарстан'),
        ('Республика Северная Осетия — Алания', 'Северная Осетия'),
        ('Карачаево-Черкесская Республика', 'Карачаево-Черкессия'), ('Республика Ингушетия', 'Ингушетия'),
        ('Чукотский автономный округ', 'Чукотский АО'), ('Республика Бурятия', 'Бурятия'),
        ('Республика Дагестан', 'Дагестан'), ('Республика Марий Эл', 'Марий Эл'), ('Республика Алтай', 'Алтай'),
        ('область', 'обл.'),  # обязательно последним, иначе может заменить другие названия
    )

    def __init__(self, latest=False):
        self.latest = latest

    def run(self):
        stopcorona_data = self._get_dataframe()
        transformed_data = self._transform_data(stopcorona_data)
        return transformed_data.to_dict('records')

    def _get_dataframe(self):
        stopcorona_data = StopCoronaData.get_transform_region_data(self.latest)
        stopcorona_data = pd.DataFrame(data=stopcorona_data)
        return stopcorona_data

    @classmethod
    def _transform_data(cls, stopcorona_data):
        stopcorona_data.rename(
            columns={'infected': 'weekly_infected', 'recovered': 'weekly_recovered', 'deaths': 'weekly_deaths', },
            inplace=True)
        cls._rename_regions(stopcorona_data)
        cls._add_cumulative_stats(stopcorona_data)
        stopcorona_data = TransformingFunctions.add_per_100000_stats(stopcorona_data)
        stopcorona_data = TransformingFunctions.add_ratio_stats(stopcorona_data, True)

        stopcorona_data.replace({nan: None}, inplace=True)

        return stopcorona_data

    @classmethod
    def _rename_regions(cls, stopcorona_data):
        for mapping in cls._regions_map:
            stopcorona_data['region'] = stopcorona_data['region'].str.replace(*mapping)

    @classmethod
    def _add_cumulative_stats(cls, stopcorona_data):
        latest_data_map = RegionTransformedData.get_latest_data_map()

        stopcorona_data['infected'] = None
        stopcorona_data['recovered'] = None
        stopcorona_data['deaths'] = None

        for key, item in latest_data_map.items():
            region_data = stopcorona_data[stopcorona_data.region == key]

            region_data['infected'].iloc[0] = item['infected'] + region_data['weekly_infected'].iloc[0]
            region_data['recovered'].iloc[0] = item['recovered'] + region_data['weekly_recovered'].iloc[0]
            region_data['deaths'].iloc[0] = item['deaths'] + region_data['weekly_deaths'].iloc[0]

            for i in range(1, len(region_data)):
                region_data['infected'].iloc[i] = region_data['infected'].iloc[i - 1] + \
                                                  region_data['weekly_infected'].iloc[i]
                region_data['recovered'].iloc[i] = region_data['recovered'].iloc[i - 1] + \
                                                   region_data['weekly_recovered'].iloc[i]
                region_data['deaths'].iloc[i] = region_data['deaths'].iloc[i - 1] + region_data['weekly_deaths'].iloc[i]

            stopcorona_data[stopcorona_data.region == key] = region_data
