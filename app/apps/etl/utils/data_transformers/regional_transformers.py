# -*- coding: utf-8 -*-
import logging
from datetime import timedelta

import pandas as pd
from numpy import nan

from apps.etl.models import ExternalDatabaseStatistic, StopCoronaData, RegionTransformedData
from .transforming_functions import GenericTransformingFunctions
from apps.etl.utils.logging import get_task_logger

pd.options.mode.chained_assignment = None


class LegacyRegionDataTransformer:
    @classmethod
    def run(cls):
        data_df = cls._get_dataframe()
        transformed_df = cls._apply_transforms(data_df)
        result = transformed_df.to_dict('records')

        cls._log_result(result)
        return result

    @classmethod
    def _get_dataframe(cls):
        data = ExternalDatabaseStatistic.get_all_transform_data(with_region=True)
        data_df = pd.DataFrame(data=data)
        return data_df

    @classmethod
    def _apply_transforms(cls, data_df):
        weekly_df = cls._transform_to_weekly_format(data_df)
        weekly_df = GenericTransformingFunctions.apply_all_transforms(weekly_df, True)

        return weekly_df

    @classmethod
    def _transform_to_weekly_format(cls, data_df):
        data_df['date'] = pd.to_datetime(data_df['date'])
        data_df = data_df.groupby('region').resample('W-MON', on='date').sum(numeric_only=True)
        data_df.reset_index(['date', 'region'], inplace=True)
        data_df['date'] = pd.to_datetime(data_df['date'])

        data_df.rename(
            columns={'death_per_day': 'weekly_deaths',
                     'infection_per_day': 'weekly_infected',
                     'recovery_per_day': 'weekly_recovered'}, inplace=True)

        data_df['start_date'] = data_df.apply(lambda row: (row.date - timedelta(days=6)).date(), axis=1)
        data_df.rename(columns={'date': 'end_date'}, inplace=True)
        data_df['end_date'] = data_df.apply(lambda row: row.end_date.date(), axis=1)

        return data_df

    @classmethod
    def _log_result(cls, result):
        logger = get_task_logger()

        for item in result:
            log_data = {**item,
                        'start_date': item['start_date'].strftime('%d-%m-%Y'),
                        'end_date': item['end_date'].strftime('%d-%m-%Y')}
            logger.log(logging.INFO, 'Transformed legacy region data', **log_data)


class RegionDataTransformer:
    _regions_map = (
        ('Республика Карелия', 'Карелия'), ('Еврейская автономная область', 'Еврейская АО'),
        ('Республика Коми', 'Коми'), ('Республика Адыгея', 'Адыгея'),
        ('Кабардино-Балкарская Республика', 'Кабардино-Балкария'), ('Республика Хакасия', 'Хакасия'),
        ('Ямало-Ненецкий автономный округ', 'Ямало-Ненецкий АО'), ('Республика Крым', 'Крым'),
        ('Чувашская Республика', 'Чувашия'), ('Ханты-Мансийский АО', 'ХМАО – Югра'),
        ('Ханты-Мансийский автономный округ', 'ХМАО – Югра'), ('Ненецкий автономный округ', 'Ненецкий АО'),
        ('Чеченская Республика', 'Чечня'), ('Республика Тыва', 'Тыва'), ('Республика Калмыкия', 'Калмыкия'),
        ('Республика Саха (Якутия)', 'Саха (Якутия)'), ('Республика Мордовия', 'Мордовия'),
        ('Удмуртская Республика', 'Удмуртия'), ('Республика Башкортостан', 'Башкортостан'),
        ('Республика Татарстан', 'Татарстан'), ('Республика Северная Осетия — Алания', 'Северная Осетия'),
        ('Карачаево-Черкесская Республика', 'Карачаево-Черкессия'), ('Республика Ингушетия', 'Ингушетия'),
        ('Чукотский автономный округ', 'Чукотский АО'), ('Республика Бурятия', 'Бурятия'),
        ('Республика Дагестан', 'Дагестан'), ('Республика Марий Эл', 'Марий Эл'), ('Республика Алтай', 'Алтай'),
    )

    def __init__(self, latest=False):
        self.latest = latest

    def run(self):
        stopcorona_data = self._get_dataframe()
        if stopcorona_data.empty:
            return []

        transformed_data = self._transform_data(stopcorona_data)
        result = transformed_data.to_dict('records')

        self._log_result(result)
        return result

    def _get_dataframe(self):
        stopcorona_data = StopCoronaData.get_region_transform_data(self.latest)
        stopcorona_data = pd.DataFrame(data=stopcorona_data)
        return stopcorona_data

    def _transform_data(self, stopcorona_data):
        stopcorona_data.rename(
            columns={'infected': 'weekly_infected', 'recovered': 'weekly_recovered', 'deaths': 'weekly_deaths', },
            inplace=True)
        self._rename_regions(stopcorona_data)
        self._add_cumulative_stats(stopcorona_data)
        stopcorona_data = GenericTransformingFunctions.add_per_100000_stats(stopcorona_data)
        stopcorona_data = GenericTransformingFunctions.add_ratio_stats(stopcorona_data, True)

        stopcorona_data.replace({nan: None}, inplace=True)

        return stopcorona_data

    @classmethod
    def _rename_regions(cls, stopcorona_data):
        for mapping in cls._regions_map:
            stopcorona_data['region'] = stopcorona_data['region'].str.replace(*mapping)
        stopcorona_data['region'] = stopcorona_data['region'].str.replace('область', 'обл.')

    def _add_cumulative_stats(self, stopcorona_data):
        latest_data_map = RegionTransformedData.get_highest_not_null_values(self.latest)

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

    @classmethod
    def _log_result(cls, result):
        logger = get_task_logger()

        for item in result:
            log_data = {**item,
                        'start_date': item['start_date'].strftime('%d-%m-%Y'),
                        'end_date': item['end_date'].strftime('%d-%m-%Y')}
            logger.log(logging.INFO, 'Transformed region data', **log_data)
