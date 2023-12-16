# -*- coding: utf-8 -*-
from datetime import timedelta

import pandas as pd

from apps.etl.models import ExternalDatabaseStatistic, StopCoronaData, RegionTransformedData
from .transforming_functions import TransformingFunctions


class LegacyRegionalDataTransformer:
    @classmethod
    def run(cls):
        external_data = cls._get_dataframes()
        transformed_external_data = cls._transform_external_data(external_data)
        return transformed_external_data

    @classmethod
    def _get_dataframes(cls):
        external_data = ExternalDatabaseStatistic.get_all_transform_data(with_region=True)

        external_data = pd.DataFrame(data=external_data)

        return external_data

    @classmethod
    def _transform_external_data(cls, external_data):
        external_data['date'] = pd.to_datetime(external_data['date'])
        external_data = external_data.groupby('region').resample('W-MON', on='date').sum(numeric_only=True)
        external_data.reset_index('date', inplace=True)
        external_data['date'] = pd.to_datetime(external_data['date'])

        external_data.rename(
            columns={'death_per_day': 'weekly_deaths',
                     'infection_per_day': 'weekly_infected',
                     'recovery_per_day': 'weekly_recovered'}, inplace=True)

        external_data['start_date'] = external_data.apply(lambda row: row.date - timedelta(days=6))
        external_data.rename(columns={'date': 'end_date'}, inplace=True)

        return TransformingFunctions.apply_all_transforms(external_data, True)


class RegionsDataTransformer:
    @classmethod
    def run(cls, latest=False):
        stopcorona_data, gogov_data = cls._get_dataframes(latest)

        stopcorona_data.rename(
            columns={'infected': 'weekly_infected', 'recovered': 'weekly_recovered', 'deaths': 'weekly_deaths', },
            inplace=True)

        transformed_data = cls._transform_data(stopcorona_data)

        return transformed_data

    @classmethod
    def _get_dataframes(cls, latest):
        stopcorona_data = StopCoronaData.get_transform_region_data(latest)
        stopcorona_data = pd.DataFrame(data=stopcorona_data)
        return stopcorona_data

    @classmethod
    def _transform_data(cls, stopcorona_data):
        cls._add_cumulative_stats(stopcorona_data)
        stopcorona_data = TransformingFunctions.add_per_100000_stats(stopcorona_data)
        stopcorona_data = TransformingFunctions.add_ratio_stats(stopcorona_data)

        return stopcorona_data

    @classmethod
    def _add_cumulative_stats(cls, stopcorona_data):
        latest_data_map = RegionTransformedData.get_latest_data_map()

        for key, item in latest_data_map.items():
            region_data = stopcorona_data[stopcorona_data.region == key]

            region_data['infected'][0] = item['infected'] + region_data['weekly_infected'][0]
            region_data['recovered'][0] = item['recovered'] + region_data['weekly_recovered'][0]
            region_data['deaths'][0] = item['deaths'] + region_data['weekly_deaths'][0]

            for i in range(1, len(region_data)):
                region_data['infected'][i] = region_data['infected'][i - 1] + region_data['weekly_infected'][i]
                region_data['recovered'][i] = region_data['recovered'][i - 1] + region_data['weekly_recovered'][i]
                region_data['deaths'][i] = region_data['deaths'][i - 1] + region_data['weekly_deaths'][i]
