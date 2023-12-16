# -*- coding: utf-8 -*-
from datetime import timedelta, date

import pandas as pd

from apps.etl.models import ExternalDatabaseStatistic, ExternalDatabaseVaccination, CsvData, StopCoronaData, \
    GogovGlobalData, GlobalTransformedData
from .transforming_functions import TransformingFunctions


class LegacyGlobalDataTransformer:
    @classmethod
    def run(cls):
        external_data_main, external_data_vaccinations, csv_data = cls._get_dataframes()

        external_data_main = cls._transform_external_data_main(external_data_main)
        external_data_vaccinations = cls._transform_external_data_vaccinations(external_data_vaccinations)
        csv_data = cls._transform_csv_data(csv_data)

        resulted_df = cls._prepare_resulted_df(external_data_main, external_data_vaccinations, csv_data)

        return resulted_df

    @classmethod
    def _get_dataframes(cls):
        external_data_main = ExternalDatabaseStatistic.get_all_transform_data()
        external_data_vaccinations = ExternalDatabaseVaccination.get_all_transform_data()
        csv_data = CsvData.get_all_transform_data()

        external_data_main = pd.DataFrame(data=external_data_main)
        external_data_vaccinations = pd.DataFrame(data=external_data_vaccinations)
        csv_data = pd.DataFrame(data=csv_data)

        return external_data_main, external_data_vaccinations, csv_data

    @classmethod
    def _transform_external_data_main(cls, external_data_main):
        external_data_main = external_data_main.groupby("date").sum()
        external_data_main.reset_index('date', inplace=True)

        external_data_main['date'] = pd.to_datetime(external_data_main['date'])
        external_data_main = external_data_main.loc[external_data_main["date"] >= "2020-12-15"]
        external_data_main = external_data_main.resample('W-MON', on='date').sum()
        external_data_main.reset_index('date', inplace=True)

        external_data_main.rename(
            columns={'death_per_day': 'weekly_deaths',
                     'infection_per_day': 'weekly_infected',
                     'recovery_per_day': 'weekly_recovered'}, inplace=True)

        return external_data_main

    @classmethod
    def _transform_external_data_vaccinations(cls, external_data_vaccinations):
        external_data_vaccinations['date'] = pd.to_datetime(external_data_vaccinations['date'])
        external_data_vaccinations = external_data_vaccinations.resample('W-MON', on='date').sum()
        external_data_vaccinations.reset_index('date', inplace=True)

        external_data_vaccinations.rename(columns={'daily_vaccinations': 'weekly_vaccinations',
                                                   'daily_people_vaccinated': 'weekly_first_component',
                                                   }, inplace=True)
        external_data_vaccinations['weekly_second_component'] = external_data_vaccinations.apply(
            lambda row: row.weekly_vaccinations - row.weekly_first_component, axis=1)

        return external_data_vaccinations

    @classmethod
    def _transform_csv_data(cls, csv_data):
        csv_data['date'] = pd.to_datetime(csv_data['date'])
        csv_data = csv_data.resample('W-MON', on='date').sum()
        csv_data.reset_index('date', inplace=True)

        csv_data.rename(columns={'cases': 'weekly_infected', 'deaths': 'weekly_deaths'}, inplace=True)

        return csv_data

    @classmethod
    def _prepare_resulted_df(cls, external_data_main, external_data_vaccinations, csv_data):
        resulted_df = cls._merge_all_dfs(external_data_main, external_data_vaccinations, csv_data)

        resulted_df['start_date'] = resulted_df.apply(lambda row: row.date - timedelta(days=6), axis=1)
        resulted_df.rename(columns={'date': 'end_date'}, inplace=True)

        resulted_df = TransformingFunctions.apply_all_transforms(resulted_df)

        return resulted_df

    @classmethod
    def _merge_all_dfs(cls, external_data_main, external_data_vaccinations, csv_data):
        resulted_df = pd.concat([csv_data, external_data_main], ignore_index=True)
        resulted_df['date'] = pd.to_datetime(resulted_df['date'])
        resulted_df = pd.merge(resulted_df, external_data_vaccinations, on='date', how='left')
        resulted_df.sort_values(by='date', ascending=True, inplace=True)

        return resulted_df


class GlobalDataTransformer:
    @classmethod
    def run(cls, latest=False):
        stopcorona_data, gogov_data = cls._get_dataframes(latest)

        stopcorona_data.rename(
            columns={'infected': 'weekly_infected', 'recovered': 'weekly_recovered', 'deaths': 'weekly_deaths', },
            inplace=True)
        gogov_data = cls._transform_gogov_data(gogov_data, stopcorona_data)

        resulted_df = cls._prepare_resulted_df(stopcorona_data, gogov_data)

        return resulted_df

    @classmethod
    def _get_dataframes(cls, latest):
        stopcorona_data = StopCoronaData.get_transform_global_data(latest)
        dates = stopcorona_data[0]['start_date'], stopcorona_data[0]['end_date'] if latest else None, None

        gogov_data = GogovGlobalData.get_transform_data(*dates)

        stopcorona_data = pd.DataFrame(data=stopcorona_data)
        gogov_data = pd.DataFrame(data=gogov_data)

        return stopcorona_data, gogov_data

    @classmethod
    def _transform_gogov_data(cls, gogov_data, stopcorona_data):
        for i in range(1, len(gogov_data)):
            gogov_data['daily_first_component'][i] = gogov_data['first_component'][i] - gogov_data['first_component'][
                i - 1]
            gogov_data['daily_second_component'][i] = gogov_data['full_vaccinated'][i] - gogov_data['full_vaccinated'][
                i - 1]
            gogov_data['daily_vaccinations'][i] = gogov_data['daily_first_component'][i] + \
                                                  gogov_data['daily_second_component'][i]

        transformed_gogov_data = []
        for row in stopcorona_data[['start_date', 'end_date', ]].iterrows():
            gogov_weekly_data = (gogov_data[gogov_data.date >= row.start_date and gogov_data <= row.end_date]
                                 .sort_values(by='date', ascending=True, inplace=True))

            new_row = gogov_weekly_data[['daily_first_component', 'daily_second_component', 'daily_vaccinations']].sum()
            new_row[['first_component', 'second_component', 'end_date']] = \
                gogov_weekly_data[['first_component', 'second_component', 'date']].iloc[-1]
            new_row['start_date'] = gogov_weekly_data['date'].iloc[0]

            transformed_gogov_data.append(new_row)

        transformed_gogov_data = pd.DataFrame(data=transformed_gogov_data)
        transformed_gogov_data.rename(columns={'daily_vaccinations': 'weekly_vaccinations',
                                               'daily_first_component': 'weekly_first_component',
                                               'daily_second_component': 'weekly_second_component'
                                               }, inplace=True)
        return transformed_gogov_data

    @classmethod
    def _prepare_resulted_df(cls, stopcorona_data, gogov_data):
        resulted_df = cls._merge_all_dfs(stopcorona_data, gogov_data)
        transformed_resulted_df = cls._transform_resulted_df(resulted_df)
        return transformed_resulted_df

    @classmethod
    def _merge_all_dfs(cls, stopcorona_data, gogov_data):
        resulted_df = pd.merge(stopcorona_data, gogov_data, left_on=['start_date', 'end_date'], right_on=[
            'start_date', 'end_date'], how='inner')
        resulted_df.sort_values(by='start_date', ascending=True, inplace=True)

        return resulted_df

    @classmethod
    def _transform_resulted_df(cls, resulted_df):
        cls._add_cumulative_stats(resulted_df)
        resulted_df = TransformingFunctions.add_per_100000_stats(resulted_df)
        resulted_df = TransformingFunctions.add_ratio_stats(resulted_df)
        return resulted_df

    @classmethod
    def _add_cumulative_stats(cls, resulted_df):
        latest_values = GlobalTransformedData.get_latest_not_null_values()

        resulted_df['infected'][0] = latest_values['infected'] + resulted_df['weekly_infected'][0]
        resulted_df['recovered'][0] = latest_values['recovered'] + resulted_df['weekly_recovered'][0]
        resulted_df['deaths'][0] = latest_values['deaths'] + resulted_df['weekly_deaths'][0]

        for i in range(1, len(resulted_df)):
            resulted_df['infected'][i] = resulted_df['infected'][i - 1] + resulted_df['weekly_infected'][i]
            resulted_df['recovered'][i] = resulted_df['recovered'][i - 1] + resulted_df['weekly_recovered'][i]
            resulted_df['deaths'][i] = resulted_df['deaths'][i - 1] + resulted_df['weekly_deaths'][i]
