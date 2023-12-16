import pandas as pd
import numpy as np

RF_POPULATION = 146447424


class TransformingFunctions:
    _ratio_keys = (
        'weekly_recovered_infected_ratio', 'weekly_deaths_infected_ratio', 'weekly_vaccinations_infected_ratio')

    @staticmethod
    def add_cumulative_stats(df, region=False):
        base_query = df.groupby('region') if region else df
        df['recovered'] = base_query['weekly_recovered'].cumsum()
        df['deaths'] = base_query['weekly_deaths'].cumsum()
        df['infected'] = base_query['weekly_infected'].cumsum()

        if not region:
            df['first_component'] = base_query['weekly_first_component'].cumsum()
            df['second_component'] = base_query['weekly_second_component'].cumsum()

    @staticmethod
    def add_per_100000_stats(df):
        def calculate_columns(row):
            columns = {
                'weekly_infected_per_100000': row.weekly_infected / RF_POPULATION * 100000,
                'weekly_deaths_per_100000': row.weekly_deaths / RF_POPULATION * 100000,
                'weekly_recovered_per_100000': row.weekly_recovered / RF_POPULATION * 100000,
                'infected_per_100000': row.infected / RF_POPULATION * 100000,
                'deaths_per_100000': row.deaths / RF_POPULATION * 100000,
                'recovered_per_100000': row.recovered / RF_POPULATION * 100000,
            }
            return columns

        applied_df = df.apply(lambda row: calculate_columns(row), axis=1, result_type='expand')
        df = pd.concat([df, applied_df], axis=1)

        return df

    @classmethod
    def add_ratio_stats(cls, df, region=False):
        def calculate_columns(row):
            columns = dict.fromkeys(cls._ratio_keys, np.nan)

            if row.weekly_infected:
                columns = {
                    'weekly_recovered_infected_ratio': row.weekly_recovered / row.weekly_infected,
                    'weekly_deaths_infected_ratio': row.weekly_deaths / row.weekly_infected,
                    'weekly_vaccinations_infected_ratio': row.weekly_vaccinations / row.weekly_infected,
                }

            if not region:
                columns['vaccinations_population_ratio'] = row.second_component / RF_POPULATION

            return columns

        applied_df = df.apply(lambda row: calculate_columns(row), axis=1, result_type='expand')
        df = pd.concat([df, applied_df], axis=1)

        return df

    @classmethod
    def apply_all_transforms(cls, df, region=False):
        cls.add_cumulative_stats(df, region)
        df = cls.add_per_100000_stats(df)
        df = cls.add_ratio_stats(df, region)

        return df
