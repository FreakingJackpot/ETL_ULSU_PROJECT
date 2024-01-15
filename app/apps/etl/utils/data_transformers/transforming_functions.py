import pandas as pd
from numpy import nan

from apps.etl.models import Population


class GenericTransformingFunctions:
    _default_ratio_keys = ('weekly_recovered_infected_ratio', 'weekly_deaths_infected_ratio',)

    @staticmethod
    def add_cumulative_stats(df, region=False):
        base_query = df.groupby('region') if region else df
        df['recovered'] = base_query['weekly_recovered'].cumsum()
        df['deaths'] = base_query['weekly_deaths'].cumsum()
        df['infected'] = base_query['weekly_infected'].cumsum()
        df[['recovered', 'deaths', 'infected']].ffill(inplace=True)

        if not region:
            df['first_component'] = base_query['weekly_first_component'].cumsum()
            df['second_component'] = base_query['weekly_second_component'].cumsum()
            df[['first_component', 'second_component', ]].ffill(inplace=True)

    @staticmethod
    def add_per_100000_stats(df, region=False):
        mapping = Population.get_region_population_map() if region else Population.get_global_population_map()

        def calculate_columns(row):
            if region:
                year_mapping = mapping.get(row.start_date.year)
                population = year_mapping[row.region] if year_mapping else mapping[row.start_date.year - 1][row.region]
            else:
                population = mapping.get(row.start_date.year)
                population = population if population else mapping[row.start_date.year - 1]

            columns = {
                'weekly_infected_per_100000': row.weekly_infected / population * 100000,
                'weekly_deaths_per_100000': row.weekly_deaths / population * 100000,
                'weekly_recovered_per_100000': row.weekly_recovered / population * 100000,
                'infected_per_100000': row.infected / population * 100000,
                'deaths_per_100000': row.deaths / population * 100000,
                'recovered_per_100000': row.recovered / population * 100000,
            }
            return columns

        applied_df = df.apply(lambda row: calculate_columns(row), axis=1, result_type='expand')
        df = pd.concat([df, applied_df], axis=1)

        return df

    @classmethod
    def add_ratio_stats(cls, df, region=False):
        mapping = Population.get_global_population_map() if not region else {}

        def calculate_columns(row):
            columns = dict.fromkeys(cls._default_ratio_keys, None)

            if row.weekly_infected:
                columns = {
                    'weekly_recovered_infected_ratio': row.weekly_recovered / row.weekly_infected,
                    'weekly_deaths_infected_ratio': row.weekly_deaths / row.weekly_infected,
                }

            if not region:
                population = mapping.get(row.start_date.year)
                population = population if population else mapping[row.start_date.year - 1]

                columns['vaccinations_population_ratio'] = row.second_component / population

                if row.weekly_infected:
                    columns['weekly_vaccinations_infected_ratio'] = row.weekly_vaccinations / row.weekly_infected
                else:
                    columns['weekly_vaccinations_infected_ratio'] = None

            return columns

        applied_df = df.apply(lambda row: calculate_columns(row), axis=1, result_type='expand')
        df = pd.concat([df, applied_df], axis=1)

        return df

    @classmethod
    def replace_nan_with_none(cls, df):
        df.replace({nan: None}, inplace=True)

    @classmethod
    def apply_all_transforms(cls, df, region=False):
        cls.add_cumulative_stats(df, region)
        df = cls.add_per_100000_stats(df, region)
        df = cls.add_ratio_stats(df, region)
        cls.replace_nan_with_none(df)

        return df
