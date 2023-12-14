RF_POPULATION = 146447424


class TransformingFunctions:
    @staticmethod
    def add_cumulative_stats(df, region=False):
        group_columns = ['region', 'start_date'] if region else ['start_date']
        df['recovered'] = df.groupby(group_columns)['weekly_recovered'].cumsum()
        df['deaths'] = df.groupby(group_columns)['weekly_deaths'].cumsum()
        df['infected'] = df.groupby(group_columns)['weekly_infected'].cumsum()

        if not region:
            df['first_component'] = df.groupby(group_columns)['weekly_first_component'].cumsum()
            df['second_component'] = df.groupby(group_columns)['weekly_second_component'].cumsum()

    @staticmethod
    def add_per_100000_stats(df):
        def calculate_columns(row):
            columns = {
                'weekly_infected_per_100000': row['weekly_infected'] / RF_POPULATION * 100000,
                'weekly_deaths_per_100000': row['weekly_deaths'] / RF_POPULATION * 100000,
                'weekly_recovered_per_100000': row['weekly_recovered'] / RF_POPULATION * 100000,
                'infected_per_100000': row['infected'] / RF_POPULATION * 100000,
                'deaths_per_100000': row['deaths'] / RF_POPULATION * 100000,
                'recovered_per_100000': row['recovered'] / RF_POPULATION * 100000,
            }
            return columns

        df.apply(lambda row: calculate_columns(row), axis=1, result_type='expand')

    @staticmethod
    def add_ratio_stats(df, region=False):
        def calculate_columns(row):
            columns = {
                'weekly_recovered_infected_ratio': row['weekly_recovered'] / row['weekly_infected'],
                'weekly_deaths_infected_ratio': row['weekly_deaths'] / row['weekly_infected'],
                'weekly_vaccinations_infected_ratio': row['weekly_vaccinations'] / row['weekly_infected'],
                'vaccinations_population_ratio': row['second_component'] / RF_POPULATION,
            }

            if not region:
                columns['vaccinations_population_ratio'] = row['second_component'] / RF_POPULATION

            return columns

        df.apply(lambda row: calculate_columns(row), axis=1, result_type='expand')

    @classmethod
    def apply_all_transforms(cls, df, region=False):
        cls.add_cumulative_stats(df, region)
        cls.add_per_100000_stats(df)
        cls.add_ratio_stats(df, region)
