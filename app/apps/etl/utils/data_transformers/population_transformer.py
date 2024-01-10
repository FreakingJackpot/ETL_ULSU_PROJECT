import pandas as pd

from apps.etl.models import Region, StopCoronaData
from apps.etl.utils.data_transformers.regional_transformers import RegionDataTransformer


class PopulationTransformer:

    def __init__(self):
        self.existing_regions = set(Region.objects.values_list('name', flat=True))
        self.existing_regions.add(StopCoronaData.RUSSIAN_FEDERATION)

    def run(self, data):
        df = pd.DataFrame(data)
        RegionDataTransformer.rename_regions(df)
        resulted_df = df[df['region'].isin(self.existing_regions)]
        return resulted_df
