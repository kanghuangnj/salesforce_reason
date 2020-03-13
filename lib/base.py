from lib.header import features_mappings, datapaths
import pandas as pd
class Reason:
    def __init__(self, sources, reason_type):
        self.sources = sources
        self.features_mappings = features_mappings[reason_type]
        self.formalize()
        
    def formalize(self):
        sources = self.sources
        self.sources = {}
        for source_name in sources:
            source_df = pd.read_csv(datapaths[source_name])
            features_mapping = self.features_mappings[source_name]
            source_df = source_df[features_mapping]
            source_df = source_df.rename(columns=features_mapping)
            self.sources[source_name] = source_df