from reason_lib.header import features_mappings, datapaths, CACHEPATH
import pandas as pd
import os
pj = os.path.join
class Reason:
    def __init__(self, sources, reason_type):
        cache_path = pj(CACHEPATH, reason_type + '.csv')
        if os.path.exists(cache_path):
            self.cache_reason_df = pd.read_csv(cache_path)
        else:
            self.sources = sources
            self.feature_mappings = features_mappings[reason_type]
            self.formalize()
            self.cache_reason_df = None
            self.cache_reason_df = self.export()
            self.cache_reason_df.to_csv(cache_path)
        
    def formalize(self):
        sources = self.sources
        self.sources = {}
        for source_name in sources:
            source_df = pd.read_csv(datapaths[source_name])
            feature_mapping = self.feature_mappings[source_name]
            source_df = source_df[feature_mapping]
            if type(feature_mapping) is dict:
                source_df = source_df.rename(columns=feature_mapping)
            self.sources[source_name] = source_df