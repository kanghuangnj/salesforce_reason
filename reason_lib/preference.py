from reason_lib.base import Reason
from reason_lib.header import reason_type
import pandas as  pd
class Industry(Reason):
    def __init__(self, sources):
        Reason.__init__(self, sources, reason_type['preference']['industry'])

    @staticmethod
    def _popular_industry(row):
        industry = row.groupby('industry').count()
        industry['percentage'] = industry['account_id'] / industry['account_id'].sum()
        industry['count'] = industry['account_id']
        industry = industry.sort_values("percentage", ascending=False)
        industry['rank'] = industry['percentage'].rank(method='first', ascending=False)
        return industry[['count', 'percentage', 'rank']]

    def export(self):
        account_df = self.sources['account']
        geo_df = self.sources['geography']
        company_df = self.sources['company']
        building_df = self.sources['building']
        us_building_df = building_df[(building_df['country'] == 'USA') &
                                    (~building_df['atlas_location_uuid'].isna()) & 
                                    (building_df['atlas_location_uuid'] != 'TestBuilding')]
        company_df = company_df[~company_df.industry.isna()]
        acc_com_df = account_df.merge(company_df, on='company_id')
        geo_df = geo_df.merge(us_building_df, on='building_id')
        acc_com_geo_df = acc_com_df.merge(geo_df, on='geo_id')
        industry_df = acc_com_geo_df.groupby(['atlas_location_uuid']).apply(Industry._popular_industry).reset_index(drop=False)
        industry_df = industry_df.sort_values('count', ascending=False).reset_index(drop=True)
        return industry_df

    @staticmethod
    def _concat_reason(rows):
        template = 'the %.2f%% company of this building is %s'
        reasons = []
        for index, row in rows.iterrows():
            reasons.append(template % (round(row['percentage']*100, 2), row['industry']))
        return pd.DataFrame({'atlas_location_uuid': [row['atlas_location_uuid']], 'reason': ['; '.join(reasons)]})

    def export_reason(self):
        industry_df = self.cache_reason_df
        topk = 3
        topk_industry_df = industry_df[industry_df['rank'].isin([1, 2, 3])]
        topk_industry_df = topk_industry_df.groupby('atlas_location_uuid').apply(Industry._concat_reason).reset_index(drop=True)
        keycol = ['atlas_location_uuid', 'reason']
        return topk_industry_df[keycol]
