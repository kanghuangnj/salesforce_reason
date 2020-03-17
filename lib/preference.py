from lib.base import Reason
from lib.header import reason_type
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
        company_df = company_df[~company_df.industry.isna()]
        acc_com_df = account_df.merge(company_df, on='company_id')
        geo_df = geo_df.merge(building_df, on='building_id')
        acc_com_geo_df = acc_com_df.merge(geo_df, on='geo_id')
        industry_df = acc_com_geo_df.groupby(['atlas_location_uuid']).apply(Industry._popular_industry).reset_index(drop=False)
        industry_df = industry_df.sort_values('count', ascending=False).reset_index(drop=True)
        return industry_df