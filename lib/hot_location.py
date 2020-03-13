from lib.base import Reason
from lib.header import reason_type, DATAPATH
from collections import defaultdict
from datetime import datetime, date
import time
import pandas as pd
import networkx as nx 

class Longterm(Reason):
    def __init__(self, sources):
        Reason.__init__(self, sources, reason_type['hot_location']['long_term'])

    def decay(self, row):
        max_contract_revenue = 3780
        cur_date = date.fromtimestamp(time.time())
        year, month, day = row['date'].split('-')
        delta_month = (cur_date.year-int(year))*12+cur_date.month-int(month)
        row['decay_contract_revenue'] = (1.02**(delta_month//12))*(0.98**delta_month) * min(max_contract_revenue, row['contract_revenue'])
        return row

    def expect_revenue(self, row):
        valid_row = row[~row.contract_revenue.isna()]
        valid_row = valid_row[valid_row['stage']=='Closed Won']
        if len(valid_row) == 0:
            revenue = len(row)
        else:
            valid_row = valid_row.apply(self.decay, axis=1)
            revenue = valid_row['decay_contract_revenue'].sum()
            revenue /= len(valid_row)
        top_row = row.head(1)
        top_row['avg_revenue'] = revenue 
        return top_row


    def export(self):
        # op_df: Salesforce opportunity table
        op_df = self.sources['opportunity']
        building_df = self.sources['building']
        us_building_df = building_df[building_df['country'] == 'USA']
        op_df = op_df[op_df['atlas_location_uuid'].isin(us_building_df['atlas_location_uuid'].unique())]
        op_df = op_df.merge(us_building_df[['atlas_location_uuid', 'city']], on='atlas_location_uuid')
        headquarter_id = 'cec7a8c2-4a49-4db0-8b89-53c80e2fa83a'
        op_df = op_df[op_df.atlas_location_uuid != headquarter_id]
        group_op_df = op_df.groupby(['account_id', 'atlas_location_uuid']).apply(self.expect_revenue)

        loc2id = defaultdict(int)
        acc2id = defaultdict(int)

        for loc_uuid in op_df['atlas_location_uuid'].unique():
            loc2id[loc_uuid] = len(loc2id)
        for acc_id in op_df['account_id'].unique():
            acc2id[acc_id] = len(acc2id)+len(loc2id)

        id2loc={idx:loc_uuid for loc_uuid, idx in loc2id.items()}  
        id2acc={idx:acc_id for acc_id, idx in acc2id.items()}
        G = nx.DiGraph() 
        for edge, row in group_op_df.iterrows():
            G.add_edge(acc2id[edge[0]], loc2id[edge[1]], weight=edge[2]) 
        hubs, authorities = nx.hits(G, max_iter = 100, normalized = True) 
        acc_hub_scores = []
        loc_auth_scores = []
        for acc_hub_score, loc_auth_score in zip(hubs.items(), authorities.items()):
            acc_hub, hub_score = acc_hub_score
            loc_auth, auth_score = loc_auth_score
            if acc_hub >= len(loc2id):
                acc_hub_scores.append((id2acc[acc_hub], hub_score))
            if loc_auth < len(loc2id):
                loc_auth_scores.append((id2loc[loc_auth], auth_score))
        acc_hub_scores = sorted(acc_hub_scores, key=lambda x: x[1], reverse=True)
        loc_auth_scores = sorted(loc_auth_scores, key=lambda x: x[1], reverse=True)
        company_scores = pd.DataFrame(acc_hub_scores, columns=['account_id', 'score'])
        location_scores = pd.DataFrame(loc_auth_scores, columns=['atlas_location_uuid', 'score'])
        city_location_scores = pd.merge(location_scores,
                                   op_df[['atlas_location_uuid', 'city']], 
                                   how='left', 
                                   on='atlas_location_uuid')
        city_location_scores = city_location_scores[~city_location_scores.duplicated(['atlas_location_uuid'])]
        city_location_scores = city_location_scores[['atlas_location_uuid', 'score', 'city']].reset_index(drop=True)
        city_num = len(loc2id)
        top_location = city_location_scores[:city_num//3]
        k = 3
        top_count = top_location.groupby(['city']).count()
        good_cities = top_count[top_count.score >= k].index.to_list()
        mediocre_df = city_location_scores.groupby(['city']).apply(lambda row: row.sort_values(by=['score'], ascending=False).head(k)).reset_index(drop=True)
        good_df = top_location[top_location.city.isin(good_cities)]
        hot_df = pd.concat([mediocre_df, good_df], axis=0)
        hot_df = hot_df[~hot_df.duplicated(['atlas_location_uuid'])]
        hot_df = hot_df.sort_values(by=['score'], ascending=False).reset_index(drop=True)
    #     notop_hot_df = hot_df.iloc[1:]
    #     notop_score_sum = notop_hot_df['score'].sum()
    #     notop_hot_df['score'] = notop_hot_df.apply(lambda row: row['score']/notop_score_sum, axis=1)
    #     hot_df = pd.concat([hot_df.iloc[0:1], notop_hot_df], axis=0)
        headquarter_row = pd.DataFrame([[headquarter_id, 1.0, 'New York']], columns=['atlas_location_uuid', 'score', 'city'])
        hot_df = pd.concat([headquarter_row, hot_df], axis=0).reset_index(drop=False)
        hot_df = hot_df.rename(columns={'index': 'global_rank'})
        hot_df['city_rank'] = hot_df.groupby(['city'])['score'].rank(method='first', ascending=False)
        return hot_df

if __name__ == '__main__':
    sources = ['opportunity', 'building']
    reason = Longterm(sources)
    reason.export()