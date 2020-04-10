import os
import json
import numpy as np
from reason_lib.base import Reason
from reason_lib.header import reason_type, DATAPATH

pj = os.path.join

class Salesforce_Loader(Reason):
    """
    generate [salesforce_pair] from [opportunities]
    """
    def __init__(self, sources):
        Reason.__init__(self, sources, 'salesforce_cotext')

    def export(self):
        building_df = self.sources['building']
        op_df = self.sources['opportunity']
        us_building_df = building_df[(building_df['country'] == 'USA') &
                            (~building_df['atlas_location_uuid'].isna()) & 
                            (building_df['atlas_location_uuid'] != 'TestBuilding')]
        us_building_df = us_building_df.drop(columns='country')
        op_city_df = op_df.merge(us_building_df, on='atlas_location_uuid').drop(columns='atlas_location_uuid')
        op_city_df = op_city_df.drop_duplicates(['account_id', 'city'], keep='last')
        # op_atlas_df = op_city_df.merge(us_building_df, on='city')
        # op_atlas_df = op_atlas_df.drop(columns='city')
        return {'acc2city': op_city_df,
                }


    # def generate(self , save_pos_pair_name ,save_opp_x_atlas_name='salesforce/salesforce_opp_x_atlas.csv'):
    #     datapath = self.datapath
    #     bid = self.bid
    #     fid = self.fid

    #     dtld = data_process(root_path=datapath)
    #     city = dtld.ls_col['city']

    #     origin_opp_file = self.opp_file
    #     lscardfile = self.lscard_file

    #     opp_pos_pair = dtld.load_opportunity(db='', dbname=origin_opp_file, save_dbname=save_pos_pair_name)
    #     print('opp_pos_pair:%d saved' % len(opp_pos_pair))
    #     lscard = dtld.load_location_scorecard_msa(db='', dbname=lscardfile, is_wework=True)

    #     opp_pos_city = opp_pos_pair[[bid, fid]].merge(lscard, on=bid, suffixes=sfx)[[fid, city]]
    #     opp_pos_city = opp_pos_city.drop_duplicates([fid, city], keep='last')
    #     print('opp_pos_city:%d' % len(opp_pos_city))

    #     opp_atlas = opp_pos_city.merge(lscard[[bid, city]], on=city, suffixes=sfx)[[fid, bid, city]]

    #     savepath = pj(datapath, save_opp_x_atlas_name)
    #     opp_atlas.to_csv(savepath)
    #     print('%d opp_x_atlas saved.' % len(opp_atlas))
    #     return opp_atlas


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)

class Formatter():
    def __init__(self, dataframe):
        self.key_col = ['account_id', 'atlas_location_uuid'] 
        self.dataframe = dataframe
        self.df_cols = dataframe.columns.to_list()
        self.key_col = [col for col in self.key_col if col in self.df_cols]

    def trans_reason_df2json(self, rows):
        payload = {}
        multivalue_field = set()
        singlevalue_field = set()
        for col in self.df_cols:
            if col in self.key_col: continue 
            subtype, top_level, second_level = col.rsplit('_', 2)
            _type = top_level+'_'+second_level
    
            if len(rows[col].unique()) > 1:
                multivalue_field.add(_type)
            elif not _type in multivalue_field:
                singlevalue_field.add(_type)

        for col in self.df_cols:
            if col in self.key_col: continue 
            subtype, top_level, second_level = col.rsplit('_', 2)           
            if not top_level in payload:
                payload[top_level] = {}
            if not second_level in payload[top_level]:
                payload[top_level][second_level] = {}
            
            _type = top_level+'_'+second_level
            
            if _type in singlevalue_field:
                if rows.iloc[0][col] == rows.iloc[0][col]:
                    payload[top_level][second_level][subtype] = rows.iloc[0][col]  
                else:
                    del payload[top_level][second_level]
            elif _type in multivalue_field:
                payload[top_level][second_level] = []

        multivalue_keycol = []
        for field in multivalue_field:
            multivalue_keycol.extend([col for col in self.df_cols if col.endswith(field)])

        for idx, row in rows.iterrows():
            packet = {}
            for col in multivalue_keycol:
                subtype, top_level, second_level = col.rsplit('_', 2)
                if not top_level in packet:
                    packet[top_level] = {}
                if not second_level in packet[top_level]:
                    packet[top_level][second_level] = {}
                packet[top_level][second_level][subtype] = row[col]
       
            for top_level in packet:
                for second_level in packet[top_level]:
                    payload[top_level][second_level].append(packet[top_level][second_level])

        for top_level in list(payload.keys()):
            if len(payload[top_level]) == 0:
                del payload[top_level]
        row['reason'] = self.trans_reason_json2str(payload)
        row[['reason']] = row[['reason']].astype(str)
        return row[['reason']]

    def trans_reason_json2str(self, reason_json):
        reason_list = {}
        for top_key in reason_json:
            subreason_list = [subreason['reason'] for subreason in reason_json[top_key].values()]
            sub_reason_str = '; '.join(subreason_list)
            reason_list[top_key] = sub_reason_str
        reason_json = json.dumps(reason_list)
        return reason_json

    def transform(self):
        dataframe = self.dataframe.groupby('atlas_location_uuid').apply(self.trans_reason_df2json)
        df_cols = dataframe.columns.to_list()
        key_col = [col for col in self.key_col if col in df_cols]
        dataframe = dataframe[key_col+['reason']]
        return dataframe

def recall(gold, pred): 
    recall = 0
    goldset = set()
    for g in gold:
        goldset.add(g)
    for loc in pred:
        if loc in goldset:
            recall += 1.0
    return recall / len(gold)
    

def recall_evaluate(candidates_df):
    gt_df = pj(DATAPATH, 'reason_evaluation_pairs.csv')
    target_accounts = gt_df.account_id.unique()
    avg_sim = 0
    for acc_id in target_accounts:
        acc_gt_df = gt_df[gt_df['account_id'] == acc_id]
        query = acc_gt_df.loc[acc_gt_df['date'].idxmax()]['atlas_location_uuid']
        gt_loc_list = acc_gt_df[acc_gt_df['atlas_location_uuid']!=query].unique().tolist()
        pred_loc_list = candidates_df[candidates_df['account_id'] == acc_id]
        sim = recall(pred_loc_list, gt_loc_list) 
        avg_sim += sim
    avg_sim /= len(target_accounts)
    return avg_sim
