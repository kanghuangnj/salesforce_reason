import os
pj = os.path.join
class Salesforce_Pair(object):
    """
    generate [salesforce_pair] from [opportunities]
    """
    def __init__(self, datapath, opp_file, lscard_file):
        self.datapath = datapath
        self.opp_file = opp_file
        self.lscard_file = lscard_file
        self.bid = bid
        self.fid = fid

    def generate(self , save_pos_pair_name ,save_opp_x_atlas_name='salesforce/salesforce_opp_x_atlas.csv'):
        datapath = self.datapath
        bid = self.bid
        fid = self.fid

        dtld = data_process(root_path=datapath)
        city = dtld.ls_col['city']

        origin_opp_file = self.opp_file
        lscardfile = self.lscard_file

        opp_pos_pair = dtld.load_opportunity(db='', dbname=origin_opp_file, save_dbname=save_pos_pair_name)
        print('opp_pos_pair:%d saved' % len(opp_pos_pair))
        lscard = dtld.load_location_scorecard_msa(db='', dbname=lscardfile, is_wework=True)

        opp_pos_city = opp_pos_pair[[bid, fid]].merge(lscard, on=bid, suffixes=sfx)[[fid, city]]
        opp_pos_city = opp_pos_city.drop_duplicates([fid, city], keep='last')
        print('opp_pos_city:%d' % len(opp_pos_city))

        opp_atlas = opp_pos_city.merge(lscard[[bid, city]], on=city, suffixes=sfx)[[fid, bid, city]]

        savepath = pj(datapath, save_opp_x_atlas_name)
        opp_atlas.to_csv(savepath)
        print('%d opp_x_atlas saved.' % len(opp_atlas))
        return opp_atlas


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
                payload[top_level][second_level][subtype] = rows.iloc[0][col]   
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

        row['reason'] = payload
        return row

    def transform(self):
        dataframe = self.dataframe.groupby('atlas_location_uuid').apply(self.trans_reason_df2json)
        df_cols = dataframe.columns.to_list()
        key_col = [col for col in self.key_col if col in df_cols]
        dataframe = dataframe[key_col+['reason']]
        return dataframe




