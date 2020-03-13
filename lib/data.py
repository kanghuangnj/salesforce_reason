class salesforce_pair(object):
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