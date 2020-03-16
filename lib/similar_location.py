import pandas as pd
import numpy as np
import scipy.sparse as sps 
from sklearn.cluster import AffinityPropagation
from sklearn.preprocessing import normalize,scale
from lib.base import Reason
from lib.header import reason_type
from datetime import date

class Lookalike(Reason):
    def __init__(self, sources):
        Reason.__init__(self, sources, reason_type['item2item']['lookalike'])
    
    @staticmethod
    def _transpd2np_single(featdat, cont_col_name: list, dum_col_name: list):
        XC = featdat.loc[:, cont_col_name].to_numpy()   
        XD = featdat.loc[:, dum_col_name].to_numpy()
        return XC, XD

    @staticmethod
    def _get_para_normalize_dat(trX, axis=0):
        center = trX.mean(axis=axis)
        scale = trX.std(axis=axis)
        scale += 1e-4
        return center, scale

    @staticmethod
    def _apply_para_normalize_dat(X, center, scale, axis=0):
        """
        X can be pd or numpy!
        """
        center = np.expand_dims(center, axis)
        scale = np.expand_dims(scale, axis)
        X = (X - center) / scale
        return X

    @staticmethod
    def _percentage(row):
        ind_sum = 0
        for column in row.columns.to_list():
            if column.startswith('industry'):
                ind_sum += row[column]
        for column in row.columns.to_list():
            if column.startswith('industry'):
                row[column] = row[column]*1.0 / ind_sum 
        return row

    def export(self):
        ls_df =  self.sources['location_scorecard']
        building_df = self.sources['building']
        company_df = self.sources['company']
        geo_df = self.sources['geography']
        account_df = self.sources['account']

        key_col = 'atlas_location_uuid'
        us_building_df = building_df[(building_df['country'] == 'USA') & 
                                    (~building_df['atlas_location_uuid'].isna()) & 
                                    (building_df['atlas_location_uuid']!='TestBuilding')]
        company_df = company_df[~company_df.industry.isna()]
        acc_com_df = account_df.merge(company_df, on='company_id')
        us_building_geo_df = geo_df.merge(us_building_df, on='building_id')
        us_acc_com_building_geo_df = acc_com_df.merge(us_building_geo_df, on='geo_id')
        us_industry_df = pd.get_dummies(us_acc_com_building_geo_df['industry'], prefix='industry')
        us_industry_df = pd.concat([us_acc_com_building_geo_df[[key_col]], us_industry_df], axis=1)
        us_industry_df = us_industry_df.groupby(key_col).apply(Lookalike._percentage).reset_index(drop=False)
        us_ls_industry_df = ls_df.merge(us_industry_df, on=key_col)

        column_single = []
        for column in us_ls_industry_df.columns.to_list():
            if len(us_ls_industry_df[column].unique()) == 1:
                column_single.append(column)
        if len(column_single) > 0:
            us_ls_industry_df = us_ls_industry_df.drop(columns=column_single)
        location_features, industry_features = [], []
        loc_ind_features = []
        for name in us_ls_industry_df.columns.to_list():
            if name in [key_col, 'Unnamed: 0']: continue
            loc_ind_features.append(name)
            if name.startswith('industry'):
                industry_features.append(name)
            else:
                location_features.append(name)
        print (loc_ind_features)
        XC = us_ls_industry_df.loc[:, loc_ind_features].to_numpy()   
        #XC, XD = Lookalike._transpd2np_single(us_ls_industry_df, location_features, industry_features)
        center, scale = Lookalike._get_para_normalize_dat(XC)
        XC = Lookalike._apply_para_normalize_dat(XC, center, scale)
        Xloc = XC
        #Xloc = np.concatenate([XC, XD], axis=1)
        
        print ('data prepared ...')
        clustering = AffinityPropagation().fit(Xloc)
        loc_group_df = pd.DataFrame({'atlas_location_uuid': us_ls_industry_df.atlas_location_uuid.to_list(),
                                    'city': us_ls_industry_df.city.to_list(),
                                    'cluster_id': clustering.labels_.to_list()})
        return loc_group_df

class Covisit(Reason):
    def __init__(self, sources):
        Reason.__init__(self, sources, reason_type['item2item']['covisit'])

    @staticmethod
    def _slide_window(row):
        window_size = 3 
        tour = row.sort_values('tour_date', ascending=True)
    
        covisit_pairs = []
        city = tour['city'].to_list()
        location_id = tour['atlas_location_uuid'].to_list()
        tour_date = tour['tour_date'].to_list()
        p1, p2 = 0, 1
        for i, date_str in enumerate(tour_date):
            year, month, day = date_str.split('-')
            tour_date[i] = date(year=int(year), month=int(month), day=int(day))
            
        while p2 < len(tour_date):
            delta_days = (tour_date[p2] - tour_date[p1]).days
            if delta_days <= window_size and location_id[p2] != location_id[p1]:
                covisit_pairs.append((location_id[p2], location_id[p1], city[p2], city[p1]))
                covisit_pairs.append((location_id[p1], location_id[p2], city[p1], city[p2]))
                #covisit_pairs.append((location_ids[p1], cities[p1], location_ids[p2], cities[p2]))
                p2 += 1
            else:
                p1 += 1
            if p2 == p1:
                p2 += 1
        if len(covisit_pairs) > 0:
            return pd.DataFrame(data=covisit_pairs, columns=['atlas_location_uuid', 'atlas_location_uuid_covisit', 'city', 'city_covisit'])


    def export(self):
        tour_df = self.sources['tour']
        tour_df = tour_df[(~tour_df['atlas_location_uuid'].isna()) & 
                        (~tour_df['account_id'].isna()) &
                        (~tour_df['tour_date'].isna())].reset_index(drop=True)
        sorted_tour_df = tour_df.groupby('account_id').apply(Covisit._slide_window)
        covisit_df = sorted_tour_df.reset_index(drop=False)
        covisit_df = covisit_df.drop(columns='level_1')
        covisit_df = covisit_df.groupby(['atlas_location_uuid', 'atlas_location_uuid_covisit', 'city', 'city_covisit']).count()
        covisit_df = covisit_df.reset_index(drop=False)
        covisit_df = covisit_df.rename(columns={'account_id': 'count'})
        covisit_df = covisit_df.groupby(['atlas_location_uuid']).apply(lambda row: row.loc[row['count'].idxmax()])
        return covisit_df


class CF(Reason):
    def __init__(self, sources):
        Reason.__init__(self, sources, reason_type['item2item']['CF'])

    @staticmethod
    def _geo_distance(lon1,lon2,lat1,lat2):
        try:
            radius = 6371 # km
            dlat = math.radians(lat2-lat1)
            dlon = math.radians(lon2-lon1)
            a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
                * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
            d = radius * c
        except:
            d = 1000
        return d

    @staticmethod
    def _memory_based(parameter, train, test_vec):
        '''
        K: number of neighbors
        metric: cosine or dot-product
        method: mean or weighted-mean
        '''
        K = int(parameter.get("K"))
        metric = parameter.get("metric")
        method = parameter.get("method")
        pcc = True if parameter.get("pcc") == "true" else False
        neighbor_explored = {}
        start = time.time()
        train_norm = train.copy()
        avg = np.zeros(train.shape[0])
        std = np.zeros(train.shape[0])
        if pcc:
            train_norm, avg, std = CF._pccAccount(train_norm)
        if metric == "cosine":
            train_norm = normalize(train_norm)   # normalize by row
        #print ('data prepared!')
        [neighbor_explored, test_pred] = CF.memory_helper(K, method, pcc, neighbor_explored, train, train_norm, avg, std, test_vec)
        duration = time.time()-start
        print ('running time:', duration)
        # print "KNN for account 4321: ", neibor_explored[4321][0]
        return test_pred, neighbor_explored

    @staticmethod
    def _memory_helper(K, method, pcc, neighbor_explored, train, train_norm, avgs, stds, vec):
        pred = []
        for pair in vec:
            account = pair[0]
            location = pair[1]
            if not account in neighbor_explored:
                sim = train_norm.dot(train_norm[account,:].T)
                sim[account,0] = 0
                sim = sim.transpose()
                sim = sim.todense()
                knn_account1 = np.argpartition(-sim, K)[0,:K]
                knn_account2 = np.expand_dims(np.where(sim == 1)[1], axis=0)

                if knn_account1.shape[1] > knn_account2.shape[1]:
                    knn_account = knn_account1
                else:
                    knn_account = knn_account1
        
                knn_weight = sim[0, knn_account]
                neighbor_explored[account] = (knn_account, knn_weight)
            (knn_account, knn_weight) = neighbor_explored.get(account)
            rate, std, avg = 0, 1, 0
            if pcc:
                if avgs.shape[0] == train.shape[0]:
                    std = stds[account] if stds[account] > 0 else 1
                    avg = avgs[account]
                else:
                    std = stds[location] if stds[location] > 0 else 1
                    avg = avgs[location]
            if method == "mean":
                for i in range(K):
                    rate += (train[knn_account[0,i], location] - avg) / std
                rate /= K
            else:
                weight_norm = normalize(knn_weight,norm='l1')
                for i in range(K):
                    rate += weight_norm[0,i] * (train[knn_account[0,i], location] - avg) / std
            rate = rate * std + avg
            rate += 3
            pred.append(rate)
        return neighbor_explored, pred

    @staticmethod
    # account bias reduction
    def _pccAccount(train):
        avgs = np.zeros(train.shape[0])
        stds = np.zeros(train.shape[0])
        for row in range(train.shape[0]):
            if train[row].size:
                avg = np.mean(train[row,:].data)
                std = np.std(train[row,:].data)
                avgs[row] = avg
                stds[row] = std
                train.data[train.indptr[row]:train.indptr[row+1]] -= avg
                if std > 0:
                    train.data[train.indptr[row]:train.indptr[row+1]] /= std
        return train, avgs, stds
   
    @staticmethod
    # location bias reduction
    def _pccLocation(train):
        train = train.transpose()
        avgs = np.zeros(train.shape[0])
        stds = np.zeros(train.shape[0])
        for row in range(train.shape[0]):
            if train[row].size:
                avg = np.mean(train[row,:].data)
                std = np.std(train[row,:].data)
                avgs[row] = avg
                stds[row] = std
                train.data[train.indptr[row]:train.indptr[row+1]] -= avg
                if std > 0:
                    train.data[train.indptr[row]:train.indptr[row+1]] /= std
        return train.transpose(), avgs, stds

    @staticmethod
    def _negative_sampling(us_building_geo_df, covisit_df):
        covisit_set = set((covisit_df['atlas_location_uuid'] + '|' + covisit_df['atlas_location_uuid_covisit']).to_list())
        neutral_location_pairs = []
        negative_location_pairs = []
        locations = []
        for index, row in us_building_geo_df.iterrows():
            locations.append((row['atlas_location_uuid'], row['longitude'], row['latitude'], row['city']))
            
        for loc1 in locations:
            city1 = loc1[3]
            for loc2 in locations:
                city2 = loc2[3]
                if loc1 == loc2 or city1 != city2: continue
                distance = CF._geo_distance(loc1[1], loc2[1], loc1[2], loc2[2])
                if distance < 5:
                    neutral_location_pairs.append((loc1[0], loc2[0]))
                elif distance < 100 and not (loc1[0] + '|' + loc2[0]) in covisit_set:
                    negative_location_pairs.append((loc1[0], loc2[0], loc1[3]))
        # neutral_df = pd.DataFrame(neutral_location_pairs, columns={'atlas_location_uuid', 'atlas_location_uuid_covisit'})
        # neutral_df = pd.concat([neutral_df, covisit_df], axis=0)
        # neutral_df = neutral_df[~neutral_df.duplicated(['atlas_location_uuid', 'atlas_location_uuid_covisit'])]
        negpairs_df = pd.DataFrame(negative_location_pairs, columns={'atlas_location_uuid', 'atlas_location_uuid_covisit', 'city'})
        return negpairs_df

    @staticmethod
    def _rating_generation(op_df, negpairs_df):
    
        op_df.loc[:,'rating'] = 5
        nop_df = op_df.groupby('city').apply(lambda city_op_df: city_op_df.merge(negpairs_df[negpairs_df['city'].isin(city_op_df.city)], on='atlas_location_uuid')).reset_index(drop=False)
        nop_df = nop_df.drop(columns='atlas_location_uuid')
        nop_df = nop_df.rename(columns={'atlas_location_uuid_covisit': 'atlas_location_uuid'})
        #nop_df = nop_df[(~nop_df['atlas_location_uuid'].isna()) & (~nop_df['account_id'].isna())]
        nop_df.loc[:, 'rating'] = 1
        columns = ['account_id', 'atlas_location_uuid', 'rating']
        rating_df = pd.concat([op_df[columns], nop_df[columns]], axis=0)
        rating_df = rating_df.groupby(['account_id', 'atlas_location_uuid']).apply(lambda row: row.loc[row['rating'].idxmax()])
        NUM_ACCOUNT, NUM_LOCATION = len(rating_df['account_id'].unique()), len(rating_df['atlas_location_uuid'].unique())
        acc2id = {acc_id: idx for idx, acc_id in enumerate(rating_df['account_id'].unique())}
        loc2id = {loc_id: idx for idx, loc_id in enumerate(rating_df['atlas_location_uuid'].unique())}
        rating = sps.lil_matrix((NUM_ACCOUNT, NUM_LOCATION))
        for index, row in rating_df.iterrows():
            rating[acc2id[row['account_id']], loc2id[row['atlas_location_uuid']]] = row['rating']-3.0
        rating = sps.csr_matrix(rating)
        return rating 

    def export(self, covisit_df):
        geo_df = self.sources['geography']
        building_df = self.sources['building']
        op_df = self.sources['opportunity']
        us_building_df = building_df[(building_df['country'] == 'USA') & 
                                    (~building_df['atlas_location_uuid'].isna()) & 
                                    (building_df['atlas_location_uuid']!='TestBuilding')]
        op_df = op_df[op_df['atlas_location_uuid'].isin(us_building_df['atlas_location_uuid'].unique()) &
                        (~op_df['account_id'].isna())]
        covisit_df = covisit_df[['atlas_location_uuid', 'atlas_location_uuid_covisit']]    
        us_building_geo_df = pd.merge(us_building_df, geo_df, on='geo_id') 
        negpairs_df = CF._negative_sampling(us_building_geo_df, covisit_df)
        
        us_op_df = op_df.merge(us_building_df, on='atlas_location_uuid')
        rating = CF._rating_generation(us_op_df, negpairs_df)