from lib.hot_location import Longterm, Occupancy, Shortterm
from lib.header import CACHEPATH
import os, glob
import pandas as pd
from functools import reduce
pj = os.path.join

def hot_location_longterm():
    sources = ['opportunity', 'building']
    save_path = pj(CACHEPATH, 'longterm.csv')
    if os.path.exists(save_path):
        hot_df = pd.read_csv(save_path)
    else:
        reason = Longterm(sources)
        hot_df = reason.export()
        hot_df.to_csv(save_path)
    return hot_df

def hot_location_occupancy():
    sources = ['building']
    save_path = pj(CACHEPATH, 'occupancy.csv')
    if os.path.exists(save_path):
        occupancy_df = pd.read_csv(save_path)
    else:
        reason = Occupancy(sources)
        occupancy_df = reason.export()
        occupancy_df.to_csv(save_path)
    return occupancy_df

def hot_location_shortterm():
    sources = ['tour']
    save_path = pj(CACHEPATH, 'shortterm.csv')
    if os.path.exists(save_path):
        recent_tour_df = pd.read_csv(save_path)
    else:
        reason = Shortterm(sources)
        recent_tour_df = reason.export()
        recent_tour_df.to_csv(save_path)
    return recent_tour_df

def merge_reasons(reason_names, **context):
    reason_dfs = {}
    key_col = 'atlas_location_uuid'
    drop_list = ['Unnamed: 0', 'country', 'city']
    if len(reason_names) == 0:
        for path in glob.glob(pj(CACHEPATH, '*.csv')):
            reason_df = pd.read_csv(path)
            name = path.strip('.csv').split('/')[-1]
            reason_dfs[name] = reason_df.drop(columns=[col for col in reason_df.columns if col in drop_list])
            reason_names.append(name)
    else:
        for name in reason_names:
            reason_df = context['task_instance'].xcom_pull(task_ids=name)
            reason_dfs[name] = reason_df.drop(columns=[col for col in reason_df.columns if col in drop_list])
    print (reason_names)
    get_suffix = lambda reason_name: '_' + reason_name.rsplit('_', 1)[-1]
    # initial merge
    first_reason = reason_dfs[reason_names[0]]
    reason_names = [first_reason] + reason_names[1:]
    merged_reason_df = reduce(lambda  left_df, right_name: pd.merge(left_df,reason_dfs[right_name],on=[key_col],
                                                            how='outer', suffixes=('', get_suffix(right_name))), reason_names)

    merged_reason_df = merged_reason_df.rename(columns={col: col+'_'+reason_names[0] for col in first_reason.columns.to_list() if col != key_col} )
    merged_reason_df.to_csv(pj(CACHEPATH, 'merge_reasons.csv'))
    return merged_reason_df

def generate_pairs():
    pass
if __name__ == '__main__':
    merge_reasons([])