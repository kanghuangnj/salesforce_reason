from lib.hot_location import Longterm, Occupancy, Shortterm
from lib.similar_location import Lookalike, Covisit, CF
from lib.preference import Industry
from lib.header import CACHEPATH, reason_type, features_mappings
from functools import reduce
import os, glob
import pandas as pd
pj = os.path.join
reason_function = {}

def define_reason(sources, reason_type, Reason):
    key_col = ['account_id', 'atlas_location_uuid']
    def execute_reason():
        save_path = pj(CACHEPATH, reason_type + '.csv')
        
        if os.path.exists(save_path):
            reason_df = pd.read_csv(save_path)
        else:
            reason = Reason(sources)
            reason_df = reason.export()
            cols = reason_df.columns.to_list()
            reason_df = reason_df.rename(columns={col: col+'_'+reason_type for col in cols if not col in key_col })
            reason_df.to_csv(save_path)
        return reason_df
    return execute_reason


unfinished_subtype = ['similar_location_lookalike', 'similar_location_CF']

for toplevel_type in reason_type:
    # if not toplevel_type in ['hot_location']: continue
    for second_type in reason_type[toplevel_type]:
        subtype = reason_type[toplevel_type][second_type]
        sources = list(features_mappings[subtype].keys())
        if subtype in unfinished_subtype: continue
        reason_class = second_type[0].upper() + second_type[1:]
        reason_function[subtype] = define_reason(sources, subtype, eval(reason_class))


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
    
    get_suffix = lambda reason_name: '_' + reason_name.rsplit('_', 1)[-1]
    # initial merge
    first_reason_name = reason_names[0]
    first_reason = reason_dfs[first_reason_name]
    
    reason_names = [first_reason] + reason_names[1:]
    merged_reason_df = reduce(lambda  left_df, right_name: pd.merge(left_df,reason_dfs[right_name],on=key_col,
                                                            how='outer', suffixes=('', get_suffix(right_name))), reason_names)

    merged_reason_df = merged_reason_df.rename(columns={col: col+'_'+first_reason_name for col in first_reason.columns.to_list() if col != key_col} )
    merged_reason_df.to_csv(pj(CACHEPATH, 'merge_reasons.csv'))
    return merged_reason_df

def generate_pairs():
    pass

def print_msg(msg):
    # This is the outer enclosing function

    def printer():
# This is the nested function
        print(msg)

    return printer  # this got changed
    

reason_function['preference_industry']()