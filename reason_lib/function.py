from reason_lib.hot_location import Longterm, Occupancy, Shortterm
from reason_lib.similar_location import Lookalike, Covisit, CF
from reason_lib.preference import Industry
from reason_lib.utils import Salesforce_Loader, Formatter
from reason_lib.header import DATAPATH, CACHEPATH, reason_type, features_mappings
from functools import reduce
import os, glob, csv
import pandas as pd
pj = os.path.join
reason_function = {}

def define_reason(sources, reason_type, Reason):
    key_col = ['account_id', 'atlas_location_uuid']
    def execute_reason(**context):
        data = context['task_instance'].xcom_pull(task_ids='salesforce_context')
        reason = Reason(sources)
        reason_df = reason.export_reason()
        metric = reason.evaluate(data, reason_df)
        #cols = reason_df.columns.to_list()
        #reason_df = reason_df.rename(columns={col: col+'_'+reason_type for col in cols if not col in key_col })
        save_path = pj(CACHEPATH, reason_type+'_reason.csv')
        reason_df.to_csv(save_path)
        return metric
    return execute_reason


unfinished_subtype = ['similar-location_lookalike', 'similar-location_CF']

for toplevel_type in reason_type:
    # if not toplevel_type in ['hot_location']: continue
    for second_type in reason_type[toplevel_type]:
        subtype = reason_type[toplevel_type][second_type]
        sources = list(features_mappings[subtype].keys())
        if subtype in unfinished_subtype: continue
        reason_class = '_'.join(list(map(lambda name: name[0].upper() + name[1:],  second_type.split('_')))) 
        reason_function[subtype] = define_reason(sources, subtype, eval(reason_class))


def merge_reasons(reason_names, **context):
    reason_dfs = {}
    key_col = 'atlas_location_uuid'
    drop_list = ['Unnamed: 0', 'country', 'city']
    filename_suffix = '*_reason.csv'
    if len(reason_names) == 0:
        for path in glob.glob(pj(CACHEPATH, filename_suffix)):
            reason_df = pd.read_csv(path)
            name = path.strip(filename_suffix).split('/')[-1]
            reason_dfs[name] = reason_df.drop(columns=[col for col in reason_df.columns if col in drop_list])
            reason_names.append(name)
    else:
        for name in reason_names:
            reason_df = context['task_instance'].xcom_pull(task_ids=name)
            reason_dfs[name] = reason_df.drop(columns=[col for col in reason_df.columns if col in drop_list])
    
    get_suffix = lambda reason_name: '_' + reason_name.rsplit('_', 1)[-1]

    for reason_name in reason_dfs:
        reason_df = reason_dfs[reason_name]
        reason_dfs[reason_name] = reason_df.rename(columns={col: col+'_'+reason_name for col in reason_df.columns.to_list() if col != key_col})

    # initial merge
    first_reason_name = reason_names[0]
    first_reason = reason_dfs[first_reason_name]

    reason_names = [first_reason] + reason_names[1:]  #suffixes=('', get_suffix(right_name))), 
    merged_reason_df = reduce(lambda  left_df, right_name: pd.merge(left_df,reason_dfs[right_name],on=key_col,how='outer'), reason_names)
    merged_reason_df.to_csv(pj(CACHEPATH, 'merge_reasons.csv'))
    return merged_reason_df

def post_processing(*args, **context):
    save_path = pj(CACHEPATH, 'merge_reasons.csv')
    if os.path.exists(save_path):
        reason_df = pd.read_csv(save_path, index_col=[0])
    else:
        reason_df = context['task_instance'].xcom_pull(task_ids='merge_all_reasons')
    formatter = Formatter(reason_df)
    new_reason_df = formatter.transform()
    salesforce_pair_df = pd.read_csv(pj(DATAPATH, 'salesforce_pair_prediction.csv'))
    salesforce_reason_df = salesforce_pair_df.merge(new_reason_df, on='atlas_location_uuid')
    #new_reason_df.to_json(pj(CACHEPATH, 'formatted_merged_reasons.json'), orient='index')
    salesforce_reason_df = salesforce_reason_df.dropna(thresh=len(salesforce_reason_df.columns)).reset_index(drop=True)
    salesforce_reason_df.to_csv(pj(CACHEPATH, 'formatted_merged_reasons.csv'),index=False)

def generate_context():
    salesforce_data = Salesforce_Loader(features_mappings['salesforce_context'])
    # cache_path = pj(CACHEPATH, 'salesforce_pair.csv')
    # if not os.path.exist(cache_path):
    #     salesforce_pair =  dataloader.export()
    #     salesforce_pair.to_csv(cache_path)
    # else:
    #     salesforce_pair = pd.read_csv(cache_path)
    return salesforce_data.cache_reason_df


# for name in reason_function:
#     reason_function[name]()
# merge_reasons([])

# post_processing()
#reason_function['similar-location_covisit']()
#reason_function['hot-location_shortterm']()
#reason_function['preference_industry']()
#post_processing()
#merge_reasons([])
#generate_pairs()