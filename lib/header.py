import os
pj = os.path.join
reason_type= {
    'hot_location':{
        'long_term': 'hot_location|long_term',
        'occupancy': 'hot_location|occupancy',
        'short_term': 'hot_location|short_term'
    },
    'item2item': {
        'lookalike': 'similar_location|lookalike',
        'co-visit': 'similar_location|co-visit',
        'CF': 'similar_location|CF'
    },
    'preference': {
        'industry': 'preference|industry'
    }
}


features_mappings = {
    reason_type['hot_location']['long_term']:{
        'opportunity':{
            'AccountId': 'account_id',
            'Building_uuid__c': 'atlas_location_uuid',
            'Total_Contract_Revenue__c': 'contract_revenue',
            'StageName': 'stage',
            'LastModifiedDate': 'date'
        },
        'building':{
            'UUID__c': 'atlas_location_uuid',
            'City__c': 'city',
            'Country__c': 'country',
        }
    },
    reason_type['hot_location']['occupancy']:{
        'building':{
            'UUID__c': 'atlas_location_uuid', 
            'Occupancy_Rate__c': 'occupancy', 
            'Occupancy_Rating__c': 'occupancy_rating',
            'City__c': 'city'
        }
    },
    reason_type['hot_location']['short_term']:{
        'tour':{
            'Location_UUID__c': 'atlas_location_uuid',
            'Tour_Date_Time__c': 'tour_date',
            'City__c': 'city'
        }
    },
    reason_type['item2item']['lookalike']:{
        'location_scorecard':{
            'not_feat_col': ['duns_number', 'atlas_location_uuid', 'longitude_loc', 'latitude_loc', 'city'],
            'cont_col_nameC': ['emp_here', 'emp_total', 'sales_volume_us', 'square_footage', 'emp_here_range'],
            'spec_col_nameC': 'emp_here_range',
            'cont_col_nameL': ['score_predicted_eo', 'score_employer', 'num_emp_weworkcore', 'num_poi_weworkcore',
                                'pct_wwcore_employee', 'pct_wwcore_business', 'num_retail_stores', 'num_doctor_offices',
                                'num_eating_places', 'num_drinking_places', 'num_hotels', 'num_fitness_gyms',
                                'population_density', 'pct_female_population', 'median_age', 'income_per_capita',
                                'pct_masters_degree', 'walk_score', 'bike_score'],
        }
    },
    reason_type['item2item']['co-visit']:{
        'tour':{
            'Account_ID__c': 'account_id',
            'Location_UUID__c': 'atlas_location_uuid',
            'Tour_Date__c': 'tour_date',
            'City__c': 'city',
        }
    },
    reason_type['item2item']['CF']:{
        'geo':{
            'Id': 'geo_id',
            'Geocode__Longitude__s': 'longitude',
            'Geocode__Latitude__s': 'latitude'
        },
        'building':{
            'UUID__c': 'atlas_location_uuid',
            'Geography__c': 'geo_id',
            'City__c': 'city',
            'Country__c': 'country',
        }
    },
    reason_type['preference']['industry']:{
        'company':{
            'Industry__c': 'industry',
            'CI_Company_ID__c': 'company_id',
        },
        'geo':{
            'Id': 'geo_id',
            'Nearest_Building__c': 'building_id',    
        },
        'account':{
            'Id': 'account_id',
            'Geography__c': 'geo_id',
            'Unomy_Company_ID__c': 'company_id'
        },
        'building':{
            'Id': 'building_id',
            'UUID__c': 'atlas_location_uuid'
        }
    },
    
}


DATAPATH = '/Users/kanghuang/Documents/work/location_recommendation/salesforce_data'
CACHEPATH = '/Users/kanghuang/Documents/work/location_recommendation/salesforce_reason/cache'
datapaths= {
    'opportunity': pj(DATAPATH, 'sfdc_opportunities_all.csv'),
    'account': pj(DATAPATH, 'sfdc_accounts_all.csv'),
    'building': pj(DATAPATH, 'sfdc_buildings_all.csv'),
    'geography': pj(DATAPATH, 'sfdc_geography_all.csv'),
    'company': pj(DATAPATH, 'sfdc_company_all.csv'),
}