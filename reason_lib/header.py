import os
pj = os.path.join
reason_type= {

    'hot_location':{
        'longterm': 'hot-location_longterm',
        'occupancy': 'hot-location_occupancy',
        'shortterm': 'hot-location_shortterm'
    },
    'item2item': {
        'lookalike': 'similar-location_lookalike',
        'covisit': 'similar-location_covisit',
        'CF': 'similar-location_CF'
    },
    'preference': {
        'industry': 'preference_industry'
    }
}


features_mappings = {
    'salesforce_pair':{
        'opportunity':{
            'AccountId': 'account_id',
            'Building_uuid__c': 'atlas_location_uuid',
        },
        'building':{
            'UUID__c': 'atlas_location_uuid',
            'City__c': 'city',
            'Country__c': 'country',
        }
        
    },
    reason_type['hot_location']['longterm']:{
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
            'City__c': 'city',
            'Country__c': 'country',
        }
    },
    reason_type['hot_location']['shortterm']:{
        'tour':{
            'Location_UUID__c': 'atlas_location_uuid',
            'Tour_Date_Time__c': 'date',
            'City__c': 'city'
        }
    },
    reason_type['item2item']['lookalike']:{
        'location_scorecard':['atlas_location_uuid', 'score_predicted_eo', 'score_employer', 'num_emp_weworkcore', 'num_poi_weworkcore',
                                'pct_wwcore_employee', 'pct_wwcore_business', 'num_retail_stores', 'num_doctor_offices',
                                'num_eating_places', 'num_drinking_places', 'num_hotels', 'num_fitness_gyms',
                                'population_density', 'pct_female_population', 'median_age', 'income_per_capita',
                                'pct_masters_degree', 'walk_score', 'bike_score'],
        'geography':{
            'Id': 'geo_id',
            'Nearest_Building__c': 'building_id',  
        },
        'account':{
            'Id': 'account_id',
            'Geography__c': 'geo_id',
            'Unomy_Company_ID__c': 'company_id'
        },
        'company':{
            'Industry__c': 'industry',
            'CI_Company_ID__c': 'company_id',
        },
        'building':{
            'Id': 'building_id',
            'UUID__c': 'atlas_location_uuid',
            'City__c': 'city',
            'Country__c': 'country',
        }

    },
    reason_type['item2item']['covisit']:{
        'tour':{
            'Account_ID__c': 'account_id',
            'Location_UUID__c': 'atlas_location_uuid',
            'Tour_Date__c': 'tour_date',
            'City__c': 'city',
        },
        'building':{
            'UUID__c': 'atlas_location_uuid',
            'Address__c': 'address',
        }
    },
    reason_type['item2item']['CF']:{
        'opportunity':{
            'AccountId': 'account_id',
            'Building_uuid__c': 'atlas_location_uuid',
        },
        'geography':{
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
        'geography':{
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
            'UUID__c': 'atlas_location_uuid',
            'Country__c': 'country',
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
    'tour': pj(DATAPATH, 'sfdc_tour_all.csv'),
    'location_scorecard': pj(DATAPATH, 'location_scorecard_200106.csv'),
}

