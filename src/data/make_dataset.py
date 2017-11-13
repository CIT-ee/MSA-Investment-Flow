from __future__ import print_function

import os, sys, argparse, pdb
sys.path.insert(0, os.environ['PROJECT_PATH'])

import subprocess, pandas as pd

from config.resources import path_to
from src.data.msa_mapper.map_loc_to_msa import MSAMapper

def setup_raw_data_dump():
    subprocess.call('./src/data/setup_data_dump.sh')

def map_investor_investee(path_to_dest): 
    print('\nPreparing to form investor - investee bridge. Please wait..')

    #  load the dataframes necessary for the join
    investments_df = pd.read_csv(path_to['investments'], encoding='latin1')
    investors_df = pd.read_csv(path_to['investors'], encoding='latin1')
    funding_rounds_df = pd.read_csv(path_to['funding_rounds'], encoding='latin1')
    
    #  list out the fields needed from either dataframe
    investor_fields = ['uuid', 'investor_name', 'country_code', 'state_code', 'city', 'investor_type' ]
    funding_rounds_fields = [ 'funding_round_uuid', 'company_uuid', 'country_code', 'state_code', 'city', 'investment_type', 'raised_amount_usd' ]
    
    #  filter out the unnecessary fields from either dataframe
    investors_df = investors_df[investor_fields]
    funding_rounds_df = funding_rounds_df[funding_rounds_fields]

    #  join investment with investors
    investment_investor_bridge = pd.merge(investments_df, investors_df, 
                                        left_on='investor_uuid', right_on='uuid')

    #  drop redundant fields
    investment_investor_bridge.drop('uuid', inplace=True, axis=1)

    #  join investors-investment bridge with investees
    investor_investee_bridge = pd.merge(investment_investor_bridge, 
                                        funding_rounds_df, how='left', 
                                        on='funding_round_uuid', 
                                        suffixes=('_investors', '_investees'))

    print('Investor - investee bridge formed! Dumping data to {}'.format(path_to_dest))
    investor_investee_bridge.to_csv(path_to_dest, index=False, encoding='latin1')

def _add_msa_data(source_df, suffix):
    msa_mapper_investor = MSAMapper(source_df.fillna(''))
    source_df = msa_mapper_investor.map_data()
    column_renamer = lambda x: x.lower() + suffix if suffix not in x else x
    source_df.rename(column_renamer, axis=1, inplace=True)
    return source_df

def filter_data(path_to_src, path_to_dest, investment_type, investor_type, 
                add_msa_data, country_code='USA'):
    print('\nPreparing to filter out non relevant data. Please wait..')

    #  filter out non US countries
    master_df = pd.read_csv(path_to_src, encoding='latin1')

    master_df = master_df[master_df['country_code_investees'] == country_code]
    master_df = master_df[master_df['country_code_investors'] == country_code]

    #  filter out non relevant investor and investment types
    filtered_by_investment_df = master_df[master_df['investor_type'] == investor_type]
    filtered_by_investor_df = master_df[master_df['investment_type'] == investment_type]
    filtered_df = pd.merge(filtered_by_investor_df, filtered_by_investment_df, 
                            how='inner', on=master_df.columns.tolist())

    print('Filtering out non relevant data completed!\n')

    if add_msa_data:
        investor_loc_df = filtered_df[ ['city_investors', 'state_code_investors', 'country_code_investors'] ]
        investee_loc_df = filtered_df[ ['city_investees', 'state_code_investees', 'country_code_investees'] ]
        
        #  map investor locations to msa
        print('\nPreparing to add msa data to investor locations')
        investor_loc_df = _add_msa_data(investor_loc_df, '_investors')
        common_columns = investor_loc_df.columns.tolist()[:3]
        filtered_df = pd.merge(filtered_df, investor_loc_df, how='inner', 
                                on=common_columns).drop_duplicates()

        #  map investee locations to msa
        print('\nPreparing to add msa data to investee locations')
        investee_loc_df = _add_msa_data(investee_loc_df, '_investees')
        common_columns = investee_loc_df.columns.tolist()[:3]
        filtered_df = pd.merge(filtered_df, investee_loc_df, how='inner', 
                                on=common_columns).drop_duplicates()

    print('Dumping data to {}'.format(path_to_dest))
    filtered_df.to_csv(path_to_dest, index=False, encoding='latin1')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--op', default='invst2org', help='operation to perform')
    parser.add_argument('--investment', default='series_a', help='type of investment to focus on')
    parser.add_argument('--investor', default='angel_group', help='type of investor to focus on')
    parser.add_argument('--map_msa', action='store_true', help='flag: add msa data or not')
    
    args = parser.parse_args()

    if args.op == 'dump_data':
        setup_raw_data_dump() 

    elif args.op == 'invst2org':
        map_investor_investee(path_to['investment_flow_master'])

    elif args.op == 'filter_data':
        path_to_data = path_to['investment_flow_master']
        path_to_dump = path_to['filtered_data'].format(args.investment, args.investor)
        assert os.path.exists(path_to_data), \
            'Please build investor - investee master data first!'
        filter_data(path_to_data, path_to_dump, args.investment, args.investor, args.map_msa)
