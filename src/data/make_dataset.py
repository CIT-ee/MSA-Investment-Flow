from __future__ import print_function

import os, sys, argparse, pdb
sys.path.insert(0, os.environ['PROJECT_PATH'])

import subprocess, pandas as pd
from datetime import datetime

from config.resources import path_to
from config.api_specs import fields
from src.data.msa_mapper.map_loc_to_msa import MSAMapper
from src.data.utils import fetch_investments

def setup_raw_data_dump(fname):
    subprocess.call( [ './src/data/setup_data_dump.sh', fname ] )

def _add_msa_data(source_df, suffix):
    msa_mapper_investor = MSAMapper(source_df.fillna(''))
    source_df = msa_mapper_investor.map_data()
    column_renamer = lambda x: x.lower() + suffix if suffix not in x else x
    source_df.rename(column_renamer, axis=1, inplace=True)
    return source_df

def _add_fr_data(data, fr_data, fields):
    final_data = data.copy()
    final_data.update(fr_data[ fields ].to_dict())
    return final_data

def filter_investments(path_to_src, nb_years):
    print('\nPreparing to filter dataframe. Please wait ..')
    source_df = pd.read_csv(path_to_src, encoding='latin1')

    #  filter out data before `nb_years` from today
    curr_year = datetime.now().year + 1
    year_range = list(map(str, range(curr_year - nb_years, curr_year)))

    source_df['announced_on'] = pd.to_datetime(source_df['announced_on'])
    source_df = source_df[source_df['announced_on'].dt.year.isin(year_range) ]

    #  filter out funding rounds which may not have tangible location data
    print('Filtering dataframe completed!\n')
    return source_df[ source_df['company_name'] != 'Distributed ID' ].reset_index()

def build_investment_flow_df(source_df, path_to_dest, fields):
    #  set the url template
    url_template = "https://api.crunchbase.com/v3.1/funding-rounds/{uuid}/{relationship}?user_key={api_key}"
    
    nrows, _ = source_df.shape
    df_batches = []
    print('\nPreparing to build investment dataframe. Please wait..')
    for index, row in source_df.iterrows():
        print('Processed {} of {} funding rounds'.format(index, nrows), end='\r', )
        pdb.set_trace()
        uuid, rel_name = row['funding_round_uuid'], 'investments'
        url = url_template.format(uuid=uuid, relationship=rel_name, api_key=os.environ['CRUNCHBASE_API_KEY'])

        investment_data = fetch_investments(url, fields['relationships'].keys(), None)
        if len(investment_data) > 0:
            funding_round_data = list(map(lambda x: _add_fr_data(x, row, fields['properties']), investment_data))
            df_batches.append(pd.DataFrame(funding_round_data))

    dest_df = pd.concat(df_batches).reset_index()

    #  format the column names to a more readable format
    col_renamer = lambda x: x if x not in fields['relationships'].keys() else fields['relationships'][x]
    dest_df.rename(col_renamer, axis='columns', inplace=True)

    print('Building investment dataframe completed!. Dumping data to {}\n'.format(path_to_dest))
    dest_df.to_csv(path_to_dest, encoding='latin1', index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--op', default='invst2org', help='operation to perform')
    
    args = parser.parse_args()

    if args.op == 'dump_data':
        fname = raw_input('Please enter the name of the data dump you want to setup: ')
        setup_raw_data_dump(fname) 

    elif args.op == 'map_investments':
        path_to_src = path_to['csv_export'].format('funding_rounds')
        path_to_dest = path_to['investment_flow_master']

        source_df = filter_investments(path_to_src, 3)
        build_investment_flow_df(source_df, path_to_dest, fields['funding_rounds'])
