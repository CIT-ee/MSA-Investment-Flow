from __future__ import print_function

import os, sys, argparse, pdb
sys.path.insert(0, os.environ['PROJECT_PATH'])

import subprocess, pandas as pd
from datetime import datetime

from config.resources import path_to
from config.api_specs import fields
from src.data.msa_mapper.map_loc_to_msa import MSAMapper
from src.data.utils import fetch_investments

def _add_fr_data(data, fr_data, fields):
    final_data = data.copy()
    final_data.update(fr_data[ fields ].to_dict())
    return final_data

def _assert_paths(path_to_source, path_to_dest):
    assert os.path.exists(path_to_source), \
        'Please create file at {} first!'.format(path_to_source)

    assert os.path.exists(os.path.dirname(path_to_dest)), \
        'Please create directory at {} first!'.format(os.path.dirname(path_to_dest))

def setup_raw_data_dump(fname):
    subprocess.call( [ './src/data/setup_data_dump.sh', fname ] )

def _filter_investments(source_df, nb_years=3):
    print('\nPreparing to filter dataframe. Please wait ..')

    #  filter out data before `nb_years` from today
    curr_year = datetime.now().year + 1
    year_range = list(map(str, range(curr_year - nb_years, curr_year)))

    source_df['announced_on'] = pd.to_datetime(source_df['announced_on'])
    source_df = source_df[source_df['announced_on'].dt.year.isin(year_range) ]

    #  filter out funding rounds which may not have tangible location data
    print('Filtering dataframe completed!\n')
    return source_df[ source_df['company_name'] != 'Distributed ID' ].reset_index()

def build_investment_flow_df(path_to_source, path_to_dest, fields):
    source_df = _filter_investments(pd.read_csv(path_to_source, encoding='latin1'))

    #  set the url template
    url_template = "https://api.crunchbase.com/v3.1/funding-rounds/{uuid}/{relationship}?user_key={api_key}"
    
    nrows, _ = source_df.shape
    df_batches = []
    print('\nPreparing to build investment dataframe. Please wait..')
    for index, row in source_df.iterrows():
        print('Processed {} of {} funding rounds'.format(index, nrows), end='\r', )
        #  pdb.set_trace()
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
    dest_df.to_csv(path_to_dest, encoding='utf-8', index=False)

def add_msa_data(path_to_source, path_to_dest, src_loc_fields, dest_loc_fields, data_format):
    source_df = pd.read_csv(path_to_source, encoding='latin1')

    #  filter the dataframe if its related to funding rounds
    source_df = _filter_investments(source_df) if 'funding_rounds' in path_to_source else source_df

    print('\nPreparing to add MSA data to the data frame in question. Please wait ..')
    src_loc_df = source_df[ src_loc_fields ]
    msa_mapping_client = MSAMapper(src_loc_df.fillna(''))
    dest_loc_df = msa_mapping_client.map_data(data_format)
    dest_df = pd.concat([ source_df, dest_loc_df[ dest_loc_fields ] ], axis=1)

    print('Adding MSA data to dataframe completed! Dumping data to {}'.format(path_to_dest))
    dest_df.to_csv(path_to_dest, encoding='utf-8', index=False)

def batchify_data(path_to_source, path_to_dump, batch_size):
    source_df = pd.read_csv(path_to_source, encoding='latin1')
    nrows, _ = source_df.shape

    print('\nPreparing to batchify the dataframe. Please wait ..')
    for _idx, start in enumerate(range(0, nrows, batch_size)):
        stop = start + batch_size
        batch_df = source_df.iloc[start:, :] if stop > nrows else source_df.iloc[start:stop, :]
        batch_df.to_csv(path_to_dump.format(idx=_idx), encoding='latin1', index=False)
    print('Batchification of dataframe completed!\n')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--op', help='operation to perform')
    parser.add_argument('--node', help='crunchbase node/entity to work on')
    parser.add_argument('--batch_size', type=int, help='size of batch to break data frame into')
    parser.add_argument('--batch', type=int, help='batch idx to operate on')

    args = parser.parse_args()

    if args.op == 'dump_data':
        fname = raw_input('Please enter the name of the data dump you want to setup: ')
        setup_raw_data_dump(fname) 

    elif args.op == 'batchify':
        assert args.batch_size is not None, 'Please batch size for dataframe batchification'
        
        #  set the paths to source and destination
        path_to_source = path_to['csv_export'].format(args.node)
        path_to_dest = path_to['batch_csv'].format(node=args.node, idx='{idx}')

        _assert_paths(path_to_source, path_to_dest)

        batchify_data(path_to_source, path_to_dest, args.batch_size) 

    elif args.op == 'add_msa':
        assert args.node is not None, 'Please choose node to operate on first!'

        #  set the paths to source and destination
        if args.batch is None:
            path_to_source = path_to['csv_export'].format(args.node)
            path_to_dest = path_to['with_msa_csv'].format(node=args.node)
        else:
            path_to_source = path_to['batch_csv'].format(node=args.node, idx=args.batch)
            path_to_dest = path_to['with_msa_batch_csv'].format(node=args.node, idx=args.batch)

        _assert_paths(path_to_source, path_to_dest)

        if args.node == 'funding_rounds':
            src_loc_fields = [ 'city', 'state_code', 'country_code' ]
            dest_loc_fields = [ 'MSA_NAME', 'MSA_CODE' ]
            data_format = 'address'

        add_msa_data(path_to_source, path_to_dest, src_loc_fields, dest_loc_fields, data_format)

    elif args.op == 'map_investments':
        assert args.node is not None, 'Please choose node to operate on first!'

        #  set the paths to source and destination
        if args.batch is None:
            path_to_source = path_to['csv_export'].format(args.node)
            path_to_dest = path_to['scraped_csv'].format(name='investment_flow')
        else:
            path_to_source = path_to['batch_csv'].format(node=args.node, idx=args.batch)
            path_to_dest = path_to['batch_scraped_csv'].format(name='investment_flow', idx=args.batch)

        _assert_paths(path_to_source, path_to_dest)

        build_investment_flow_df(path_to_source, path_to_dest, fields['funding_rounds'])
