from __future__ import print_function

import os, sys, argparse, pdb
sys.path.insert(0, os.environ['PROJECT_PATH'])

import subprocess, pandas as pd
from itertools import islice
from datetime import datetime

from config.resources import path_to
from config.api_specs import fields
from src.data.msa_mapper.map_loc_to_msa import MSAMapper
from src.data.utils import fetch_investments, get_chkpnt

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

def _filter_investments(source_df, start_year, end_year):
    print('\nPreparing to filter dataframe. Please wait ..')

    #  filter out data before `nb_years` from today
    year_range = list(map(str, range(start_year, end_year + 1)))

    source_df['announced_on'] = pd.to_datetime(source_df['announced_on'])
    source_df = source_df[ source_df['announced_on'].dt.year.isin(year_range) ]

    #  filter out funding rounds which do not have associated MSA Code
    source_df = source_df[ source_df['MSA_CODE'].notnull() ]

    #  filter out funding rounds which may not have tangible location data
    source_df = source_df[ source_df['company_name'] != 'Distributed ID' ]

    #  reset the index and then drop any unnecessary columns
    print('Filtering dataframe completed!\n')
    return source_df.reset_index().drop('index', axis=1)

def _save_dataframe(path_to_dest, df_batches, fields):
    dest_df = pd.concat(df_batches).reset_index(drop=True)

    #  format the column names to a more readable format
    col_renamer = lambda x: x if x not in fields['relationships'].keys() else fields['relationships'][x]
    dest_df.rename(col_renamer, axis='columns', inplace=True)

    #  drop duplicates and dump to disk
    dest_df.drop_duplicates(subset='investment_uuid', inplace=True)
    dest_df.to_csv(path_to_dest, encoding='utf-8', index=False)

def build_investment_flow_df(path_to_source, path_to_dest, fields, save_freq, use_checkpoint):
    #  load the dataframe and filter it down to necessary rows
    source_df = _filter_investments(pd.read_csv(path_to_source, encoding='latin1'),
                                    fields['year_range'][0], fields['year_range'][1])

    #  set the url template
    url_template = "https://api.crunchbase.com/v3.1/funding-rounds/{uuid}/{relationship}?user_key={api_key}"
    
    nrows, _ = source_df.shape
    start_idx, num_fr, df_batches = 0, 0, []
    chkpnt_template = path_to['scraped_csv_checkpoint']
    
    #  load the investments partial dataframe from checkpoint if specified
    if use_checkpoint:
        path_to_chkpnt_dir = os.path.dirname(chkpnt_template)
        scraping_stats, chkpnt_path = get_chkpnt(path_to_chkpnt_dir, name='investment_flow')
        start_idx, num_fr = scraping_stats
        investments_df = pd.read_csv(chkpnt_path, encoding='latin1')
        df_batches.append(investments_df)

    print('\nPreparing to build investment dataframe. Please wait..')
    for index, row in islice(source_df.iterrows(), start_idx, None):
        print('Processed {} of {} funding rounds'.format(num_fr, nrows), end='\r', )
        uuid, rel_name = row['funding_round_uuid'], 'investments'

        url = url_template.format(uuid=uuid, relationship=rel_name, 
                                    api_key=os.environ['CRUNCHBASE_API_KEY'])
        investment_data = fetch_investments(url, fields['relationships'].keys(), None)
        num_fr += 1

        #  process investment data if any
        if len(investment_data) > 0:
            funding_round_data = list(map(lambda x: _add_fr_data(x, row, fields['properties']), investment_data))
            df_batches.append(pd.DataFrame(funding_round_data))

        #  checkpoint at regular intervals if interval is specified
        if save_freq is not None and ( num_fr % save_freq  ) == 0: 
            print('\nMaking checkpoint: Processed {} funding rounds\n'.format(num_fr))
            chkpnt_path = chkpnt_template.format(name='investment_flow', index = index, 
                                                    num_fr=num_fr)
            _save_dataframe(chkpnt_path, df_batches, fields)

    print('Building investment dataframe completed!. Dumping data to {}\n'.format(path_to_dest))
    _save_dataframe(path_to_dest, df_batches, fields) 

def add_msa_data(path_to_source, path_to_dest, src_loc_fields, dest_loc_fields, data_format):
    source_df = pd.read_csv(path_to_source, encoding='latin1')

    #  filter the dataframe if its related to funding rounds
    #  source_df = _filter_investments(source_df) if 'funding_rounds' in path_to_source else source_df

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
    parser.add_argument('--chkpnt_freq', default=None, type=int, 
                        help='frequency at which to perform checkpoints' )
    parser.add_argument('--resume', action='store_true', help='flag: resume from checkpoint or not')

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

        save_freq, use_checkpoint = None, False 
        #  set the paths to source and destination
        if args.batch is None:
            path_to_source = path_to['with_msa_csv'].format(node=args.node)
            path_to_dest = path_to['scraped_csv'].format(name='investment_flow')
            save_freq, use_checkpoint = args.chkpnt_freq, args.resume
        else:
            path_to_source = path_to['with_msa_batch_csv'].format(node=args.node, idx=args.batch)
            path_to_dest = path_to['batch_scraped_csv'].format(name='investment_flow', idx=args.batch)

        _assert_paths(path_to_source, path_to_dest)

        build_investment_flow_df(path_to_source, path_to_dest, fields['funding_rounds'],
                                save_freq, use_checkpoint)
