from __future__ import print_function

import os, sys, argparse, pdb
sys.path.insert(0, os.environ['PROJECT_PATH'])

import subprocess, pandas as pd

from config.resources import path_to

def setup_raw_data_dump():
    subprocess.call('./src/data/setup_data_dump.sh')

def map_investor_investee(): 
    investments_df = pd.read_csv(path_to['investments'])
    investors_df = pd.read_csv(path_to['investors'])
    funding_rounds_df = pd.read_csv(path_to['funding_rounds'])

    investors_df = investors_df[['uuid', 'investor_name', 'city']]
    investment_investor_bridge = pd.merge(investments_df, investors_df, 
                                        left_on='investor_uuid', right_on='uuid')

    funding_rounds_df = funding_rounds_df[['funding_round_uuid', 'company_name', 'city']]
    investor_investee_brige = pd.merge(investment_investor_bridge, funding_rounds_df,
                                        on='funding_round_uuid', how='left',
                                        suffixes=('_investors', '_investees'))
    print(investment_investor_bridge.head())

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--op', default='invst2org', help='operation to perform')
    
    args = parser.parse_args()

    if args.op == 'dump_data':
        setup_raw_data_dump() 
    elif args.op == 'invst2org':
        map_investor_investee()
