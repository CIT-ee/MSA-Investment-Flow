from __future__ import print_function
import os, pdb

import requests
from time import sleep

def _throttle_request_rate_by(header):
    try:
        num_req_remaining = int(header['X-RateLimit-Remaining'])
        time_to_ratelimit_reset = int(header['X-RateLimit-Reset'])
        if num_req_remaining == 0:
            throttle_for = time_to_ratelimit_reset + 1
        else:
            throttle_for = round((1. * time_to_ratelimit_reset)/num_req_remaining, 3)
            return throttle_for
    except Exception as e:
        print('Could not calculate throttle value!', e)
        return 0

def _make_api_call(url):
    '''Make a request to the API endpoint.

    Keyword arguments:
    url -- the url to make the request to (default None)
    '''

    res = requests.get(url) #fetch the response

    #  make sure that request rates don't go over limit
    #  if one exists
    if 'X-RateLimit-Limit' in res.headers.keys():
        pause_time = _throttle_request_rate_by(res.headers)
    else:
        pause_time = 0

    sleep(pause_time) 

    if res.status_code >= 400 and res.status_code != 404:
        print('URL in question: ', url)
        pdb.set_trace() #  TODO: deal with request errors
                
    # make sure the server responded with OK status
    elif res.status_code == requests.codes.ok:
        payload = res.json()
        next_page_url = payload['data']['paging']['next_page_url']
        items = payload['data']['items']
        return items, next_page_url
            
    # default value to return to indicate something went wrong
    return None

def _flatten_dict(d, delimiter=':'):
    def _expand_key_value(key, value):
        if isinstance(value, dict):
            return [
                (delimiter.join([key, k]), v)
                for k, v in _flatten_dict(value, delimiter).items()
            ]
        else:
            return [(key, value)]
                        
    return dict(
        [item for k, v in d.items() for item in _expand_key_value(k, v)]
    )


def fetch_investments(url, fields, data):
    data = [] if data is None else data

    #  return whatever data is captured if 404 is encountered
    payload = _make_api_call(url)
    if payload is None: return data 
    
    items, next_page_url = payload 

    #  check if there is data to parse in current page
    if len(items) > 0:
        #  flatten the list of nested data points
        flattened_items = list(map(lambda x: _flatten_dict(x), items))

        #  filter out the unnecessary fields
        filtered_data = [ { k: v for k, v in list(flat_item.items()) if k in fields } \
                            for flat_item in flattened_items ]

        data += filtered_data
        
        #  check if there is another page to parse
        if next_page_url is not None:
            data = fetch_investments(next_page_url, fields, data)

    return data

def get_chkpnt(path_to_chkpnt_dir, name=''):
    path_to_chkpnts = path_to_chkpnt_dir.format(name=name)
    #  check if the path to the checkpoint store exists
    assert os.path.exists(path_to_chkpnts), \
            "Sorry there is no checkpoint folder at {}".format(path_to_chkpnts)
     
    #  get list of paths to relevant checkpoints
    checkpoints = [ os.path.join(path_to_chkpnts, fname) \
                     for fname in os.listdir(path_to_chkpnts) ]

    #  get the latest file (in terms of date created) in the list of checkpoints
    latest_chkpnt = max(checkpoints, key=os.path.getctime)

    #  extract information about stopping point in scraping process 
    #  from the checkpoint filename
    tokens = latest_chkpnt.split('.')[0].split('_')
    numbers = [ int(token) for token in tokens if token.lstrip('-').isdigit()  ]
    return numbers, latest_chkpnt

if __name__ == '__main__':
    url_template = "https://api.crunchbase.com/v3.1/funding-rounds/{uuid}/{relationship}?user_key={api_key}"
    uuid, rel_name = 'dc4c0682-85c3-402b-80a3-fb97a52585ed', 'investments'
    url = url_template.format(uuid=uuid, relationship=rel_name, api_key=os.environ['CRUNCHBASE_API_KEY'])
    
    fields = [
            'relationships:investors:properties:permalink',
            'relationships:investors:properties:founded_on',
            'relationships:investors:properties:short_description',
            'relationships:invested_in:properties:short_description',
            'relationships:invested_in:properties:short_description',
            'relationships:invested_in:properties:short_description',
            'properties:is_lead_investor',
            'properties:money_invested_usd',
            ]

    fetch_investments(url, fields, None)
