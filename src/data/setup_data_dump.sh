#!/bin/bash

path_to_data_dump=${DATA_PATH}/raw
rm -rf ${path_to_data_dump}/*
echo -e '\nSetting up data dump at '${path_to_data_dump}'\n'
wget https://api.crunchbase.com/v3.1/csv_export/csv_export.tar.gz?user_key=${CRUNCHBASE_API_KEY} -O ${path_to_data_dump}/csv_export.tar.gz
tar -xzvf ${path_to_data_dump}/csv_export.tar.gz -C $path_to_data_dump/
rm ${path_to_data_dump}/csv_export.tar.gz 
echo -e '\nSetting up data completed!'
