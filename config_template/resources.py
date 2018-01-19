import os

path_to = {
    'shape_files': os.path.join(os.environ['DATA_PATH'], 'external', 'cb_2016_us_cbsa_500k'),
    'filtered_data': os.path.join(os.environ['DATA_PATH'], 'interim', '{}_{}_filtered.csv'),
    'csv_export': os.path.join(os.environ['DATA_PATH'], 'raw', 'csv_export', '{}.csv'),
    'batch_csv': os.path.join(os.environ['DATA_PATH'], 'interim', '{node}', 'batches', '{node}_batch_{idx}.csv'),
    'scraped_csv': os.path.join(os.environ['DATA_PATH'], 'interim', '{name}', '{name}_master.csv'),
    'scraped_csv_checkpoint': os.path.join(os.environ['DATA_PATH'], 'interim', '{name}',  'checkpoints', '{name}_{index}_{num_fr}.csv'),
    'batch_scraped_csv': os.path.join(os.environ['DATA_PATH'], 'interim', '{name}', 'batches', '{name}_batch_{idx}.csv'),
    'with_msa_csv': os.path.join(os.environ['DATA_PATH'], 'interim', '{node}', '{node}_with_msa.csv'),
    'with_msa_batch_csv': os.path.join(os.environ['DATA_PATH'], 'interim', '{node}', 'batches_with_msa', '{node}_batch_{idx}.csv'),
    'augmented_csv': os.path.join(os.environ['DATA_PATH'], 'interim', 'augmented_csv', '{}_with_msa.csv'),
    'node_keys': os.path.join(os.environ['DATA_PATH'], 'raw', 'node_keys', '{}.csv')
}
