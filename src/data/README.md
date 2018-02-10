## Overview

This directory houses scripts related to creating the data sources associated with this project - *make_dataset.py* and *setup_data_dump.sh*

**NOTE:** These scripts require a environment variable called *PROJECT_PATH* which you can set by running the following command - `export PROJECT_PATH=<path to project dir>`

## Details

* `make_dataset.py`: This script has methods useful for building the data sources associated with this project. To start the data building process, simply run `cd <project_dir>` and then `python src/data/make_dataset.py` with the necessary accompanying arguments. The general structure of the command is shown below:

    ```
    python src/data/make_dataset.py [OPTIONS]

    DESCRIPTION
        --op                operation to perform
        --node              crunchbase node/entity to work on
        --batch_size=INT    size of batch to break data frame into    
        --batch=INT         batch idx to operate on
        --chkpnt_freq=INT   frequency at which to perform checkpoints
        --resume=BOOLEAN    flag: resume from checkpoint or not
    ```

* `setup_data_dump.sh`: This script serves to download the CSV export provided by the Crunchbase API for the specified node. For ease of use, this script can be called from the `make_dataset.py` script. To do so, simply run `python src/data/make_dataset.py --op dump_data`. On running that command, the user is prompted for the name of the required node and with that information, the download of the CSV export for the node is triggered. To run the script directly, simply run the command: `bash setup_data_dump.sh <node_name>`
