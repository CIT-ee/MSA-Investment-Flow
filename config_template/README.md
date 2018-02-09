## Overview

This is a template to use for setting up your config files needed to make the source code for this repository work as expected. To get started, after cloning the repository, run the following command: `cp -r config_template config`. Once the proper config directory is created, feel free to adjust/change the contents of the different constituent files to suit your needs. Please refrain from modifying its contents without prior notification. The description of the usage of the different underlying files is provided below:

1. `resources.py`: Primarily consists of a dictionary of paths useful at various points in the source code of this repository. You may set the DATA_PATH environment variable (pointing to the root of the directory housing the data files) explicitly on command line or as part of an `.env` file. However, please make sure the variable has been properly exported, failing which the paths defined in this file will not work as expected. The paths defined in this template adhere to the overall project's structure, especially with regards to the data files with suitable storage under the *raw*, *interim*, *external* and *filtered* subdirectories.

2. `api_specs.py`: Primarily consists of a dictionary of API related parameters and other information that may prove useful in the execution of the source code in the repository. The parameters are generally found under the *relationships* and *properties* key, each under the key named after the appropriate endpoint/entity. 

The *relationships* key refers to the properties obtained from the API endpoint response, consisting of a list of mappings between the desired dataframe column names and the dictionary keys to target in the (flattened) response. 

The *properties* key is a list of properties to collect from the parent dataframe to add to the dataframe being built at the time. Feel to make necessary changes to suit your needs, keeping in mind these conventions.
