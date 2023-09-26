import os
import importlib
# we use importlib from standard library to customize how we load the configuration

"""
the SATROMO_CONFIG environmental variable is used to define which configuration is loaded.
The value has to correspond to a python filename without extension found in the configuration directory.

On ubuntu, you can define the environmental variable with the "export" statement.
For example, use

> export SATROMO_CONFIG=integration_config

to use the the file location configuration\\integration_config.py as configuration file.

By default, the file dev_config.py will be loading as configuration.
"""

module_loaded = False
if 'SATROMO_CONFIG' in os.environ:
    # If SATROMO_CONFIG is defined as environmental variable, try to load the configration
    print('SATROMO_CONFIG key found in environment variable.')
    config_filename = os.environ['SATROMO_CONFIG']

    # current direct, i.e. configuration
    directory = os.path.dirname(os.path.realpath(__file__))
    # construction of the absolute filepath of the targeted configuration file.
    config_filepath = os.path.join(directory, config_filename + '.py')

    if os.path.exists(config_filepath):
        # The specified file is loaded as module
        module = importlib.import_module('configuration.{}'.format(config_filename), package='configuration')

        # We simulate here a file load similar to 'from configuration import *'.
        # This way, the original code using this statement does not need to be modified.
        def __getattr__(name):
            return getattr(module, name)

        module_loaded = True

    else:
        # The configuration file defined in the environmental variable does not exist.
        # Make sure that this file is found in the configuration directory.
        print('Configuration file pointed by SATROMO_CONFIG variable is not existing in the configuration folder')

if not module_loaded:
    # If the module is not already loaded, i.e. if not specified as environmental variable
    # or if the specified file does not exist, we load the dev_config as default.
    print('Loading dev_config as default configuration.')
    from .dev_config import *


