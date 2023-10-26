import os
import sys
import importlib
# we use importlib from standard library to customize how we load the configuration

"""

The first command-line argument is used to define which configuration is loaded.
The value has to correspond to a python filename found in the configuration directory.
For example, to launch the program satromo_processor with the specific_config.py file, use:

> python satromo_processor.py specific_config.py


By default, the file dev_config.py will be loading as configuration.
"""


if len(sys.argv) > 1:
    # If a configuration provided in command line argument, try to load the configration
    print('configuration file specified as command line argument')
    config_filename = sys.argv[1]

    # current direct, i.e. configuration
    directory = os.path.dirname(os.path.realpath(__file__))
    # construction of the absolute filepath of the targeted configuration file.
    config_filepath = os.path.join(directory, config_filename)

    if os.path.exists(config_filepath):
        # The specified file is loaded as module
        config_filemodule = os.path.splitext(config_filename)[0]
        module = importlib.import_module('configuration.{}'.format(config_filemodule), package='configuration')


        # We simulate here a file load similar to 'from configuration import *'.
        # This way, the original code using this statement does not need to be modified.
        def __getattr__(name):
            return getattr(module, name)

    else:
        # The configuration file defined by the command-line argument does not exist.
        # Make sure that this file is found in the configuration directory.
        print('Configuration file {} does not exist'.format(config_filepath))
        raise BrokenPipeError('Configuration file {} does not exist'.format(config_filepath))

else:
    # If nothing is specified as command-line argument, load the dev_config.py file.
    print('Loading dev_config as default configuration.')
    from .dev_config import *


