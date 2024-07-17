import os
import sys
import importlib
from datetime import datetime

# Global variable to hold the argument date string
arg_date_str = None

# The first command-line argument is used to define which configuration is loaded.
# The value has to correspond to a python filename found in the configuration directory.
# For example, to launch the program satromo_processor with the specific_config.py file, use:
# > python satromo_processor.py specific_config.py 2024-06-12

# By default, the file dev_config.py will be loaded as configuration.

if len(sys.argv) > 1:
    # If a configuration is provided in command line arguments, try to load the configuration
    print('Configuration file specified as command line argument')
    config_filename = sys.argv[1]

    # Check if a date is provided
    if len(sys.argv) > 2:
        date_arg = sys.argv[2]
        try:
            # Validate and parse the date argument
            date = datetime.strptime(date_arg, "%Y-%m-%d").date()
            arg_date_str = date.strftime("%Y-%m-%d")
            print(f'Date argument is valid: {arg_date_str}')
        except ValueError:
            print('Invalid date format. Please use YYYY-MM-DD.')
            sys.exit(1)
    else:
        print('No date argument provided.')

    # current directory, i.e., configuration
    directory = os.path.dirname(os.path.realpath(__file__))
    # construct the absolute filepath of the targeted configuration file.
    config_filepath = os.path.join(directory, config_filename)

    if os.path.exists(config_filepath):
        # The specified file is loaded as module
        config_filemodule = os.path.splitext(config_filename)[0]
        module = importlib.import_module('configuration.{}'.format(config_filemodule), package='configuration')

        # Simulate here a file load similar to 'from configuration import *'.
        # This way, the original code using this statement does not need to be modified.
        def __getattr__(name):
            return getattr(module, name)

        def __dir__():
            return dir(module)

    else:
        # The configuration file defined by the command-line argument does not exist.
        # Make sure that this file is found in the configuration directory.
        print('Configuration file {} does not exist'.format(config_filepath))
        raise BrokenPipeError('Configuration file {} does not exist'.format(config_filepath))

else:
    # If nothing is specified as command-line argument, load the dev_config.py file.
    print('Loading dev_config as default configuration.')
    from .dev_config import *

# Export the arg_date_str variable for use in other modules
__all__ = ['arg_date_str']
