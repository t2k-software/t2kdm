"""Module handling the configuration of the Data Manager."""

from six.moves import configparser, input
from six import print_
from appdirs import AppDirs
app_dirs = AppDirs('t2kdm', 't2k.org')
import os
from os import path

default_values = {
    'backend':      'lcg',
    'basedir':      '/t2k.org',
    'location':     '/',
}

descriptions = {
    'backend':      "Which backend should be used?\n"\
                    "Supported backends: lcg",
    'basedir':      "What base directory should be assumed for all files on the grid?",
    'location':     "What is your location?\n"\
                    "This is used to determine the closest storage element when downloading files.\n"\
                    "Must follow the general pattern of '/continent/country/site'.\n"\
                    "Examples: /europe/uk/ral\n"\
                    "          /americas/ca/triumf\n"\
                    "          /asia/jp/kek",
}

class Configuration(object):
    """Class containing the actual configuration information."""

    def __init__(self, filename=None):
        """Initialise the configuration.

        Optionally load the configuration from a file.
        """

        # Load the default values
        for key, val in default_values.items():
            setattr(self, key, val)

        # Load values from the provided file
        if filename is not None:
            self.load_config(filename)

    def load_config(self, filename):
        """Load configuration from a file."""

        # Create parser for config file
        parser = configparser.SafeConfigParser(default_values)
        parser.read(filename)

        # Get values from parser
        for key in default_values:
            setattr(self, key, parser.get('DEFAULT', key))

    def save_config(self, filename):
        """Load configuration from a file."""

        # Create parser for config file
        parser = configparser.SafeConfigParser(default_values)

        # Set values from config
        for key in default_values:
            parser.set('DEFAULT', key, getattr(self, key))

        # Save configuration to file
        with open(filename, 'wt') as f:
            parser.write(f)

    def ConfigError(self, item, message):
        return ConfigurationError("%s = %s: %s"%(item, getattr(self, item), message))

class ConfigurationError(Exception):
    """Error to be thrown if there is something wrong with the configuration."""
    pass

def load_config():
    """Load the standard configuration."""

    # Try different paths to find the configuration file
    for testpath in [
            path.join(os.getcwd(), '.t2kdm.conf'), # 1. ./.t2kdm.conf
            path.join(app_dirs.user_config_dir, 't2kdm.conf'), # 2. user_config_dir, on linux: ~/.config/t2kdm/t2kdm.conf
            path.join(app_dirs.site_config_dir, 't2kdm.conf'), # 2. site_config_dir, on linux: /etc/t2kdm/t2kdm.conf
            ]:
        if path.isfile(testpath):
            return Configuration(testpath)

    # Did not find any file, return default configuration
    return Configuration()

def run_configuration_wizard():
    """Run a configuration wizard to create a valid configuration file."""

    import argparse # import here because it is usually not needed by this module
    parser = argparse.ArgumentParser(description="Configure the T2K Data Manager")
    parser.add_argument('-l', '--local', action='store_true',
        help="save the configuration file in the current diractory as '.t2kdm.conf'")

    args = parser.parse_args()

    # Load current configuration
    conf = load_config()

    # Go through all items and ask user what should be used
    for key in default_values:
        current_value = getattr(conf, key)
        default_value = default_values[key]
        help_text = descriptions.pop(key, "-")
        text =  "\nConfiguration parameter: %s\n"\
                "\n"\
                "%s\n"\
                "\n"\
                "Current value: %s\n"\
                "Default value: %s\n"\
                %(key, help_text, current_value, default_value)
        print_(text)

        new_value = input('Enter new value [keep current]: ').strip()
        if new_value != '':
            setattr(conf, key, new_value)

    if args.local:
        outf = path.join(os.getcwd(), '.t2kdm.conf')
    else:
        outf = path.join(app_dirs.user_config_dir, 't2kdm.conf')

    print_("Saving configuration in %s"%(outf,))
    try:
        os.makedirs(path.dirname(outf))
    except OSError:
        pass
    conf.save_config(outf)

if __name__ == '__main__':
    run_configuration_wizard()