""" Main container class that will be composed of other task related classes."""


class Index(object):
    """Main index class."""
    def __init__(self):
        index_wd = None

    def create_index(self, data_path, index_wd):
        """Creates index using data and index_wd."""
        self.validate_args(data_path, index_wd)
        self.data = data
        self.index_wd = index_wd
        _validate_index_wd()

    def validate_args(self, data_path, index_wd):
        #check the data_path exists
        if not os.path.isfile(data_path) or not data_path.endswith('.gz'):
            print('Current path: {} is an invalid path to latimes.gz.  Please provide \
                    a valid path.'.format(data_path))
            print('Exiting program.')
            raise ValueError("Data path does point to valid source.")

        #check that index_wd does not exist
        if os.path.isdir(index_wd):
            print('Current dir: {} already exists and cannot be used to store the \
                    new index.'.format(index_wd))
            print('Exiting program.')
            raise ValueError("Index working dir does point to valid directory.")
