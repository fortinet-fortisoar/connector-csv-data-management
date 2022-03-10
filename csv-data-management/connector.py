from connectors.core.connector import Connector
from connectors.core.connector import get_logger, ConnectorError
from django.utils.module_loading import import_string
from .builtins import *
from .constants import LOGGER_NAME
logger = get_logger(LOGGER_NAME)

class Csvdatamanagement(Connector):

    def dev_execute(self, config, operation, params, *args, **kwargs):
        # Call dev_execute from the 'execute' function if you are doing very frequent changes to the connector code and changes don't reflect without a service restart
        # dev_execute re-imports the function on every invocation and is performance intensive
        # So, once the development is complete and the connector is moved to production, you must not use the 'dev_execute' function
        parent_path = __name__.split('.')[:-1]
        parent_path.extend([operation, operation])
        func = import_string('.'.join(parent_path))
        return func(config, params)

    def execute(self, config, operation, params, *args, **kwargs):
        return supported_operations.get(operation)(config, params)

    def check_health(self, config=None, *args, **kwargs):
        pass