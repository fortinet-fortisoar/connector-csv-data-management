import requests
import pandas as pd
import numpy as np
from os.path import join
import json
from connectors.core.connector import get_logger, ConnectorError
from connectors.cyops_utilities.builtins import download_file_from_cyops
from integrations.crudhub import make_request
from .constants import LOGGER_NAME

logger = get_logger(LOGGER_NAME)


def extract_data_from_csv(config, params):
    try:
        endpoint = "files"
        logger.info(params)
        file_iri = handle_params(params)
        file_path = join('/tmp', download_file_from_cyops(file_iri)['cyops_file_path'])
        logger.info("file from code:" + file_path)
        if params.get('columnNames'):
            columnNames = params.get('columnNames')
            columNames = columNames.split(",")
            df = pd.read_csv('{}'.format(file_path), delimiter=',', encoding="utf-8-sig")[[columNames]]
        else:
            df = pd.read_csv('{}'.format(file_path), delimiter=',', encoding="utf-8-sig")

        if params.get('deDupValuesOn'):
            deDupValuesOn = params.get('deDupValuesOn')
            deDupValuesOn = deDupValuesOn.split(",")
            df.drop_duplicates(subset=[deDupValuesOn], keep='first')

        smaller_datasets = np.array_split(df, 20)
        all_records = []
        for batch in smaller_datasets:
            all_records.append(batch.to_dict("records"))
            final_result = {"records": all_records}

        return (json.dumps(final_result))

    except Exception as Err:
        logger.error('Error in extract_data_from_csv(): %s' % Err)
        logger.exception('Error in extract_data_from_csv(): %s' % Err)


def handle_params(params):
    value = str(params.get('value'))
    input_type = params.get('input')
    try:
        if isinstance(value, bytes):
            value = value.decode('utf-8')
        if input_type == 'Attachment ID':
            if not value.startswith('/api/3/attachments/'):
                value = '/api/3/attachments/{0}'.format(value)
            attachment_data = make_request(value, 'GET')
            file_iri = attachment_data['file']['@id']
            file_name = attachment_data['file']['filename']
            logger.info('file id = {0}, file_name = {1}'.format(file_iri, file_name))
            return file_iri
        elif input_type == 'File IRI':
            if value.startswith('/api/3/files/'):
                return value
            else:
                raise ConnectorError('Invalid File IRI {0}'.format(value))
    except Exception as err:
        logger.info('handle_params(): Exception occurred {0}'.format(err))
        raise ConnectorError('Requested resource could not be found with input type "{0}" and value "{1}"'.format
                             (input_type, value.replace('/api/3/attachments/', '')))

