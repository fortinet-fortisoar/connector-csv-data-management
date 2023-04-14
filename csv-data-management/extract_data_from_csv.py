""" Copyright start
  Copyright (C) 2008 - 2022 Fortinet Inc.
  All rights reserved.
  FORTINET CONFIDENTIAL & FORTINET PROPRIETARY SOURCE CODE
  Copyright end """
from asyncore import read
from operator import truediv
from webbrowser import Elinks
from datetime import datetime
from uuid import uuid4
import requests
import pandas as pd
import numpy as np
import csv
from os.path import join
from os import remove
import json
from connectors.core.connector import get_logger, ConnectorError, SDK_VERSION
from connectors.cyops_utilities.builtins import download_file_from_cyops, create_cyops_attachment
from integrations.crudhub import make_request
from .constants import LOGGER_NAME

if int(SDK_VERSION.replace('.', '')) < 740:
    read_csv_extra_param = {"error_bad_lines": False}
else:
    read_csv_extra_param = {"on_bad_lines": "skip"}

logger = get_logger(LOGGER_NAME)


def extract_data_from_csv(config, params):
    try:
        numberOfRowsToSkip = None
        isSingleColumn = None
        no_of_columns = None
        isCSVWithoutHeaders = False

        file_iri = handle_params(params, params.get('value'))
        file_path = join('/tmp', download_file_from_cyops(file_iri)['cyops_file_path'])

        if params.get('numberOfRowsToSkip'):
            numberOfRowsToSkip = params.get('numberOfRowsToSkip')
        res = _check_if_csv(file_path, numberOfRowsToSkip)
        logger.info(res)

        if res.get('headers') == False:
            isCSVWithoutHeaders = True
            no_of_columns = res.get('columns')
        if res.get('columns') == 1:
            isSingleColumn = True

        if params.get(
                'columnNames') != "":  # CSV file with column header and specific columns to use in creating recordset
            columnNames = params.get('columnNames')
            columnNames = columnNames.split(",")
            # We are passing  specific columns name to filter data from here
            df = _read_file_specific_columns(file_path, columnNames, numberOfRowsToSkip)

        elif isSingleColumn and not isCSVWithoutHeaders:  # CSV file with one  column and header
            df = _read_file_single_column(file_path, numberOfRowsToSkip)

        elif isSingleColumn and isCSVWithoutHeaders:  # CSV file with one  column and no header
            df = _read_file_single_column_no_header(file_path, numberOfRowsToSkip, no_of_columns)

        elif isCSVWithoutHeaders:  # CSV file without column header and all columns
            df = _read_file_no_headers(file_path, numberOfRowsToSkip, no_of_columns)

        else:
            # We are reading complete file assuming it has column header
            df = _read_file_all_columns(file_path, numberOfRowsToSkip)

        # If user has selected to deduplicate recordset
        try:
            if params.get('deDupValuesOn'):
                deDupValuesOn = params.get('deDupValuesOn')
                deDupValuesOn = deDupValuesOn.split(",")
                df = df.drop_duplicates(subset=deDupValuesOn, keep='first')
        except Exception as err:
            logger.error('Error in deduplicating data  extract_data_from_csv(): %s' % str(err))
            raise ConnectorError('Error in deduplicating data  extract_data_from_csv(): %s' % str(err))

        # Replace empty values with N/A 
        df = df.fillna('N/A')

        # Filter Dataset
        if params.get('filterInput'):
            df = _ds_filter(params, df)

        # Create CSV file as attachment for resultant recordset
        if params.get('saveAsAttachment') and not df.empty:
            attachmentDetail = _df_to_csv(df)
        else:
            attachmentDetail = None

        final_result = _format_return_result(params=params, attDetail=attachmentDetail, df=df)
        return final_result

    except Exception as err:
        logger.error('Error in extract_data_from_csv(): %s' % str(err))
        raise ConnectorError('Error in processing CSV File: %s' % str(err))


def merge_two_csv_and_extract_data(config, params):
    try:
        if (params.get('mergeColumnNames')):
            mergeColumn = params.get('mergeColumnNames')
            mergeColumn = mergeColumn.split(",")
        fileOneIRI = handle_params(params, params.get('file_one_value'))
        fileOnePath = join('/tmp', download_file_from_cyops(fileOneIRI)['cyops_file_path'])
        fileTwoIRI = handle_params(params, params.get('file_two_value'))
        fileTwoPath = join('/tmp', download_file_from_cyops(fileTwoIRI)['cyops_file_path'])
        logger.info(params)
        # Read First File
        df1 = _read_and_return_ds(fileOnePath, params, config, filePassed="First")
        # Read Second File
        df2 = _read_and_return_ds(fileTwoPath, params, config, filePassed="Second")

        # Merge both files
        combined_recordSet = pd.merge(df1, df2, how='left', left_on=mergeColumn, right_on=mergeColumn)

        # If user has selected to deduplicate recordset
        try:
            if params.get('deDupValuesOn'):
                deDupValuesOn = params.get('deDupValuesOn')
                deDupValuesOn = deDupValuesOn.split(",")
                combined_recordSet = combined_recordSet.drop_duplicates(subset=deDupValuesOn, keep='first')
        except Exception as err:
            logger.error('Error in deduplicating data  extract_data_from_csv(): %s' % str(err))
            raise ConnectorError('Error in deduplicating data  extract_data_from_csv(): %s' % str(err))

        # Replace empty values with N/A 
        combined_recordSet = combined_recordSet.fillna('N/A')

        # Filter Dataset
        if params.get('filterInput'):
            combined_recordSet = _ds_filter(params, combined_recordSet)

        # Create CSV file as attachment for resultant recordset
        if params.get('saveAsAttachment') and not combined_recordSet.empty:
            attachmentDetail = _df_to_csv(combined_recordSet)
        else:
            attachmentDetail = None

        final_result = _format_return_result(params=params, attDetail=attachmentDetail, df=combined_recordSet)
        return final_result

    except Exception as err:
        logger.error('Error in merge_two_csv_and_extract_data(): %s' % str(err))
        raise ConnectorError('Error in processing CSV File: %s' % str(err))


def concat_two_csv_and_extract_data(config, params):
    try:
        fileOneIRI = handle_params(params, params.get('file_one_value'))
        fileOnePath = join('/tmp', download_file_from_cyops(fileOneIRI)['cyops_file_path'])
        fileTwoIRI = handle_params(params, params.get('file_two_value'))
        fileTwoPath = join('/tmp', download_file_from_cyops(fileTwoIRI)['cyops_file_path'])

        logger.info(params)
        df1 = _read_and_return_ds(fileOnePath, params, config, filePassed="First")
        df2 = _read_and_return_ds(fileTwoPath, params, config, filePassed="Second")

        # concat both files
        combined_recordSet = pd.concat([df1, df2])

        # If user has selected to deduplicate recordset
        try:
            if params.get('deDupValuesOn'):
                deDupValuesOn = params.get('deDupValuesOn')
                deDupValuesOn = deDupValuesOn.split(",")
                combined_recordSet = combined_recordSet.drop_duplicates(subset=deDupValuesOn, keep='first')
        except Exception as err:
            logger.error('Error in deduplicating data  extract_data_from_csv(): %s' % str(err))
            raise ConnectorError('Error in deduplicating data  extract_data_from_csv(): %s' % str(err))

        # Replace empty values with N/A 
        combined_recordSet = combined_recordSet.fillna('N/A')

        # Filter Dataset
        if params.get('filterInput'):
            combined_recordSet = _ds_filter(params, combined_recordSet)

        # Create CSV file as attachment for resultant recordset
        if params.get('saveAsAttachment') and not combined_recordSet.empty:
            attachmentDetail = _df_to_csv(combined_recordSet)
        else:
            attachmentDetail = None

        final_result = _format_return_result(params=params, attDetail=attachmentDetail, df=combined_recordSet)
        return final_result

    except Exception as err:
        logger.error('Error in concat_two_csv_and_extract_data(): %s' % str(err))
        raise ConnectorError('Error in processing CSV File: %s' % str(err))


def join_two_csv_and_extract_data(config, params):
    try:
        fileOneIRI = handle_params(params, params.get('file_one_value'))
        fileOnePath = join('/tmp', download_file_from_cyops(fileOneIRI)['cyops_file_path'])
        fileTwoIRI = handle_params(params, params.get('file_two_value'))
        fileTwoPath = join('/tmp', download_file_from_cyops(fileTwoIRI)['cyops_file_path'])

        df1 = _read_and_return_ds(fileOnePath, params, config, filePassed="First")
        df2 = _read_and_return_ds(fileTwoPath, params, config, filePassed="Second")

        # Join both files
        combined_recordSet = df1.join(df2, lsuffix='_FirstFile', rsuffix='_SecondFile')

        # If user has selected to deduplicate recordset
        try:
            if params.get('deDupValuesOn'):
                deDupValuesOn = params.get('deDupValuesOn')
                deDupValuesOn = deDupValuesOn.split(",")
                combined_recordSet = combined_recordSet.drop_duplicates(subset=deDupValuesOn, keep='first')
        except Exception as err:
            logger.error('Error in deduplicating data  extract_data_from_csv(): %s' % str(err))
            raise ConnectorError('Error in deduplicating data  extract_data_from_csv(): %s' % str(err))

        # Replace empty values with N/A 
        combined_recordSet = combined_recordSet.fillna('N/A')

        # Filter Dataset
        if params.get('filterInput'):
            combined_recordSet = _ds_filter(params, combined_recordSet)

        # Create CSV file as attachment for resultant recordset
        if params.get('saveAsAttachment') and not combined_recordSet.empty:
            attachmentDetail = _df_to_csv(combined_recordSet)
        else:
            attachmentDetail = None

        final_result = _format_return_result(params=params, attDetail=attachmentDetail, df=combined_recordSet)
        return final_result


    except Exception as err:
        logger.error('Error in join_two_csv_and_extract_data(): %s' % str(err))
        raise ConnectorError('Error in processing CSV File: %s' % str(err))


def convert_json_to_csv_file(config, params):
    try:
        file_iri = handle_params(params, params.get('value'))
        file_path = join('/tmp', download_file_from_cyops(file_iri)['cyops_file_path'])
        fileDetails = _json_to_csv(params, file_path)
        return {"fileDetails": fileDetails}
    except Exception as err:
        logger.error('Error in convert_json_to_csv_file(): %s' % str(err))
        raise ConnectorError('Error in processing CSV File: %s' % str(err))


def _read_file_specific_columns(filepath, columns_t, numberOfRowsToSkip=None):
    try:
        chunk = pd.read_csv('{}'.format(filepath), delimiter=',', encoding="utf-8-sig", skiprows=numberOfRowsToSkip,
                            chunksize=100000, usecols=columns_t, **read_csv_extra_param)
        df = pd.concat(chunk)
        return df
    except Exception as err:
        logger.error('Error in _read_file_specific_columns(): %s' % str(err))
        raise ConnectorError('Error in processing CSV File: %s' % str(err))


def _read_file_all_columns(filepath, numberOfRowsToSkip=None):
    try:
        chunk = pd.read_csv('{}'.format(filepath), delimiter=',', encoding="utf-8-sig", skiprows=numberOfRowsToSkip,
                            chunksize=100000, **read_csv_extra_param)
        df = pd.concat(chunk)
        return df
    except Exception as err:
        logger.error('Error in _read_file_all_columns(): %s' % str(err))
        raise ConnectorError('Error in processing CSV File: %s' % str(err))


def _read_file_no_headers(filepath, numberOfRowsToSkip=None, no_of_columns=None):
    try:
        colList = []
        if no_of_columns:
            for i in range(no_of_columns):
                colList.append("Column" + str(i))
        chunk = pd.read_csv('{}'.format(filepath), delimiter=',', encoding="utf-8-sig", header=None,
                            skiprows=numberOfRowsToSkip, chunksize=100000, **read_csv_extra_param)
        df = pd.concat(chunk)
        df.columns = colList
        return df
    except Exception as err:
        logger.error('Error in _read_file_no_headers(): %s' % str(err))
        raise ConnectorError('Error in processing CSV File: %s' % str(err))


def _read_file_single_column(filepath, numberOfRowsToSkip=None):
    try:
        chunk = pd.read_csv('{}'.format(filepath), usecols=[0], skiprows=numberOfRowsToSkip, chunksize=100000,
                            **read_csv_extra_param)
        df = pd.concat(chunk)
        return df
    except Exception as err:
        logger.error('Error in _read_file_single_column(): %s' % str(err))
        raise ConnectorError('Error in processing CSV File: %s' % str(err))


def _read_file_single_column_no_header(filepath, numberOfRowsToSkip=None, no_of_columns=None):
    try:
        colList = []
        if no_of_columns:
            for i in range(no_of_columns):
                colList.append("Column" + str(i))
        chunk = pd.read_csv('{}'.format(filepath), usecols=[0], header=None, skiprows=numberOfRowsToSkip,
                            chunksize=100000, **read_csv_extra_param)
        df = pd.concat(chunk)
        df.columns = colList
        return df
    except Exception as err:
        logger.error('Error in _read_file_single_column_no_header(): %s' % str(err))
        raise ConnectorError('Error in processing CSV File: %s' % str(err))


def _json_to_csv(params, filepath):
    try:
        filename = params.get('csvFileName')
        fileDetails = pd.read_json('{}'.format(filepath))
        csvData = _df_to_csv(fileDetails, filename)
        return csvData
    except Exception as err:
        logger.error('Error in _json_to_csv(): %s' % str(err))
        raise ConnectorError('Error in processing CSV File: %s' % str(err))


def _check_if_csv(filepath, numberOfRowsToSkip=None):
    sniffer = csv.Sniffer()
    # bailing out incase CSV file encoding is not UTF-8
    # To-Do  Read CSV file encoding and then use it for reading file. use -chardet.detect
    try:
        res = sniffer.has_header(open(filepath).read(2048))
    except Exception as err:
        if "UnicodeDecodeError" in repr(err):
            raise ConnectorError("CSV file has unsupported encoding. Supported encoding is UTF-8")
        else:
            logger.info("Ignorable exception occured, continuing execution. Exception {}:".format(err))
            pass
    try:
        if numberOfRowsToSkip:
            with open(filepath) as fileobj:
                for row in range(numberOfRowsToSkip):
                    reader = next(fileobj)
                res = sniffer.has_header(fileobj.read(2048))
        else:
            res = sniffer.has_header(open(filepath).read(2048))
        df = pd.read_csv('{}'.format(filepath), nrows=10, skiprows=numberOfRowsToSkip, **read_csv_extra_param)
        row, col = df.shape
        if res:
            return {"headers": True, "columns": col}

        return {"headers": False, "columns": col}
    except Exception as err:
        logger.error(
            'Error in _check_if_csv(), checking with pandas only due to exception reading file with csv module : %s' % str(
                err))
        try:
            df = pd.read_csv('{}'.format(filepath), nrows=10, skiprows=numberOfRowsToSkip, **read_csv_extra_param)
            row, col = df.shape
            return {"headers": False, "columns": col}
        except Exception as err:
            raise ConnectorError("Not a valid CSV: %s" % str(err))


def _read_and_return_ds(filepath, params, config, filePassed=None):
    try:
        numberOfRowsToSkip = None
        isSingleColumn = None
        isCSVWithoutHeaders = False
        columnNames = None

        if params.get('numberOfRowsToSkipFirst') and filePassed == "First":
            numberOfRowsToSkip = params.get('numberOfRowsToSkipFirst')

        if params.get('numberOfRowsToSkipSecond') and filePassed == "Second":
            numberOfRowsToSkip = params.get('numberOfRowsToSkipSecond')

        res = _check_if_csv(filepath, numberOfRowsToSkip)
        logger.info(res)

        if res.get('headers') == False:
            isCSVWithoutHeaders = True
            noOfColumns = res.get('columns')

        if res.get('columns') == 1:
            isSingleColumn = True

        # Lets read file

        if params.get(
                'file1_column_names') != "" and filePassed == "First":  # CSV file with column header and specific columns to use in creating recordset
            columnNames = params.get('file1_column_names')
            columnNames = columnNames.split(",")

        if params.get(
                'file2_column_names') != "" and filePassed == "Second":  # CSV file with column header and specific columns to use in creating recordset
            columnNames = params.get('file2_column_names')
            columnNames = columnNames.split(",")

        if columnNames:
            # We are passing  specific columns name to filter data from here
            df_file = _read_file_specific_columns(filepath, columnNames, numberOfRowsToSkip)

        elif isSingleColumn and not isCSVWithoutHeaders:  # CSV with single column and header
            df_file = _read_file_single_column(filepath, numberOfRowsToSkip)

        elif isSingleColumn and isCSVWithoutHeaders:  # CSV file with one  column and no header
            df_file = _read_file_single_column_no_header(filepath, numberOfRowsToSkip, noOfColumns)

        elif isCSVWithoutHeaders:  # CSV file without column header and more than one column
            df_file = _read_file_no_headers(filepath, numberOfRowsToSkip, noOfColumns)

        else:
            # We are reading complete file assuming it has column header
            df_file = _read_file_all_columns(filepath, numberOfRowsToSkip)
        return df_file
    except Exception as err:
        logger.error('Error in _read_and_return_ds(): %s' % str(err))
        raise ConnectorError('Error in processing CSV File: %s' % str(err))


def handle_params(params, file_param):
    value = str(file_param)
    input_type = params.get('input')
    try:
        if isinstance(value, bytes):
            value = value.decode('utf-8')
        if input_type == 'Attachment IRI':
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
        raise ConnectorError(
            'Requested resource could not be found with input type "{0}" and value "{1}"'.format(input_type,
                                                                                                 value.replace(
                                                                                                     '/api/3/attachments/',
                                                                                                     '')))


def _ds_filter(params, ds):
    df = ds
    if (params.get('filterInput')):
        input_type = params.get('filterInput')
        if input_type == 'On Values Matching a Regex':
            reg = params.get('filter')
            columnName = params.get('filterColumnName')
            df = df[df[columnName].str.match(reg) == True]
        elif input_type == 'On Specified Values':
            filterValue = params.get('filter').split(",")
            columnName = params.get('filterColumnName')
            df = df[df[columnName].isin(filterValue)]

    return df


def _df_to_csv(df, filename=None):
    try:
        id = str(uuid4().fields[-1])
        file_name = filename + ".csv" if filename else "dataset-{}.csv".format(id)
        compression = dict(method='zip', archive_name=file_name)
        df.to_csv('/tmp/{}'.format(file_name.split(".")[0]) + '.zip', encoding='utf-8', header='true',
                  compression=compression, index=False)
        filepath = '/tmp/{}'.format(file_name.split(".")[0]) + '.zip'
        ch_res = create_cyops_attachment(filename=filepath, name=file_name,
                                         description='Created by CSV Data Management Connector')
        remove(filepath)
        return ch_res
    except Exception as err:
        remove(filepath)
        logger.error("Error creating attachment record for CSV file")
        raise ConnectorError('Error in creating attachment record for CSV file: %s' % str(err))


def _format_return_result(params, attDetail, df):
    # Create small chunks of dataset to consume by playbook if requested by user otherwise return complete recordset
    if params.get('recordBatch'):
        smaller_datasets = np.array_split(df, 20)
        all_records = []
        for batch in smaller_datasets:
            all_records.append(batch.to_dict("records"))
        if params.get('saveAsAttachment'):
            final_result = {"records": all_records, "attachment": attDetail}
            return final_result
        final_result = {"records": all_records}
        return final_result
    else:
        if params.get('saveAsAttachment'):
            final_result = {"records": df.to_dict("records"), "attachment": attDetail}
            return final_result
    final_result = {"records": df.to_dict("records")}
    return final_result
