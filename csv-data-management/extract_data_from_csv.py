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
import polars as pl
import numpy as np
import csv
from os.path import join
from os import remove
import json
from connectors.core.connector import get_logger, ConnectorError
from connectors.cyops_utilities.builtins import download_file_from_cyops, create_cyops_attachment
from integrations.crudhub import make_request
from .constants import LOGGER_NAME

logger = get_logger(LOGGER_NAME)


def _build_payload(params):
    return {key: val for key, val in params.items() if val is not None and val != ''}


def extract_data_from_csv(config, params):
    try:
        params = _build_payload(params)
        numberOfRowsToSkip = 0
        isSingleColumn = False
        no_of_columns = None
        isCSVWithoutHeaders = False

        file_iri = handle_params(params, params.get('value'))
        file_path = join('/tmp', download_file_from_cyops(file_iri)['cyops_file_path'])

        if params.get('numberOfRowsToSkip'):
            numberOfRowsToSkip = params.get('numberOfRowsToSkip')
        res = _check_if_csv(file_path, numberOfRowsToSkip)
        logger.info(res)

        if not res.get('headers'):
            isCSVWithoutHeaders = True
            no_of_columns = res.get('columns')
        if res.get('columns') == 1:
            isSingleColumn = True

        if params.get(
                'columnNames') is not None:  # CSV file with column header and specific columns to use in creating
            # recordset
            columnNames = params.get('columnNames')
            columnNames = columnNames.split(",")
            # We are passing  specific columns name to filter data from here
            df = _read_file_specific_columns(file_path, columnNames, numberOfRowsToSkip)

        elif isSingleColumn and not isCSVWithoutHeaders:  # CSV file with one  column and header
            df = _read_file_single_column(file_path, numberOfRowsToSkip)
            logger.info("isSingleColumn and not isCSVWithoutHeaders")

        elif isSingleColumn and isCSVWithoutHeaders:  # CSV file with one  column and no header
            df = _read_file_single_column_no_header(file_path, numberOfRowsToSkip, no_of_columns)
            logger.info("isSingleColumn and isCSVWithoutHeaders")

        elif isCSVWithoutHeaders:  # CSV file without column header and all columns
            df = _read_file_no_headers(file_path, numberOfRowsToSkip, no_of_columns)
            logger.info("isCSVWithoutHeaders")

        else:
            # We are reading complete file assuming it has column header
            df = _read_file_all_columns(file_path, numberOfRowsToSkip)
            logger.info("Inside Read all columns" + str(type(df)))

        # If user has selected to deduplicate recordset
        try:
            if params.get('deDupValuesOn'):
                deDupValuesOn = params.get('deDupValuesOn')
                deDupValuesOn = deDupValuesOn.split(",")
                df = df.unique(subset=deDupValuesOn, keep='first')
        except Exception as Err:
            logger.error('Error in deduplicating data  extract_data_from_csv(): %s' % Err)
            raise ConnectorError('Error in deduplicating data  extract_data_from_csv(): %s' % Err)

        # Replace empty values with N/A 
        df = df.fill_null("N/A")

        # Filter Dataset
        if params.get('filterInput'):
            df = _ds_filter(params, df)

        # Create CSV file as attachment for resultant recordset
        if params.get('saveAsAttachment') and not df.is_empty():
            attachmentDetail = _df_to_csv(df)
        else:
            attachmentDetail = None

        final_result = _format_return_result(params=params, attDetail=attachmentDetail, df=df)
        return final_result

    except Exception as Err:
        logger.error('Error in extract_data_from_csv(): %s' % Err)
        raise ConnectorError('Error in processing CSV File: %s' % Err)


def merge_two_csv_and_extract_data(config, params):
    try:
        params = _build_payload(params)
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
        combined_recordSet = df1.join(df2, on=mergeColumn, how='left')

        # If user has selected to deduplicate recordset
        try:
            if params.get('deDupValuesOn'):
                deDupValuesOn = params.get('deDupValuesOn')
                deDupValuesOn = deDupValuesOn.split(",")
                combined_recordSet = combined_recordSet.unique(subset=deDupValuesOn, keep='first')
        except Exception as Err:
            logger.error('Error in deduplicating data  extract_data_from_csv(): %s' % Err)
            raise ConnectorError('Error in deduplicating data  extract_data_from_csv(): %s' % Err)

        # Replace empty values with N/A 
        combined_recordSet = combined_recordSet.fill_null('N/A')

        # Filter Dataset
        if params.get('filterInput'):
            combined_recordSet = _ds_filter(params, combined_recordSet)

        # Create CSV file as attachment for resultant recordset
        if params.get('saveAsAttachment') and not combined_recordSet.is_empty():
            attachmentDetail = _df_to_csv(combined_recordSet)
        else:
            attachmentDetail = None

        final_result = _format_return_result(params=params, attDetail=attachmentDetail, df=combined_recordSet)
        return final_result

    except Exception as Err:
        logger.error('Error in merge_two_csv_and_extract_data(): %s' % Err)
        raise ConnectorError('Error in processing CSV File: %s' % Err)


def concat_two_csv_and_extract_data(config, params):
    try:
        params = _build_payload(params)
        fileOneIRI = handle_params(params, params.get('file_one_value'))
        fileOnePath = join('/tmp', download_file_from_cyops(fileOneIRI)['cyops_file_path'])
        fileTwoIRI = handle_params(params, params.get('file_two_value'))
        fileTwoPath = join('/tmp', download_file_from_cyops(fileTwoIRI)['cyops_file_path'])

        logger.info(params)
        df1 = _read_and_return_ds(fileOnePath, params, config, filePassed="First")
        df2 = _read_and_return_ds(fileTwoPath, params, config, filePassed="Second")

        # concat both files
        combined_recordSet = pl.concat([df1, df2])

        # If user has selected to deduplicate recordset
        try:
            if params.get('deDupValuesOn'):
                deDupValuesOn = params.get('deDupValuesOn')
                deDupValuesOn = deDupValuesOn.split(",")
                combined_recordSet = combined_recordSet.unique(subset=deDupValuesOn, keep='first')
        except Exception as Err:
            logger.error('Error in deduplicating data  extract_data_from_csv(): %s' % Err)
            raise ConnectorError('Error in deduplicating data  extract_data_from_csv(): %s' % Err)

        # Replace empty values with N/A 
        combined_recordSet = combined_recordSet.fill_null('N/A')

        # Filter Dataset
        if params.get('filterInput'):
            combined_recordSet = _ds_filter(params, combined_recordSet)

        # Create CSV file as attachment for resultant recordset
        if params.get('saveAsAttachment') and not combined_recordSet.is_empty():
            attachmentDetail = _df_to_csv(combined_recordSet)
        else:
            attachmentDetail = None

        final_result = _format_return_result(params=params, attDetail=attachmentDetail, df=combined_recordSet)
        return final_result

    except Exception as Err:
        logger.error('Error in concat_two_csv_and_extract_data(): %s' % Err)
        raise ConnectorError('Error in processing CSV File: %s' % Err)


def join_two_csv_and_extract_data(config, params):
    try:
        params = _build_payload(params)
        fileOneIRI = handle_params(params, params.get('file_one_value'))
        fileOnePath = join('/tmp', download_file_from_cyops(fileOneIRI)['cyops_file_path'])
        fileTwoIRI = handle_params(params, params.get('file_two_value'))
        fileTwoPath = join('/tmp', download_file_from_cyops(fileTwoIRI)['cyops_file_path'])

        df1 = _read_and_return_ds(fileOnePath, params, config, filePassed="First")
        df2 = _read_and_return_ds(fileTwoPath, params, config, filePassed="Second")

        df1.columns, df2.columns = _find_duplicate_columns_add_suffix(df1.columns, df2.columns)
        # Join both files
        combined_recordSet = pl.concat([df1, df2], how="horizontal")

        # If user has selected to deduplicate recordset
        try:
            if params.get('deDupValuesOn'):
                deDupValuesOn = params.get('deDupValuesOn')
                deDupValuesOn = deDupValuesOn.split(",")
                combined_recordSet = combined_recordSet.unique(subset=deDupValuesOn, keep='first')
        except Exception as Err:
            logger.error('Error in deduplicating data  extract_data_from_csv(): %s' % Err)
            raise ConnectorError('Error in deduplicating data  extract_data_from_csv(): %s' % Err)

        # Replace empty values with N/A 
        combined_recordSet = combined_recordSet.fill_null('N/A')

        # Filter Dataset
        if params.get('filterInput'):
            combined_recordSet = _ds_filter(params, combined_recordSet)

        # Create CSV file as attachment for resultant recordset
        if params.get('saveAsAttachment') and not combined_recordSet.is_empty():
            attachmentDetail = _df_to_csv(combined_recordSet)
        else:
            attachmentDetail = None

        final_result = _format_return_result(params=params, attDetail=attachmentDetail, df=combined_recordSet)
        return final_result


    except Exception as Err:
        logger.error('Error in join_two_csv_and_extract_data(): %s' % Err)
        raise ConnectorError('Error in processing CSV File: %s' % Err)


def convert_json_to_csv_file(config, params):
    try:
        params = _build_payload(params)
        if params.get('input') == "JSON":
            logger.info("In JSON Field")
            rp = _check_if_present(params.get("record_path"))
            meta = _check_if_present(params.get("meta"))

            df = pd.json_normalize(params.get('json_data'), record_path=rp, meta=meta)
            result = _df_to_csv(df, params.get('csvFileName'))
            return {"fileDetails": result}
        else:
            file_iri = handle_params(params, params.get('value'))
            file_path = join('/tmp', download_file_from_cyops(file_iri)['cyops_file_path'])
            fileDetails = _json_to_csv(params, file_path)
            return {"fileDetails": fileDetails}
    except Exception as Err:
        logger.error('Error in convert_json_to_csv_file(): %s' % Err)
        raise ConnectorError('Error in processing CSV File: %s' % Err)


def _find_duplicate_columns_add_suffix(fColumn, sColumn):
    for x in range(len(fColumn)):
        if fColumn[x] in sColumn:
            idx = sColumn.index(fColumn[x])
            sColumn[idx] = sColumn[idx] + "_SecondFile"
            fColumn[x] = fColumn[x] + "_FirstFile"

    return fColumn, sColumn


def _check_if_present(param):
    if param is None or param == "":
        return None
    else:
        return param.split(",")


def _check_if_series_change_to_df(df):
    if isinstance(df, pl.Series):
        return df.to_frame()
    else:
        return df


def _read_file_specific_columns(filepath, columns_t, numberOfRowsToSkip=0):
    try:
        chunk = pl.read_csv('{}'.format(filepath), separator=',', encoding="utf-8-sig", skip_rows=numberOfRowsToSkip,
                            batch_size=100000, ignore_errors=True, columns=columns_t)
        return _check_if_series_change_to_df(chunk)
    except Exception as Err:
        logger.error('Error in _read_file_specific_columns(): %s' % Err)
        raise ConnectorError('Error in processing CSV File: %s' % Err)


def _read_file_all_columns(filepath, numberOfRowsToSkip=0):
    try:
        chunk = pl.read_csv('{}'.format(filepath), separator=',', encoding="utf-8-sig", skip_rows=numberOfRowsToSkip,
                            batch_size=100000, ignore_errors=True)
        return _check_if_series_change_to_df(chunk)
    except Exception as Err:
        logger.error('Error in _read_file_all_columns(): %s' % Err)
        raise ConnectorError('Error in processing CSV File: %s' % Err)


def _read_file_no_headers(filepath, numberOfRowsToSkip=None, no_of_columns=None):
    try:
        if no_of_columns:
            colList = []
            for i in range(no_of_columns):
                colList.append("Column" + str(i))
        chunk = pl.read_csv('{}'.format(filepath), separator=',', encoding="utf-8-sig", has_header=False,
                            skip_rows=numberOfRowsToSkip, batch_size=100000, ignore_errors=True)
        df = _check_if_series_change_to_df(chunk)
        df.columns = colList
        return df
    except Exception as Err:
        logger.error('Error in _read_file_no_headers(): %s' % Err)
        raise ConnectorError('Error in processing CSV File: %s' % Err)


def _read_file_single_column(filepath, numberOfRowsToSkip=None):
    try:
        chunk = pl.read_csv('{}'.format(filepath), columns=[0], skip_rows=numberOfRowsToSkip, batch_size=100000,
                            ignore_errors=True)
        return _check_if_series_change_to_df(chunk)
    except Exception as Err:
        logger.error('Error in _read_file_single_column(): %s' % Err)
        raise ConnectorError('Error in processing CSV File: %s' % Err)


def _read_file_single_column_no_header(filepath, numberOfRowsToSkip=None, no_of_columns=None):
    try:
        if no_of_columns:
            colList = []
            for i in range(no_of_columns):
                colList.append("Column" + str(i))
        chunk = pl.read_csv('{}'.format(filepath), columns=[0], has_header=False, skip_rows=numberOfRowsToSkip,
                            batch_size=100000, ignore_errors=True)
        df = _check_if_series_change_to_df(chunk)
        df.columns = colList
        return df
    except Exception as Err:
        logger.error('Error in _read_file_single_column_no_header(): %s' % Err)
        raise ConnectorError('Error in processing CSV File: %s' % Err)


def _json_to_csv(params, filepath):
    try:
        filename = params.get('csvFileName')
        fileDetails = pd.read_json('{}'.format(filepath))
        csvData = _df_to_csv(fileDetails, filename)
        return csvData
    except Exception as Err:
        logger.error('Error in _json_to_csv(): %s' % Err)
        raise ConnectorError('Error in processing CSV File: %s' % Err)


def _check_if_csv(filepath, numberOfRowsToSkip=0):
    sniffer = csv.Sniffer()
    # bailing out incase CSV file encoding is not UTF-8
    # To-Do  Read CSV file encoding and then use it for reading file. use -chardet.detect
    try:
        res = sniffer.has_header(open(filepath).read(2048))
    except Exception as Err:
        if "UnicodeDecodeError" in repr(Err):
            raise ConnectorError("CSV file has unsupported encoding. Supported encoding is UTF-8")
        else:
            logger.info("Ignorable exception occured, continuing execution. Exception {}:".format(Err))
            pass
    try:
        if numberOfRowsToSkip != 0:
            with open(filepath) as fileobj:
                for row in range(numberOfRowsToSkip):
                    reader = next(fileobj)
                res = sniffer.has_header(fileobj.read(2048))
        else:
            res = sniffer.has_header(open(filepath).read(2048))
        df = pl.read_csv('{}'.format(filepath), ignore_errors=True, n_rows=10, skip_rows=numberOfRowsToSkip)

        row, col = df.shape
        if res:
            return {"headers": True, "columns": col}

        return {"headers": False, "columns": col}
    except Exception as Err:
        logger.error(
            'Error in _check_if_csv(), checking with polars only due to exception reading file with csv module : %s' % Err)
        try:
            df = pl.read_csv('{}'.format(filepath), ignore_errors=True, n_rows=10, skip_rows=numberOfRowsToSkip)

            row, col = df.shape
            return {"headers": False, "columns": col}
        except Exception as Err:
            raise ConnectorError("Not a valid CSV: " + Err)


def _read_and_return_ds(filepath, params, config, filePassed=None):
    try:
        numberOfRowsToSkip = 0
        isSingleColumn = False
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
                'file1_column_names') != None and filePassed == "First":  # CSV file with column header and specific columns to use in creating recordset
            columnNames = params.get('file1_column_names')
            columnNames = columnNames.split(",")

        if params.get(
                'file2_column_names') != None and filePassed == "Second":  # CSV file with column header and specific columns to use in creating recordset
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
    except Exception as Err:
        logger.error('Error in _read_and_return_ds(): %s' % Err)
        raise ConnectorError('Error in processing CSV File: %s' % Err)


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


def _ds_filter(params, df):
    if (params.get('filterInput')):
        input_type = params.get('filterInput')
        if input_type == 'On Values Matching a Regex':
            reg = params.get('filter')
            columnName = params.get('filterColumnName')
            df = df.filter(pl.col(columnName).str.contains(reg))
        elif input_type == 'On Specified Values':
            filterValue = str(params.get('filter')).split(",")
            columnName = params.get('filterColumnName')
            df = df.filter(pl.col(columnName).cast(pl.Utf8).is_in(filterValue))

    return df


def _df_to_csv(df, filename=None):
    try:
        id = str(uuid4().fields[-1])
        file_name = filename + ".csv" if filename else "dataset-{}.csv".format(id)
        compression = dict(method='zip', archive_name=file_name)

        if isinstance(df, pd.DataFrame):
            df.to_csv('/tmp/{}'.format(file_name.split(".")[0]) + '.zip', encoding='utf-8', header='true',
                      compression=compression, index=False)
            filepath = '/tmp/{}'.format(file_name.split(".")[0]) + '.zip'

        else:
            df.write_csv('/tmp/{}'.format(file_name.split(".")[0]) + '.csv', has_header=True)
            filepath = '/tmp/{}'.format(file_name.split(".")[0]) + '.csv'

        ch_res = create_cyops_attachment(filename=filepath, name=file_name,
                                         description='Created by CSV Data Management Connector')
        remove(filepath)
        return ch_res
    except Exception as err:
        remove(filepath)
        logger.error("Error creating attachment record for CSV file")
        raise ConnectorError('Error in creating attachment record for CSV file: %s' % err)


def _format_return_result(params, attDetail, df):
    # Create small chunks of dataset to consume by playbook if requested by user otherwise return complete recordset
    if params.get('recordBatch'):
        smaller_datasets = [df[i:i+20] for i in range(0, len(df), 20)]
        all_records = []
        for batch in smaller_datasets:
            # all_records.append(batch.to_dict(as_series=False))
            all_records.append(batch.rows())
        if params.get('saveAsAttachment'):
            final_result = {"records": all_records, "attachment": attDetail}
            return final_result
        final_result = {"records": all_records}
        return final_result
    else:
        if params.get('saveAsAttachment'):
            # final_result = {"records": df.to_dict(as_series=False), "attachment": attDetail}
            final_result = {"records": df.rows(), "attachment": attDetail}
            return final_result
    # final_result = {"records": df.to_dict(as_series=False)}
    final_result = {"records": df.rows()}
    return final_result
