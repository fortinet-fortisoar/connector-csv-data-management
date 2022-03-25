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
        numberOfRowsToSkip = None
        file_iri = handle_params(params,params.get('value'))
        file_path = join('/tmp', download_file_from_cyops(file_iri)['cyops_file_path'])
  

        if params.get('numberOfRowsToSkip'):
          numberOfRowsToSkip = params.get('numberOfRowsToSkip')
          
        if params.get('columnNames'):  # CSV file with column header and specifci columns to use in creating recordset 
            columnNames = params.get('columnNames')
            columnNames = columnNames.split(",")
            # We are passing  specific columns name to filter data from here

            df = _read_file_specific_columns(file_path,columnNames,numberOfRowsToSkip)
            
        elif params.get('isCSVWithoutHeaders') and params.get('columnNames') : # CSV file without column header and specifci columns to use in creating recordset 
            # We are passing no columns as option but adding a top level
            
            df = _read_file_specific_columns_no_headers(file_path,columnNames,numberOfRowsToSkip)
            
        elif params.get('isCSVWithoutHeaders'): # CSV file without column header and all columns to use in creating recordset 
            # We are reading file which has no column headers
            df = _read_file_no_headers(file_path,numberOfRowsToSkip)

        else:  
            # We are reading complete file assuming it has column header
            df = _read_file_all_columns(file_path,numberOfRowsToSkip)

        # If user has selected to dedeuplicate recordset
        try:
            if params.get('deDupValuesOn'):
                deDupValuesOn = params.get('deDupValuesOn')
                deDupValuesOn = deDupValuesOn.split(",")
                df.drop_duplicates(subset=[deDupValuesOn], keep='first')
        except Exception as Err:
            logger.error('Error in dedeuplicating data  extract_data_from_csv(): %s' % Err)
        
        # Replace empty values with N/A 
        df = df.fillna('N/A')

        #Create small chunks of dataset to cosume by playbook    
        smaller_datasets = np.array_split(df, 20)
        all_records = []
        for batch in smaller_datasets:
            all_records.append(batch.to_dict("records"))
            final_result = {"records": all_records}

        return final_result

    except Exception as Err:
        logger.error('Error in extract_data_from_csv(): %s' % Err)


        
def extract_data_from_two_csv(config, params):
    try:
        mergeColumn = params.get('mergeColumnNames')
        numberOfRowsToSkip = None
        
        file_1_iri_ = handle_params(params,params.get('file_one_value'))
        file_2_iri_ = handle_params(params,params.get('file_two_value'))
        logger.warning(params)
        first_file_path = join('/tmp', download_file_from_cyops(file_1_iri_)['cyops_file_path'])
        second_file_path = join('/tmp', download_file_from_cyops(file_2_iri_)['cyops_file_path'])

        #Lets read first file 
         
        if params.get('numberOfRowsToSkipFirst'):
          numberOfRowsToSkip = params.get('numberOfRowsToSkipFirst')
          
        if params.get('file1_columnNames'):  # CSV file with column header and specifci columns to use in creating recordset 
            columnNames = params.get('file1_columnNames')
            columnNames = columnNames.split(",")
            
            # We are passing  specific columns name to filter data from here
            df_file1 =  _read_file_specific_columns(first_file_path,columnNames,numberOfRowsToSkip)
            
        elif params.get('isCSVWithoutHeadersFirst') and params.get('columnNames') : # CSV file without column header and specifci columns to use in creating recordset 
            # We are passing no columns as option but adding a top level
            df_file1 = _read_file_specific_columns_no_headers(first_file_path,columnNames,numberOfRowsToSkip)
            
        elif params.get('isCSVWithoutHeadersFirst'): # CSV file without column header and all columns to use in creating recordset 
            # We are reading file which has no column headers
            df_file1 =  _read_file_no_headers(first_file_path,numberOfRowsToSkip) 

        else:  
            # We are reading complete file assuming it has column header
            df_file1 = _read_file_all_columns(first_file_path,numberOfRowsToSkip)

        #Lets read second file 
        
        if params.get('numberOfRowsToSkipSecond'):
            numberOfRowsToSkip = params.get('numberOfRowsToSkipSecond')
          
        if params.get('file2_columnNames'):  # CSV file with column header and specifci columns to use in creating recordset 
            columnNames = params.get('file2_columnNames')
            columnNames = columnNames.split(",")
    
            # We are passing  specific columns name to filter data from here
            df_file2 = _read_file_specific_columns(second_file_path,columnNames,numberOfRowsToSkip)
            
        elif params.get('isCSVWithoutHeadersSecond') and params.get('file2_columnNames') : # CSV file without column header and specifci columns to use in creating recordset 
            # We are passing no columns as option but adding a top level
            df_file2 = _read_file_specific_columns_no_headers(second_file_path,columnNames,numberOfRowsToSkip)
            
        elif params.get('isCSVWithoutHeadersSecond'): # CSV file without column header and all columns to use in creating recordset 
            # We are reading file which has no column headers
            df_file2 = _read_file_no_headers(second_file_path,numberOfRowsToSkip)  

        else:  
            # We are reading complete file assuming it has column header
            df_file2 = _read_file_all_columns(second_file_path,numberOfRowsToSkip)

        
        #Merge both files
        combined_recordSet =pd.merge(df_file1,df_file2,how='left',left_on=mergeColumn,right_on=mergeColumn)    

        # If user has selected to dedeuplicate recordset
        try:
            if params.get('deDupValuesOn'):
                deDupValuesOn = params.get('deDupValuesOn')
                deDupValuesOn = deDupValuesOn.split(",")
                combined_recordSet.drop_duplicates(subset=[deDupValuesOn], keep='first')
        except Exception as Err:
            logger.error('Error in dedeuplicating data  extract_data_from_csv(): %s' % Err)

        # Replace empty values with N/A 
        combined_recordSet = combined_recordSet.fillna('N/A')

        #Create small chunks of dataset to cosume by playbook    
        smaller_datasets = np.array_split(combined_recordSet, 20)
        all_records = []
        for batch in smaller_datasets:
            all_records.append(batch.to_dict("records"))
            final_result = {"records": all_records}

        return final_result

    except Exception as Err:
        logger.error('Error in extract_data_from_csv(): %s' % Err)


def _read_file_specific_columns(filepath,columns_list,numberOfRowsToSkip=None):
    df = pd.read_csv('{}'.format(filepath), delimiter=',', encoding="utf-8-sig",skiprows=numberOfRowsToSkip)[[columns_list]]
    return df

def _read_file_all_columns(filepath,numberOfRowsToSkip=None):
    df = pd.read_csv('{}'.format(filepath), delimiter=',', encoding="utf-8-sig",skiprows=numberOfRowsToSkip)
    return df

def _read_file_specific_columns_no_headers(filepath,columns_list,numberOfRowsToSkip=None):
    df = pd.read_csv('{}'.format(filepath), delimiter=',', encoding="utf-8-sig",header = None,skiprows=numberOfRowsToSkip)[[columns_list]]
    return df

def _read_file_no_headers(filepath,numberOfRowsToSkip=None):
    df = pd.read_csv('{}'.format(filepath), delimiter=',', encoding="utf-8-sig",header = None,skiprows=numberOfRowsToSkip)
    return df



def handle_params(params,file_param):
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
        raise ConnectorError('Requested resource could not be found with input type "{0}" and value "{1}"'.format
                             (input_type, value.replace('/api/3/attachments/', '')))