import requests
import pandas as pd
import numpy as np
import csv
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
        isSingleColumn = None
        no_of_columns = None 
        isCSVWithoutHeaders = False

        file_iri = handle_params(params,params.get('value'))
        file_path = join('/tmp', download_file_from_cyops(file_iri)['cyops_file_path'])

        res = _check_if_csv(file_path)
        logger.info(res)
  
        if res.get('headers') == False:
            isCSVWithoutHeaders = True
            no_of_columns = res.get('columns')
        if res.get('columns') == 1:
            isSingleColumn = True

        if params.get('numberOfRowsToSkip'):
          numberOfRowsToSkip = params.get('numberOfRowsToSkip')
          
        if params.get('columnNames') != "":  # CSV file with column header and specific columns to use in creating recordset 
            columnNames = params.get('columnNames')
            columnNames = columnNames.split(",")
            # We are passing  specific columns name to filter data from here
            df = _read_file_specific_columns(file_path,columnNames,numberOfRowsToSkip)
            
        elif isSingleColumn : # CSV file with one  column and header 
            df = _read_file_single_column(file_path,numberOfRowsToSkip)

        elif isSingleColumn and isCSVWithoutHeaders: # CSV file with one  column and no header 
            df = _read_file_single_column_no_header(file_path,numberOfRowsToSkip)

        elif isCSVWithoutHeaders: # CSV file without column header and all columns
            df = _read_file_no_headers(file_path,numberOfRowsToSkip)

        else:  
            # We are reading complete file assuming it has column header
            df = _read_file_all_columns(file_path,numberOfRowsToSkip)

        # If user has selected to dedeuplicate recordset
        try:
            if params.get('deDupValuesOn'):
                deDupValuesOn = params.get('deDupValuesOn')
                deDupValuesOn = deDupValuesOn.split(",")
                df.drop_duplicates(subset=deDupValuesOn, keep='first')
        except Exception as Err:
            logger.error('Error in dedeuplicating data  extract_data_from_csv(): %s' % Err)
        
        # Replace empty values with N/A 
        df = df.fillna('N/A')

        #Create small chunks of dataset to cosume by playbook if requested by user otherwise return complete recordset
        if params.get('recordBatch'):
            smaller_datasets = np.array_split(df, 20)
            all_records = []
            for batch in smaller_datasets:
                all_records.append(batch.to_dict("records"))
                final_result = {"records": all_records}
        else:
            final_result = {"records": df.to_dict()}

        return final_result

    except Exception as Err:
        logger.error('Error in extract_data_from_csv(): %s' % Err)


        
def merge_two_csv_and_extract_data(config, params):
    try:
        if (params.get('mergeColumnNames')):
             mergeColumn = params.get('mergeColumnNames')
             mergeColumn = mergeColumn.split(",")
        logger.info(mergeColumn)
        numberOfRowsToSkipFirst = None
        numberOfRowsToSkipSecond = None
        isSingleColumnFirst = None
        isSingleColumnSecond = None
        isCSVWithoutHeadersFirst = False
        isCSVWithoutHeadersSecond = False

        file_1_iri_ = handle_params(params,params.get('file_one_value'))
        file_2_iri_ = handle_params(params,params.get('file_two_value'))
        logger.warning(params)
        first_file_path = join('/tmp', download_file_from_cyops(file_1_iri_)['cyops_file_path'])
        second_file_path = join('/tmp', download_file_from_cyops(file_2_iri_)['cyops_file_path'])

        res = _check_if_csv(first_file_path)
        logger.info(res)
  
        if res.get('headers') == False:
            isCSVWithoutHeadersFirst = True

        if res.get('columns') == 1:
            isSingleColumnFirst = True
        
        res = _check_if_csv(second_file_path)

        if res.get('headers') == False:
            isCSVWithoutHeadersSecond = True
        elif res.get('columns') == 1:
            isSingleColumnSecond = True


        #Lets read first file 
         
        if params.get('numberOfRowsToSkipFirst'):
          numberOfRowsToSkipFirst = params.get('numberOfRowsToSkipFirst')
          
        if params.get('file1_column_names') != "":  # CSV file with column header and specific columns to use in creating recordset 
            columnNames = params.get('file1_column_names')
            columnNames = columnNames.split(",")
            
            # We are passing  specific columns name to filter data from here
            df_file1 =  _read_file_specific_columns(first_file_path,columnNames,numberOfRowsToSkipFirst)
            
        elif isCSVWithoutHeadersFirst: # CSV file without column header and more than one column
            df_file1 =  _read_file_no_headers(first_file_path,numberOfRowsToSkipFirst) 

        elif isSingleColumnFirst: #CSV with single column and header    
            df_file1 = _read_file_single_column(first_file_path,numberOfRowsToSkipFirst)
        
        elif isSingleColumnFirst and not isCSVWithoutHeadersFirst: # CSV file with one  column and no header 
            df_file1 = _read_file_single_column_no_header(first_file_path,numberOfRowsToSkipFirst)

        else:  
            # We are reading complete file assuming it has column header
            df_file1 = _read_file_all_columns(first_file_path,numberOfRowsToSkipFirst)

        #Lets read second file 
        
        if params.get('numberOfRowsToSkipSecond'):
            numberOfRowsToSkipSecond = params.get('numberOfRowsToSkipSecond')
          
        if params.get('file2_column_names') != "":  # CSV file with column header and specific columns to use in creating recordset 
            columnNames = params.get('file2_column_names')
            columnNames = columnNames.split(",")
    
            # We are passing  specific columns name to filter data from here
            df_file2 = _read_file_specific_columns(second_file_path,columnNames,numberOfRowsToSkipSecond)
            
        elif isCSVWithoutHeadersSecond: # CSV file without column header and more than one column
            df_file2 = _read_file_no_headers(second_file_path,numberOfRowsToSkipSecond)  

        elif isSingleColumnSecond: #CSV with single colum   
            df_file2 = _read_file_single_column(second_file_path,numberOfRowsToSkipSecond)  

        elif isSingleColumnSecond and  isCSVWithoutHeadersSecond: # CSV file with one  column and no header 
            df_file2 = _read_file_single_column_no_header(second_file_path,numberOfRowsToSkipSecond) 

        else:  
            # We are reading complete file assuming it has column header
            df_file2 = _read_file_all_columns(second_file_path,numberOfRowsToSkipSecond)

        
        #Merge both files
        combined_recordSet =pd.merge(df_file1,df_file2,how='left',left_on=mergeColumn,right_on=mergeColumn)    

        # If user has selected to dedeuplicate recordset
        try:
            if params.get('deDupValuesOn'):
                deDupValuesOn = params.get('deDupValuesOn')
                deDupValuesOn = deDupValuesOn.split(",")
                combined_recordSet.drop_duplicates(subset=deDupValuesOn, keep='first')
        except Exception as Err:
            logger.error('Error in dedeuplicating data  extract_data_from_csv(): %s' % Err)

        # Replace empty values with N/A 
        combined_recordSet = combined_recordSet.fillna('N/A')

        #Create small chunks of dataset to cosume by playbook if requested by user otherwise return complete recordset
        if params.get('recordBatch'):
            smaller_datasets = np.array_split(combined_recordSet, 20)
            all_records = []
            for batch in smaller_datasets:
                all_records.append(batch.to_dict("records"))
                final_result = {"records": all_records}
        else:
            final_result = {"records": combined_recordSet.to_dict()}
            
        return final_result

    except Exception as Err:
        logger.error('Error in extract_data_from_csv(): %s' % Err)


def concat_two_csv_and_extract_data(config, params):
    try:
        numberOfRowsToSkipFirst = None
        numberOfRowsToSkipSecond = None
        isSingleColumnFirst = None
        isSingleColumnSecond = None
        isCSVWithoutHeadersFirst = False
        isCSVWithoutHeadersSecond = False

        file_1_iri_ = handle_params(params,params.get('file_one_value'))
        file_2_iri_ = handle_params(params,params.get('file_two_value'))
        logger.warning(params)
        first_file_path = join('/tmp', download_file_from_cyops(file_1_iri_)['cyops_file_path'])
        second_file_path = join('/tmp', download_file_from_cyops(file_2_iri_)['cyops_file_path'])

        res = _check_if_csv(first_file_path)
        logger.info(res)
  
        if res.get('headers') == False:
            isCSVWithoutHeadersFirst = True

        if res.get('columns') == 1:
            isSingleColumnFirst = True
        
        res = _check_if_csv(second_file_path)

        if res.get('headers') == False:
            isCSVWithoutHeadersSecond = True
        elif res.get('columns') == 1:
            isSingleColumnSecond = True


        #Lets read first file 
         
        if params.get('numberOfRowsToSkipFirst'):
          numberOfRowsToSkipFirst = params.get('numberOfRowsToSkipFirst')
          
        if params.get('file1_column_names') != "":  # CSV file with column header and specific columns to use in creating recordset 
            columnNames = params.get('file1_column_names')
            columnNames = columnNames.split(",")
            
            # We are passing  specific columns name to filter data from here
            df_file1 =  _read_file_specific_columns(first_file_path,columnNames,numberOfRowsToSkipFirst)
            
        elif isCSVWithoutHeadersFirst: # CSV file without column header and more than one column
            df_file1 =  _read_file_no_headers(first_file_path,numberOfRowsToSkipFirst) 

        elif isSingleColumnFirst: #CSV with single column and header    
            df_file1 = _read_file_single_column(first_file_path,numberOfRowsToSkipFirst)
        
        elif isSingleColumnFirst and not isCSVWithoutHeadersFirst: # CSV file with one  column and no header 
            df_file1 = _read_file_single_column_no_header(first_file_path,numberOfRowsToSkipFirst)

        else:  
            # We are reading complete file assuming it has column header
            df_file1 = _read_file_all_columns(first_file_path,numberOfRowsToSkipFirst)

        #Lets read second file 
        
        if params.get('numberOfRowsToSkipSecond'):
            numberOfRowsToSkipSecond = params.get('numberOfRowsToSkipSecond')
          
        if params.get('file2_column_names') != "":  # CSV file with column header and specific columns to use in creating recordset 
            columnNames = params.get('file2_column_names')
            columnNames = columnNames.split(",")
    
            # We are passing  specific columns name to filter data from here
            df_file2 = _read_file_specific_columns(second_file_path,columnNames,numberOfRowsToSkipSecond)
            
        elif isCSVWithoutHeadersSecond: # CSV file without column header and more than one column
            df_file2 = _read_file_no_headers(second_file_path,numberOfRowsToSkipSecond)  

        elif isSingleColumnSecond: #CSV with single colum   
            df_file2 = _read_file_single_column(second_file_path,numberOfRowsToSkipSecond)  

        elif isSingleColumnSecond and  isCSVWithoutHeadersSecond: # CSV file with one  column and no header 
            df_file2 = _read_file_single_column_no_header(second_file_path,numberOfRowsToSkipSecond) 

        else:  
            # We are reading complete file assuming it has column header
            df_file2 = _read_file_all_columns(second_file_path,numberOfRowsToSkipSecond)

        
        #Merge both files
        combined_recordSet =pd.concat([df_file1,df_file2])    

        # If user has selected to dedeuplicate recordset
        try:
            if params.get('deDupValuesOn'):
                deDupValuesOn = params.get('deDupValuesOn')
                deDupValuesOn = deDupValuesOn.split(",")
                combined_recordSet.drop_duplicates(subset=deDupValuesOn, keep='first')
        except Exception as Err:
            logger.error('Error in dedeuplicating data  extract_data_from_csv(): %s' % Err)

        # Replace empty values with N/A 
        combined_recordSet = combined_recordSet.fillna('N/A')

        #Create small chunks of dataset to cosume by playbook if requested by user otherwise return complete recordset
        if params.get('recordBatch'):
            smaller_datasets = np.array_split(combined_recordSet, 20)
            all_records = []
            for batch in smaller_datasets:
                all_records.append(batch.to_dict("records"))
                final_result = {"records": all_records}
        else:
            final_result = {"records": combined_recordSet.to_dict()}
            
        return final_result

    except Exception as Err:
        logger.error('Error in extract_data_from_csv(): %s' % Err)
    
def _read_file_specific_columns(filepath,columns_list,numberOfRowsToSkip=None):
    try:
        chunk = pd.read_csv('{}'.format(filepath), delimiter=',', encoding="utf-8-sig",skiprows=numberOfRowsToSkip,chunksize=100000,error_bad_lines=False,usecols=columns_list)
        df = pd.concat(chunk)
        return df
    except Exception as Err:
        logger.error('Error in _read_file_specific_columns(): %s' % Err)    

def _read_file_all_columns(filepath,numberOfRowsToSkip=None):
    try:
        chunk = pd.read_csv('{}'.format(filepath), delimiter=',', encoding="utf-8-sig",skiprows=numberOfRowsToSkip,chunksize=100000,error_bad_lines=False)
        df = pd.concat(chunk)
        return df
    except Exception as Err:
        logger.error('Error in _read_file_all_columns(): %s' % Err)

def _read_file_no_headers(filepath,numberOfRowsToSkip=None,no_of_columns=None):
    try:
        if no_of_columns:
            colList = []
            for i in range(no_of_columns):
                colList.append("Column"+str(i))
        chunk = pd.read_csv('{}'.format(filepath), delimiter=',', encoding="utf-8-sig",header = None,skiprows=numberOfRowsToSkip,chunksize=100000,error_bad_lines=False)
        df = pd.concat(chunk)
        df.columns = colList
        return df
    except Exception as Err:
        logger.error('Error in _read_file_no_headers(): %s' % Err)    

def _read_file_single_column(filepath,numberOfRowsToSkip=None):
    try:
        chunk = pd.read_csv('{}'.format(filepath),usecols=[0],skiprows=numberOfRowsToSkip,chunksize=100000,error_bad_lines=False)
        df = pd.concat(chunk)
        return df
    except Exception as Err:
        logger.error('Error in _read_file_no_headers(): %s' % Err) 
            
def _read_file_single_column_no_header(filepath,numberOfRowsToSkip=None,no_of_columns=None):
    try:
        if no_of_columns:
            colList = []
            for i in range(no_of_columns):
                colList.append("Column"+str(i))
        chunk = pd.read_csv('{}'.format(filepath),usecols=[0],header = None,skiprows=numberOfRowsToSkip,chunksize=100000,error_bad_lines=False)
        df = pd.concat(chunk)
        df.columns = colList
        return df
    except Exception as Err:
        logger.error('Error in _read_file_single_column_no_header(): %s' % Err) 

def _check_if_csv(filepath):
    try:
        sniffer = csv.Sniffer()
        sample_bytes = 32
        res=sniffer.has_header(open(filepath).read(sample_bytes))
        df = pd.read_csv('{}'.format(filepath),error_bad_lines=False,nrows=10)
        row, col = df.shape
        return {"headers": True,"columns": col }
    except:
        logger.error('Error in _check_if_csv() - csv check header: %s' % Err) 
        try:
            df = pd.read_csv('{}'.format(filepath),error_bad_lines=False,nrows=100)
            row, col = df.shape
            return {"headers": False,"columns": col }
        except Exception as Err:
             ("Not a valid CSV: "+ Err)




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