import zipfile
import os
import pandas as pd
import configparser
import boto3
import psycopg2
from helpers.helpers_sql.sql_statements import sql_copy_statements, sql_merge_statements, sql_delete_staging_statments
from helpers.helpers_sql.table_params import staging_tables, dim_tables, fact_tables, staging_fact_tables, helper_tables
from helpers.helpers_python.database_class import Database
from helpers.helpers_python.aws_s3_class import aws_s3
from helpers.helpers_python.python_functions import zip_downloader, extract_and_clean_crime_dataset, extract_and_clean_crime_helper_dataset_gdi, extract_and_clean_crime_helper_dataset_population, extract_and_clean_postcode_helper_dataset
import time
import datetime
from datetime import timedelta


def extract_and_transform_crime_dataset():
    zip_downloader(input_dir = 'data/uk_crime_data_raw', output_dir = 'data/uk_crime_data')
    
    input_dir = 'data/uk_crime_data'
    input_file_name = 'UK_Police_Street_Crime_2018-10-01_to_2021_09_31.csv'
    output_dir = 'data/uk_crime_data'
    output_file_name = 'UK_Police_Street_Crime_2018-10-01_to_2021_09_31.csv'
    
    extract_and_clean_crime_dataset(input_dir, input_file_name, output_dir, output_file_name)
    


def extract_and_transform_crime_helper_population_dataset():
    input_dir = 'data/uk_helper_data_raw'
    input_file_name = 'vcregionalgdhibylareordered.xlsx'
    output_dir = 'data/uk_helper_data'
    output_file_name = 'uk_regional_population_1997_2016.csv'
    extract_and_clean_crime_helper_dataset_population(input_dir, input_file_name, output_dir, output_file_name)

    

def extract_and_transform_crime_helper_gdi_dataset():
    input_dir = 'data/uk_helper_data_raw'
    input_file_name = 'vcregionalgdhibylareordered.xlsx'
    output_dir = 'data/uk_helper_data'
    output_file_name = 'uk_regional_gross_disposable_income_1997_2016.csv'
    extract_and_clean_crime_helper_dataset_gdi(input_dir, input_file_name, output_dir, output_file_name)


def extract_and_transform_postcode_helper_dataset():
    input_dir = 'data/uk_postcode_conversion_raw'
    input_file_name = 'PCD_OA_LSOA_MSOA_LAD_AUG19_UK_LU.csv'
    output_dir = 'data/uk_postcode_conversion'
    output_file_name = 'uk_postcode_mapping.csv'
    extract_and_clean_postcode_helper_dataset(input_dir, input_file_name, output_dir, output_file_name)
    
    
    
def load_data_to_s3(aws_s3_client):
    tables = staging_tables + helper_tables
    aws_s3_client.upload_multiple_file_to_s3(tables)
    


def create_dwh_tables(db):
    all_table_params = staging_tables + dim_tables + fact_tables + helper_tables + staging_fact_tables
    db.create_missing_database_tables(all_table_params)
       

def load_staging_tables(db, config):
    tables = staging_tables

    sql_copy_statement = sql_copy_statements[0]
    sql_insert_statement = sql_merge_statements[2]

    db.load_staging_tables_s3_based(tables, config, sql_copy_statement)
    db.load_staging_tables_dwh_based(tables, sql_insert_statement)
    
    

def load_dim_tables(db):
    tables = dim_tables
    db.load_dim_tables(tables, sql_upsert_statement=sql_merge_statements[3])
    
    
    
def load_helper_tables(db, config):
    tables = helper_tables
    sql_copy_statement = sql_copy_statements[0]
    db.load_staging_tables_s3_based(tables, config, sql_copy_statement)
    


def load_staging_fact_tables(db):
    tables = staging_fact_tables
    sql_insert_statement = sql_merge_statements[2]
    db.load_staging_tables_dwh_based(tables, sql_insert_statement)

    

def load_fact_tables(db):
    tables = fact_tables
    sql_upsert_statement = sql_merge_statements[3]
    db.load_fact_tables(tables, sql_upsert_statement)
    
    

def delete_data_in_staging_tables(db):
    tables = staging_tables + staging_fact_tables
    sql_delete_statement = sql_delete_staging_statments[0]
    db.delete_staging_data(tables, sql_delete_statement)
    
        

def data_quality_checks(db):
    dim_and_fact_tables = dim_tables + fact_tables
    db.qa_dim_fact_tables_contain_values(tables=dim_and_fact_tables)
    
    staging_tables_complete = staging_tables + staging_fact_tables
    db.qa_staging_tables_empty(tables=staging_tables_complete)
    


def delete_s3_files(aws_s3_client):
    tables = staging_tables + helper_tables
    aws_s3_client.delete_multiple_s3_files(tables)

    

def main():
    
    start_time = time.time()
    
    config = configparser.ConfigParser()
    config.read_file(open('dl.cfg'))
    
    aws_access_key_id = config['AWS']['key'] 
    aws_secret_access_key = config['AWS']['secret']
    host = config['REDSHIFT_CLUSTER']['db_host']
    port = config['REDSHIFT_CLUSTER']['db_port']
    dbname = config['REDSHIFT_CLUSTER']['db_name']
    user = config['REDSHIFT_CLUSTER']['db_user']
    password = config['REDSHIFT_CLUSTER']['db_password']
    
    db = Database(host, dbname, port, user, password)
    db.connect()    
    aws_s3_client = aws_s3(aws_access_key_id, aws_secret_access_key)
    
   
    try:
        load_data_to_s3(aws_s3_client)
        create_dwh_tables(db)
        load_staging_tables(db, config)
        load_dim_tables(db)
        load_helper_tables(db,config)
        load_staging_fact_tables(db)
        load_fact_tables(db)
        delete_data_in_staging_tables(db)
        data_quality_checks(db)
        delete_s3_files(aws_s3_client)
    except Exception as e:
        print(e)        
    
    end_time = time.time()
    elapsed_seconds = (end_time - start_time)
    run_duration = str(timedelta(seconds=elapsed_seconds))
    print(f'ETL finished. Runtime: {run_duration}')

    

if __name__ == "__main__":
    main()