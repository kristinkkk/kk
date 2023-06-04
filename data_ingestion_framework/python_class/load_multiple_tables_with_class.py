import psycopg2
import boto3
import json
import csv
import sys
#sys.path.append(
#    'data_ingestion_framework/utils')
from utils.get_aws_secret import get_secret
from utils.ssm_manager import get_parameter_value
from postgressql_connector import PostgresSqlConnector


def extract_data(secret_name,
                 source_schema_name,
                 source_table_name,
                 load_method,
                 export_file_name,
                 incremental_load_column=None,
                 incremental_load_parameter_name=None,
                 delimiter=','
                 ):
    crm_postgres_connector = PostgresSqlConnector(secret_name)
    extract_sql = f"select * from {source_schema_name}.{source_table_name}"
    if load_method == "incremental_load":
        last_processed_datetime = get_parameter_value(
            incremental_load_parameter_name)
        extract_sql = extract_sql + \
            f"where {incremental_load_column} >='{last_processed_datetime}' "
    else:
        extract_sql = extract_sql + \
            f"limit 100"
    
    crm_postgres_connector.export_sql_to_csv(sql= extract_sql,
                                             export_file_name=export_file_name,
                                             delimiter=delimiter)

def upload_file_to_s3(file_name, s3_bucket_name, s3_file_path):
    s3_client = boto3.client('s3')
    s3_client.upload_file(file_name, s3_bucket_name, s3_file_path)


def copy_csv_into_redshift_table(secret_name, s3_file_path, staging_schema_name, staging_table_name, delimiter=","):
    redshift_connection = PostgresSqlConnector(secret_name)
    copy_sql_cmd = f"""
    truncate  {staging_schema_name}.{staging_table_name};
        
    copy {staging_schema_name}.{staging_table_name}
    from 's3://{s3_file_path}'
    credentials 'aws_iam_role=arn:aws:iam::721495903582:role/redshift-admin'
    format csv
    delimiter '{delimiter}'
    ignoreheader 1"""
    print(copy_sql_cmd)
    cursor = redshift_connection.cursor()
    cursor.execute(copy_sql_cmd)
    cursor.execute("COMMIT")

def upsert_data(secret_name,
                destination_schema_name,
                destination_table_name,
                primary_key,
                staging_schema_name,
                staging_table_name):
    redshift_connection = PostgresSqlConnector(secret_name)
    upsert_sql = f"""
        delete from {destination_schema_name}.{destination_table_name}
        where {primary_key} in (select {primary_key} from {staging_schema_name}.{staging_table_name});
        
        insert into {destination_schema_name}.{destination_table_name}
        select * from {staging_schema_name}.{staging_table_name}"""
    cursor=redshift_connection.cursor()
    cursor.execute(upsert_sql)
    cursor.execute('COMMIT')

def parameter_value_update(secret_name, max_sql, parameter_store_name):
    redshift_connection = PostgresSqlConnector(secret_name)
    redshift_cursor = redshift_connection.cursor()
    max_sql=max_sql
    redshift_cursor.execute(max_sql)
    date_processed = redshift_cursor.fetchall()[0][0]
    ssm_client = boto3.client('ssm')
    ssm_client.put_parameter(
        Name=parameter_store_name,
        Value=str(date_processed),
        Type='String',
        Overwrite=True)
        
def main():
    source_db_aws_secret_name = 'postgres_ac_master'
    source_schema_name = 'ac_shopping'
    s3_bucket_name = 'ac-shopping-datalake'
    destination_db_aws_secret_name = 'redshift_ac_master'
    staging_schema_name = 'staging_kristin'
    destination_schema_name = 'kristin'
    redshift_aws_secret_name = 'redshift_ac_master'

    tables_list_configure = {
        "customer": {'source_schema_table': 'customer',
                        'load_method': 'incremental_load',
                        'incremental_load_parameter_store_name': 'de_daily_load_ac_shopping_crm_customer',
                        'incremental_load_column': 'updated_at',
                        'export_file_name': 'customers_incremental_load200.csv',
                        's3_file_path': 'ac_shopping_crm_kristin/customers_incremental_load.csv',
                        'staging_table_name': 'customers',
                        'destination_table_name': 'customer',
                        'primary_key': 'customer_id',
                        'max_sql': 'select max(updated_at) from kristin.customer',
                        'parameter_store_name': 'de_daily_load_ac_shopping_crm_customer'},
        
        "order": {'source_schema_table': 'order',
                'load_method': 'full_load',
                'incremental_load_parameter_store_name': 'None',
                'incremental_load_column': 'None',
                'saved_file_name': 'orders200.csv',
                's3_file_path': 'ac_shopping_crm_kristin/orders100.csv',
                'staging_table_name': 'orders',
                'destination_table_name': 'orders',
                'primary_key': 'order_id',
                'max_sql': 'select max(updated_at) from kristin.orders',
                'parameter_store_name': 'de_daily_load_ac_shopping_crm_order'}
                }
    
    for table_name, parameters in tables_list_configure.items():
        extract_data(secret_name=source_db_aws_secret_name,
                        source_schema_name=source_schema_name,
                        source_table_name=parameters['source_schema_table'],
                        load_method=parameters['load_method'],
                        incremental_load_parameter_name=parameters[
                            'incremental_load_parameter_store_name'],
                        incremental_load_column=parameters['incremental_load_column'],
                        export_file_name=parameters['export_file_name'])

        upload_file_to_s3(file_name=parameters['export_file_name'],
                            s3_bucket_name=s3_bucket_name,
                            s3_file_path=parameters['s3_file_path'])
        
        copy_csv_into_redshift_table(secret_name=destination_db_aws_secret_name,
                                        staging_schema_name=staging_schema_name,
                                        staging_table_name=parameters['staging_table_name'],
                                        s3_bucket_name=s3_bucket_name,
                                        s3_file_path=parameters['s3_file_path'])
        
        upsert_data(secret_name=destination_db_aws_secret_name,
                    destination_schema_name=destination_schema_name,
                    destination_table_name=parameters['destination_table_name'],
                    primary_key=parameters['primary_key'],
                    staging_schema_name=staging_schema_name,
                    staging_table_name=parameters['staging_table_name'])
                    
        parameter_value_update(secret_name=redshift_aws_secret_name,
                            max_sql=parameters['max_sql'],
                            parameter_store_name=parameters['parameter_store_name'])
main()
