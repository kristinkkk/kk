import psycopg2
import boto3
import json
import csv
import sys
sys.path.append(
    '/Users/guiyuezheng/Desktop/Data Engineer/ETL _pipeline_learning/data_ingestion_framwork/utils')
from get_aws_secret import get_secret
from ssm_manager import get_parameter_value,update_parameter_value
from postgressql_connector import PostgresSqlConnector


def extract_data(
                 source_schema_name,
                 source_table_name,
                 load_method,
                 export_file_name,
                 incremental_load_column=None,
                 incremental_load_parameter_name=None
                 ):
    crm_postgres_connector = PostgresSqlConnector(secret_name="postgres_ac_master")
    extract_sql = f"select * from {source_schema_name}.{source_table_name}"
    if load_method=="incremental_load":
        last_processed_datetime = get_parameter_value(incremental_load_parameter_name)
        extract_sql = extract_sql + f" where {incremental_load_column} >='{last_processed_datetime}' "
    else:
        extract_sql=extract_sql + \
        f'limit 100'
        
    crm_postgres_connector.export_sql_to_csv( sql = extract_sql, export_file_name=export_file_name,
                                              delimiter=",")



def upload_file_to_s3(local_file_name,s3_bucket_name,s3_folder_path):
    s3_client = boto3.client("s3")
    s3_client.upload_file(local_file_name,s3_bucket_name,s3_folder_path)


def copy_csv_into_redshift_table(redshift_db_connection,s3_file,schema_name,table_name,delimiter=","):
    copy_sql_cmd = f"""
    truncate  {schema_name}.{table_name};
    copy {schema_name}.{table_name}
    from 's3://{s3_file}'
    credentials 'aws_iam_role=arn:aws:iam::721495903582:role/redshift-admin'
    format csv
    delimiter '{delimiter}'
    ignoreheader 1"""
    print(copy_sql_cmd)
    cursor = redshift_db_connection.cursor()
    cursor.execute(copy_sql_cmd)
    cursor.execute("COMMIT")

def upsert_from_staging_into_destination_table(redshift_db_connection,
                                               staging_schema_name,
                                               table_name,
                                               destination_schema_name,
                                               primary_key_column):
    upsert_sql_cmd = f"""
    delete from {destination_schema_name}.{table_name}
    where {primary_key_column} in (select {primary_key_column} from {staging_schema_name}.{table_name});
    insert into {destination_schema_name}.{table_name}
    select * from {staging_schema_name}.{table_name}
    """
    cursor=redshift_db_connection.cursor()
    cursor.execute(upsert_sql_cmd)
    cursor.execute("COMMIT")

def main():
    # define all common variables
    source_db_aws_secret_name = "postgres_ac_master"
    source_schema = "ac_shopping"
    s3_bucket_name = "ac-shopping-datalake"
    s3_folder_path = "ac_shopping_crm"
    destination_schema_name="ac_shopping_crm"
    redshift_aws_secret_name ="redshift_ac_master"

    # define specific variables for each table to ingest

    tables_list_config = {
            "customer":{"table_name":"customer",
                        "load_method":"incremental_load",
                         "incremental_load_column":"updated_at",
                         "incremental_load_parameter_name":"de_daily_load_ac_shopping_crm_customer",
                         "export_file_name":"customer.csv",
                         "primary_key_column":"customer_id"},

            "order" :{"table_name":"order","load_method":"incremental_load",
                         "incremental_load_column":"updated_at",
                         "incremental_load_parameter_name":"de_daily_load_ac_shopping_crm_order",
                         "export_file_name":"order.csv",
                         "primary_key_column":"order_id"}
                         }
    
    for table_name, parameters in tables_list_config.items():
        extract_data(
                    source_schema_name=source_schema,
                    source_table_name=table_name,
                    load_method=parameters["load_method"],
                    export_file_name=parameters["export_file_name"],
                    incremental_load_column=parameters["incremental_load_column"],
                    incremental_load_parameter_name=parameters["incremental_load_parameter_name"])
        
        upload_file_to_s3(local_file_name=parameters["export_file_name"],
                          s3_bucket_name=s3_bucket_name,
                          s3_folder_path=s3_folder_path+"/{}.csv".format(parameters["table_name"]))

        redshift_connector = PostgresSqlConnector(secret_name="redshift_ac_master")
        redshift_connection = redshift_connector.get_connection()

        copy_csv_into_redshift_table(redshift_db_connection=redshift_connection,
                                    s3_file="{s3_bucket_name}/{s3_folder_path}/{export_file_name}".format(
                                    s3_bucket_name = s3_bucket_name,s3_folder_path=s3_folder_path,
                                    export_file_name=parameters["export_file_name"]
                                    ),schema_name="staging",
                                    table_name=table_name)
        
        upsert_from_staging_into_destination_table(redshift_db_connection=redshift_connection,
                                                staging_schema_name="staging",
                                                table_name=table_name,
                                                destination_schema_name=destination_schema_name,
                                                primary_key_column=parameters["primary_key_column"])
        

        date_processed=redshift_connector.get_execute_sql_result("select max({incremental_load_column}) from staging.{table_name}".format(
                incremental_load_column=parameters["incremental_load_column"],
                table_name= table_name

        ))[0][0]

        update_parameter_value(parameters["incremental_load_parameter_name"], str(date_processed))
    
main()