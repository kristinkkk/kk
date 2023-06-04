from data_ingestion import DataIngestionTask
from postgressql_connector import PostgresSqlConnector
from data_ingestion_config import DataIngestionConfig, PipelineConfig, TableConfig

#my_data_ingestion=DataIngestionTask(task_name='my_test')
#print(my_data_ingestion.s3_conn)
#print(my_data_ingestion.task_start)
#print(my_data_ingestion.table_name)

#my_data_ingestion.source_connection=PostgresSqlConnector('postgres_ac_master')
#my_data_ingestion.destination_connection=PostgresSqlConnector('redshift_ac_master')

#source_schema='ac_shopping'
#source_table='customer'
#destination_schema='staging_kristin'
#destination_table='customer'
#df=my_data_ingestion.source_connection.get_column_metadata(source_table,source_schema)
#print(df)#拿到customer table的column name &  datatype

#schema_sql="create schema if not exists {}".format(destination_schema)
#my_data_ingestion.destination_connection.execute_sql(schema_sql)

my_data_task=DataIngestionTask(task_name='my_test',
                               config_path='ac_shopping_crm.yml')
#db_connection=PostgresSqlConnector('postgres_ac_master')

#data_config=DataIngestionConfig(yaml_file='/Users/guiyuezheng/Desktop/Data Engineer/ETL - Python file/6.data_ingestion/ac_shopping_crm.yml', 
#                                table_id='customer')

#table_config=data_config.get_table_config()
#print(table_config)

#for i in table_config:
#    print(i.column_inclusions)
#print(table_config[0].column_inclusions)

#pipe_config=data_config.get_pipeline_config()
#print(pipe_config)


my_data_task.main()

            