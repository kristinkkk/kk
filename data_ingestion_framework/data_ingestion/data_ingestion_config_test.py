import sys
#sys.path.append(
#    'data_ingestion_framework/python_class')
#sys.path.append(
#    'data_ingestion_framework/utils')
import yaml
import os
from python_class.postgressql_connector import PostgresSqlConnector
from utils.ssm_manager import (
    get_parameter_value,
    create_parameter)
from data_ingestion_config import DataIngestionConfig, PipelineConfig, TableConfig
from python_class.s3_connector import S3Connector



my_data_config=DataIngestionConfig(
    yaml_file='6.data_ingestion/ac_shopping_crm.yml'
)

#print(my_data_config._get_yaml()) #返回整个
#print(my_data_config._get_yaml()['dag_name'])
#print(my_data_config.get_yaml_attr('dag_name'))


my_pipeline_config=my_data_config.get_pipeline_config()
#print(my_pipeline_config.destination_credentials)



#my_table_config_list=my_data_config.get_table_config()
#for table_config in my_table_config_list:
#    print(table_config.parameter_name)



#table_config_list=[]

#for table_config in my_data_config.yaml_string.get('tables'):
#    for table_name, table_attr in table_config.items():
#        table_config_class=TableConfig(table_name, my_pipeline_config, table_attr)
#        table_config_list.append(table_config_class)
#        print (table_config_list)


table_config = my_data_config.get_table_config()
for i in table_config:
    print(i.column_inclusions)


