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

class DataIngestionConfig:
    """
    Class for dealing with data ingestion config. Reads the given YAML file and
    resolves it to a PipelineConfig class and TableConfig class
    :param yaml_file: The YAML filename and path (without .yml) to process in
        data_ingestion/config/
    :type yaml_file: str
    :param table_id: (Optional) The specific table name to process.
        If not set, all tables will be processed
    :type table_id: str
    :param config_group: (Optional) Currently not in use. If needed, we can use
        it to specify a config_group to process.
        If not set, all config_groups will be processed
    :type config_group: str
    """

    def __init__(self, yaml_file, table_id=None):
        self.yaml_file=yaml_file
        self.table_id=table_id
        self.yaml_string=self._get_yaml()

    def _get_yaml(self): #带有_开头的function，属于private function 只能内部调用
        #link to yaml file and get all data from yaml file.
        print(f"working directory: {os.getcwd()}")
        print(f"yaml_file: {self.yaml_file}")
        with open(self.yaml_file) as config_file:
            return yaml.full_load(config_file) #[yaml.full_load]: load data from trusted source
    
    def get_yaml_attr(self, attribute_name):
        return self.yaml_string.get(attribute_name)

    def get_pipeline_config(self):
        return PipelineConfig(self)

    def get_table_config(self):
        pipe_config=self.get_pipeline_config() #内部调用时，前面都需要加一个self
        table_config_list=[]
        if self.table_id is None:
            for table in self.yaml_string.get('tables'): #loop a list first(tables) [{}]
                for table_name, table_config_attr in table.items():#loop a dictionary inside
                    table_config_list.append(
                        TableConfig(table_name, pipe_config, table_config_attr)
                    )
            return table_config_list
        else:
            for table in self.yaml_string.get('tables'):
                for table_name,table_config_attr in table.items():
                    if table_name == self.table_id:
                        table_config_list.append(TableConfig(table_name, pipe_config, table_config_attr
                                        ))
            return table_config_list

class PipelineConfig:
    def __init__(self, config): #这里的config 就是DataIngestionConfig
        self.dag_name=config.get_yaml_attr('dag_name')
        self.s3_bucket_name=config.get_yaml_attr('s3_bucket_name')
        self.source_platform=config.get_yaml_attr('source_platform') or 'postfres'
        self.source_credentials=config.get_yaml_attr('source_credentials')
        self.source_schema=config.get_yaml_attr('source_schema')
        self.destination_platform=config.get_yaml_attr('destination_platform') or 'Redshift'
        self.destination_credentials=config.get_yaml_attr('destination_credentials')
        self.destination_schema=config.get_yaml_attr('destination_schema')
        self.staging_schema=config.get_yaml_attr('staging_schema')
        self.source_conn=self._get_connection()

    def _get_connection(self):
        return PostgresSqlConnector(self.source_credentials)

class TableConfig:
    def __init__(self, table, pipe_config, table_config_attr):
        self._resolve_parameters(table, pipe_config, table_config_attr)

    def _resolve_parameters(self,table, pipe_config, table_config_attr):
        print(f'resolve table level config')
        self.source_table=table
        self.update_method=table_config_attr.get('update_method')
        self.destination_table=table_config_attr.get('destination_table') or table
        self.staging_table=table_config_attr.get('staging_table') or f'{self.destination_table}'
        self.filename=f'{table}.csv.gz'
        self.parameter_name=f'{pipe_config.dag_name}_{table}'  #parameter store
        create_parameter(self.parameter_name) #put parameter value as 2000-01-01
        self.incremental_load_parameter=get_parameter_value(self.parameter_name)
        self.s3_object=table_config_attr.get('s3_object')
        self.specify_copy_columns = (True if table_config_attr.get('column_inclusions')
                                  else False)
        #get columns from source table or get it from yaml file
        self.column_inclusions=table_config_attr.get(
            'column_inclusions') or self._resolve_column_inclusions(pipe_config)   
        #get primary key 
        self.update_keys=table_config_attr.get('update_keys') or self._resolve_update_keys(pipe_config)   
        self.incremental_load_columns=table_config_attr.get('incremental_load_columns')
        self.distkey=table_config_attr.get('distkey')
        self.sort_key=table_config_attr.get('sortkey')
        self.export_file_name=table_config_attr.get('export_file_name')

    def _resolve_sort_key(self,sort_key):
        if sort_key:
            return sort_key
        elif self.incremental_load_columns:
            return self.incremental_load_columns[1]
        elif self.update_keys:
            return self.update_key[0]
        else:
            return None

#retrieve column metadata(column_name,data_type)
    def _resolve_column_inclusions(self, pipe_config):
        df=pipe_config.source_conn.get_column_metadata(
                            table_name=self.source_table,
                            schema_name=pipe_config.source_schema)
        return df['column_name'].tolist()

    def _resolve_update_keys(self,pipe_config):
        return pipe_config.source_conn.get_table_primary_keys(
                            table_name=self.source_table,
                            schema_name=pipe_config.source_schema
        )
    
#my_data_config=DataIngestionConfig(yaml_file='6.data_ingestion/ac_shopping_crm.yml',table_id='customer')
#response=my_data_config.get_table_config()
#table_config = response[0]
#print(table_config)

