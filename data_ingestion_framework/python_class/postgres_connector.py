# 1.purpose: interact with postgres database

# 2.functions:
# connect to postgres db,
# execute sql on connected db,
# export sql from connected db to csv.file

import pandas as pd
import csv
import psycopg2
#利用绝对路径解决文件的路径问题
import sys
sys.path.append(
    '/Users/guiyuezheng/Desktop/Data Engineer/ETL - Python file/utils')
from get_aws_secret import get_secret  # NOQA: E402

class PostgresConnector:
    def __init__(self, secret_name):
        self.secret_name = secret_name

    def connect_to_postgres_database(self):
        credentials = get_secret(self.secret_name)
        postgres_connection = psycopg2.connect(
            host=credentials['host'],
            user=credentials['username'],
            password=credentials['password'],
            port=credentials['port'],
            dbname=credentials['database'])
        return postgres_connection

    def execute_sql(self, sql_cmd):
        connection = self.connect_to_postgres_database()
        cursor = connection.cursor()
        cursor.execute(sql_cmd)
        print(cursor.fetchall())


# z在使用class之前，必须要实例化它，然后再进行使用
my_connection = PostgresConnector('postgres_ac_master')
result= my_connection.execute_sql('select count(*) from ac_shopping.customer')
print(result)

# 如果调用class里面其他方法，只有self一个参数，是不需要传参的
