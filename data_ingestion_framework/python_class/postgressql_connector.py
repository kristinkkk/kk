import sys
#sys.path.append(
#    'data_ingestion_framework/utils')
#sys.path.append('..) >>>表示上一层路径
#ls. >>>当前目录
#ls.. >>>上一个层级目录 
import csv
from utils.get_aws_secret import get_secret
import pandas as pd
import psycopg2


class PostgresSqlConnector:
    def __init__(self, secret_name):  # 为什么secret_name默认是none值？ >>如果不传，应该会报错
        self.secret_name = secret_name
        self.credentials = get_secret(self.secret_name)  # 在这边直接拿到credential的值

    def get_connection(self):
        try:
            connection = psycopg2.connect(
                #这边是不是可以不写成self.credentials.get('host)?
                host=self.credentials['host'],
                dbname=self.credentials['database'],
                port=self.credentials['port'],
                user=self.credentials['username'],
                password=self.credentials['password']
            )
        except Exception as e:
            print(e)
            raise e
        return connection

    # 执行一段sql但是并不会返回任何结果
    def execute_sql(self, sql, parameter=None, connection=None):  # 这里的parameter 是什么？
        if connection is None:
            connection = self.get_connection()  # 调用自己class里面的function是不需要传参的
            connection.autocommit = True
        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            print("successfully executed{}".format(sql))
        except Exception as e:
            raise e

    # 执行一段sql，然后返回一个结果
    def get_execute_sql_result(self, sql, connection=None):
        if connection is None:
            connection = self.get_connection()
            connection.autocommit = True

        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            result = cursor.fetchall()
        except Exception as e:
            raise e  
        else:
            return result

    def export_sql_to_csv(self, sql, export_file_name, delimiter, connection=None):
        if connection is None:
            connection = self.get_connection()
            connection.autocommit = True

        try:
            cursor = connection.cursor()
            cursor.execute(sql)

            with open(export_file_name, 'wt') as f:
                write = csv.writer(f, quoting=csv.QUOTE_ALL,
                                   delimiter=delimiter)
                write.writerow(col[0] for col in cursor.description)
                write.writerows(cursor.fetchall())

        except Exception as e:
            print('Export sql result to csv file failed')
            raise ValueError

    def query_sql(self, query, connection=None):
        if connection is None:
            connection = self.get_connection()
            connection.autocommit = True

        return pd.read_sql_query(query, connection)


    def table_exists(self, table_name,schema_name):
        schema_name=schema_name.lower()
        table_name=table_name.lower()

#        sql=(f"select * from {schema_name}.{table_name}")
        sql = ("select table_name from information_schema.tables "
                f"where table_name = '{table_name}' "
                f"and table_schema = '{schema_name}';"
        )
        
        df=self.query_sql(query=sql)

        if df.empty:
            return False
        else:
            return True
        
    def get_column_metadata(self, table_name, schema_name, column_list=[]):
        column_filter = ""
        if column_list:
            col_string = "'{0}'".format("', '".join(column_list))
            column_filter = f"and column_name in ({col_string.strip()}) "

        sql = f"""
            select
                c.column_name,
                c.data_type,
                c.ordinal_position,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                null as column_type
            from information_schema.tables t
                left join information_schema.columns c
                    on t.table_schema = c.table_schema
                    and t.table_name = c.table_name
            where t.table_name = '{table_name}'
            and t.table_schema = '{schema_name}'
            {column_filter}
            order by c.ordinal_position;
        """

        df = self.query_sql(query=sql)

        return df

    def get_table_primary_keys(self, table_name, schema_name):
        # this is the only method that works in postgres and redshift
        sql = f"""
            SELECT attname column_name
            FROM pg_index ind, pg_class cl, pg_attribute att
            WHERE cl.oid = '{schema_name}.{table_name}'::regclass
            AND ind.indrelid = cl.oid
            AND att.attrelid = cl.oid
            AND att.attnum::text =
                ANY(string_to_array(textin(int2vectorout(ind.indkey)), ' '))
            AND attnum > 0
            AND ind.indisprimary
            ORDER BY att.attnum;
        """

        df = self.query_sql(query=sql)
        return df["column_name"].tolist()
