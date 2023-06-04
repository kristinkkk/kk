import base64
import json
import boto3
from botocore.exceptions import ClientError
from connect_postgres_db import connect_postgres_db
import csv


def extract_data_to_csv(source_db_aws_secret_name,
                        extract_sql,
                        saved_file_name,
                        delimiters=','):
    try:
        source_db_connection = connect_postgres_db(source_db_aws_secret_name)
        db_cursor = source_db_connection.cursor()
        db_cursor.execute(extract_sql)
        with open(saved_file_name, 'wt') as f:
            csv_writer = csv.writer(
                f, quoting=csv.QUOTE_ALL, delimiter=delimiters)
            csv_writer.writerow(col[0] for col in db_cursor.description)
            csv_writer.writerows(db_cursor.fetchall())

    except Exception as e:
        print(e)
