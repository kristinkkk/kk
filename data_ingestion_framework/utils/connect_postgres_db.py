import base64
import json
import boto3
from botocore.exceptions import ClientError
import psycopg2
from get_aws_secret import get_secret


def connect_postgres_db(aws_secret_name):
    try:
        credentials = get_secret(aws_secret_name)
        database_connection = psycopg2.connect(
            host=credentials['host'],
            user=credentials['username'],
            password=credentials['password'],
            port=credentials['port'],
            dbname=credentials['database'])
        return database_connection

    except Exception as e:
        print(e)
