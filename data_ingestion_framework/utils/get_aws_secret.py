
import base64
import json
import boto3
from botocore.exceptions import ClientError

AWS_REGION = 'ap-southeast-2'


def get_secret(aws_secret_name):
    secret_manager_client = boto3.client(
        'secretsmanager', region_name=AWS_REGION)

    try:
        get_secret_value_response = secret_manager_client.get_secret_value(
            SecretId=aws_secret_name)

    except ClientError as e:

        if e.response['Error']['Code'] == 'InternalServiceError':
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
        print(e)
    else:
        if 'SecretString' in get_secret_value_response:
            credential = get_secret_value_response['SecretString']
        else:
            credential = base64.b64decode(
                get_secret_value_response['SecretBinary'])
    return json.loads(credential)


# a secret can have two types of values:
# a plaintext string or a binary value.
# The SecretBinary field is used to store the binary value of a secret.
# 有一些返回的值可能是incode，所以需要用base64.b64decode来解码
