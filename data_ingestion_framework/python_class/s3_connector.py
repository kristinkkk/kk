import boto3
from botocore.exceptions import ClientError

class S3Connector:
    def __init__(self):
        self.s3_client=None
        self.s3_resource=None

    def configure(self): #connect to s3
        try:
            region_name='ap-southeast-2'
            self.s3_client=boto3.client(
                service_name='s3',region_name=region_name)
            self.s3_resource=boto3.resource(service_name='s3', region_name=region_name)
            
        except Exception as e:
            print(e)
            raise

    def list_bucket(self):
        #call s3 to list current bucket
        response = self.s3_client.list_buckets()
        #get a list of all buckets name from response
        buckets= [bucket['Name'] for bucket in response['Buckets']] #for左边是要返回的结果
        #print out the bucket list
        print('bucket list: %s' %buckets)

    def create_bucket(self,bucket_name,location_constraint):
        try:
            self.s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location_constraint)
            print(f"Bucket '{bucket_name} created successfully")
        except Exception as e:
            print(e)

#object name : s3bucket下面的文件路径 
    def upload_file(self, file_name, bucket=None, object_name=None):
        if bucket is None:
            bucket = 'ac-shoppint-datalake'

        if object_name is None:
            object_name=file_name
        try:
            self.s3_client.upload_file(file_name, bucket,object_name)
        except ClientError as e:
            self.parent_task.logging.log_erroe(f's3 file upload failed:{e}')
            return False
        return True
    
    def clean_up_s3_folder(self, bucket, folder_path):
        s3=self.s3_resource
        bucket=s3.Bucket(bucket)

        for obj in bucket.objects.filter(Prefix=folder_path):
            s3.Object(bucket.name, obj.key).delete()

s3_connector = S3Connector()
s3_connector.configure()
#s3_connector.create_bucket('ac-shopping-datalake',location_constraint = {'LocationConstraint': 'ap-southeast-2'})
#s3_connector.list_bucket()
#s3_connector.upload_file(file_name='6.data_ingestion/customers12.csv',
#                         bucket='ac-shopping-datalake',
#                         object_name='ac_shopping_crm_kristin/customers12.csv')


#s3_connector.clean_up_s3_folder(bucket='ac-shopping-datalake',
#                                folder_path='ac_shopping_crm_kristin/')

#***S3 connector practice - delete files which were created 2 mins ago
import boto3
from datetime import datetime as dt, timedelta

s3_client=boto3.resource('s3')
bucket=s3_client.Bucket('ac-shopping-datalake')
for obj in bucket.objects.filter(Prefix='ac_shopping_crm_kristin'):
    #print(obj) #可以把folder里面的object都遍历出来
    #print(obj.last_modified) #把folder以及里面object last modified时间展示出来
    print(obj.last_modified.replace(tzinfo=None)) #在我们知道时区的情况下，可以利用这个把时区去掉
    print(dt.utcnow()-timedelta(minutes=2))
    if obj.last_modified.replace(tzinfo=None) <= dt.utcnow()-timedelta(minutes=2):
        s3_client.Object(bucket.name, obj.key).delete()
