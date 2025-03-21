import boto3
import json
from ssm import Credentials


# Region for S3 operations
region_name = 'ap-south-1'

# S3ClientFactory: Creates an S3 client
class S3ClientFactory:
    @staticmethod
    def get_client(region_name='ap-south-1', credential_name='s3'):
        cred = Credentials(credential_name=credential_name)
        s3_config = cred.get_credentials()
        return boto3.client(
            's3',
            aws_access_key_id=s3_config["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=s3_config["AWS_SECRET_ACCESS_KEY"],
            region_name=region_name
        )

# S3BucketHandler: Manages bucket operations
class S3BucketHandler:
    def __init__(self, s3_client, bucket_name):
        self.s3_client = s3_client
        self.bucket_name = bucket_name
    
    ##create bucket
    def create_bucket(self):
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f"✅  Bucket '{self.bucket_name}' already exists.")
        except self.s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"✅  Bucket '{self.bucket_name}' does not exist. Creating...")
                self.s3_client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region_name}
                )
                print(f"✅  Bucket '{self.bucket_name}' created successfully.")
            else:
                raise e

    ##delete bucket
    def delete_bucket(self, delete_all_files=False):
        try:
            if delete_all_files:
                self.delete_all_files()
            self.s3_client.delete_bucket(Bucket=self.bucket_name)
            print(f"✅  Bucket '{self.bucket_name}' deleted successfully.")
        except Exception as e:
            print(f"❌  Error deleting bucket '{self.bucket_name}': {e}")
    
    ##delete all files
    def delete_all_files(self):
        paginator = self.s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket_name):
            if 'Contents' in page:
                for obj in page['Contents']:
                    print(f"❌  Deleting object: {obj['Key']}")
                    self.s3_client.delete_object(Bucket=self.bucket_name, Key=obj['Key'])

# S3FileManager: Manages file operations
class S3FileManager:
    def __init__(self, s3_client, bucket_name, file_name):
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.file_name = file_name

    def file_exists(self):
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=self.file_name)
            return True
        except self.s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise e

    def create_file(self, initial_data=None):
        if self.file_exists():
            print(f"File '{self.file_name}' already exists.")
            return
        initial_data = initial_data or {'alpha_ts_values': []}
        self.s3_client.put_object(Bucket=self.bucket_name, Key=self.file_name, Body=json.dumps(initial_data))
        print(f"✅  File '{self.file_name}' created successfully with initial data.")

    def delete_file(self):
        if not self.file_exists():
            print(f"❌  File '{self.file_name}' does not exist.")
            return
        self.s3_client.delete_object(Bucket=self.bucket_name, Key=self.file_name)
        print(f"✅  File '{self.file_name}' deleted successfully.")
    
    def load_file_content(self):
        if not self.file_exists():
            print(f"❌  File '{self.file_name}' does not exist.")
            return None
        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.file_name)
        file_content = response['Body'].read().decode('utf-8')

        if not file_content:
            print(f"❌  File '{self.file_name}' is empty.")
            return {'alpha_ts_values': []}
        
        try:
            return json.loads(file_content)
        except json.JSONDecodeError:
            print(f"❌  File '{self.file_name}' does not contain valid JSON.")
            return file_content 

    def save_file_content(self, content):
        if not self.file_exists():
            print(f"❌  File '{self.file_name}' does not exist.")
            return
        self.s3_client.put_object(Bucket=self.bucket_name, Key=self.file_name, Body=json.dumps(content))
        print(f"✅  File '{self.file_name}' updated successfully.")
    
    def delete_file_content(self, default_content=None):
        if not self.file_exists():
            print(f"❌  File '{self.file_name}' does not exist.")
            return
        default_content = default_content or {'alpha_ts_values': []}
        self.s3_client.put_object(Bucket=self.bucket_name, Key=self.file_name, Body=json.dumps(default_content))
        print(f"✅  Content of '{self.file_name}' deleted and replaced with default content: {default_content}")
        
    def upload_file(self, local_file_path):
        try:
            self.s3_client.upload_file(local_file_path, self.bucket_name, self.file_name)
            print(f"✅  File '{local_file_path}' uploaded successfully to '{self.bucket_name}/{self.file_name}'.")
        except Exception as e:
            print(f"❌  Error uploading file '{local_file_path}' to S3: {e}")
    


# S3ManagerFacade: Combines bucket and file operations
class S3ManagerFacade:
    def __init__(self, bucket_name=None, file_name=None):
        # Get S3 client and credentials
        self.s3_client = S3ClientFactory.get_client()
        s3_config = Credentials(credential_name='s3').get_credentials()

        # Default to values from s3_config if not provided
        self.bucket_name = bucket_name if bucket_name else s3_config['bucket_name']
        self.file_name = file_name if file_name else s3_config['file_name']

        # Initialize handlers
        self.bucket_handler = S3BucketHandler(self.s3_client, self.bucket_name)
        self.file_manager = S3FileManager(self.s3_client, self.bucket_name, self.file_name)

    def setup(self):
        self.bucket_handler.create_bucket()
        self.file_manager.create_file()

    def teardown(self):
        self.file_manager.delete_file()
        self.bucket_handler.delete_bucket(delete_all_files=True)