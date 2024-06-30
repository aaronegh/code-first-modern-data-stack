from boto3 import Session
from deltalake import write_deltalake
import logging
import os
import dlt
from pandas import DataFrame

# Configure logging
logging.basicConfig(level=logging.INFO)


class writeToS3:
    def __init__(self, 
                 endpoint_url: str, 
                 aws_access_key_id: str,
                 aws_secret_access_key: str,
                 aws_region: str,
                 bucket_name: str, 
                 path_name: str
                ): 

                #should be templatised to fit differnet FS - these params are minio specific
                os.environ['AWS_S3_ALLOW_UNSAFE_RENAME'] = 'true'
                os.environ['AWS_STORAGE_ALLOW_HTTP']= "true"

                self.endpoint_url = endpoint_url
                self.aws_access_key_id = aws_access_key_id
                self.aws_secret_access_key = aws_secret_access_key
                self.aws_region = aws_region
                self.bucket_name = bucket_name
                self.path_name = path_name

                self.load_path = f"s3://{self.bucket_name}/{self.path_name}"
                self.storage_options = {
                    "AWS_ACCESS_KEY_ID": aws_access_key_id,
                    "AWS_SECRET_ACCESS_KEY": aws_secret_access_key,
                    "AWS_ENDPOINT_URL": endpoint_url,
                    "AWS_REGION": aws_region,
                    'AWS_ALLOW_HTTP':'true',
                    'AWS_STORAGE_ALLOW_HTTP': "true",
                    "AWS_S3_ALLOW_UNSAFE_RENAME": "true" 
                }

                session = Session()
                self.client = session.client(
                    's3',
                    endpoint_url=endpoint_url,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key
                )


    def create_bucket(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
            logging.info(f"Bucket {self.bucket_name} already exists.")
        except self.client.exceptions.NoSuchBucket:
            self.bucket_nameclient.create_bucket(Bucket=self.bucket_name)
            logging.info(f"Bucket {self.bucket_name} created.")


    def write_to_deltalake(self,df) -> None: 
        write_deltalake(self.load_path
                        ,df
                        ,storage_options=self.storage_options
                        ,mode="append"
                        ,engine="rust"
                        ,schema_mode="merge")
        
    def write_to_json(self,json) -> None:
         # dont write it to one data.json
         self.client.put_object(Body=json,
                                Bucket=self.bucket_name,
                                Key=f"{self.path_name}/data.json")