import os
import logging
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


class S3Manager:
    """S3 manager class"""
    def __init__(self, region_name='us-east-1'):
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.s3_resources = boto3.resource('s3', region_name=region_name)

    def create_bucket(self, bucket_name, region=None):
        """Create an S3 bucket"""
        try:
            if region is None or region == 'us-east-1':
                self.s3_client.create_bucket(Bucket=bucket_name)
            else:
                location = {'LocationConstraint': region}
                self.s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration=location
                )
            logger.info(f"Bucket {bucket_name} created successflly")
            return True
        except ClientError as ce:
            logger.error(f"Error: {ce}")
            return False
        
    def upload_file(self, file_path, bucket_name, object_name=None):
        """Upload a file to S3 bucket"""
        if object_name is None:
            object_name = os.path.basename(file_path)
        
        try:
            self.s3_client.upload_file(file_path, bucket_name, object_name)
            logger.info(f"File {file_path} uploaded to {bucket_name}/{object_name}")
            return True
        except ClientError as ce:
            logger.error(f"Error: {ce}")
            return False
        
    def download_file(self, bucket_name, object_name, file_path):
        """Download a file from S3 bucket"""
        try:
            self.s3_client.download_file(bucket_name, object_name, file_path)
            logger.info(f"File downloaded to {file_path}")
            return True
        except ClientError as ce:
            logger.error(f"Error:{ce}")
            return False
        
    def list_objects(self, bucket_name, prefix=''):
        """List objects in a bucket"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )

            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            return []
        except ClientError as ce:
            logger.error(f"Error : {ce}")
            return []
        
    def delete_object(self, bucket_name, object_name):
        """Delete an object from S3 bucket"""
        try:
            self.s3_client.delete_object(Bucket=bucket_name, Key=object_name)
            logger.info(f"Object {object_name} deleted from {bucket_name}")
            return True
        except ClientError as ce:
            logger.error(f"Error: {ce}")
            return False
        
    def generate_presigned_url(self, bucket_name, object_name, expiration=3600):
        """Generate a presigned URL for temporary access"""
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': object_name},
                ExpiresIn=expiration
            )
            return url
        except ClientError as ce:
            logger.error(f"Error: {ce}")
            return None
        

# Usage
s3 = S3Manager()
s3.upload_file('data.csv', 'my-bucket', 'uploads/data.csv')
url = s3.generate_presigned_url('my-bucket', 'uploads/data.csv')

