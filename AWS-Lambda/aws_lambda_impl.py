import json
import boto3
import logging
from datetime import datetime
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def lambda_handler(event, context):
    """
    Lambda function that processes S3 events
    Triggered when a file is uploaded to S3
    """
    # Parse the S3 event
    try:
        bucket = event["Records"][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        logger.info(f"Processing file: {key} from bucket: {bucket}")

        # Initialize S3 Client
        s3_client = boto3.client('s3')

        # Get the object
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')

        # Process the content
        lines = content.split('\n')
        processed_data = {
            'file_name': key,
            'line_count': len(lines),
            'processed_at': datetime.now().isoformat(),
            'status': 'success'
        }

        # Return response
        return {
            "statusCode": 200,
            "body": json.dumps(processed_data)
        }
    except Exception as e:
        logger.error(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    

# Deploying Lambda with Python (using boto3)

class LambdaManager:
    """Lambda Manger"""
    def __init__(self, region_name='us-east-1'):
        self.lambda_client = boto3.client('lambda', region_name=region_name)

    def create_function(self, function_name, role_arn, zip_file_path):
        """Create a Lambda function"""
        try:
            with open(zip_file_path, 'rb') as f:
                zipped_code = f.read()

            response = self.lambda_client.create_function(
                FunctionName=function_name,
                Runtime='python3.11',
                Role=role_arn,
                Handler='lambda_function.lambda_handler',
                Code={'ZipFile': zipped_code},
                Timeout=60,
                MemorySize=256,
                Environment={
                    'Variables': {
                        'ENV': 'production'
                    }
                }
            )
            logger.info(f"Function {function_name} created successfully")
            return response
        except ClientError as ce:
            logger.error(f"Error: {ce}")
            return None
        
    def invoke_function(self, function_name, payload):
        """Invoke a Lambda function"""
        try:
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )
            result = json.loads(response['Payload'].read())
            return result
        except ClientError as ce:
            logger.error(f"Error: {ce}")
            return None
        
    def update_function_code(self, function_name, zip_file_path):
        """Update Lambda function code"""
        try:
            with open(zip_file_path, 'rb') as f:
                zipped_code = f.read()

            response = self.lambda_client.update_function_code(
                FunctionName=function_name,
                ZipFile=zipped_code
            )
            logger.info(f"Function {function_name} updated successfully")
            return response
        except ClientError as ce:
            logger.error(f"Error: {ce}")
            return None
        
# Usage
lambda_mgr = LambdaManager()
result = lambda_mgr.invoke_function('my-function', {'key': 'value'})