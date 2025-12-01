import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import json
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


class DynamoDBManager:
    """
    AWS DynamoDB Manager
    """
    def __init__(self, region_name='us-east-1'):
        self.dynamodb = boto3.resource('dynamodb', region_name=region_name)
        self.client=boto3.client('dynamodb', region_name=region_name)

    def create_table(self, table_name):
        """Create a DynamoDB table"""
        try:
            table = self.dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'user_id', 'KeyType': 'HASH'}, # Partition Key
                    {'AttributeName': 'timestamp', 'KeyType': 'RANGE'} # Sort Key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'user_id', 'AttributeType': 'S'},
                    {'AttributeName': 'timestamp', 'AttributeType': 'N'},
                    {'AttributeName': 'email', 'AttributeType': 'S'},
                ],
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'EmailIndex',
                        'KeySchema': [
                            {'AttributeName': 'email', 'KeyType': 'HASH'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                        }
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )

            # wait for table to be created
            table.wait_until_exists()
            logger.info(f"Table {table_name} created successfully")
            return table
        except Exception as e:
            logger.error(f"Error: {e}")
            return None
        
    def put_item(self, table_name, item):
        """Insert an item into DynamoDB table"""
        try:
            table = self.dynamodb.Table(table_name)
            table.put_item(Item=item)
            logger.info("Item inserted successfully")
            return True
        except Exception as e:
            logger.error(f"Error: {e}")
            return False
        
    def get_item(self, table_name, key):
        """Get an item from dynamodb table"""
        try:
            table = self.dynamodb.Table(table_name)
            response = table.get_item(Key=key)
            return response.get('Item', None)
        except Exception as e:
            logger.error(f"Error: {e}")
            return None
        
    def query_items(self, table_name, key_condition):
        """Query items from DynamoDB table"""
        try:
            table = self.dynamodb.Table(table_name)
            response = table.query(KeyConditionExpression=key_condition)
            return response.get('Items', [])
        except Exception as e:
            logger.error(f"Error: {e}")
            return []
        
    def scan_items(self, table_name, filter_expression=None):
        """Scan items in DynamoDB table"""
        try:
            table = self.dynamodb.Table(table_name)
            if filter_expression:
                response = table.scan(FilterExpression=filter_expression)
            else:
                response = table.scan()
            return response.get('Items', [])
        except Exception as e:
            logger.error(f"Error: {e}")
            return []
        
    def update_items(self, table_name, key, update_expression, expression_values):
        """Update an item in DynamoDB table"""
        try:
            table = self.dynamodb.Table(table_name)
            response = table.update_item(
                Key=key,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values,
                ReturnValues='UPDATED_NEW'
            )
            return response
        except Exception as e:
            logger.error(f"Error: {e}")
            return None
        
    def delete_item(self, table_name, key):
        """Delete an item from DynamoDB table"""
        try:
            table = self.dynamodb.Table(table_name)
            table.delete_item(Key=key)
            logger.info("Item deleted successfully")
            return True
        except Exception as e:
            logger.error(f"Error: {e}")
            return False
    
    def batch_write(self, table_name, items):
        """Batch write items to DynamoDB"""
        try:
            table = self.dynamodb.Table(table_name)
            with table.batch_writer() as batch:
                for item in items:
                    batch.put_item(Item=item)
            logger.info(f"{len(items)} items written successfully")
            return True
        except Exception as e:
            logger.error(f"Error: {e}")
            return False
        
# Usage
db = DynamoDBManager()

# Insert Item
item = {
    'user_id': 'user123',
    'timestamp': Decimal('1234567890'),
    'email': 'user@example.com',
    'name': 'John Doe',
    'data': {'key': 'value'}
}
db.put_item('Users', item)

# Query items
items = db.query_items('Users', Key('user_id').eq('user123'))

# Update items
db.update_items(
    'Users',
    {'user_id': 'user123', 'timestamp': Decimal('1234567890')},
    'SET #name = :name',
    {':name': 'John Doe'}
)