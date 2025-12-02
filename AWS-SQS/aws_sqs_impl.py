import boto3
import json
from botocore.exceptions import ClientError
import logging

logging.basicConfig(
    level=logging.info,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


class SQSManager:
    """AWS Queue service Manager"""

    def __init__(self, region_name='us-east-1'):
        self.sqs = boto3.client('sqs', region_name=region_name)
        self.resource = boto3.resource('sqs', region_name=region_name)

    def create_queue(self, queue_name, fifo=False):
        """Create an SQS queue"""
        try:
            attributes = {}
            if fifo:
                queue_name = f"{queue_name}.fifo"
                attributes = {
                    'FifoQueue': 'true',
                    'ContentBasedDeduplication': 'true'
                }
            
            response = self.sqs.create_queue(
                QueueName=queue_name,
                Attributes=attributes
            )
            queue_url = response['QueueUrl']
            logger.info(f"Queue created: {queue_url}")
            return None

        except ClientError as ce:
            logger.error(f"Error: {ce}")
            return None
        
    def get_queue_url(self, queue_name):
        """Get queue URL by name"""
        try:
            response = self.sqs.get_queue_url(QueueName=queue_name)
            return response['QueueUrl']
        except ClientError as ce:
            logger.error(f"Error: {ce}")
            return None
        
    def send_message(self, queue_url, message_body, message_attributes=None):
        """Send a message to SQS queue"""
        try:
            params = {
                'QueueUrl': queue_url,
                'MessageBody': json.dumps(message_body) if isinstance(message_body, dict) else message_body
            }
            
            if message_attributes:
                params['MessageAttributes'] = message_attributes

            response = self.sqs.send_message(**params)
            logger.info(f"Message sent: {response['MessageId']}")
            return response['MessageId']
        except ClientError as ce:
            logger.error(f"Error: {ce}")
            return None
        
    def send_message_batch(self, queue_url, messages):
        """Send multiple messages to SQS queue"""
        try:
            entries = []
            for i, msg in enumerate(messages):
                entries.append({
                    'Id': str(i),
                    'MessageBody': json.dumps(msg) if isinstance(msg, dict) else msg
                })
            response = self.sqs.send_message_batch(
                QueueUrl=queue_url,
                Entries=entries
            )
            logger.info(f"{len(response["Successful"])} messages sent successfully")
            return response
        except ClientError as ce:
            logger.error(f"Error : {ce}")
            return None
        
    def receive_messages(self, queue_url, max_messages=10, wait_time=10):
        """Receive messages from SQS queue"""
        try:
            response = self.sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time, # Long polling
                MessageAttributeNames=['All'],
                AttributeNames=['All']
            )
            messages = response.get('Messages', [])
            logger.info(f"Received {len(messages)} messages")
            return messages
        except ClientError as ce:
            logger.error(f"Error: {ce}")
            return []
        
    def delete_message(self, queue_url, receipt_handle):
        """Delete a message from SQS queue"""
        try:
            self.sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            logger.info("Message deleted successfully")
            return True
        except ClientError as ce:
            logger.error(f"Error: {ce}")
            return False
        
    def process_messages(self, queue_url, processor_func):
        """Process messages from queue with a custom function"""
        messages = self.receive_messages(queue_url)

        for message in messages:
            try:
                # process the message
                body = json.loads(message['Body'])
                processor_func(body)

                # Delete message after successful processing
                self.delete_message(queue_url, message['ReceiptHandle'])
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # Message will become visible again after visibility timeout

# Usage Example: Producer-Consumer Pattern
sqs = SQSManager()

# Producer
queue_url = sqs.create_queue('my-task-queue')
task = {
    'task_id': '123',
    'action': 'process_data',
    'data': {'user_id': 'user123', 'amount': 100}
}
sqs.send_message(queue_url, task)

# Consumer
def process_task(task_data):
    logger.info(f"Processing task: {task_data['task_id']}")
    # Business Logic

    logger.info(f"Action: {task_data['action']}")

sqs.process_messages(queue_url, process_task)
