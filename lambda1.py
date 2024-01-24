import boto3
import json
import time
from datetime import datetime, timedelta

def lambda_handler(event, context):
    
    utc_time = datetime.utcnow()

    ist_offset = timedelta(hours=5, minutes=30)
    ist_time = utc_time + ist_offset
    formatted_time = ist_time.strftime('%d%m%Y_%H%M%S')

    print(json.dumps(event))

    message_id = formatted_time
    phone_number = event['phone_number']
    message = event['message']
    channel = event['channel']

    payload = {
        'message_id': message_id,
        'phone_number': phone_number,
        'message': message,
        'channel': channel
    }

    # AWS SQS configuration
    sqs_queue_url = 'https://sqs.us-east-1.amazonaws.com/581741715630/messages.fifo'
    sqs_client = boto3.client('sqs')

    try:
        # Send message to SQS
        response = sqs_client.send_message(
            QueueUrl=sqs_queue_url,
            MessageBody=json.dumps(payload),
            MessageGroupId=message_id,
            MessageDeduplicationId=message_id
        )

        return {
            'statusCode': 200,
            'body': json.dumps('Message sent to SQS successfully!')
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
