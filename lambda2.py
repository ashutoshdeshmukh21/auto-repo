import boto3
import json

def lambda_handler(event, context):
    sqs = boto3.client('sqs', region_name='us-east-1')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/581741715630/messages.fifo'

    # Receive messages from SQS
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['All'],
        MaxNumberOfMessages=1,
        MessageAttributeNames=['All'],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )

    # Check if there are messages in the queue
    messages = response.get('Messages', [])
    if not messages:
        return "No messages available in the queue."

    # Parse JSON message body
    message_body = json.loads(messages[0]['Body'])

    phone_number = message_body.get('phone_number')
    channel = message_body.get('channel')
    message = message_body.get('message')
    message_group_id = message_body.get('message_group_id') or 'default_group_id'
    message_deduplication_id = message_body.get('message_deduplication_id') or 'default_deduplication_id'

    if (channel == 'whatsapp' and phone_number in range(0, 5)) or (channel == 'sms' and phone_number in range(5, 10)):
        return f"Message sent via {channel}: {message}"
    else:
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=f"Failed message: {message}, Channel: {channel}, Phone Number: {phone_number}",
            MessageGroupId=message_group_id,
            MessageDeduplicationId=message_deduplication_id
        )
        return f"Failed message sent to SQS: {message}"

# Assuming the message body is a JSON object containing the required information
result = lambda_handler(None, None)
print(result)
