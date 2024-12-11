import boto3
import json

def sendToSQS(msg_body,queue_url,msg_attr=None, region_name=None):
    sqs_client = boto3.client('sqs',region_name)
    data = json.dumps(msg_body)
    
    try:
        response = sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody = data
            )
        
    except Exception as e:
            error = f"Error sending message: {e}"
            return error
            
